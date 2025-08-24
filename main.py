import asyncio
import sqlite3
from typing import List, Optional
from urllib.parse import quote_plus
import aiohttp
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from contextlib import asynccontextmanager
import httpx
import os
import json
import re
from datetime import datetime, timezone, timedelta
import dateutil.parser
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS", "")
BASE_URL = os.getenv("BASE_URL")
DEFAULT_CHAT_IDS = [cid.strip() for cid in TELEGRAM_CHAT_IDS.split(",") if cid.strip()]
DB_PATH = "daangn.db"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "20"))  # 초 단위

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield
    # 종료 시점에 할 작업 있으면 여기에

app = FastAPI(lifespan=lifespan)

# static 폴더와 templates 폴더 설정
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

_monitor_tasks = {}

KST = timezone(timedelta(hours=9))  # 한국 표준시
server_start_time = datetime.now(KST) - timedelta(days=3) # timedelta(days=?) 값을 빼는 건 테스트 목적입니다.

# DB 초기화
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # 업로드 시간 추가 필요. db 등록 시점 기록 필요한지 고민해보자.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_table (
            id TEXT PRIMARY KEY,
            keyword TEXT,
            title TEXT,
            description TEXT,
            price INTEGER,
            seller TEXT,
            location TEXT,
            url TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def product_exists(item_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM product_table WHERE id = ?", (item_id,))
    r = cur.fetchone()
    conn.close()
    return bool(r)

def mark_product(item_id: str, keyword: str, title: str, description: str, price, seller: str, location: str, url: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        price_int = int(float(price)) if price else 0
        cur.execute("""
            INSERT OR IGNORE INTO product_table (id, keyword, title, description, price, seller, location, url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (item_id, keyword, title, description, price_int, seller, location, url))
        conn.commit()
    except Exception as e:
        print(f"[ERROR] DB 저장 실패 - id: {item_id}, error: {e}")
    finally:
        if conn:
            conn.close()

async def send_telegram(chat_ids: list[str] = None, text: str = ""):
    if chat_ids is None or len(chat_ids) == 0:
        chat_ids = DEFAULT_CHAT_IDS

    async with aiohttp.ClientSession() as session:
        async def send_to_one(chat_id):
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
            try:
                async with session.post(TELEGRAM_API, json=payload, timeout=10) as resp:
                    return await resp.json()
            except Exception as e:
                print(f"[ERROR] Telegram send error (chat_id={chat_id}): {e}")
                return None
        
        results = await asyncio.gather(*[send_to_one(cid) for cid in chat_ids])
        return results

async def fetch_item_detail(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    print(f"[ERROR] 상세 페이지 요청 실패: {resp.status}")
                    return ""
                raw_bytes = await resp.read()
                text = raw_bytes.decode("utf-8", errors="replace")
        except Exception as e:
            print(f"[ERROR] 상세 페이지 요청 예외: {e}")
            return ""

    soup = BeautifulSoup(text, "html.parser")
    
    time_tag = soup.select_one("time[datetime]")
    if time_tag:
        datetime_value = time_tag.get("datetime", "")
        print("[DEBUG] time_tag found:", time_tag)
        print(f"[DEBUG] datetime attribute: '{datetime_value}' (length: {len(datetime_value)})")

        if datetime_value == "":
            print("[WARN] time_tag의 datetime 속성이 빈 문자열입니다.")
            return ""
        else:
            return datetime_value
    else:
        print("[WARN] pull_up_time_text 요소를 찾지 못했습니다.")
        return ""

# 크롤링 (당근 JSON-LD 파싱)
async def fetch_search_results(location: str, keyword: str, min_price: Optional[int], max_price: Optional[int]) -> List[dict]:
    price_param = ""
    if min_price is not None or max_price is not None:
        min_price_val = min_price if min_price is not None else 0
        max_price_val = max_price if max_price is not None else 999999999
        price_param = f"&price={min_price_val}__{max_price_val}"

    url = f"https://www.daangn.com/kr/buy-sell/?in={location}&search={quote_plus(keyword)}&only_on_sale=true{price_param}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    print(f"[INFO] 요청 URL: {url}")

    async with aiohttp.ClientSession(headers=headers) as session:
            try:
                async with session.get(url, timeout=15) as resp:
                    print(f"[INFO] HTTP 상태 코드: {resp.status}")
                    if resp.status != 200:
                            print("[ERROR] 정상 응답이 아닙니다.")
                            return []
                    raw_bytes = await resp.read()
                    text = raw_bytes.decode('utf-8')  # 혹은 'euc-kr', 'cp949' 등 상황에 따라 변경
            except Exception as e:
                print(f"[ERROR] HTTP 요청 실패: {e}")
                return []

    soup = BeautifulSoup(text, "html.parser")
    print(f"[INFO] HTML 문서 길이: {len(text)}")
    items = []

    scripts = soup.find_all('script', type='application/ld+json')
    print(f"[INFO] 발견된 JSON-LD script 개수: {len(scripts)}")

    for idx, script in enumerate(scripts, start=1):
        print(f"[INFO] 스크립트 {idx} 시작 - 내용 일부: {str(script.string)[:50]}...")  # 스크립트 식별 로그
        try:
            data = json.loads(script.string)
            if data.get('@type') == 'ItemList':
                for element in data.get('itemListElement', []):
                    print(f"[INFO] ItemList 내 itemListElement 개수: {len(element)}")
                    item = element.get('item', {})
                    offers = item.get('offers', {})
                    seller = offers.get('seller', {})
                    price = offers.get('price')
                    price_int = int(float(price)) if price else 0

                    item_url = item.get('url')

                    # 상세 페이지에서 pull_up_time_text 얻기
                    pull_up_time_text = ""
                    pull_up_time = None
                    if item_url:
                        pull_up_time_text = await fetch_item_detail(item_url)

                    if pull_up_time_text:
                        try:
                            # ISO 8601 문자열 → datetime 객체 변환 (timezone 포함됨)
                            pull_up_time = dateutil.parser.isoparse(pull_up_time_text)
                        except Exception as e:
                            print(f"[WARN] pull_up_time_text 파싱 실패: {e}")
                            pull_up_time = None

                    print(f"  - 이름: {item.get('name', '')}")
                    print(f"  - URL: {item.get('url')}")
                    print(f"  - 가격: {price_int} {offers.get('priceCurrency')}")
                    print(f"  - 판매자: {seller.get('name')}")
                    print(f"  - 올린 시간: {pull_up_time_text}")
                    
                    items.append({
                        "id": item.get('@id') or item.get('url'),
                        "title": item.get('name'),
                        "description": item.get('description'),
                        "url": item.get('url'),
                        "price": offers.get('price'),
                        "priceCurrency": offers.get('priceCurrency'),
                        "seller": seller.get('name'),
                        "location": location,
                        "search_keyword": keyword,
                        "pull_up_time_text": pull_up_time
                    })
            print(f"[INFO] 스크립트 {idx} 처리 완료\n\n\n")
        except json.JSONDecodeError:
            print(f"[ERROR] 스크립트 {idx} JSON 파싱 실패")
            continue
        except Exception as e:
            print(f"[ERROR] 스크립트 {idx} 예외 발생: {e}")
            continue

    return items


# 모니터링
async def monitor_keyword(location: str, keyword: str, chat_ids: list[str], min_price: Optional[int], max_price: Optional[int]):
    try:
        while True:
            try:
                results = await fetch_search_results(location, keyword, min_price, max_price)
                for it in results:
                    # 상품 등록 시간 문자열
                    pull_up_text = it.get("pull_up_time_text", "")

                    if pull_up_text:
                        formatted_time_for_telegram = pull_up_text.strftime("%Y년 %-m월 %-d일 %H시 %M분 %S초")
                    else:
                        formatted_time_for_telegram = ""
                    
                    # 서버 시작 시간 이후에 업로드된 경우에만 처리
                    if pull_up_text > server_start_time:
                        if not product_exists(it["id"]):
                            try:
                                price_int = int(float(it["price"])) if it["price"] else 0
                                text = (
                                    f"🔔 <b>{it['title']}</b>\n"
                                    f"가격: {price_int}원\n"
                                    f"동네: {it['location']}\n"
                                    f"검색 키워드: {keyword}\n"
                                    f"게시글 업로드 시간: {formatted_time_for_telegram}\n"
                                    f"상세 설명: {it['description']}\n"
                                    f"구매 URL: {it['url']}"
                                )
                                await send_telegram(chat_ids, text)
                                mark_product(it["id"], keyword, it["title"], it["description"], it["price"], it["seller"], it["location"], it["url"])
                            except Exception as e:
                                print(f"[ERROR] mark_product or send_telegram 실패 - id: {it.get('id')} error: {e}")
                    else:
                        # 서버 시작 이전 업로드는 무시
                        pass
                await asyncio.sleep(POLL_INTERVAL)
            except Exception as e:
                print(f"[ERROR] monitor_keyword 루프 내 에러: {e}")
                await asyncio.sleep(5)
    except asyncio.CancelledError:
        print("[INFO] 모니터링 작업 취소됨")
        return


# API 모델
class WatchRequest(BaseModel):
    location: str
    keyword: str
    min_price: Optional[int] = None
    max_price: Optional[int] = None

class ScanRequest(BaseModel):
    location: str
    keyword: str
    days: int = 1 # 기본값 1일 이내
    min_price: Optional[int] = None
    max_price: Optional[int] = None

class TelegramTestRequest(BaseModel):
    chat_ids: list[str]
    text: str

@app.post("/test/telegram")
async def test_telegram(req: TelegramTestRequest):
    result = await send_telegram(req.chat_ids, req.text)
    if result is None:
        return {"status": "failed", "detail": "Telegram send error"}
    return {"status": "success", "telegram_response": result}

@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    locations = {
        "서울특별시": [],
        "부산광역시": [],
        "대구광역시": [],
        "인천광역시": [],
        "광주광역시": [],
        "대전광역시": [],
        "울산광역시": [],
        "세종특별자치시": [],
        "경기도": [],
        "강원도": [],
        "충청북도": [],
        "충청남도": [],
        "전라북도": [],
        "전라남도": [],
        "경상북도": [],
        "경상남도": [],
        "제주특별자치도": []
    }
    return templates.TemplateResponse("index.html", {"request": request, "locations": locations})

@app.post("/watch")
async def watch(req: WatchRequest):
    key = f"{req.location}:{req.keyword}:{req.min_price}:{req.max_price}"
    if key in _monitor_tasks and not _monitor_tasks[key].done():
        return {"status": "already_watching"}

    loop = asyncio.get_event_loop()
    print("DEFAULT_CHAT_IDS:", DEFAULT_CHAT_IDS)
    task = loop.create_task(monitor_keyword(req.location, req.keyword, DEFAULT_CHAT_IDS, req.min_price, req.max_price))
    _monitor_tasks[key] = task
    return {"status": "watching", "location": req.location, "keyword": req.keyword, "min_price": req.min_price, "max_price": req.max_price}

@app.post("/scan")
async def scan_products(req: ScanRequest):
    try:
        results = await fetch_search_results(req.location, req.keyword, req.min_price, req.max_price)

        now_kst = datetime.now(KST)
        cutoff_time = now_kst - timedelta(days=req.days)

        sent_items = []

        for it in results:
            pull_up_time = it.get("pull_up_time_text")
            if not pull_up_time or not isinstance(pull_up_time, datetime):
                continue

            # n일 이내 상품만 필터링
            if pull_up_time >= cutoff_time:
                price_int = int(float(it["price"])) if it["price"] else 0
                text = (
                    f"🔔 <b>{it['title']}</b>\n"
                    f"가격: {price_int}원\n"
                    f"동네: {it['location']}\n"
                    f"검색 키워드: {req.keyword}\n"
                    f"게시글 업로드 시간: {pull_up_time.strftime('%Y년 %-m월 %-d일 %H시 %M분 %S초')}\n"
                    f"상세 설명: {it['description']}\n"
                    f"구매 URL: {it['url']}"
                )
                await send_telegram(DEFAULT_CHAT_IDS, text)
                sent_items.append(id["id"])

        return {
            "status": "success",
            "sent_count": len(sent_items),
            "sent_ids": sent_items
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/active")
async def active_watches():
    result = {}
    for key, task in _monitor_tasks.items():
        if not task.done():
            loc, kw, *_ = key.split(":")
            result[key] = {"location": loc, "keyword": kw}
    return result

@app.post("/stop")
async def stop_watch(req: WatchRequest):
    key = f"{req.location}:{req.keyword}:{req.min_price}:{req.max_price}"
    t = _monitor_tasks.get(key)
    if t:
        t.cancel()
        return {"status": "stopping"}
    return {"status": "not_found"}
