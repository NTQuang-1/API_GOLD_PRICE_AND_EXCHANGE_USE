# py -m pip install fastapi uvicorn requests beautifulsoup4
# pip install fastapi uvicorn sqlalchemy apscheduler httpx xmltodict
# py -m uvicorn main:app 0.0.0.0 --port 8000 or py -m uvicorn main:app 0.0.0.0 --port 8000
# uvicorn main:app --reload
# http://localhost:8000/docs

from fastapi import FastAPI
import httpx
import xmltodict
import datetime
import asyncio
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()

# --- CẤU HÌNH DATABASE ---
SQLALCHEMY_DATABASE_URL = "sqlite:///./data_history.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class GoldHistory(Base):
    __tablename__ = "gold_history"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    buy = Column(Float)
    sell = Column(Float)
    created_at = Column(DateTime, default=datetime.datetime.now)

class ExchangeHistory(Base):
    __tablename__ = "exchange_history"
    id = Column(Integer, primary_key=True, index=True)
    currency = Column(String)
    buy_cash = Column(Float)
    sell = Column(Float)
    created_at = Column(DateTime, default=datetime.datetime.now)

Base.metadata.create_all(bind=engine)

# --- LOGIC LẤY DỮ LIỆU ---
GOLD_API = "http://api.btmc.vn/api/BTMCAPI/getpricebtmc?key=3kd8ub1llcg9t45hnoh8hmn7t5kc2v"
VCB_API = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"

async def fetch_and_save_data():
    print(f"\n[{datetime.datetime.now()}] --- BẮT ĐẦU QUÉT DỮ LIỆU ---")
    db = SessionLocal()
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # === 1. VÀNG BTMC (CHIẾN THUẬT QUÉT HẬU TỐ) ===
        try:
            res = await client.get(GOLD_API)
            try:
                data_raw = res.json()
                items = data_raw.get('DataList', {}).get('Data', [])
                mode = "JSON"
            except:
                # Fallback sang XML nếu JSON lỗi
                content = res.content.decode('utf-8-sig').strip()
                if not content.startswith("<root>"): content = f"<root>{content}</root>" # Bọc root nếu thiếu
                data_raw = xmltodict.parse(content)
                items = data_raw.get('root', {}).get('Data', [])
                mode = "XML"

            if isinstance(items, dict): items = [items]
            
            print(f"-> Chế độ: {mode} | Số lượng dòng: {len(items)}")

            found_sjc = False
            for item in items:
                # Bước 1: Tìm xem dòng này có phải SJC không
                current_id = None
                current_name = None
                
                # Duyệt qua từng cặp key-value trong item để tìm tên
                for k, v in item.items():
                    if isinstance(v, str) and "VÀNG MIẾNG SJC" in v.upper():
                        current_name = v
                        # Lấy ID từ key (ví dụ: n_148 -> lấy 148, @n_148 -> lấy 148)
                        current_id = k.split('_')[-1]
                        print(f"-> [TÌM THẤY] ID: {current_id} | Tên: {current_name}")
                        break # Đã tìm thấy tên, thoát vòng lặp key
                
                # Bước 2: Nếu đã xác định được ID (ví dụ 148), đi tìm giá
                if current_id:
                    buy = 0.0
                    sell = 0.0
                    
                    # Quét lại item một lần nữa để tìm key giá khớp với ID
                    # Mẹo: Tìm key nào kết thúc bằng pb_148 (mua) và ps_148 (bán)
                    # Bất kể phía trước là gì (@, hay không có gì)
                    target_buy_suffix = f"pb_{current_id}"
                    target_sell_suffix = f"ps_{current_id}"

                    for k, v in item.items():
                        if k.endswith(target_buy_suffix):
                            try:
                                buy = float(str(v).replace(',', ''))
                                print(f"   -> Khớp giá MUA (Key: {k}): {buy}")
                            except: pass
                        
                        if k.endswith(target_sell_suffix):
                            try:
                                sell = float(str(v).replace(',', ''))
                                print(f"   -> Khớp giá BÁN (Key: {k}): {sell}")
                            except: pass

                    # Lưu vào DB nếu lấy được giá (khác 0)
                    if buy > 0:
                        db.add(GoldHistory(name=current_name, buy=buy, sell=sell))
                        print(f"-> [LƯU THÀNH CÔNG] {current_name}: {buy} - {sell}")
                        found_sjc = True
                        break # Xong việc với vàng, thoát vòng lặp items

            if not found_sjc:
                print("-> CẢNH BÁO: Không tìm thấy SJC hoặc không lấy được giá.")

        except Exception as e:
            print(f"[LỖI VÀNG]: {e}")

        # === 2. USD VCB (Giữ nguyên) ===
        try:
            res = await client.get(VCB_API)
            xml_str = res.content.decode('utf-8-sig').strip()
            data = xmltodict.parse(xml_str)
            rates = data.get('ExrateList', {}).get('Exrate', [])
            for r in rates:
                if r.get('@CurrencyCode') == 'USD':
                    b = float(r.get('@Buy').replace(',', ''))
                    s = float(r.get('@Sell').replace(',', ''))
                    db.add(ExchangeHistory(currency="USD", buy_cash=b, sell=s))
                    print(f"-> [LƯU THÀNH CÔNG] USD: {b} - {s}")
                    break
        except Exception as e:
            print(f"[LỖI USD]: {e}")

        db.commit()
    db.close()
    print("--- HOÀN TẤT ---\n")

# --- CHẠY ---
def run_task(): asyncio.run(fetch_and_save_data())
scheduler = BackgroundScheduler()
scheduler.add_job(run_task, 'cron', hour=8, minute=30)
scheduler.start()

@app.on_event("startup")
async def startup_event(): await fetch_and_save_data()

@app.get("/history/gold")
def get_gold_history():
    db = SessionLocal()
    return db.query(GoldHistory).order_by(GoldHistory.created_at.desc()).limit(10).all()

@app.get("/history/usd")
def get_usd_history():
    db = SessionLocal()
    return db.query(ExchangeHistory).order_by(ExchangeHistory.created_at.desc()).limit(10).all()

@app.get("/")
def home(): return {"status": "ok"}