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
    print(f"\n[{datetime.datetime.now()}] --- KIỂM TRA DỮ LIỆU MỚI ---")
    db = SessionLocal()
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # === 1. VÀNG BTMC ===
        try:
            res = await client.get(GOLD_API)
            try:
                data_raw = res.json()
                items = data_raw.get('DataList', {}).get('Data', [])
            except:
                content = res.content.decode('utf-8-sig').strip()
                if not content.startswith("<root>"): content = f"<root>{content}</root>"
                data_raw = xmltodict.parse(content)
                items = data_raw.get('root', {}).get('Data', [])

            if isinstance(items, dict): items = [items]
            
            found_sjc = False
            for item in items:
                # Logic tìm SJC (như cũ)
                current_id = None
                current_name = None
                for k, v in item.items():
                    if isinstance(v, str) and "VÀNG MIẾNG SJC" in v.upper():
                        current_name = v
                        current_id = k.split('_')[-1]
                        break
                
                if current_id:
                    buy = 0.0
                    sell = 0.0
                    # Tìm giá
                    for k, v in item.items():
                        if k.endswith(f"pb_{current_id}"):
                            try: buy = float(str(v).replace(',', ''))
                            except: pass
                        if k.endswith(f"ps_{current_id}"):
                            try: sell = float(str(v).replace(',', ''))
                            except: pass

                    if buy > 0:
                        # --- LOGIC MỚI: KIỂM TRA TRÙNG LẶP ---
                        # Lấy bản ghi cuối cùng trong DB ra xem
                        latest_gold = db.query(GoldHistory).order_by(GoldHistory.created_at.desc()).first()
                        
                        # Nếu đã có dữ liệu cũ VÀ giá mua/bán GIỐNG HỆT nhau -> BỎ QUA
                        if latest_gold and latest_gold.buy == buy and latest_gold.sell == sell:
                            print(f"-> [BỎ QUA VÀNG] Giá không đổi ({buy}-{sell})")
                        else:
                            # Nếu chưa có hoặc giá đã đổi -> LƯU MỚI
                            db.add(GoldHistory(name=current_name, buy=buy, sell=sell))
                            print(f"-> [CẬP NHẬT VÀNG] Giá mới: {buy} - {sell}")
                        
                        found_sjc = True
                        break

        except Exception as e:
            print(f"[LỖI VÀNG]: {e}")

        # === 2. USD VCB ===
        try:
            res = await client.get(VCB_API)
            xml_str = res.content.decode('utf-8-sig').strip()
            data = xmltodict.parse(xml_str)
            rates = data.get('ExrateList', {}).get('Exrate', [])
            for r in rates:
                if r.get('@CurrencyCode') == 'USD':
                    b = float(r.get('@Buy').replace(',', ''))
                    s = float(r.get('@Sell').replace(',', ''))
                    
                    # --- LOGIC MỚI: KIỂM TRA TRÙNG LẶP CHO USD ---
                    latest_usd = db.query(ExchangeHistory).order_by(ExchangeHistory.created_at.desc()).first()
                    
                    if latest_usd and latest_usd.buy_cash == b and latest_usd.sell == s:
                        print(f"-> [BỎ QUA USD] Giá không đổi ({b}-{s})")
                    else:
                        db.add(ExchangeHistory(currency="USD", buy_cash=b, sell=s))
                        print(f"-> [CẬP NHẬT USD] Giá mới: {b} - {s}")
                    break
        except Exception as e:
            print(f"[LỖI USD]: {e}")

        db.commit()
    db.close()
    print("--- XONG ---\n")

# --- CHẠY ---
def run_task(): asyncio.run(fetch_and_save_data())
scheduler = BackgroundScheduler()
scheduler.add_job(run_task, 'interval', hours=1)
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
