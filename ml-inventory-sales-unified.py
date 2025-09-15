#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ML Inventory & Sales - Sistema Unificado (versão corrigida)
- Flask + SQLAlchemy
- Inventário FULL e Vendas (30d)
- Dashboard + CSV
- Integração WMS (SmartGO) e Google Sheets (opcional)

Dependências mínimas:
  pip install flask requests python-dotenv sqlalchemy psycopg2-binary

Opcional (se usar WMS/Sheets/Reposição):
  pip install gspread google-auth
"""

import os, sys, json, time, csv, threading
from typing import Any, Dict, List, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, request, send_file, Response
from dotenv import load_dotenv

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Boolean, DateTime, JSON,
    UniqueConstraint, text, DECIMAL, BigInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import func

# ---------------------------- Dependências opcionais ----------------------------
# Esses módulos podem não existir no seu ambiente. Carregamos sob demanda.
WMS_AVAILABLE = True
SHEETS_AVAILABLE = True
REPO_AVAILABLE = True
try:
    from wms_client import WMSSmartGO
except Exception:
    WMS_AVAILABLE = False

try:
    from google_sheets_client import GoogleSheetsClient
except Exception:
    SHEETS_AVAILABLE = False

try:
    from reposicao_analyzer import ReposicaoAnalyzer
except Exception:
    REPO_AVAILABLE = False

# CSV Importer (opcional, mas faz parte do projeto original)
CSV_IMPORT_AVAILABLE = True
try:
    from csv_importer import CSVImporter
except Exception:
    CSV_IMPORT_AVAILABLE = False

# ---------------------------- Config ----------------------------
BASE = "https://api.mercadolibre.com"
OUT_DIR = os.path.join(os.path.dirname(__file__), "out")
os.makedirs(OUT_DIR, exist_ok=True)

load_dotenv()
ML_USER_ID       = os.getenv("ML_USER_ID", "").strip()
ML_APP_ID        = os.getenv("ML_APP_ID", "").strip()
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET", "").strip()
ML_REFRESH_TOKEN = os.getenv("ML_REFRESH_TOKEN", "").strip()
ML_ACCESS_TOKEN  = os.getenv("ML_ACCESS_TOKEN", "").strip()

# WMS
WMS_API_KEY = os.getenv("WMS_API_KEY", "").strip()
SMARTGO_BASE_URL = os.getenv("SMARTGO_BASE_URL", "").strip()

# Google Sheets
GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_URL", "").strip()

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///ml_inventory_sales.db")

# Performance
SCAN_LIMIT_PER_PAGE = 100
ITEMS_BATCH_SIZE    = 1
ITEMS_WORKERS       = 8
STOCK_WORKERS       = 12
SALES_WORKERS       = 16
REQ_TIMEOUT         = (10, 60)

# ---------------------------- Database Models ----------------------------
Base = declarative_base()

class InventoryItem(Base):
    __tablename__ = 'inventory_items'
    id = Column(Integer, primary_key=True)
    item_id = Column(String(50), nullable=False)
    variation_id = Column(Integer, nullable=True)
    inventory_id = Column(String(100), nullable=True)
    title = Column(Text, nullable=True)
    sku = Column(String(100), nullable=True)
    thumbnail_url = Column(Text, nullable=True)
    is_full = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    raw = Column(JSON, nullable=True)
    height = Column(DECIMAL(10, 2), nullable=True)
    width = Column(DECIMAL(10, 2), nullable=True)
    length = Column(DECIMAL(10, 2), nullable=True)
    weight = Column(DECIMAL(10, 2), nullable=True)
    __table_args__ = (UniqueConstraint('item_id', 'variation_id', name='_item_variation_uc'),)

class InventoryStock(Base):
    __tablename__ = 'inventory_stock'
    id = Column(Integer, primary_key=True)
    inventory_id = Column(String(100), nullable=False, unique=True)
    available_quantity = Column(Integer, nullable=True)
    total_quantity = Column(Integer, nullable=True)
    not_available_quantity = Column(Integer, nullable=True)
    transfer_quantity = Column(Integer, default=0)
    inbound_quantity = Column(Integer, default=0)
    not_available_detail = Column(JSON, nullable=True)
    external_references = Column(JSON, nullable=True)
    last_updated = Column(DateTime(timezone=True), default=func.now())
    raw = Column(JSON, nullable=True)

class StockHistory(Base):
    __tablename__ = 'stock_history'
    id = Column(Integer, primary_key=True)
    inventory_id = Column(String(100), nullable=False)
    available_quantity = Column(Integer, nullable=True)
    total_quantity = Column(Integer, nullable=True)
    not_available_quantity = Column(Integer, nullable=True)
    change_type = Column(String(20), nullable=False)
    recorded_at = Column(DateTime(timezone=True), default=func.now())
    notes = Column(Text, nullable=True)

class SalesSummary(Base):
    __tablename__ = 'sales_summary'
    id = Column(Integer, primary_key=True)
    item_id = Column(String(50), nullable=False)
    variation_id = Column(Integer, nullable=True)
    inventory_id = Column(String(100), nullable=True)
    units_30d = Column(Integer, default=0)
    gmv_30d = Column(DECIMAL(12,2), default=0.00)
    last_updated = Column(DateTime(timezone=True), default=func.now())
    created_at = Column(DateTime(timezone=True), default=func.now())
    __table_args__ = (UniqueConstraint('item_id', 'variation_id', name='_sales_item_variation_uc'),)

class SalesOrder(Base):
    __tablename__ = 'sales_orders'
    id = Column(Integer, primary_key=True)
    order_id = Column(BigInteger, nullable=False, unique=True)
    order_status = Column(String(50), nullable=True)
    date_created = Column(DateTime(timezone=True), nullable=True)
    date_closed = Column(DateTime(timezone=True), nullable=True)
    shipping_id = Column(BigInteger, nullable=True)
    logistic_type = Column(String(50), nullable=True)
    is_full = Column(Boolean, default=False)
    total_amount = Column(DECIMAL(12,2), nullable=True)
    currency_id = Column(String(10), nullable=True)
    buyer_id = Column(BigInteger, nullable=True)
    seller_id = Column(BigInteger, nullable=True)
    raw_data = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now())

class SalesOrderItem(Base):
    __tablename__ = 'sales_order_items'
    id = Column(Integer, primary_key=True)
    order_id = Column(BigInteger, nullable=False)
    item_id = Column(String(50), nullable=False)
    variation_id = Column(Integer, nullable=True)
    inventory_id = Column(String(100), nullable=True)
    title = Column(Text, nullable=True)
    sku = Column(String(100), nullable=True)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(DECIMAL(12,2), nullable=False)
    total_price = Column(DECIMAL(12,2), nullable=False)
    currency_id = Column(String(10), nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now())

class SalesSyncHistory(Base):
    __tablename__ = 'sales_sync_history'
    id = Column(Integer, primary_key=True)
    sync_type = Column(String(50), nullable=False)
    period_start = Column(DateTime(timezone=True), nullable=True)
    period_end = Column(DateTime(timezone=True), nullable=True)
    orders_processed = Column(Integer, default=0)
    items_processed = Column(Integer, default=0)
    full_orders_found = Column(Integer, default=0)
    sync_status = Column(String(20), default='running')
    error_message = Column(Text, nullable=True)
    execution_time_seconds = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)

# ---------------------------- Database Setup ----------------------------
def create_database():
    engine = create_engine(DATABASE_URL, echo=False)
    Base.metadata.create_all(engine)
    return engine

def get_db_session():
    engine = create_database()
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()

# --------------------- Sessão HTTP resiliente ------------------------
def create_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6, status=6, backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET","POST","PUT","PATCH","DELETE"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=128, pool_maxsize=128)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = create_session()
HEADERS_BASE = {"Content-Type":"application/json; charset=utf-8"}

# --------------------- Progresso/UI state -----------------------
progress = {
    "fase": "parado",
    "scan_paginas": 0,
    "ids_coletados": 0,
    "total_ids": 0,
    "lotes_items_total": 0,
    "lotes_items_ok": 0,
    "lotes_items_refeitos": 0,
    "full_count": 0,
    "full_sem_inventory": 0,
    "inventories_total": 0,
    "inventories_ok": 0,
    "inventories_refeitos": 0,
    "lotes_stock_total": 0,
    "lotes_stock_ok": 0,
    "lotes_stock_refeitos": 0,
    "sales_orders_total": 0,
    "sales_orders_processed": 0,
    "sales_items_processed": 0,
    "ultimo_msg": ""
}
progress_lock = threading.Lock()

def set_progress(**kw):
    with progress_lock:
        progress.update(kw)

# ---------------------- Tokens e .env ---------------------------
def _env_path() -> str:
    return os.path.join(os.path.dirname(__file__), ".env")

def _write_env_kv(text: str, key: str, value: str) -> str:
    lines = text.splitlines()
    out, found = [], False
    for ln in lines:
        if ln.strip().startswith(f"{key}="):
            out.append(f"{key}={value}"); found = True
        else:
            out.append(ln)
    if not found: out.append(f"{key}={value}")
    if not out or out[-1] != "": out.append("")
    return "\n".join(out)

def save_tokens_to_env(access_token: str, refresh_token: Optional[str]) -> None:
    try:
        p = _env_path()
        txt = ""
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f: txt = f.read()
        txt = _write_env_kv(txt, "ML_ACCESS_TOKEN", access_token)
        if refresh_token: txt = _write_env_kv(txt, "ML_REFRESH_TOKEN", refresh_token)
        with open(p, "w", encoding="utf-8") as f: f.write(txt)
    except Exception:
        pass

def refresh_access_token() -> str:
    if not (ML_APP_ID and ML_CLIENT_SECRET and ML_REFRESH_TOKEN):
        raise RuntimeError("Defina ML_APP_ID, ML_CLIENT_SECRET e ML_REFRESH_TOKEN no .env.")
    url = f"{BASE}/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": ML_APP_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": ML_REFRESH_TOKEN
    }
    r = SESSION.post(url, data=data, timeout=(10, 40))
    r.raise_for_status()
    js = r.json()
    access = js["access_token"]
    new_ref = js.get("refresh_token")
    save_tokens_to_env(access, new_ref)
    globals()["ML_ACCESS_TOKEN"] = access
    if new_ref: globals()["ML_REFRESH_TOKEN"] = new_ref
    return access

def auth_headers() -> Dict[str, str]:
    if not ML_ACCESS_TOKEN:
        refresh_access_token()
    return {"Authorization": f"Bearer {ML_ACCESS_TOKEN}"}

def ml_request(method: str, url: str, timeout=REQ_TIMEOUT, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    h = {}
    h.update(HEADERS_BASE)
    h.update(auth_headers())
    h.update(headers)
    r = SESSION.request(method, url, headers=h, timeout=timeout, **kwargs)
    if r.status_code == 401:
        refresh_access_token()
        h["Authorization"] = f"Bearer {ML_ACCESS_TOKEN}"
        r = SESSION.request(method, url, headers=h, timeout=timeout, **kwargs)
    return r

# --------------------- Utils ----------------------------
def chunked(seq: List[Any], n: int) -> List[List[Any]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

# --------------------- SCAN de itens ----------------------------
def list_all_item_ids(seller_id: str) -> List[str]:
    url = f"{BASE}/users/{seller_id}/items/search"
    ids: List[str] = []
    scroll_id: Optional[str] = None
    page = 0
    set_progress(fase="scan_ids", ultimo_msg="Listando itens (scan)...",
                 scan_paginas=0, ids_coletados=0, total_ids=0)
    while True:
        params = {"search_type":"scan","limit":SCAN_LIMIT_PER_PAGE}
        if scroll_id: params["scroll_id"] = scroll_id
        r = ml_request("GET", url, params=params)
        if not r.ok:
            time.sleep(1.0)
            r = ml_request("GET", url, params=params)
        if not r.ok:
            raise RuntimeError(f"/users/{seller_id}/items/search => {r.status_code} {r.text[:200]}")
        js = r.json()
        page += 1
        batch = js.get("results", []) or []
        ids.extend(batch)
        scroll_id = js.get("scroll_id")
        set_progress(scan_paginas=page, ids_coletados=len(ids), total_ids=len(ids),
                     ultimo_msg=f"+{len(batch)} (acum: {len(ids)})")
        if not batch: break
        time.sleep(0.03)
    return ids

# --------------- /items em lotes -------------------
def multi_get_items(item_ids: List[str]) -> List[Dict[str, Any]]:
    url_multi = f"{BASE}/items"
    batches = chunked(item_ids, ITEMS_BATCH_SIZE)
    set_progress(fase="multi_items", lotes_items_total=len(batches),
                 lotes_items_ok=0, lotes_items_refeitos=0,
                 ultimo_msg="Carregando detalhes em lotes...")

    results: List[Dict[str, Any]] = []
    redo: List[List[str]] = []

    def fetch_batch(batch: List[str]) -> Tuple[bool, List[Dict[str, Any]]]:
        # para 1 item, buscar com atributos completos
        if len(batch) == 1:
            item_id = batch[0]
            url_individual = f"{BASE}/items/{item_id}"
            params = {"include_attributes": "all"}
            tries = 3
            for i in range(tries):
                try:
                    r = ml_request("GET", url_individual, params=params)
                    if r.ok:
                        js = r.json()
                        return True, [js] if js else []
                except requests.RequestException:
                    pass
                time.sleep(0.8 + 0.8*i)
            return False, []
        else:
            # multi-itens: selecionar campos principais (sem atributos completos)
            params = {
                "ids": ",".join(batch),
                "attributes": "id,title,seller_custom_field,pictures,variations,inventory_id,shipping"
            }
            tries = 3
            for i in range(tries):
                try:
                    r = ml_request("GET", url_multi, params=params)
                    if r.ok:
                        out = []
                        for el in (r.json() or []):
                            body = (el or {}).get("body") or {}
                            if body: out.append(body)
                        return True, out
                except requests.RequestException:
                    pass
                time.sleep(0.8 + 0.8*i)
            return False, []

    # Primeira passada
    with ThreadPoolExecutor(max_workers=ITEMS_WORKERS) as ex:
        futs = {ex.submit(fetch_batch, b): b for b in batches}
        for fut in as_completed(futs):
            ok, part = fut.result()
            if ok:
                results.extend(part)
                with progress_lock: progress["lotes_items_ok"] += 1
            else:
                redo.append(futs[fut])

    # Refile dos lotes com erro
    if redo:
        set_progress(ultimo_msg=f"Refazendo {len(redo)} lotes /items...")
        with ThreadPoolExecutor(max_workers=max(2, ITEMS_WORKERS//2)) as ex:
            futs = {ex.submit(fetch_batch, b): b for b in redo}
            for fut in as_completed(futs):
                ok, part = fut.result()
                if ok:
                    results.extend(part)
                with progress_lock: progress["lotes_items_refeitos"] += 1

    return results

def multi_get_stock(inventory_ids: List[str]) -> List[Dict[str, Any]]:
    url = f"{BASE}/inventory/stock"
    batches = chunked(inventory_ids, 20)
    set_progress(fase="multi_stock", lotes_stock_total=len(batches),
                 lotes_stock_ok=0, lotes_stock_refeitos=0,
                 ultimo_msg="Carregando estoque em lotes...")

    results: List[Dict[str, Any]] = []
    redo: List[List[str]] = []

    def fetch_batch(batch: List[str]) -> Tuple[bool, List[Dict[str, Any]]]:
        params = {"ids": ",".join(batch)}
        tries = 3
        for i in range(tries):
            try:
                r = ml_request("GET", url, params=params)
                if r.ok:
                    return True, r.json() or []
            except requests.RequestException:
                pass
            time.sleep(0.8 + 0.8*i)
        return False, []

    with ThreadPoolExecutor(max_workers=STOCK_WORKERS) as ex:
        futures = {ex.submit(fetch_batch, b): b for b in batches}
        for fut in as_completed(futures):
            ok, part = fut.result()
            if ok:
                results.extend(part)
                with progress_lock: progress["lotes_stock_ok"] += 1
            else:
                redo.append(futures[fut])

    if redo:
        set_progress(ultimo_msg=f"Refazendo {len(redo)} lotes /stock...")
        with ThreadPoolExecutor(max_workers=max(2, STOCK_WORKERS//2)) as ex:
            futures = {ex.submit(fetch_batch, b): b for b in redo}
            for fut in as_completed(futures):
                ok, part = fut.result()
                if ok:
                    results.extend(part)
                with progress_lock: progress["lotes_stock_refeitos"] += 1

    return results

# ------------------ FULL helpers -------------------------------
def is_full_item(it: Dict[str, Any]) -> bool:
    shp = it.get("shipping") or {}
    return shp.get("logistic_type") == "fulfillment"

def extract_inventory_links(it: Dict[str, Any]) -> List[Tuple[Optional[str], Optional[int]]]:
    out: List[Tuple[Optional[str], Optional[int]]] = []
    base_inv = it.get("inventory_id")
    variations = it.get("variations") or []
    if variations:
        for v in variations:
            inv = v.get("inventory_id") or base_inv
            out.append((inv, v.get("id")))
        if not out and base_inv:
            out.append((base_inv, None))
    else:
        out.append((base_inv, None))
    seen = set(); uniq = []
    for inv, var in out:
        key = (inv, var)
        if key in seen: continue
        seen.add(key)
        uniq.append((inv, var))
    return uniq

def get_full_stock(inventory_id: str) -> Optional[Dict[str, Any]]:
    url = f"{BASE}/inventories/{inventory_id}/stock/fulfillment"
    tries = 4
    for i in range(tries):
        try:
            r = ml_request("GET", url)
            if r.ok:
                js = r.json() or {}
                transfer_qty = 0
                inbound_qty = 0
                not_available_details = js.get("not_available_detail", [])
                if isinstance(not_available_details, list):
                    for detail in not_available_details:
                        state = detail.get("state")
                        quantity = detail.get("quantity", 0)
                        if state in ["in_transfer", "transfer_in_progress"]:
                            transfer_qty += quantity or 0
                        elif state == "inbound":
                            inbound_qty += quantity or 0
                js["transfer_quantity"] = transfer_qty
                js["inbound_quantity"] = inbound_qty
                ext = js.get("external_references") or []
                if isinstance(ext, dict): ext = [ext]
                js["external_references"] = ext
                js["inventory_id"] = inventory_id
                return js
        except requests.RequestException:
            pass
        time.sleep(0.8 + i*0.8)
    return None

# ------------------ Sales helpers -------------------------------
def orders_last_30d(seller_id: str, status: Optional[str] = "paid") -> List[dict]:
    to = datetime.now(timezone.utc)
    frm = to - timedelta(days=30)
    url = f"{BASE}/orders/search"
    out: List[dict] = []
    offset, limit = 0, 50
    set_progress(fase="sales_orders", sales_orders_total=0, sales_orders_processed=0,
                 ultimo_msg="Buscando pedidos dos últimos 30 dias...")
    while True:
        params = {
            "seller": seller_id,
            "order.date_created.from": frm.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "order.date_created.to":   to.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sort": "date_desc",
            "offset": offset,
            "limit": limit,
        }
        if status:
            params["order.status"] = status
        r = ml_request("GET", url, params=params)
        if not r.ok:
            time.sleep(1.0)
            r = ml_request("GET", url, params=params)
        if not r.ok:
            raise RuntimeError(f"Erro ao buscar pedidos: {r.status_code} {r.text[:200]}")
        js = r.json() or {}
        results = js.get("results") or js.get("orders") or []
        if not results:
            break
        out.extend(results)
        offset += limit
        set_progress(sales_orders_processed=len(out),
                     ultimo_msg=f"Pedidos coletados: {len(out)}")
        if len(results) < limit:
            break
        time.sleep(0.02)
    set_progress(sales_orders_total=len(out))
    return out

def fetch_shipment(sh_id: int) -> Optional[dict]:
    try:
        r = ml_request("GET", f"{BASE}/shipments/{sh_id}", timeout=35)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None

def shipments_by_ids(shipping_ids: Set[int], max_workers=SALES_WORKERS) -> Dict[int, dict]:
    out: Dict[int, dict] = {}
    if not shipping_ids:
        return out
    set_progress(ultimo_msg=f"Buscando {len(shipping_ids)} shipments...")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_shipment, sh_id): sh_id for sh_id in shipping_ids}
        for fut in as_completed(futs):
            sh = fut.result()
            if sh:
                out[futs[fut]] = sh
    return out

def build_inventory_map(items: List[dict]) -> Dict[Tuple[str, Optional[int]], Optional[str]]:
    inv_map = {}
    for it in items:
        iid = it.get("id")
        if not iid: continue
        base_inv = it.get("inventory_id")
        vars_ = it.get("variations") or []
        if vars_:
            for v in vars_:
                inv_map[(iid, v.get("id"))] = v.get("inventory_id") or base_inv
        else:
            inv_map[(iid, None)] = base_inv
    return inv_map

# ------------------ Database Operations -------------------------
def save_inventory_item(db: Session, item_data: Dict[str, Any], inventory_id: Optional[str], variation_id: Optional[int]):
    pics = item_data.get("pictures") or []
    thumb = None
    if pics:
        p0 = pics[0] or {}
        thumb = p0.get("secure_url") or p0.get("url")
    item = db.query(InventoryItem).filter(
        InventoryItem.item_id == item_data.get("id"),
        InventoryItem.variation_id == variation_id
    ).first()
    if not item:
        item = InventoryItem()
    item.item_id = item_data.get("id")
    item.variation_id = variation_id
    item.inventory_id = inventory_id
    item.title = item_data.get("title")
    item.sku = item_data.get("seller_custom_field")
    item.thumbnail_url = thumb
    item.is_full = True
    item.raw = item_data
    # dimensões
    dimensions = extract_package_dimensions(item_data)
    if not any(v is not None for v in dimensions.values()) and variation_id:
        for var in (item_data.get("variations") or []):
            if var.get("id") == variation_id:
                dimensions = extract_package_dimensions(var)
                break
    item.height = dimensions.get("height")
    item.width  = dimensions.get("width")
    item.length = dimensions.get("length")
    item.weight = dimensions.get("weight")
    db.merge(item)

def save_inventory_stock(db: Session, stock_data: Dict[str, Any]):
    inventory_id = stock_data.get("inventory_id")
    if not inventory_id:
        return
    current_stock = db.query(InventoryStock).filter(
        InventoryStock.inventory_id == inventory_id
    ).first()
    old_available = current_stock.available_quantity if current_stock else None
    if not current_stock:
        current_stock = InventoryStock()
    current_stock.inventory_id = inventory_id
    current_stock.available_quantity = stock_data.get("available_quantity")
    current_stock.total_quantity = stock_data.get("total")
    current_stock.not_available_quantity = stock_data.get("not_available_quantity")
    current_stock.transfer_quantity = stock_data.get("transfer_quantity", 0)
    current_stock.inbound_quantity = stock_data.get("inbound_quantity", 0)
    current_stock.not_available_detail = stock_data.get("not_available_detail", [])
    current_stock.external_references = stock_data.get("external_references", [])
    current_stock.raw = stock_data
    db.merge(current_stock)
    if old_available != current_stock.available_quantity:
        history = StockHistory(
            inventory_id=inventory_id,
            available_quantity=current_stock.available_quantity,
            total_quantity=current_stock.total_quantity,
            not_available_quantity=current_stock.not_available_quantity,
            change_type="sync",
            notes=f"Sync automático: {old_available} → {current_stock.available_quantity}"
        )
        db.add(history)

def save_sales_data(db: Session, orders: List[dict], shipments: Dict[int, dict], inventory_map: Dict[Tuple[str, Optional[int]], Optional[str]]):
    sync_record = SalesSyncHistory(
        sync_type="summary_30d",
        period_start=datetime.now(timezone.utc) - timedelta(days=30),
        period_end=datetime.now(timezone.utc),
        sync_status="running"
    )
    db.add(sync_record)
    db.commit()
    try:
        full_ship_ids = {sid for sid, sh in shipments.items() if (sh or {}).get("logistic_type") == "fulfillment"}
        order_to_ship = {}
        for o in orders:
            sh = (o.get("shipping") or {}).get("id")
            if sh:
                try:
                    order_to_ship[o.get("id")] = int(sh)
                except:
                    pass
        sales_summary: Dict[Tuple[str, Optional[int]], Dict[str, Any]] = {}
        orders_processed = 0
        items_processed = 0
        full_orders_found = 0
        for o in orders:
            order_id = o.get("id")
            if not order_id:
                continue
            sid = order_to_ship.get(order_id)
            is_full_order = bool(sid and sid in full_ship_ids)
            if is_full_order:
                full_orders_found += 1
                order_record = db.query(SalesOrder).filter(SalesOrder.order_id == order_id).first()
                if not order_record:
                    order_record = SalesOrder()
                order_record.order_id = order_id
                order_record.order_status = o.get("status")
                order_record.date_created = datetime.fromisoformat(o.get("date_created", "").replace("Z", "+00:00")) if o.get("date_created") else None
                order_record.date_closed = datetime.fromisoformat(o.get("date_closed", "").replace("Z", "+00:00")) if o.get("date_closed") else None
                order_record.shipping_id = sid
                order_record.logistic_type = "fulfillment"
                order_record.is_full = True
                order_record.total_amount = float(o.get("total_amount", 0) or 0)
                order_record.currency_id = o.get("currency_id")
                order_record.buyer_id = (o.get("buyer") or {}).get("id")
                order_record.seller_id = int(ML_USER_ID) if ML_USER_ID else None
                order_record.raw_data = o
                db.merge(order_record)
                for line in (o.get("order_items") or []):
                    it = line.get("item") or {}
                    iid = it.get("id")
                    vid = it.get("variation_id")
                    if not iid:
                        continue
                    quantity = float(line.get("quantity") or 0)
                    unit_price = float(line.get("unit_price") or 0)
                    total_price = quantity * unit_price
                    item_record = SalesOrderItem(
                        order_id=order_id,
                        item_id=iid,
                        variation_id=vid,
                        inventory_id=inventory_map.get((iid, vid)),
                        title=it.get("title"),
                        sku=it.get("seller_custom_field"),
                        quantity=int(quantity),
                        unit_price=unit_price,
                        total_price=total_price,
                        currency_id=line.get("currency_id")
                    )
                    db.add(item_record)
                    key = (iid, vid)
                    if key not in sales_summary:
                        sales_summary[key] = {"units": 0, "gmv": 0.0, "inventory_id": inventory_map.get(key)}
                    sales_summary[key]["units"] += quantity
                    sales_summary[key]["gmv"] += total_price
                    items_processed += 1
            orders_processed += 1
            if orders_processed % 100 == 0:
                set_progress(sales_orders_processed=orders_processed,
                             ultimo_msg=f"Processados: {orders_processed} pedidos, {full_orders_found} FULL")
        for (item_id, variation_id), data in sales_summary.items():
            summary_record = db.query(SalesSummary).filter(
                SalesSummary.item_id == item_id,
                SalesSummary.variation_id == variation_id
            ).first()
            if not summary_record:
                summary_record = SalesSummary()
            summary_record.item_id = item_id
            summary_record.variation_id = variation_id
            summary_record.inventory_id = data["inventory_id"]
            summary_record.units_30d = int(data["units"])
            summary_record.gmv_30d = round(data["gmv"], 2)
            db.merge(summary_record)
        sync_record.orders_processed = orders_processed
        sync_record.items_processed = items_processed
        sync_record.full_orders_found = full_orders_found
        sync_record.sync_status = "completed"
        sync_record.completed_at = datetime.now(timezone.utc)
        db.commit()
        return {
            "orders_processed": orders_processed,
            "items_processed": items_processed,
            "full_orders_found": full_orders_found,
            "sales_items_saved": len(sales_summary)
        }
    except Exception as e:
        sync_record.sync_status = "failed"
        sync_record.error_message = str(e)
        sync_record.completed_at = datetime.now(timezone.utc)
        db.commit()
        raise e

# ------------------ Pipeline principal -------------------------
def collect_and_save_inventory() -> Dict[str, Any]:
    db = get_db_session()
    try:
        set_progress(fase="scan_ids", ultimo_msg="Listando itens (scan)...")
        ids = list_all_item_ids(ML_USER_ID)
        set_progress(fase="multi_items", ultimo_msg="Carregando detalhes em lotes...")
        items = multi_get_items(ids)
        full_items = [it for it in items if is_full_item(it)]
        unique_invs: List[str] = []
        seen_inv: Set[str] = set()
        missing_inv_counter = 0
        items_saved = 0
        for it in full_items:
            links = extract_inventory_links(it)
            for inv, var_id in links:
                if inv is None:
                    missing_inv_counter += 1
                else:
                    save_inventory_item(db, it, inv, var_id)
                    items_saved += 1
                    if inv not in seen_inv:
                        seen_inv.add(inv)
                        unique_invs.append(inv)
        db.commit()
        set_progress(full_count=len(full_items),
                     full_sem_inventory=missing_inv_counter,
                     fase="estoque_full",
                     inventories_total=len(unique_invs),
                     inventories_ok=0,
                     ultimo_msg="Buscando estoque FULL por inventory_id...")
        stock_map: Dict[str, Dict[str, Any]] = {}
        redo_invs: List[str] = []
        def fetch_inv(inv: str) -> Tuple[str, Optional[Dict[str, Any]]]:
            return inv, get_full_stock(inv)
        with ThreadPoolExecutor(max_workers=STOCK_WORKERS) as ex:
            futures = {ex.submit(fetch_inv, inv): inv for inv in unique_invs}
            for fut in as_completed(futures):
                inv, js = fut.result()
                if js is not None:
                    stock_map[inv] = js
                    with progress_lock: progress["inventories_ok"] += 1
                else:
                    redo_invs.append(inv)
        if redo_invs:
            set_progress(ultimo_msg=f"Refazendo {len(redo_invs)} inventories...")
            with ThreadPoolExecutor(max_workers=max(2, STOCK_WORKERS//2)) as ex:
                futures = {ex.submit(fetch_inv, inv): inv for inv in redo_invs}
                for fut in as_completed(futures):
                    inv, js = fut.result()
                    if js is not None:
                        stock_map[inv] = js
                        with progress_lock: progress["inventories_refeitos"] += 1
        stocks_saved = 0
        for inv, stock_data in stock_map.items():
            save_inventory_stock(db, stock_data)
            stocks_saved += 1
        db.commit()
        result = {
            "items_processed": len(full_items),
            "items_saved": items_saved,
            "stocks_saved": stocks_saved,
            "missing_inventory": missing_inv_counter,
            "total_inventories": len(unique_invs)
        }
        set_progress(fase="inventario_concluido",
                     ultimo_msg=f"Inventário: {items_saved} itens, {stocks_saved} estoques salvos")
        return result
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def collect_and_save_sales() -> Dict[str, Any]:
    db = get_db_session()
    try:
        set_progress(fase="sales_orders", ultimo_msg="Buscando pedidos dos últimos 30 dias...")
        orders = orders_last_30d(ML_USER_ID, status="paid")
        set_progress(fase="sales_shipments", ultimo_msg="Buscando dados de envio...")
        ship_ids = set()
        for o in orders:
            sh = (o.get("shipping") or {}).get("id")
            if sh:
                try:
                    ship_ids.add(int(sh))
                except:
                    pass
        shipments = shipments_by_ids(ship_ids)
        set_progress(fase="sales_items", ultimo_msg="Mapeando inventory_ids...")
        items_in_orders: Set[str] = set()
        for o in orders:
            for line in (o.get("order_items") or []):
                it = line.get("item") or {}
                iid = it.get("id")
                if iid:
                    items_in_orders.add(iid)
        items = multi_get_items(sorted(list(items_in_orders)))
        inventory_map = build_inventory_map(items)
        set_progress(fase="sales_items", ultimo_msg="Salvando dimensões dos itens...")
        for item_data in items:
            for inv, var_id in extract_inventory_links(item_data):
                save_inventory_item(db, item_data, inv, var_id)
        db.commit()
        set_progress(fase="sales_save", ultimo_msg="Salvando dados de vendas...")
        result = save_sales_data(db, orders, shipments, inventory_map)
        set_progress(fase="vendas_concluido",
                     ultimo_msg=f"Vendas: {result['full_orders_found']} pedidos FULL, {result['sales_items_saved']} produtos")
        return result
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def full_sync_ml_only() -> Dict[str, Any]:
    inventory_result = collect_and_save_inventory()
    sales_result = collect_and_save_sales()
    return {"inventory": inventory_result, "sales": sales_result}

def full_sync() -> Dict[str, Any]:
    try:
        set_progress(fase="iniciando", ultimo_msg="Iniciando sincronização completa...")
        inventory_result = collect_and_save_inventory()
        sales_result = collect_and_save_sales()
        db = get_db_session()
        try:
            export_to_csv(db)
        finally:
            db.close()
        result = {"inventory": inventory_result, "sales": sales_result}
        set_progress(fase="finalizado", ultimo_msg="Sincronização completa finalizada")
        return result
    except Exception as e:
        set_progress(fase="erro", ultimo_msg=f"Falhou: {type(e).__name__} — {str(e)[:180]}")
        raise e

def executar_sync_wms_smartgo() -> Dict[str, Any]:
    if not WMS_AVAILABLE:
        return {'status': 'erro', 'erro': 'Módulo WMS não disponível'}
    try:
        set_progress(ultimo_msg="Iniciando sync WMS...")
        if not WMS_API_KEY:
            return {'status': 'erro', 'erro': 'Credenciais WMS não configuradas'}
        db = get_db_session()
        try:
            query = text("SELECT DISTINCT sku FROM inventory_items WHERE sku IS NOT NULL")
            result = db.execute(query)
            skus = [row[0] for row in result if row[0]]
            wms_client = WMSSmartGO(WMS_API_KEY)
            resultados = wms_client.consultar_estoque_lote(skus)
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS wms_stock (
                    sku TEXT PRIMARY KEY,
                    disponivel INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'sucesso',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            for r in resultados:
                db.execute(text("""
                    INSERT OR REPLACE INTO wms_stock (sku, disponivel, status, last_updated)
                    VALUES (:sku, :disponivel, :status, CURRENT_TIMESTAMP)
                """), {'sku': r['codigo'], 'disponivel': r['disponivel'], 'status': r['status']})
            db.commit()
            sucessos = len([r for r in resultados if r['status'] == 'sucesso'])
            return {'status': 'sucesso', 'produtos_processados': len(resultados), 'sucessos': sucessos}
        finally:
            db.close()
    except Exception as e:
        return {'status': 'erro', 'erro': str(e)}

def full_sync_paralelo() -> Dict[str, Any]:
    try:
        set_progress(fase="sync_paralelo", ultimo_msg="Iniciando sync paralelo ML + WMS...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_ml = executor.submit(full_sync_ml_only)
            future_wms = executor.submit(executar_sync_wms_smartgo)
            while not (future_ml.done() and future_wms.done()):
                time.sleep(2)
                ml_status = "executando" if not future_ml.done() else "concluído"
                wms_status = "executando" if not future_wms.done() else "concluído"
                set_progress(ultimo_msg=f"ML: {ml_status} | WMS: {wms_status}")
            result_ml = future_ml.result()
            result_wms = future_wms.result()
            db = get_db_session()
            try:
                export_to_csv(db)
            finally:
                db.close()
            set_progress(fase="finalizado", ultimo_msg=f"Sync paralelo concluído! ML + WMS sincronizados")
            return {'ml': result_ml, 'wms': result_wms, 'status': 'sucesso'}
    except Exception as e:
        set_progress(fase="erro", ultimo_msg=f"Erro no sync paralelo: {str(e)}")
        return {'status': 'erro', 'erro': str(e)}

# ------------------ Export to CSV ------------------------------
def export_to_csv(db: Session):
    csv_path = os.path.join(OUT_DIR, "inventory_sales_full.csv")
    try:
        items_exists = db.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")).fetchone()
        if items_exists:
            query = text("""
                SELECT 
                    inventory_id, sku, title, num_anuncios,
                    available_quantity, total_quantity, not_available_quantity,
                    transfer_quantity, inbound_quantity, vendas_consolidadas, receita_consolidada,
                    necessidade_envio, pode_atender, height_cm, width_cm, length_cm, weight_g, cubagem_m3, classificacao_tamanho
                FROM items
                ORDER BY vendas_consolidadas DESC
            """)
            result = db.execute(query)
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow([
                    "inventory_id","sku","title","num_anuncios",
                    "available_quantity","total_quantity","not_available_quantity",
                    "transfer_quantity","inbound_quantity","vendas_consolidadas","receita_consolidada",
                    "necessidade_envio","pode_atender","height_cm","width_cm","length_cm","weight_g","cubagem_m3","classificacao_tamanho"
                ])
                for row in result:
                    w.writerow(row)
        else:
            query = text("""
                SELECT 
                    ii.inventory_id, ii.sku, ii.title,
                    ist.available_quantity, ist.total_quantity, ist.not_available_quantity,
                    ist.transfer_quantity, ist.inbound_quantity,
                    SUM(COALESCE(ss.units_30d, 0)) as vendas_consolidadas,
                    SUM(COALESCE(ss.gmv_30d, 0)) as receita_consolidada,
                    COUNT(ii.item_id) as num_anuncios,
                    MAX(ii.height) as height, MAX(ii.width) as width, MAX(ii.length) as length, MAX(ii.weight) as weight
                FROM inventory_items ii
                LEFT JOIN inventory_stock ist ON ii.inventory_id = ist.inventory_id
                LEFT JOIN sales_summary ss ON ii.item_id = ss.item_id 
                    AND (ii.variation_id = ss.variation_id OR (ii.variation_id IS NULL AND ss.variation_id IS NULL))
                WHERE ii.is_full = true AND ii.inventory_id IS NOT NULL
                GROUP BY ii.inventory_id, ist.available_quantity, ist.total_quantity, 
                         ist.not_available_quantity, ist.transfer_quantity, ist.inbound_quantity
                ORDER BY vendas_consolidadas DESC
            """)
            result = db.execute(query)
            wms_client = WMSSmartGO(WMS_API_KEY) if WMS_AVAILABLE and WMS_API_KEY else None
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow([
                    "inventory_id","sku","title","num_anuncios",
                    "available_quantity","total_quantity","not_available_quantity",
                    "transfer_quantity","inbound_quantity","vendas_consolidadas","receita_consolidada",
                    "necessidade_envio","pode_atender","height_cm","width_cm","length_cm","weight_g","cubagem_m3","classificacao_tamanho"
                ])
                for row in result:
                    item_data = {
                        "inventory_id": row[0], "sku": row[1], "title": row[2],
                        "available_quantity": row[3], "total_quantity": row[4], "not_available_quantity": row[5],
                        "transfer_quantity": row[6], "inbound_quantity": row[7],
                        "vendas_consolidadas": row[8] or 0, "receita_consolidada": row[9] or 0.0, "num_anuncios": row[10] or 1,
                        "height": row[11], "width": row[12], "length": row[13], "weight": row[14]
                    }
                    vendas_30d = item_data["vendas_consolidadas"] or 0
                    estoque_disponivel = item_data["available_quantity"] or 0
                    estoque_em_transito = (item_data["transfer_quantity"] or 0) + (item_data["inbound_quantity"] or 0)
                    if vendas_30d > 0:
                        necessidade = vendas_30d - (estoque_disponivel + estoque_em_transito)
                        necessidade_envio = max(0, int(necessidade))
                    elif estoque_disponivel == 0 and vendas_30d == 0:
                        necessidade_envio = "Oportunidade"
                    else:
                        necessidade_envio = 0
                    pode_atender = 0
                    if isinstance(necessidade_envio, int) and necessidade_envio > 0 and wms_client and item_data["sku"]:
                        try:
                            produtos_wms = wms_client.consultar_por_codigo_universal(item_data["sku"])
                            if isinstance(produtos_wms, list) and produtos_wms:
                                estoque_wms = (produtos_wms[0].get('quantidadeDisponivel', 0)
                                               or produtos_wms[0].get('quantidade_disponivel', 0) or 0)
                            elif isinstance(produtos_wms, dict):
                                estoque_wms = (produtos_wms.get('quantidadeDisponivel', 0)
                                               or produtos_wms.get('quantidade_disponivel', 0) or 0)
                            else:
                                estoque_wms = 0
                            pode_atender = min(necessidade_envio, estoque_wms)
                        except Exception:
                            pode_atender = 0
                    elif isinstance(necessidade_envio, int) and necessidade_envio == 0 and wms_client and item_data["sku"]:
                        try:
                            produtos_wms = wms_client.consultar_por_codigo_universal(item_data["sku"])
                            if isinstance(produtos_wms, list) and produtos_wms:
                                pode_atender = (produtos_wms[0].get('quantidadeDisponivel', 0)
                                                or produtos_wms[0].get('quantidade_disponivel', 0) or 0)
                            elif isinstance(produtos_wms, dict):
                                pode_atender = (produtos_wms.get('quantidadeDisponivel', 0)
                                                or produtos_wms.get('quantidade_disponivel', 0) or 0)
                            else:
                                pode_atender = 0
                        except Exception:
                            pode_atender = 0
                    cubagem_m3 = 0
                    classificacao = ""
                    h = item_data.get("height"); w_ = item_data.get("width"); l = item_data.get("length"); weight = item_data.get("weight")
                    if h and w_ and l:
                        try:
                            cubagem_m3 = (float(h)/100) * (float(w_)/100) * (float(l)/100)
                            cubagem_m3 = round(cubagem_m3, 6)
                            if weight:
                                classificacao = classify_ml_size(float(l), float(w_), float(h), float(weight))
                        except (ValueError, TypeError):
                            cubagem_m3 = 0; classificacao = ""
                    w.writerow([
                        item_data["inventory_id"] or "", item_data["sku"] or "", item_data["title"] or "",
                        item_data["num_anuncios"], item_data["available_quantity"], item_data["total_quantity"], item_data["not_available_quantity"],
                        item_data["transfer_quantity"], item_data["inbound_quantity"], item_data["vendas_consolidadas"], float(item_data["receita_consolidada"] or 0.0),
                        necessidade_envio, pode_atender,
                        item_data["height"] or "", item_data["width"] or "", item_data["length"] or "",
                        item_data["weight"] or "", cubagem_m3, classificacao
                    ])
    except Exception as e:
        print(f"Erro ao exportar CSV: {e}")
        raise

def extract_package_dimensions(item_data: Dict[str, Any]) -> Dict[str, Any]:
    attributes = item_data.get("attributes", [])
    dimensions = {"height": None, "width": None, "length": None, "weight": None}
    for attr in attributes:
        attr_id = attr.get("id", "")
        value = attr.get("value_name") or attr.get("value_id")
        if not value:
            continue
        try:
            if attr_id == "PACKAGE_LENGTH":
                dimensions["length"] = float(str(value).replace(" cm", "").replace(",", "."))
            elif attr_id == "PACKAGE_WIDTH":
                dimensions["width"] = float(str(value).replace(" cm", "").replace(",", "."))
            elif attr_id == "PACKAGE_HEIGHT":
                dimensions["height"] = float(str(value).replace(" cm", "").replace(",", "."))
            elif attr_id == "PACKAGE_WEIGHT":
                weight_str = str(value).replace(" g", "").replace(" kg", "").replace(",", ".")
                dimensions["weight"] = float(weight_str)
        except (ValueError, AttributeError):
            continue
    return dimensions

def classify_ml_size(length: float, width: float, height: float, weight_g: float) -> str:
    weight_kg = weight_g / 1000
    dims = sorted([length, width, height])
    regras = [
        ("pequeno", [12, 15, 25], 18),
        ("médio",   [28, 36, 51], 18),
        ("grande",  [60, 60, 70], 18),
    ]
    for nome, (x, y, z), kg_lim in regras:
        if dims[0] <= x and dims[1] <= y and dims[2] <= z and weight_kg <= kg_lim:
            return nome
    return "extragrande"

# ----------------------- UI (Flask) -----------------------------
app = Flask(__name__)

INDEX_HTML = """
<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>ML Inventory & Sales - Sistema Unificado</title>
<style>
  body{font-family:system-ui,Segoe UI,Arial,sans-serif;margin:24px;background:#0b1220;color:#eef3ff}
  .card{background:#111a2b;border:1px solid #24314d;border-radius:14px;padding:16px;margin-bottom:18px;box-shadow:0 6px 20px rgba(0,0,0,.35)}
  .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
  button{background:#4f8cff;border:0;color:#fff;padding:10px 16px;border-radius:10px;font-weight:600;cursor:pointer;margin:4px}
  button:disabled{opacity:.5;cursor:not-allowed}
  .btn-secondary{background:#6b7280}
  .btn-success{background:#10b981}
  .btn-warning{background:#f59e0b}
  .muted{opacity:.8}
  .bar{background:#0e1627;border:1px solid #203055;border-radius:10px;overflow:hidden;height:16px}
  .fill{background:#4f8cff;height:100%;width:0%}
  table{width:100%;border-collapse:separate;border-spacing:0 8px}
  th,td{padding:8px 10px;text-align:left}
  th{font-size:13px;color:#a9b7d6;text-transform:uppercase;letter-spacing:.06em}
  tr{background:#0f1830;border:1px solid #233150}
  img.thumb{width:56px;height:56px;object-fit:cover;border-radius:8px;border:1px solid #2a3b63;background:#091125}
  .qty{font-weight:700}
  .grid{display:grid;grid-template-columns:1fr auto;gap:12px}
  .right{text-align:right}
  a{color:#9ec1ff}
  .err{color:#ffb1b1}
  .pill{display:inline-block;padding:4px 8px;border:1px solid #2b3a5f;border-radius:999px;font-size:12px;margin-left:6px}
  code.badge{background:#0e1b33;border:1px solid #27427a;padding:2px 6px;border-radius:6px}
  .stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:20px}
  .stat-card{background:#0f1830;border:1px solid #233150;border-radius:8px;padding:12px;text-align:center}
  .stat-number{font-size:24px;font-weight:700;color:#4f8cff}
  .stat-label{font-size:12px;color:#a9b7d6;text-transform:uppercase}
  .tabs{display:flex;gap:4px;margin-bottom:16px}
  .tab{padding:8px 16px;background:#1f2937;border:1px solid #374151;border-radius:8px;cursor:pointer;color:#9ca3af}
  .tab.active{background:#4f8cff;color:#fff;border-color:#4f8cff}
  .tab-content{display:none}
  .tab-content.active{display:block}
  .performance-indicator{padding:4px 8px;border-radius:4px;font-size:11px;font-weight:600}
  .healthy{background:#10b981;color:#fff}
  .low-stock{background:#f59e0b;color:#fff}
  .out-of-stock{background:#ef4444;color:#fff}
  .no-sales{background:#6b7280;color:#fff}
  .status-badge{display:inline-flex;align-items:center;gap:4px;padding:4px 8px;border-radius:6px;font-size:11px;font-weight:600;white-space:nowrap}
  .status-completo{background:rgba(40,167,69,0.2);color:#28a745;border:1px solid #28a745}
  .status-parcial{background:rgba(255,193,7,0.2);color:#ffc107;border:1px solid #ffc107}
  .status-deficit{background:rgba(220,53,69,0.2);color:#dc3545;border:1px solid #dc3545}
  .status-sem-necessidade{background:rgba(108,117,125,0.2);color:#6c757d;border:1px solid #6c757d}
  .tooltip{position:relative;cursor:help}
  .tooltip:hover::after{content:attr(data-tooltip);position:absolute;bottom:100%;left:50%;transform:translateX(-50%);background:#1f2937;color:#fff;padding:4px 8px;border-radius:4px;font-size:11px;white-space:nowrap;z-index:1000;margin-bottom:4px}
  .tooltip:hover::before{content:'';position:absolute;bottom:100%;left:50%;transform:translateX(-50%);border:4px solid transparent;border-top-color:#1f2937;margin-bottom:-4px}
</style>
</head>
<body>
  <div class="card">
    <div class="grid">
      <div>
        <h2 style="margin:0 0 6px 0;">ML Inventory & Sales - Sistema Unificado
          <span id="pillCounts" class="pill"></span>
        </h2>
        <div class="muted" id="status">Pronto para sincronizar</div>
      </div>
      <div class="row">
        <button id="btnSyncAll" onclick="syncAll()">🔄 Sync Completo</button>
        <button class="btn-secondary" onclick="syncInventory()">📦 Só Inventário</button>
        <button class="btn-warning" onclick="syncSales()">💰 Só Vendas</button>
        <button class="btn-success" onclick="exportCSV()">📊 Exportar CSV</button>
        <button class="btn-secondary" onclick="importCSV()">📥 Importar CSV</button>
        <button class="btn-secondary" onclick="mostrarPedidosTiny()">📦 Pedidos Tiny</button>
      </div>
    </div>
    <div class="bar"><div class="fill" id="progressBar"></div></div>
    <div id="progressText" class="muted" style="margin-top:8px;font-size:13px"></div>
  </div>
<div id="tinyCard" class="card" style="display:none">
  <h3 style="margin-top:0">Pedidos Tiny</h3>
  <table>
    <thead>
      <tr><th>Inventory ID</th><th>Quantidade a enviar</th></tr>
    </thead>
    <tbody id="tinyTbody"></tbody>
  </table>
</div>


  <div id="statsContainer" class="stats" style="display:none">
    <div class="stat-card"><div class="stat-number" id="statTotal">0</div><div class="stat-label">Total de Itens</div></div>
    <div class="stat-card"><div class="stat-number" id="statAvailable">0</div><div class="stat-label">Com Estoque</div></div>
    <div class="stat-card"><div class="stat-number" id="statSold">0</div><div class="stat-label">Vendidos 30d</div></div>
    <div class="stat-card"><div class="stat-number" id="statRevenue">R$ 0</div><div class="stat-label">Receita 30d</div></div>
    <div class="stat-card"><div class="stat-number" id="statOutOfStock">0</div><div class="stat-label">Sem Estoque</div></div>
    <div class="stat-card"><div class="stat-number" id="statTotalUnits">0</div><div class="stat-label">Unidades Disponíveis</div></div>
    <div class="stat-card" style="background: linear-gradient(135deg, #28a745, #20c997);"><div class="stat-number" id="statCompleto">0</div><div class="stat-label">🟢 Pode Atender Completo</div></div>
    <div class="stat-card" style="background: linear-gradient(135deg, #ffc107, #fd7e14);"><div class="stat-number" id="statParcial">0</div><div class="stat-label">🟡 Atendimento Parcial</div></div>
    <div class="stat-card" style="background: linear-gradient(135deg, #dc3545, #e83e8c);"><div class="stat-number" id="statDeficit">0</div><div class="stat-label">🔴 Déficit Crítico</div></div>
  </div>

  <div class="card">
    <div class="tabs">
      <div class="tab active" onclick="showTab(event, 'inventory')">📦 Inventário</div>
      <div class="tab" onclick="showTab(event, 'performance')">📈 Performance</div>
      <div class="tab" onclick="showTab(event, 'sales')">💰 Top Vendas</div>
    </div>

    <div id="inventory-tab" class="tab-content active">
      <table>
        <thead><tr>
          <th>Imagem</th><th>Título</th><th>SKU</th>
          <th>Inventory ID</th><th>Disponível</th><th>Total</th><th>Vendas 30d</th><th>Receita 30d</th><th>Pode Atender</th><th>Status Reposição</th>
        </tr></thead>
        <tbody id="inventoryTbody"></tbody>
      </table>
    </div>

    <div id="performance-tab" class="tab-content">
      <table>
        <thead><tr>
          <th>Produto</th><th>Estoque</th><th>Vendas 30d</th><th>Receita 30d</th><th>Status</th><th>Dias de Estoque</th>
        </tr></thead>
        <tbody id="performanceTbody"></tbody>
      </table>
    </div>

    <div id="sales-tab" class="tab-content">
      <table>
        <thead><tr>
          <th>Imagem</th><th>Produto</th><th>Unidades 30d</th><th>Receita 30d</th><th>Preço Médio</th><th>Estoque Atual</th>
        </tr></thead>
        <tbody id="salesTbody"></tbody>
      </table>
    </div>
  </div>

<script>
let timer = null;
function pct(a,b) { return Math.round(100*a/Math.max(1,b)); }

async function syncAll(){
  document.getElementById('btnSyncAll').disabled = true;
  document.getElementById('status').textContent = 'Sincronização completa iniciada...';
  try { await fetch('/api/sync-all', {method:'POST'}); timer = setInterval(updateProgress, 1000); }
  catch(e) { document.getElementById('status').textContent = 'Erro ao iniciar sincronização'; document.getElementById('btnSyncAll').disabled = false; }
}
async function syncInventory(){
  document.getElementById('btnSyncAll').disabled = true;
  document.getElementById('status').textContent = 'Sincronizando inventário...';
  try { await fetch('/api/sync-inventory', {method:'POST'}); timer = setInterval(updateProgress, 1000); }
  catch(e) { document.getElementById('status').textContent = 'Erro ao sincronizar inventário'; document.getElementById('btnSyncAll').disabled = false; }
}
async function syncSales(){
  document.getElementById('btnSyncAll').disabled = true;
  document.getElementById('status').textContent = 'Sincronizando vendas...';
  try { await fetch('/api/sync-sales', {method:'POST'}); timer = setInterval(updateProgress, 1000); }
  catch(e) { document.getElementById('status').textContent = 'Erro ao sincronizar vendas'; document.getElementById('btnSyncAll').disabled = false; }
}

async function updateProgress(){
  const st = await fetch('/api/progress').then(r=>r.json());
  const line = document.getElementById('progressText');
  const fill = document.getElementById('progressBar');
  let msg = st.ultimo_msg || 'Processando...';
  if (st.fase === 'scan_ids') { msg = `📋 ${msg} (${st.ids_coletados} itens)`; fill.style.width = '10%'; }
  else if (st.fase === 'multi_items') { msg = `📦 ${msg} (${st.lotes_items_ok}/${st.lotes_items_total})`; fill.style.width = (pct(st.lotes_items_ok, st.lotes_items_total)*0.2 + 10) + '%'; }
  else if (st.fase === 'estoque_full') { msg = `📊 ${msg} (${st.inventories_ok}/${st.inventories_total})`; fill.style.width = (pct(st.inventories_ok, st.inventories_total)*0.3 + 30) + '%'; }
  else if (st.fase === 'sales_orders') { msg = `💰 ${msg} (${st.sales_orders_processed})`; fill.style.width = '70%'; }
  else if (['sales_shipments','sales_items','sales_save'].includes(st.fase)) { msg = `💰 ${msg}`; fill.style.width = '85%'; }
  else if (['finalizado','inventario_concluido','vendas_concluido'].includes(st.fase)) {
    fill.style.width = '100%'; document.getElementById('btnSyncAll').disabled = false; document.getElementById('status').textContent = 'Sincronização concluída';
    if (timer) { clearInterval(timer); timer = null; } loadData();
  } else if (st.fase === 'erro') { document.getElementById('btnSyncAll').disabled = false; document.getElementById('status').textContent = 'Erro na sincronização'; line.classList.add('err'); }
  line.innerHTML = msg;
}
function showTab(event, tabName) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById(tabName + '-tab').classList.add('active');
  event.target.classList.add('active');
}
async function loadData(){
  const [items, performance, sales, stats, indicadores] = await Promise.all([
    fetch('/api/items').then(r=>r.json()),
    fetch('/api/performance').then(r=>r.json()),
    fetch('/api/top-sales').then(r=>r.json()),
    fetch('/api/stats').then(r=>r.json()),
    fetch('/api/indicadores-reposicao').then(r=>r.json())
  ]);
  document.getElementById('statsContainer').style.display = 'grid';
  document.getElementById('statTotal').textContent = stats.total_items || 0;
  document.getElementById('statAvailable').textContent = stats.items_with_stock || 0;
  document.getElementById('statSold').textContent = stats.total_units_sold || 0;
  document.getElementById('statRevenue').textContent = 'R$ ' + ((stats.total_revenue || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2}));
  document.getElementById('statOutOfStock').textContent = stats.items_out_of_stock || 0;
  document.getElementById('statTotalUnits').textContent = stats.total_available_units || 0;
  if (indicadores.status === 'sucesso') {
    document.getElementById('statCompleto').textContent = indicadores.resumo.completo || 0;
    document.getElementById('statParcial').textContent = indicadores.resumo.parcial || 0;
    document.getElementById('statDeficit').textContent = indicadores.resumo.deficit || 0;
  }
  const inventoryTb = document.getElementById('inventoryTbody');
  inventoryTb.innerHTML = '';
  const itemsToShow = indicadores.status === 'sucesso' ? indicadores.items : items;
  for (const r of itemsToShow.slice(0, 200)){
    const tr = document.createElement('tr');
    const tdImg = document.createElement('td'); const img = document.createElement('img'); img.className='thumb'; img.src = r.thumbnail_url || ''; tdImg.appendChild(img);
    const tdTitle= document.createElement('td'); tdTitle.textContent = (r.title||'').substring(0, 50) + '...';
    const tdSku  = document.createElement('td'); tdSku.textContent = r.sku||'';
    const tdInv  = document.createElement('td'); tdInv.innerHTML = r.inventory_id ? `<code class="badge">${r.inventory_id}</code>` : '';
    const tdAvail= document.createElement('td'); tdAvail.innerHTML = `<span class="qty">${r.available_quantity ?? ''}</span>`;
    const tdTotal= document.createElement('td'); tdTotal.textContent = (r.total_quantity ?? '');
    const tdSales= document.createElement('td'); tdSales.innerHTML = `<span class="qty">${r.vendas_30d || r.units_sold_30d || 0}</span>`;
    const tdRevenue = document.createElement('td'); tdRevenue.textContent = 'R$ ' + ((r.revenue_30d || 0).toFixed(2));
    const tdPodeAtender = document.createElement('td');
    if (r.pode_atender !== undefined) {
      const podeAtender = r.pode_atender; const necessidade = r.necessidade_envio || 0;
      if (podeAtender >= necessidade && necessidade > 0) tdPodeAtender.innerHTML = `<span class="qty" style="color:#28a745;font-weight:bold;">${podeAtender}</span>`;
      else if (podeAtender > 0 && necessidade > 0) tdPodeAtender.innerHTML = `<span class="qty" style="color:#ffc107;font-weight:bold;">${podeAtender}</span>`;
      else if (necessidade > 0) tdPodeAtender.innerHTML = `<span class="qty" style="color:#dc3545;font-weight:bold;">0</span>`;
      else tdPodeAtender.innerHTML = `<span class="qty" style="color:#6c757d;">${podeAtender || 0}</span>`;
    } else tdPodeAtender.innerHTML = '<span class="qty" style="color:#6c757d;">-</span>';
    const tdStatus = document.createElement('td');
    if (r.status && r.status_text) {
      const tooltip = `Necessidade: ${r.necessidade_envio || 0} | Estoque WMS: ${r.estoque_wms || 0}`;
      tdStatus.innerHTML = `<span class="status-badge status-${r.status} tooltip" data-tooltip="${tooltip}">${r.status_icon} ${r.status_text}</span>`;
    } else {
      tdStatus.innerHTML = '<span class="status-badge status-sem-necessidade">⚪ Sem dados</span>';
    }
    tr.appendChild(tdImg); tr.appendChild(tdTitle); tr.appendChild(tdSku); tr.appendChild(tdInv); tr.appendChild(tdAvail);
    tr.appendChild(tdTotal); tr.appendChild(tdSales); tr.appendChild(tdRevenue); tr.appendChild(tdPodeAtender); tr.appendChild(tdStatus);
    inventoryTb.appendChild(tr);
  }
  const performanceTb = document.getElementById('performanceTbody'); performanceTb.innerHTML = '';
  for (const r of performance.slice(0, 100)){
    const tr = document.createElement('tr');
    const tdProduct = document.createElement('td'); tdProduct.innerHTML = `<strong>${(r.title||'').substring(0, 40)}...</strong><br><small>${r.sku||''}</small>`;
    const tdStock = document.createElement('td'); tdStock.innerHTML = `<span class="qty">${r.available_quantity ?? 0}</span>`;
    const tdSales = document.createElement('td'); tdSales.innerHTML = `<span class="qty">${r.units_sold_30d || 0}</span>`;
    const tdRevenue = document.createElement('td'); tdRevenue.textContent = 'R$ ' + ((r.revenue_30d || 0).toFixed(2));
    const tdStatus = document.createElement('td');
    const statusClass = r.stock_status === 'healthy' ? 'healthy' : r.stock_status === 'low_stock' ? 'low-stock' : r.stock_status === 'out_of_stock' ? 'out-of-stock' : 'no-sales';
    const statusText  = r.stock_status === 'healthy' ? 'Saudável' : r.stock_status === 'low_stock' ? 'Estoque Baixo' : r.stock_status === 'out_of_stock' ? 'Sem Estoque' : 'Sem Vendas';
    tdStatus.innerHTML = `<span class="performance-indicator ${statusClass}">${statusText}</span>`;
    const tdDays = document.createElement('td'); tdDays.textContent = r.days_of_inventory ? r.days_of_inventory + ' dias' : '-';
    tr.appendChild(tdProduct); tr.appendChild(tdStock); tr.appendChild(tdSales); tr.appendChild(tdRevenue); tr.appendChild(tdStatus); tr.appendChild(tdDays);
    performanceTb.appendChild(tr);
  }
  const salesTb = document.getElementById('salesTbody'); salesTb.innerHTML = '';
  for (const r of sales.slice(0, 50)){
    const tr = document.createElement('tr');
    const tdImg = document.createElement('td'); const img = document.createElement('img'); img.className='thumb'; img.src = r.thumbnail_url || ''; tdImg.appendChild(img);
    const tdProduct = document.createElement('td'); tdProduct.innerHTML = `<strong>${(r.title||'').substring(0, 40)}...</strong><br><small>${r.sku||''}</small>`;
    const tdUnits = document.createElement('td'); tdUnits.innerHTML = `<span class="qty">${r.units_30d || 0}</span>`;
    const tdRevenue = document.createElement('td'); tdRevenue.textContent = 'R$ ' + ((r.gmv_30d || 0).toFixed(2));
    const tdAvgPrice = document.createElement('td'); tdAvgPrice.textContent = 'R$ ' + ((r.avg_selling_price || 0).toFixed(2));
    const tdStock = document.createElement('td'); const stockAlert = r.stock_alert || '';
    const stockClass = stockAlert.includes('URGENTE') ? 'out-of-stock' : stockAlert.includes('ATENÇÃO') ? 'low-stock' : 'healthy';
    tdStock.innerHTML = `<span class="qty">${r.available_quantity ?? 0}</span>` + (stockAlert !== 'OK' ? `<br><small class="performance-indicator ${stockClass}">${stockAlert}</small>` : '');
    tr.appendChild(tdImg); tr.appendChild(tdProduct); tr.appendChild(tdUnits); tr.appendChild(tdRevenue); tr.appendChild(tdAvgPrice); tr.appendChild(tdStock);
    salesTb.appendChild(tr);
  }
  document.getElementById('pillCounts').textContent = `${itemsToShow.length} itens`;
}
async function exportCSV(){ window.open('/api/csv', '_blank'); }
async function importCSV(){
  document.getElementById('status').textContent = 'Importando CSV...';
  try {
    const response = await fetch('/api/import-csv', {method:'POST'});
    const result = await response.json();
    if (result.status === 'sucesso') { document.getElementById('status').textContent = `CSV importado: ${result.produtos_importados} produtos, ${result.produtos_com_necessidade} com necessidade`; loadData(); }
    else { document.getElementById('status').textContent = `Erro na importação: ${result.erro}`; }
  } catch(e) { document.getElementById('status').textContent = 'Erro ao importar CSV'; }
}
async function mostrarPedidosTiny(){
  try {
    document.getElementById('status').textContent = 'Gerando Pedidos Tiny...';
    const resp = await fetch('/api/pedidos-tiny');
    const data = await resp.json();
    const card = document.getElementById('tinyCard');
    const tb = document.getElementById('tinyTbody');
    tb.innerHTML = '';
    if (data.status === 'sucesso' && (data.itens || []).length){
      for (const it of data.itens){
        const tr = document.createElement('tr');
        const tdInv = document.createElement('td'); tdInv.textContent = it.inventory_id;
        const tdQtt = document.createElement('td'); tdQtt.innerHTML = `<span class="qty">${it.quantidade_enviar}</span>`;
        tr.appendChild(tdInv); tr.appendChild(tdQtt); tb.appendChild(tr);
      }
      card.style.display = 'block';
      document.getElementById('status').textContent = `Pedidos Tiny: ${data.total} itens`;
    } else {
      card.style.display = 'block';
      const tr = document.createElement('tr');
      const td = document.createElement('td'); td.colSpan = 2; td.textContent = data.mensagem || 'Nenhum item';
      tr.appendChild(td); tb.appendChild(tr);
      document.getElementById('status').textContent = 'Sem itens para enviar';
    }
  } catch(e){
    document.getElementById('status').textContent = 'Erro ao gerar Pedidos Tiny';
  }
}
window.onload = () => { loadData(); };
</script>
</body>
</html>
"""

def background_sync_all():
    try:
        set_progress(fase="iniciando", ultimo_msg="Preparando sincronização paralela...")
        csv_path = os.path.join(OUT_DIR, "inventory_sales_full.csv")
        if os.path.exists(csv_path) and CSV_IMPORT_AVAILABLE:
            set_progress(fase="csv_import", ultimo_msg="Importando dados do CSV...")
            db = get_db_session()
            importer = CSVImporter(db)
            csv_result = importer.importar_csv(csv_path)
            if csv_result.get('status') == 'sucesso':
                set_progress(fase="csv_import", ultimo_msg=f"CSV: {csv_result['produtos_importados']} produtos importados")
            else:
                set_progress(fase="csv_import", ultimo_msg=f"CSV: {csv_result.get('erro', 'Erro desconhecido')}")
            db.close()
        result = full_sync_paralelo()
        set_progress(fase="finalizado", ultimo_msg="Sincronização paralela finalizada")
    except Exception as e:
        set_progress(fase="erro", ultimo_msg=f"Falhou: {type(e).__name__} — {str(e)[:180]}")

def background_sync_inventory():
    try:
        set_progress(fase="iniciando", ultimo_msg="Preparando sincronização de inventário...")
        result = collect_and_save_inventory()
        set_progress(fase="inventario_concluido", ultimo_msg=f"Inventário: {result['items_saved']} itens salvos")
    except Exception as e:
        set_progress(fase="erro", ultimo_msg=f"Falhou: {type(e).__name__} — {str(e)[:180]}")

def background_sync_sales():
    try:
        set_progress(fase="iniciando", ultimo_msg="Preparando sincronização de vendas...")
        result = collect_and_save_sales()
        set_progress(fase="vendas_concluido", ultimo_msg=f"Vendas: {result['full_orders_found']} pedidos FULL")
    except Exception as e:
        set_progress(fase="erro", ultimo_msg=f"Falhou: {type(e).__name__} — {str(e)[:180]}")

# ----------------------- Flask Routes ---------------------------
@app.route("/")
def index():
    missing = []
    for k in ("ML_USER_ID","ML_APP_ID","ML_CLIENT_SECRET","ML_REFRESH_TOKEN"):
        if not os.getenv(k): missing.append(k)
    banner = "Pronto para sincronizar"
    if missing:
        banner = f"❌ Variáveis faltando no .env: {', '.join(missing)}"
    else:
        banner = f"✅ Ambiente OK — User {ML_USER_ID}"
    return Response(INDEX_HTML.replace("Pronto para sincronizar", banner), mimetype="text/html")

@app.route("/api/sync-all", methods=["POST"])
def api_sync_all():
    th = threading.Thread(target=background_sync_all, daemon=True)
    th.start()
    return jsonify({"ok": True})

@app.route("/api/sync-inventory", methods=["POST"])
def api_sync_inventory():
    th = threading.Thread(target=background_sync_inventory, daemon=True)
    th.start()
    return jsonify({"ok": True})

@app.route("/api/sync-sales", methods=["POST"])
def api_sync_sales():
    th = threading.Thread(target=background_sync_sales, daemon=True)
    th.start()
    return jsonify({"ok": True})

@app.route("/api/progress")
def api_progress():
    with progress_lock:
        return jsonify(progress)

@app.route("/api/items")
def api_items():
    db = get_db_session()
    try:
        query = text("""
        SELECT 
            ii.item_id,
            ii.variation_id,
            ii.inventory_id,
            ii.title,
            ii.sku,
            ii.thumbnail_url,
            ist.available_quantity,
            ist.total_quantity,
            ist.not_available_quantity,
            ist.last_updated,
            COALESCE(ss.units_30d, 0) as units_sold_30d,
            COALESCE(ss.gmv_30d, 0) as revenue_30d
        FROM inventory_items ii
        LEFT JOIN inventory_stock ist ON ii.inventory_id = ist.inventory_id
        LEFT JOIN sales_summary ss ON ii.item_id = ss.item_id 
            AND (ii.variation_id = ss.variation_id OR (ii.variation_id IS NULL AND ss.variation_id IS NULL))
        WHERE ii.is_full = 1
        ORDER BY COALESCE(ss.units_30d, 0) DESC, ii.title
        """)
        result = db.execute(query)
        items = []
        for row in result:
            items.append({
                "item_id": row[0] or "",
                "variation_id": row[1],
                "inventory_id": row[2] or "",
                "title": row[3] or "",
                "sku": row[4] or "",
                "thumbnail_url": row[5] or "",
                "available_quantity": row[6],
                "total_quantity": row[7],
                "not_available_quantity": row[8],
                "last_updated": row[9].isoformat() if row[9] is not None else None,
                "units_sold_30d": row[10],
                "revenue_30d": float(row[11]) if row[11] else 0.0
            })
        return jsonify(items)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/api/performance")
def api_performance():
    db = get_db_session()
    try:
        query = text("""
        SELECT 
            ii.item_id,
            ii.title,
            ii.sku,
            ii.thumbnail_url,
            ist.available_quantity,
            COALESCE(ss.units_30d, 0) as units_sold_30d,
            COALESCE(ss.gmv_30d, 0) as revenue_30d,
            CASE 
                WHEN ist.available_quantity > 0 AND COALESCE(ss.units_30d, 0) > 0 
                THEN ROUND(1.0 * ist.available_quantity / NULLIF(ss.units_30d, 0), 1)
                ELSE NULL 
            END as days_of_inventory,
            CASE 
                WHEN COALESCE(ss.units_30d, 0) = 0 THEN 'no_sales'
                WHEN ist.available_quantity = 0 THEN 'out_of_stock'
                WHEN ist.available_quantity < (ss.units_30d / 30.0 * 7) THEN 'low_stock'
                ELSE 'healthy'
            END as stock_status
        FROM inventory_items ii
        LEFT JOIN inventory_stock ist ON ii.inventory_id = ist.inventory_id
        LEFT JOIN sales_summary ss ON ii.item_id = ss.item_id 
            AND (ii.variation_id = ss.variation_id OR (ii.variation_id IS NULL AND ss.variation_id IS NULL))
        WHERE ii.is_full = 1
        ORDER BY 
            CASE 
                WHEN COALESCE(ss.units_30d, 0) = 0 THEN 3
                WHEN ist.available_quantity = 0 THEN 1
                WHEN ist.available_quantity < (ss.units_30d / 30.0 * 7) THEN 2
                ELSE 4
            END,
            COALESCE(ss.units_30d, 0) DESC
        """)
        result = db.execute(query)
        items = []
        for row in result:
            items.append({
                "item_id": row[0],
                "title": row[1],
                "sku": row[2],
                "thumbnail_url": row[3],
                "available_quantity": row[4],
                "units_sold_30d": row[5],
                "revenue_30d": float(row[6]) if row[6] else 0.0,
                "days_of_inventory": float(row[7]) if row[7] else None,
                "stock_status": row[8]
            })
        return jsonify(items)
    finally:
        db.close()

@app.route("/api/top-sales")
def api_top_sales():
    db = get_db_session()
    try:
        query = text("""
        SELECT 
            ii.item_id,
            ii.title,
            ii.sku,
            ii.thumbnail_url,
            ss.inventory_id,
            ss.units_30d,
            ss.gmv_30d,
            ist.available_quantity,
            ROUND(ss.gmv_30d / NULLIF(ss.units_30d, 0), 2) as avg_selling_price,
            CASE 
                WHEN ist.available_quantity = 0 THEN 'URGENTE: Sem estoque'
                WHEN ist.available_quantity < (ss.units_30d / 30.0 * 7) THEN 'ATENÇÃO: Estoque baixo'
                ELSE 'OK'
            END as stock_alert
        FROM sales_summary ss
        JOIN inventory_items ii ON ss.item_id = ii.item_id 
            AND (ss.variation_id = ii.variation_id OR (ss.variation_id IS NULL AND ii.variation_id IS NULL))
        LEFT JOIN inventory_stock ist ON ss.inventory_id = ist.inventory_id
        WHERE ss.units_30d > 0
        ORDER BY ss.units_30d DESC, ss.gmv_30d DESC
        """)
        result = db.execute(query)
        items = []
        for row in result:
            items.append({
                "item_id": row[0],
                "title": row[1],
                "sku": row[2],
                "thumbnail_url": row[3],
                "inventory_id": row[4],
                "units_30d": row[5],
                "gmv_30d": float(row[6]) if row[6] else 0.0,
                "available_quantity": row[7],
                "avg_selling_price": float(row[8]) if row[8] else 0.0,
                "stock_alert": row[9]
            })
        return jsonify(items)
    finally:
        db.close()

@app.route("/api/stats")
def api_stats():
    db = get_db_session()
    try:
        query = text("""
        SELECT 
            COUNT(*) as total_items,
            COUNT(CASE WHEN ist.available_quantity > 0 THEN 1 END) as items_with_stock,
            COUNT(CASE WHEN ist.available_quantity = 0 THEN 1 END) as items_out_of_stock,
            COUNT(CASE WHEN ist.available_quantity IS NULL THEN 1 END) as items_no_data,
            COUNT(CASE WHEN ist.available_quantity < 5 AND ist.available_quantity > 0 THEN 1 END) as items_low_stock,
            COALESCE(SUM(ist.available_quantity), 0) as total_available_units,
            COALESCE(SUM(ist.total_quantity), 0) as total_units,
            COALESCE(SUM(ss.units_30d), 0) as total_units_sold,
            COALESCE(SUM(ss.gmv_30d), 0) as total_revenue,
            COUNT(CASE WHEN ss.units_30d > 0 THEN 1 END) as items_with_sales
        FROM inventory_items ii
        LEFT JOIN inventory_stock ist ON ii.inventory_id = ist.inventory_id
        LEFT JOIN sales_summary ss ON ii.item_id = ss.item_id 
            AND (ii.variation_id = ss.variation_id OR (ii.variation_id IS NULL AND ss.variation_id IS NULL))
        WHERE ii.is_full = 1
        """)
        result = db.execute(query).fetchone()
        return jsonify({
            "total_items": result[0],
            "items_with_stock": result[1],
            "items_out_of_stock": result[2],
            "items_no_data": result[3],
            "items_low_stock": result[4],
            "total_available_units": result[5],
            "total_units": result[6],
            "total_units_sold": result[7],
            "total_revenue": float(result[8]) if result[8] else 0.0,
            "items_with_sales": result[9]
        })
    finally:
        db.close()

@app.route("/api/csv")
def api_csv():
    path = os.path.join(OUT_DIR, "inventory_sales_full.csv")
    if not os.path.exists(path):
        db = get_db_session()
        try:
            export_to_csv(db)
        finally:
            db.close()
    if os.path.exists(path):
        return send_file(path, as_attachment=True, download_name="inventory_sales_full.csv")
    else:
        return jsonify({"error":"CSV não pôde ser gerado"}), 404

# ==================== WMS ====================
@app.route("/api/wms/status")
def api_wms_status():
    db = get_db_session()
    try:
        result = db.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='wms_stock'")).fetchone()
        if not result:
            return jsonify({'status': 'não_configurado','tabela_existe': False,'total_produtos': 0,'sucessos': 0,'erros': 0,'ultima_atualizacao': None})
        stats = db.execute(text("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'sucesso' THEN 1 ELSE 0 END) as sucessos,
                   SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) as erros,
                   MAX(last_updated) as ultima_atualizacao
            FROM wms_stock
        """)).fetchone()
        return jsonify({
            'status': 'ativo','tabela_existe': True,
            'total_produtos': stats[0] or 0,'sucessos': stats[1] or 0,'erros': stats[2] or 0,
            'ultima_atualizacao': stats[3],
            'taxa_sucesso': round((stats[1] or 0) / max(stats[0] or 1, 1) * 100, 1)
        })
    except Exception as e:
        return jsonify({'status': 'erro','erro': str(e)})
    finally:
        db.close()

@app.route("/api/wms/produtos")
def api_wms_produtos():
    db = get_db_session()
    try:
        result = db.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='wms_stock'"))
        if not result.fetchone():
            return jsonify({'erro': 'Tabela WMS não encontrada. Execute sync primeiro.'})
        query = text("""
            SELECT ws.sku, ws.disponivel, ws.status, ws.last_updated, ii.title, ii.inventory_id
            FROM wms_stock ws
            LEFT JOIN inventory_items ii ON ws.sku = ii.sku
            WHERE ws.disponivel > 0
            ORDER BY ws.disponivel DESC
            LIMIT 50
        """)
        result = db.execute(query)
        produtos = []
        for row in result:
            produtos.append({
                'sku': row[0],'estoque_wms': row[1],'status': row[2],
                'ultima_atualizacao': row[3],'titulo': row[4] or 'N/A','inventory_id': row[5] or 'N/A'
            })
        return jsonify({'produtos': produtos,'total_com_estoque': len(produtos)})
    except Exception as e:
        return jsonify({'erro': str(e)})
    finally:
        db.close()

@app.route("/api/wms/teste-conexao", methods=['POST'])
def api_wms_teste_conexao():
    try:
        if not WMS_AVAILABLE:
            return jsonify({'status': 'erro', 'erro': 'Módulo WMS não disponível'})
        if not WMS_API_KEY:
            return jsonify({'status': 'erro','erro': 'Credenciais WMS não configuradas no .env'})
        wms_client = WMSSmartGO(WMS_API_KEY)
        resultado = wms_client.consultar_estoque_produto("TESTE123")
        if (isinstance(resultado, dict) and (resultado.get('status') == 'sucesso' or 'HTTP 404' in str(resultado.get('erro', '')))):
            return jsonify({'status': 'sucesso','mensagem': 'Conexão WMS funcionando','url_testada': SMARTGO_BASE_URL,'resposta_teste': resultado})
        else:
            return jsonify({'status': 'erro','erro': f"Falha na conexão: {resultado.get('erro', 'erro desconhecido')}",'url_testada': SMARTGO_BASE_URL})
    except Exception as e:
        return jsonify({'status': 'erro','erro': str(e)})

@app.route("/api/wms/full-stock", methods=['GET'])
def api_wms_full_stock():
    try:
        if not WMS_AVAILABLE or not WMS_API_KEY:
            return jsonify({'status': 'erro','erro': 'API Key do WMS não configurada ou módulo indisponível'})
        wms_client = WMSSmartGO(WMS_API_KEY)
        produtos = wms_client.buscar_todos_produtos(delay_entre_paginas=0.3)
        if not produtos:
            return jsonify({'status': 'sucesso','total_produtos': 0,'produtos': [],'mensagem': 'Nenhum produto encontrado no WMS'})
        stats = wms_client.gerar_relatorio_estatisticas(produtos)
        return jsonify({'status': 'sucesso','total_produtos': len(produtos),'estatisticas': stats,'produtos': produtos[:100],'mensagem': f'Encontrados {len(produtos)} produtos no WMS'})
    except Exception as e:
        return jsonify({'status': 'erro','erro': str(e)})

@app.route("/api/wms/product/<codigo_produto>", methods=['GET'])
def api_wms_product(codigo_produto):
    try:
        if not WMS_AVAILABLE or not WMS_API_KEY:
            return jsonify({'status': 'erro','erro': 'API Key do WMS não configurada ou módulo indisponível'})
        wms_client = WMSSmartGO(WMS_API_KEY)
        produtos = wms_client.consultar_por_codigo_universal(codigo_produto)
        if not produtos:
            return jsonify({'status': 'sucesso','encontrado': False,'produto': None,'mensagem': f'Produto {codigo_produto} não encontrado no WMS'})
        return jsonify({'status': 'sucesso','encontrado': True,'produto': produtos[0],'total_encontrados': len(produtos),'mensagem': f'Produto {codigo_produto} encontrado no WMS'})
    except Exception as e:
        return jsonify({'status': 'erro','erro': str(e)})

@app.route("/api/wms/stats", methods=['GET'])
def api_wms_stats():
    try:
        if not WMS_AVAILABLE or not WMS_API_KEY:
            return jsonify({'status': 'erro','erro': 'API Key do WMS não configurada ou módulo indisponível'})
        wms_client = WMSSmartGO(WMS_API_KEY)
        produtos = wms_client.buscar_todos_produtos(delay_entre_paginas=0.3)
        if not produtos:
            return jsonify({'status': 'sucesso','estatisticas': {'total_produtos': 0,'total_disponivel': 0,'total_expedicao': 0,'total_geral': 0,'data_consulta': datetime.now().strftime("%Y-%m-%d %H:%M:%S")},'mensagem': 'Nenhum produto encontrado no WMS'})
        stats = wms_client.gerar_relatorio_estatisticas(produtos)
        return jsonify({'status': 'sucesso','estatisticas': stats,'mensagem': f'Estatísticas geradas para {len(produtos)} produtos'})
    except Exception as e:
        return jsonify({'status': 'erro','erro': str(e)})

# ==================== Google Sheets & Reposição (opcional) ====================
@app.route("/api/sheets/composicao", methods=['GET'])
def api_sheets_composicao():
    if not SHEETS_AVAILABLE:
        return jsonify({'status':'erro','erro':'Módulo Google Sheets não disponível'})
    if not GOOGLE_SHEETS_URL:
        return jsonify({'status': 'erro','erro': 'URL do Google Sheets não configurada'})
    try:
        sheets_client = GoogleSheetsClient()
        resultado = sheets_client.ler_planilha_composicao(GOOGLE_SHEETS_URL)
        if resultado.get('status') != 'sucesso':
            return jsonify(resultado)
        relatorio = sheets_client.gerar_relatorio_composicao(resultado['anuncios'])
        return jsonify({'status':'sucesso','composicao':resultado,'relatorio':relatorio,'mensagem': f'Analisados {resultado["total_anuncios"]} anúncios'})
    except Exception as e:
        return jsonify({'status':'erro','erro': str(e)})

@app.route("/api/analise-reposicao-completa", methods=['GET'])
def api_analise_reposicao_completa():
    if not (WMS_AVAILABLE and SHEETS_AVAILABLE and REPO_AVAILABLE):
        return jsonify({'status':'erro','erro':'Módulos de WMS/Sheets/Reposição não disponíveis'})
    if not WMS_API_KEY or not GOOGLE_SHEETS_URL:
        return jsonify({'status':'erro','erro':'Credenciais/URL ausentes'})
    try:
        db = get_db_session()
        try:
            query = text("""
                SELECT inventory_id, sku, title, available_quantity, vendas_consolidadas, necessidade_envio
                FROM items 
                WHERE necessidade_envio > 0
                ORDER BY necessidade_envio DESC
            """)
            result = db.execute(query)
            dados_ml = []
            for row in result:
                dados_ml.append({
                    'inventory_id': row.inventory_id,
                    'sku': row.sku,
                    'title': row.title,
                    'available_quantity': row.available_quantity,
                    'vendas_consolidadas': row.vendas_consolidadas,
                    'necessidade_envio': row.necessidade_envio
                })
        finally:
            db.close()
        if not dados_ml:
            return jsonify({'status':'sucesso','mensagem':'Nenhum item com necessidade de envio encontrado','total_itens':0})
        analyzer = ReposicaoAnalyzer(WMS_API_KEY, GOOGLE_SHEETS_URL)
        resultado = analyzer.analisar_reposicao_completa(dados_ml)
        resultado['total_itens_ml'] = len(dados_ml)
        resultado['url_sheets'] = GOOGLE_SHEETS_URL
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'status':'erro','erro': str(e)})

@app.route("/api/reposicao/por-ean/<ean>", methods=['GET'])
def api_reposicao_por_ean(ean):
    if not (WMS_AVAILABLE and SHEETS_AVAILABLE):
        return jsonify({'status':'erro','erro':'Módulos WMS/Sheets não disponíveis'})
    if not WMS_API_KEY or not GOOGLE_SHEETS_URL:
        return jsonify({'status':'erro','erro':'Credenciais/URL ausentes'})
    try:
        sheets_client = GoogleSheetsClient()
        dados_sheets = sheets_client.ler_planilha_composicao(GOOGLE_SHEETS_URL)
        if dados_sheets.get('status') != 'sucesso':
            return jsonify({'status':'erro','erro': f'Erro ao ler Google Sheets: {dados_sheets.get("erro")}'})
        anuncios_relacionados = []
        for inventory_id, anuncio in dados_sheets['anuncios'].items():
            for ean_data in getattr(anuncio, 'eans', []):
                if ean_data.get('ean') == ean:
                    anuncios_relacionados.append({
                        'inventory_id': inventory_id,
                        'quantidade_por_anuncio': ean_data.get('quantidade', 0),
                        'tipo_anuncio': getattr(anuncio, 'tipo', 'desconhecido')
                    })
                    break
        if not anuncios_relacionados:
            return jsonify({'status':'sucesso','encontrado': False,'ean': ean,'mensagem': f'EAN {ean} não encontrado em nenhum anúncio'})
        wms_client = WMSSmartGO(WMS_API_KEY)
        produtos_wms = wms_client.consultar_por_codigo_universal(ean)
        estoque_wms = 0
        if produtos_wms:
            p0 = produtos_wms[0] if isinstance(produtos_wms, list) else produtos_wms
            estoque_wms = p0.get('quantidadeDisponivel', 0) or p0.get('quantidade_disponivel', 0) or 0
        return jsonify({'status':'sucesso','encontrado': True,'ean': ean,'estoque_wms': estoque_wms,
                        'anuncios_relacionados': anuncios_relacionados,'total_anuncios': len(anuncios_relacionados),
                        'mensagem': f'EAN {ean} encontrado em {len(anuncios_relacionados)} anúncio(s)'})
    except Exception as e:
        return jsonify({'status':'erro','erro': str(e)})

@app.route("/api/indicadores-reposicao", methods=['GET'])
def api_indicadores_reposicao():
    db = get_db_session()
    try:
        # Se existir a tabela consolidada `items`, usa ela.
        result = db.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='items'"))
        if not result.fetchone():
            # Senão, cai no fallback (calcula a necessidade com as tabelas base)
            return api_indicadores_reposicao_fallback(db)

        query = text("""
            SELECT inventory_id, sku, title, available_quantity, total_quantity, vendas_consolidadas, necessidade_envio
            FROM items
            ORDER BY necessidade_envio DESC
        """)
        result = db.execute(query)

        items = []
        completo_count = parcial_count = deficit_count = 0
        wms_client = WMSSmartGO(WMS_API_KEY) if WMS_AVAILABLE and WMS_API_KEY else None

        for row in result:
            item_data = {
                'item_id': row[0],
                'title': row[2],
                'sku': row[1],
                'inventory_id': row[0],
                'thumbnail_url': '',
                'available_quantity': row[3] or 0,
                'total_quantity': row[4] or 0,
                'vendas_30d': row[5] or 0,
                'necessidade_envio': row[6] or 0
            }

            # Estoque WMS (se disponível)
            estoque_wms = item_data['available_quantity']
            if item_data['necessidade_envio'] > 0 and wms_client:
                try:
                    if item_data['sku']:
                        resultado_wms = wms_client.consultar_por_codigo_universal(item_data['sku'])
                    else:
                        resultado_wms = wms_client.consultar_por_codigo_universal(item_data['inventory_id'])
                    if isinstance(resultado_wms, list) and resultado_wms:
                        estoque_wms = (resultado_wms[0].get('quantidadeDisponivel', 0)
                                       or resultado_wms[0].get('quantidade_disponivel', 0)
                                       or estoque_wms)
                    elif isinstance(resultado_wms, dict):
                        estoque_wms = (resultado_wms.get('quantidadeDisponivel', 0)
                                       or resultado_wms.get('quantidade_disponivel', 0)
                                       or estoque_wms)
                except Exception:
                    pass

            pode_atender = min(item_data['necessidade_envio'], estoque_wms) if item_data['necessidade_envio'] > 0 else estoque_wms
            item_data['estoque_wms'] = estoque_wms
            item_data['pode_atender'] = pode_atender

            # Status pra UI
            if item_data['necessidade_envio'] == 0:
                status, status_text, status_icon = 'sem_necessidade', 'Sem necessidade', '⚪'
            elif estoque_wms >= item_data['necessidade_envio']:
                status, status_text, status_icon = 'completo', 'Pode atender completo', '🟢'; completo_count += 1
            elif estoque_wms > 0:
                status, status_text, status_icon = 'parcial', f'Parcial ({pode_atender}/{item_data["necessidade_envio"]})', '🟡'; parcial_count += 1
            else:
                status, status_text, status_icon = 'deficit', 'Déficit crítico', '🔴'; deficit_count += 1

            item_data.update({'status': status, 'status_text': status_text, 'status_icon': status_icon})
            items.append(item_data)

        resumo = {
            'completo': completo_count,
            'parcial': parcial_count,
            'deficit': deficit_count,
            'total_com_necessidade': completo_count + parcial_count + deficit_count
        }

        return jsonify({
            'status':'sucesso',
            'resumo':resumo,
            'items':items,
            'total_items':len(items),
            'mensagem': f'Análise de {len(items)} itens concluída'
        })
    except Exception as e:
        return jsonify({'status':'erro','erro': str(e)})
    finally:
        db.close()

    
@app.route("/api/pedidos-tiny", methods=['GET'])
def api_pedidos_tiny():
    """
    Retorna uma lista de {inventory_id, quantidade_enviar} para gerar 'Pedidos Tiny'.
    quantidade_enviar = min(necessidade_envio, estoque_wms)
    """
    db = get_db_session()
    try:
        # Verifica se existe a tabela consolidada "items"
        has_items = db.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='items'"
        )).fetchone()

        registros = []
        if has_items:
            # Usa necessidade_envio já consolidada
            query = text("""
                SELECT inventory_id, sku, title,
                       COALESCE(necessidade_envio, 0) as necessidade_envio
                FROM items
                WHERE COALESCE(necessidade_envio, 0) > 0
                ORDER BY necessidade_envio DESC
            """)
            result = db.execute(query)
            for row in result:
                registros.append({
                    "inventory_id": row[0],
                    "sku": row[1] or "",
                    "title": row[2] or "",
                    "necessidade_envio": int(row[3] or 0)
                })
        else:
            # Fallback: calcula necessidade igual ao indicadores_reposicao_fallback
            query = text("""
                SELECT 
                    ii.inventory_id,
                    ii.sku,
                    ii.title,
                    COALESCE(CASE 
                        WHEN ss.units_30d > COALESCE(ist.available_quantity,0) 
                        THEN ss.units_30d - COALESCE(ist.available_quantity,0)
                        ELSE 0 END, 0) as necessidade_envio
                FROM inventory_items ii
                LEFT JOIN inventory_stock ist ON ii.inventory_id = ist.inventory_id
                LEFT JOIN sales_summary ss    ON ii.item_id = ss.item_id
                     AND (ii.variation_id = ss.variation_id OR (ii.variation_id IS NULL AND ss.variation_id IS NULL))
                WHERE ii.is_full = 1
                ORDER BY necessidade_envio DESC
            """)
            result = db.execute(query)
            for row in result:
                if (row[3] or 0) > 0:
                    registros.append({
                        "inventory_id": row[0], "sku": row[1] or "",
                        "title": row[2] or "", "necessidade_envio": int(row[3] or 0)
                    })

        # Se não houver necessidade, retornamos vazio
        if not registros:
            return jsonify({"status":"sucesso","total":0,"itens":[],"mensagem":"Nenhum item com necessidade de envio"})

        # Consulta WMS (se disponível) para calcular 'pode_atender'
        wms_client = WMSSmartGO(WMS_API_KEY) if WMS_AVAILABLE and WMS_API_KEY else None
        saida = []
        for r in registros:
            necessidade = int(r["necessidade_envio"] or 0)
            estoque_wms = 0
            if necessidade > 0 and wms_client:
                try:
                    codigo = r["sku"] or r["inventory_id"]
                    resp = wms_client.consultar_por_codigo_universal(codigo)
                    if isinstance(resp, list) and resp:
                        estoque_wms = (resp[0].get('quantidadeDisponivel', 0)
                                       or resp[0].get('quantidade_disponivel', 0) or 0)
                    elif isinstance(resp, dict):
                        estoque_wms = (resp.get('quantidadeDisponivel', 0)
                                       or resp.get('quantidade_disponivel', 0) or 0)
                except Exception:
                    estoque_wms = 0
            quantidade_enviar = min(necessidade, int(estoque_wms or 0))
            if quantidade_enviar > 0:
                saida.append({
                    "inventory_id": r["inventory_id"],
                    "quantidade_enviar": int(quantidade_enviar)
                })

        saida.sort(key=lambda x: x["quantidade_enviar"], reverse=True)
        return jsonify({"status":"sucesso","total":len(saida),"itens":saida})
    except Exception as e:
        return jsonify({"status":"erro","erro":str(e)})
    finally:
        db.close()


def api_indicadores_reposicao_fallback(db):
    try:
        query = text("""
            SELECT 
                ii.item_id, ii.title, ii.sku, ii.inventory_id, ii.thumbnail_url,
                ist.available_quantity, ist.total_quantity,
                COALESCE(ss.units_30d,0) as vendas_30d,
                COALESCE(CASE WHEN ss.units_30d > ist.available_quantity THEN ss.units_30d - ist.available_quantity ELSE 0 END, 0) as necessidade_envio
            FROM inventory_items ii
            LEFT JOIN inventory_stock ist ON ii.inventory_id = ist.inventory_id
            LEFT JOIN sales_summary ss ON ii.item_id = ss.item_id 
                 AND (ii.variation_id = ss.variation_id OR (ii.variation_id IS NULL AND ss.variation_id IS NULL))
            WHERE ii.is_full = true
            ORDER BY necessidade_envio DESC
        """)
        result = db.execute(query)
        items = []
        completo_count = parcial_count = deficit_count = 0
        wms_client = WMSSmartGO(WMS_API_KEY) if WMS_AVAILABLE and WMS_API_KEY else None
        for row in result:
            item_data = {
                'item_id': row[0], 'title': row[1], 'sku': row[2], 'inventory_id': row[3], 'thumbnail_url': row[4],
                'available_quantity': row[5] or 0, 'total_quantity': row[6] or 0, 'vendas_30d': row[7] or 0, 'necessidade_envio': row[8] or 0
            }
            estoque_wms = 0
            if item_data['necessidade_envio'] > 0 and wms_client:
                try:
                    code = item_data['sku'] or item_data['inventory_id']
                    resultado_wms = wms_client.consultar_por_codigo_universal(code)
                    if isinstance(resultado_wms, list) and resultado_wms:
                        estoque_wms = (resultado_wms[0].get('quantidadeDisponivel', 0) or resultado_wms[0].get('quantidade_disponivel', 0) or 0)
                    elif isinstance(resultado_wms, dict):
                        estoque_wms = (resultado_wms.get('quantidadeDisponivel', 0) or resultado_wms.get('quantidade_disponivel', 0) or 0)
                except Exception:
                    estoque_wms = 0
            pode_atender = min(item_data['necessidade_envio'], estoque_wms) if item_data['necessidade_envio'] > 0 else 0
            item_data['estoque_wms'] = estoque_wms
            item_data['pode_atender'] = pode_atender
            if item_data['necessidade_envio'] == 0:
                status, status_text, status_icon = 'sem_necessidade','Sem necessidade','⚪'
            elif estoque_wms >= item_data['necessidade_envio']:
                status, status_text, status_icon = 'completo','Pode atender completo','🟢'; completo_count += 1
            elif estoque_wms > 0:
                status, status_text, status_icon = 'parcial',f'Parcial ({pode_atender}/{item_data["necessidade_envio"]})','🟡'; parcial_count += 1
            else:
                status, status_text, status_icon = 'deficit','Déficit crítico','🔴'; deficit_count += 1
            item_data.update({'status': status, 'status_text': status_text, 'status_icon': status_icon})
            items.append(item_data)
        resumo = {'completo': completo_count, 'parcial': parcial_count, 'deficit': deficit_count,
                  'total_com_necessidade': completo_count + parcial_count + deficit_count}
        return jsonify({'status':'sucesso','resumo':resumo,'items':items,'total_items':len(items),
                        'mensagem': f'Análise de {len(items)} itens concluída'})
    except Exception as e:
        return jsonify({'status':'erro','erro': str(e)})

# ----------------------- Import CSV automático -------------------
@app.route("/api/import-csv", methods=['POST'])
def api_import_csv():
    try:
        if not CSV_IMPORT_AVAILABLE:
            return jsonify({'status': 'erro','erro': 'Importer não disponível'})
        upload_dir = os.getenv("CSV_UPLOAD_DIR", os.path.join(OUT_DIR, "upload"))
        os.makedirs(upload_dir, exist_ok=True)
        csv_files = [f for f in os.listdir(upload_dir) if f.endswith('.csv') and ('ml_inventory_sales_unified' in f or 'inventory_sales_full' in f)]
        if not csv_files:
            return jsonify({'status': 'erro','erro': 'Nenhum arquivo CSV encontrado no diretório upload'})
        csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(upload_dir, x)), reverse=True)
        latest_csv = os.path.join(upload_dir, csv_files[0])
        db = get_db_session()
        try:
            importer = CSVImporter(db)
            resultado = importer.importar_csv(latest_csv)
            if resultado.get('status') == 'sucesso':
                verificacao = importer.verificar_dados()
                return jsonify({'status': 'sucesso','arquivo_importado': csv_files[0],
                                'produtos_importados': resultado['produtos_importados'],
                                'produtos_com_necessidade': resultado['produtos_com_necessidade'],
                                'verificacao': verificacao,'mensagem': f'CSV {csv_files[0]} importado com sucesso'})
            else:
                return jsonify({'status': 'erro','erro': resultado.get('erro', 'Erro desconhecido na importação')})
        finally:
            db.close()
    except Exception as e:
        return jsonify({'status': 'erro','erro': str(e)})

# ----------------------- Main ---------------------------
def main():
    print("🚀 ML INVENTORY & SALES - Sistema Unificado (corrigido)")
    print("="*80)
    print(f"👤 User ID: {ML_USER_ID or '???'}")
    print(f"🗄️  Database: {DATABASE_URL}")
    print(f"🔗 URL: http://localhost:5010\n")
    create_database()
    print("✅ Banco de dados inicializado")
    app.run(host="0.0.0.0", port=5010, debug=False)

if __name__ == "__main__":
    if not ML_USER_ID:
        print("❌ Defina ML_USER_ID no seu .env.")
        sys.exit(1)
    main()
