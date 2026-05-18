"""
del.label.py — Delhivery AWB -> Order ID -> Google Sheet -> 4x6 PDF label

Goal:
  - You paste Delhivery AWB (waybill) number(s)
  - Script fetches Order ID from Delhivery packing slip
  - Script checks the Google Sheet directly (no `python-delhlivrey.onrender.com` timeout)
  - Script generates proper 4x6 inch label PDF(s)

Setup:
  1) `secret.txt` must contain: `DELHIVERY_API_KEY=...`
  2) `hide.json` must be a Google service-account key, and that service account email
     must be shared on the Google Sheet.

Run:
  - Single AWB:  python del.label.py --awb 5279921000891
  - Multiple:    python del.label.py --awb 5279... --awb 5279...
  - Interactive: python del.label.py
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import requests


SENDER_NAME = "WONDERCARE"
SENDER_ADDRESS = "B06-11 Emaar Gomti Greens Lucknow"
SENDER_GST = "09ACFPO7516C1ZN"

# Default return address (if Delhivery packing slip doesn't send one)
DEFAULT_RETURN_ADDRESS = (
    "B06/10 Emaar Gomti Greens, Arjunganj, Lucknow, Uttar Pradesh, India, 226010"
)

# Your provided Google Sheet:
SHEET_ID = "1OvtzHInl8viaLG6f2ZLG3u5h6YQpfID2UAbI64cYhF4"
SHEET_TAB = "Sheet1"
CREDS_FILE = "hide.json"
SECRET_FILE = "secret.txt"

DELHIVERY_BASE_URL = "https://track.delhivery.com"


def _read_kv_file(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except Exception:
        pass
        
    try:
        import streamlit as st
        for k, v in st.secrets.items():
            if k not in out and isinstance(v, str):
                out[k] = v
    except Exception:
        pass
        
    return out


def load_delhivery_token() -> List[str]:
    secret_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), SECRET_FILE)
    secrets = _read_kv_file(secret_path)
    tokens = [
        secrets.get("DELHIVERY_API_KEY", "").strip(),
        secrets.get("DELHIVERY_API_KEY2", "").strip()
    ]
    tokens = [t for t in tokens if t]
    if not tokens:
        raise RuntimeError(f"DELHIVERY_API_KEY missing in {SECRET_FILE} and st.secrets")
    return tokens


def normalize_order_id(v: str) -> str:
    s = (v or "").strip()
    if s.startswith("#"):
        s = s[1:]
    return s.strip()


def normalize_tracking(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    digits = re.sub(r"\D+", "", s)
    return digits if digits else s


def normalize_header(h: str) -> str:
    s = (h or "").strip().lower()
    s = re.sub(r"[\s\-_]+", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def is_digits(v: str) -> bool:
    s = normalize_order_id(v)
    return bool(s) and s.isdigit()


def to_float(v: str) -> float:
    s = (v or "").strip()
    if not s:
        return 0.0
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def split_awbs(text: str) -> List[str]:
    parts = re.split(r"[,\s]+", (text or "").strip())
    return [p.strip() for p in parts if p.strip()]


def fetch_packing_slip(session: requests.Session, tokens: List[str], awb: str) -> Optional[dict]:
    url = f"{DELHIVERY_BASE_URL}/api/p/packing_slip?wbns={awb}"
    
    for token in tokens:
        headers = {
            "Authorization": f"Token {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        for attempt in range(1, 4):
            try:
                resp = session.get(url, headers=headers, timeout=(10, 90))
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        return data
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(0.6 * attempt)
                    continue
                break # Not 200 and not retryable, move to next token
            except requests.exceptions.Timeout:
                time.sleep(0.8 * attempt)
            except Exception:
                break # Move to next token
                
    return None


def extract_first_package(packing_data: dict) -> dict:
    if not isinstance(packing_data, dict):
        return {}
    packages = packing_data.get("packages")
    if isinstance(packages, list) and packages:
        first = packages[0]
        return first if isinstance(first, dict) else {}
    return packing_data


@dataclass(frozen=True)
class SheetIndex:
    headers: List[str]
    rows: List[List[str]]  # data rows (without header)
    order_col: int
    index: Dict[str, List[str]]  # normalized order id -> row
    tracking_col: Optional[int]
    tracking_index: Dict[str, List[str]]  # normalized tracking/awb -> row


def _choose_order_id_column(headers: List[str], rows: List[List[str]]) -> int:
    # Candidates by header names (handles duplicate "Ord" columns)
    synonyms = {
        "orderid",
        "order",
        "ord",
        "orderno",
        "ordernumber",
    }
    candidates = [i for i, h in enumerate(headers) if normalize_header(h) in synonyms]
    if not candidates:
        return 0

    def score_col(idx: int) -> Tuple[int, float]:
        lens: List[int] = []
        for r in rows[:250]:
            if idx >= len(r):
                continue
            v = r[idx].strip()
            if is_digits(v):
                lens.append(len(normalize_order_id(v)))
        if not lens:
            return (10_000, 10_000.0)
        lens.sort()
        median = int(lens[len(lens) // 2])
        avg = float(sum(lens) / len(lens))
        return (median, avg)

    best = candidates[0]
    best_score = score_col(best)
    for idx in candidates[1:]:
        sc = score_col(idx)
        if sc < best_score:
            best, best_score = idx, sc
    return best


def load_sheet_index(sheet_id: str, tab: str, creds_file: str) -> SheetIndex:
    try:
        import gspread
    except Exception as e:
        raise RuntimeError(
            "Missing dependency: gspread. Install with: pip install gspread google-auth"
        ) from e

    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), creds_file)
    
    try:
        import streamlit as st
        # First try to see if we are in Streamlit and have secrets
        try:
            creds_dict = st.secrets["gcp_service_account"]
            gc = gspread.service_account_from_dict(dict(creds_dict))
        except Exception:
            # Fallback to file
            if not os.path.exists(creds_path):
                raise FileNotFoundError(f"{creds_file} not found and st.secrets missing: {creds_path}")
            gc = gspread.service_account(filename=creds_path)
            
        ws = gc.open_by_key(sheet_id).worksheet(tab)
    except Exception as e:
        print(f"[WARN] Could not connect to Google Sheet: {e}")
        return SheetIndex([], [], 0, {}, None, {})

    values = ws.get_all_values()
    if not values or len(values) < 2:
        raise RuntimeError(f"Sheet '{tab}' is empty or missing header row.")

    headers = values[0]
    rows = values[1:]
    order_col = _choose_order_id_column(headers, rows)

    index: Dict[str, List[str]] = {}
    for r in rows:
        if order_col >= len(r):
            continue
        oid = normalize_order_id(r[order_col])
        if not oid:
            continue
        index[oid] = r

    tracking_synonyms = {
        "tracking",
        "trackingn",
        "trackingno",
        "trackingnumber",
        "trackingid",
        "trackingidawb",
        "awb",
        "awbno",
        "waybill",
        "waybillno",
        "trackingm",  # seen in your sheet screenshot
    }
    tracking_col: Optional[int] = None
    for i, h in enumerate(headers):
        if normalize_header(h) in tracking_synonyms:
            tracking_col = i
            break

    tracking_index: Dict[str, List[str]] = {}
    if tracking_col is not None:
        for r in rows:
            if tracking_col >= len(r):
                continue
            tv = normalize_tracking(r[tracking_col])
            if not tv:
                continue
            tracking_index[tv] = r

    return SheetIndex(
        headers=headers,
        rows=rows,
        order_col=order_col,
        index=index,
        tracking_col=tracking_col,
        tracking_index=tracking_index,
    )


def extract_order_id_from_pkg(pkg: dict, packing_data: Optional[dict]) -> str:
    for k in ("oid", "order_id", "orderid", "order", "refnum", "ref", "reference_no"):
        if k in (pkg or {}) and str(pkg.get(k) or "").strip():
            return normalize_order_id(str(pkg.get(k)))
    if isinstance(packing_data, dict):
        for k in ("oid", "order_id", "orderid", "order"):
            if k in packing_data and str(packing_data.get(k) or "").strip():
                return normalize_order_id(str(packing_data.get(k)))
    return ""


def find_col(headers: Sequence[str], names: Sequence[str]) -> Optional[int]:
    want = {normalize_header(n) for n in names}
    for i, h in enumerate(headers):
        if normalize_header(h) in want:
            return i
    return None


def row_get(row: Sequence[str], idx: Optional[int]) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return (row[idx] or "").strip()


def parse_sku_qty_pairs(sku_cell: str, qty_cell: str) -> Tuple[List[Tuple[str, int]], int]:
    """Split comma-separated SKUs and quantities (same rules as app.py _process_orders)."""
    skus = [s.strip() for s in str(sku_cell or "").split(",") if s.strip()]
    raw_qty = str(qty_cell or "1").strip()
    qtys_str = [q.strip() for q in raw_qty.split(",") if q.strip()]

    if not skus:
        return [], 1

    if len(qtys_str) == 1 and len(skus) > 1:
        q0 = int(qtys_str[0]) if qtys_str[0].isdigit() else 1
        qtys = [q0] * len(skus)
    else:
        qtys = [int(q) if q.isdigit() else 1 for q in qtys_str]

    while len(qtys) < len(skus):
        qtys.append(1)

    pairs = list(zip(skus, qtys))
    total = sum(q for _, q in pairs)
    return pairs, total


def build_pkg_from_sheet_and_delhivery(
    pkg: dict, order_row: List[str], headers: List[str]
) -> Tuple[dict, str, str]:
    # Pull fields from the sheet with tolerance for header naming
    i_name = find_col(headers, ["Customer Name", "Customer", "Name"])
    i_phone = find_col(headers, ["Phone Number", "Phone Num", "Phone", "PhoneNum"])
    i_addr1 = find_col(headers, ["Address 1", "Address1"])
    i_addr2 = find_col(headers, ["Adressh 2", "Address 2", "Address2"])
    i_pin = find_col(headers, ["Pin code", "Pincode", "Pin"])
    i_city = find_col(headers, ["city", "City"])
    i_state = find_col(headers, ["Stat", "State", "st"])
    i_cod = find_col(headers, ["isCodRequired", "COD", "Payment"])
    i_amount = find_col(headers, ["Amount", "Amo", "Total", "Price"])
    i_sku = find_col(headers, ["sellerSku", "SellerSku", "SKU", "Sku"])
    i_title = find_col(headers, ["Title", "Product", "Item"])
    i_qty = find_col(headers, ["quantity", "qty", "qua", "Quantity"])

    cust_name = row_get(order_row, i_name)
    phone = row_get(order_row, i_phone)
    addr1 = row_get(order_row, i_addr1)
    addr2 = row_get(order_row, i_addr2)
    pin = row_get(order_row, i_pin)
    city = row_get(order_row, i_city)
    state = row_get(order_row, i_state)
    iscod = row_get(order_row, i_cod).lower()
    amount = to_float(row_get(order_row, i_amount))
    sku = row_get(order_row, i_sku)
    title = row_get(order_row, i_title)
    qty = row_get(order_row, i_qty) or "1"

    pairs, total_units = parse_sku_qty_pairs(sku, qty)
    sku_detail_lines: List[str] = []
    for i, (sk, q) in enumerate(pairs):
        if sk:
            sku_detail_lines.append(f"{sk} x{q}")
    if not sku_detail_lines and sku:
        sku_detail_lines.append(f"{sku.strip()} x{qty or '1'}")

    new_pkg = dict(pkg or {})
    new_pkg["sku_detail_lines"] = sku_detail_lines
    new_pkg["qty"] = str(total_units if total_units else qty).strip() or "1"

    if cust_name:
        new_pkg["name"] = cust_name
    if phone:
        new_pkg["contact"] = phone
    combined_addr = " ".join([p for p in [addr1, addr2] if p]).strip()
    if combined_addr:
        new_pkg["address"] = combined_addr
    if pin:
        new_pkg["pin"] = pin
    if city:
        new_pkg["destination_city"] = city
    if state:
        new_pkg["st"] = state
    if city or state:
        new_pkg["destination"] = f"{city} ({state})".strip()

    new_pkg["rs"] = amount if amount else new_pkg.get("rs", 0)
    new_pkg["cod"] = int(amount) if ("cod" in iscod or iscod == "true") else 0

    if not new_pkg.get("client_gst_tin"):
        new_pkg["client_gst_tin"] = SENDER_GST
    if not new_pkg.get("radd"):
        new_pkg["radd"] = DEFAULT_RETURN_ADDRESS

    first_sku = pairs[0][0] if pairs else sku
    return new_pkg, first_sku, title


def wrap_text(text: str, max_chars: int) -> List[str]:
    words = str(text or "").split()
    if not words:
        return []
    lines: List[str] = []
    current = ""
    for w in words:
        if len(current) + len(w) + (1 if current else 0) <= max_chars:
            current = f"{current} {w}".strip()
        else:
            if current:
                lines.append(current)
            current = w
    if current:
        lines.append(current)
    return lines


def render_label_page(canvas, pkg: dict, awb: str, sku: str, title: str) -> None:
    try:
        from reportlab.lib import colors
        from reportlab.lib.units import inch, mm
        from reportlab.graphics.barcode import code128
    except Exception as e:
        raise RuntimeError(
            "Missing dependency: reportlab. Install with: pip install reportlab"
        ) from e

    wbn = str(pkg.get("wbn") or awb)
    oid = str(pkg.get("oid") or "")
    name = str(pkg.get("name") or "")
    address = str(pkg.get("address") or "")
    pin = str(pkg.get("pin") or "")
    contact = str(pkg.get("contact") or pkg.get("cnph") or "")

    dest_city = str(pkg.get("destination_city") or "")
    dest_state = str(pkg.get("st") or "")
    destination = str(pkg.get("destination") or f"{dest_city} ({dest_state})").strip()

    pt = str(pkg.get("pt") or "PPD")
    mot = str(pkg.get("mot") or "")
    mode_text = "Surface" if mot == "S" else "Express" if mot == "E" else (mot or "")

    product_desc = str(pkg.get("prd") or "")
    cod = to_float(str(pkg.get("cod") or "0"))
    rs = to_float(str(pkg.get("rs") or "0"))

    seller_gst = str(pkg.get("client_gst_tin") or SENDER_GST)
    radd = str(pkg.get("radd") or DEFAULT_RETURN_ADDRESS)

    cd = str(pkg.get("cd") or "")
    date_line1 = ""
    date_line2 = ""
    if cd:
        try:
            date_obj = datetime.fromisoformat(cd.replace("Z", "+00:00"))
            date_line1 = date_obj.strftime("%Y-%m-%d")
            date_line2 = date_obj.strftime("%H:%M:%S")
        except Exception:
            date_line1 = cd[:10] if len(cd) >= 10 else cd


    # Page setup: 4x6 inch
    page_w = 4 * inch
    page_h = 6 * inch
    mg = 6 * mm
    cw = page_w - 2 * mg
    cy = page_h - mg

    def draw_box(x, y, bw, bh):
        canvas.setStrokeColor(colors.black)
        canvas.setLineWidth(0.6)
        canvas.rect(x, y, bw, bh, stroke=True, fill=False)

    def draw_text(x, y, text, size=7, bold=False):
        font = "Helvetica-Bold" if bold else "Helvetica"
        canvas.setFont(font, size)
        canvas.setFillColor(colors.black)
        canvas.drawString(x, y, str(text or ""))

    # Row 1: AWB barcode
    r1h = 18 * mm
    r1y = cy - r1h
    draw_box(mg, r1y, cw, r1h)
    try:
        bc = code128.Code128(wbn, barWidth=0.75, barHeight=10 * mm)
        bc.drawOn(canvas, mg + (cw - bc.width) / 2, r1y + 6 * mm)
    except Exception:
        draw_text(mg + 20 * mm, r1y + 9 * mm, wbn, size=9, bold=True)
    canvas.setFont("Helvetica", 6)
    canvas.drawCentredString(mg + cw / 2, r1y + 2 * mm, wbn)
    cy = r1y

    # Row 2: Pincode + destination
    r2h = 6 * mm
    r2y = cy - r2h
    half = cw / 2
    draw_box(mg, r2y, half, r2h)
    draw_text(mg + 2 * mm, r2y + 1.8 * mm, pin, size=8, bold=True)
    draw_box(mg + half, r2y, half, r2h)
    dest_short = destination.split("(")[0].strip() if "(" in destination else (dest_city or destination)
    canvas.setFont("Helvetica-Bold", 7)
    canvas.drawRightString(mg + cw - 2 * mm, r2y + 1.8 * mm, dest_short[:28])
    cy = r2y

    # Row 3: Ship to + COD/mode/amount
    r3h = 28 * mm
    r3y = cy - r3h
    lw = cw * 0.65
    rw = cw * 0.35

    draw_box(mg, r3y, lw, r3h)
    ty = cy - 3 * mm
    draw_text(mg + 2 * mm, ty, "Ship To:", size=6)
    ty -= 4 * mm
    draw_text(mg + 2 * mm, ty, name[:32], size=9, bold=True)

    addr_lines = wrap_text(address, 38)
    for line in addr_lines[:3]:
        ty -= 3.2 * mm
        draw_text(mg + 2 * mm, ty, line, size=6)
    ty -= 3.2 * mm
    draw_text(mg + 2 * mm, ty, f"{dest_city} ({dest_state})".strip(), size=6)
    ty -= 3.2 * mm
    draw_text(mg + 2 * mm, ty, f"PIN: {pin}", size=7, bold=True)
    if contact:
        ty -= 3.2 * mm
        draw_text(mg + 2 * mm, ty, f"Ph: {contact}", size=6)

    rx = mg + lw
    draw_box(rx, r3y, rw, r3h)
    ry = cy - 5 * mm
    canvas.setFont("Helvetica-Bold", 12)
    canvas.drawCentredString(rx + rw / 2, ry, pt)
    ry -= 5 * mm
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawCentredString(rx + rw / 2, ry, mode_text)
    ry -= 6 * mm
    canvas.setFont("Helvetica-Bold", 10)
    amount = cod if cod else rs
    canvas.drawCentredString(rx + rw / 2, ry, f"INR {int(amount)}")
    cy = r3y

    # Row 4: Seller + date
    r4h = 20 * mm
    r4y = cy - r4h
    sw = cw * 0.65
    dw = cw * 0.35

    draw_box(mg, r4y, sw, r4h)
    sy = cy - 3.5 * mm
    draw_text(mg + 2 * mm, sy, f"Seller: {SENDER_NAME}", size=7, bold=True)
    sy -= 3.5 * mm
    draw_text(mg + 2 * mm, sy, "Address:", size=6, bold=True)
    sell_lines = wrap_text(SENDER_ADDRESS, 38)
    for line in sell_lines[:3]:
        sy -= 3 * mm
        draw_text(mg + 2 * mm, sy, line, size=5.5)
    if seller_gst:
        sy -= 3.5 * mm
        draw_text(mg + 2 * mm, sy, f"GST: {seller_gst}", size=6, bold=True)

    draw_box(mg + sw, r4y, dw, r4h)
    dy = cy - 4 * mm
    draw_text(mg + sw + 2 * mm, dy, "Date:", size=6, bold=True)
    dy -= 4 * mm
    draw_text(mg + sw + 2 * mm, dy, date_line1, size=7)
    dy -= 3.5 * mm
    draw_text(mg + sw + 2 * mm, dy, date_line2, size=7)
    cy = r4y

    # Row 5: Product header
    r5h = 5 * mm
    r5y = cy - r5h
    c1w = cw * 0.55
    c2w = cw * 0.22
    c3w = cw * 0.23

    draw_box(mg, r5y, c1w, r5h)
    draw_box(mg + c1w, r5y, c2w, r5h)
    draw_box(mg + c1w + c2w, r5y, c3w, r5h)
    draw_text(mg + 2 * mm, r5y + 1.5 * mm, "Product(Qty)", size=6, bold=True)
    draw_text(mg + c1w + 2 * mm, r5y + 1.5 * mm, "Price", size=6, bold=True)
    canvas.setFont("Helvetica-Bold", 6)
    canvas.drawRightString(mg + cw - 2 * mm, r5y + 1.5 * mm, "Total")
    cy = r5y

    # Row 6: Product lines (multi-SKU / multi-qty from sheet)
    prd_lines: List[str] = []
    sku_detail_lines = pkg.get("sku_detail_lines") or []
    if sku_detail_lines:
        for line in sku_detail_lines:
            prd_lines.extend(wrap_text(line, 32))
    elif sku:
        prd_lines.extend(wrap_text(f"SKU: {sku}", 32))
    if not prd_lines and product_desc:
        prd_lines.extend(wrap_text(product_desc, 32))
    if not prd_lines:
        prd_lines = [""]
        
    r6h = max(8 * mm, (len(prd_lines) + 1) * 3 * mm)
    r6y = cy - r6h
    draw_box(mg, r6y, c1w, r6h)
    draw_box(mg + c1w, r6y, c2w, r6h)
    draw_box(mg + c1w + c2w, r6y, c3w, r6h)

    py = cy - 3 * mm
    for line in prd_lines:
        draw_text(mg + 2 * mm, py, line, size=6)
        py -= 3 * mm

    draw_text(mg + c1w + 2 * mm, cy - 4 * mm, f"INR {int(rs)}", size=6)
    canvas.setFont("Helvetica", 6)
    canvas.drawRightString(mg + cw - 2 * mm, cy - 4 * mm, f"INR {int(rs)}")
    cy = r6y

    # Row 7: Total
    r7h = 5 * mm
    r7y = cy - r7h
    draw_box(mg, r7y, c1w, r7h)
    draw_box(mg + c1w, r7y, c2w, r7h)
    draw_box(mg + c1w + c2w, r7y, c3w, r7h)
    draw_text(mg + 2 * mm, r7y + 1.5 * mm, "Total", size=6, bold=True)
    draw_text(mg + c1w + 2 * mm, r7y + 1.5 * mm, f"INR {int(rs)}", size=6, bold=True)
    canvas.setFont("Helvetica-Bold", 6)
    canvas.drawRightString(mg + cw - 2 * mm, r7y + 1.5 * mm, f"INR {int(rs)}")
    cy = r7y

    # Row 8: Order ID barcode
    r8h = 18 * mm
    r8y = cy - r8h
    draw_box(mg, r8y, cw, r8h)
    oid_clean = normalize_order_id(oid)
    if oid_clean:
        try:
            bc2 = code128.Code128(oid_clean, barWidth=0.85, barHeight=9 * mm)
            bc2.drawOn(canvas, mg + (cw - bc2.width) / 2, r8y + 6 * mm)
        except Exception:
            pass
    canvas.setFont("Helvetica-Bold", 8)
    canvas.drawCentredString(mg + cw / 2, r8y + 2 * mm, f"#{oid_clean}" if oid_clean else "")
    cy = r8y

    # Row 9: Return address
    ret_lines = wrap_text(radd, 50) or [""]
    r9h = max(8 * mm, (len(ret_lines) + 1) * 3 * mm + 2 * mm)
    r9y = cy - r9h
    draw_box(mg, r9y, cw, r9h)
    ry2 = cy - 3 * mm
    draw_text(mg + 2 * mm, ry2, "Return Address:", size=6, bold=True)
    for line in ret_lines:
        ry2 -= 3 * mm
        draw_text(mg + 2 * mm, ry2, line, size=6)


def generate_labels(awbs: Sequence[str], out_path: Optional[str], sku_map: Optional[Dict[str, str]] = None) -> str:
    if sku_map is None:
        sku_map = {}
        
    session = requests.Session()

    sheet = load_sheet_index(SHEET_ID, SHEET_TAB, CREDS_FILE)
    order_col_name = sheet.headers[sheet.order_col] if sheet.headers else f"col {sheet.order_col+1}"
    tracking_col_name = (
        sheet.headers[sheet.tracking_col] if sheet.tracking_col is not None else ""
    )
    if tracking_col_name:
        print(
            f"[OK] Sheet loaded: {SHEET_ID} / {SHEET_TAB} (order column: '{order_col_name}', tracking column: '{tracking_col_name}')"
        )
    else:
        print(
            f"[OK] Sheet loaded: {SHEET_ID} / {SHEET_TAB} (order column: '{order_col_name}')"
        )

    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.units import inch
    except Exception as e:
        raise RuntimeError(
            "Missing dependency: reportlab. Install with: pip install reportlab"
        ) from e

    if not out_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = f"bulk_labels_{ts}.pdf" if len(awbs) > 1 else f"label_{awbs[0]}.pdf"

    page_w = 4 * inch
    page_h = 6 * inch
    c = rl_canvas.Canvas(out_path, pagesize=(page_w, page_h))

    # Load Delhivery tokens only if we actually need Delhivery API.
    delhivery_tokens: List[str] = []
    try:
        delhivery_tokens = load_delhivery_token()
    except Exception:
        delhivery_tokens = []

    ok = 0
    for i, awb in enumerate(awbs, start=1):
        print(f"\n[{i}/{len(awbs)}] AWB: {awb}")
        awb_key = normalize_tracking(awb)

        packing: Optional[dict] = None
        pkg: dict = {"wbn": awb}
        order_id = ""

        # Try Delhivery packing slip first (only if token exists)
        if delhivery_tokens:
            packing = fetch_packing_slip(session, delhivery_tokens, awb)
            if packing:
                pkg = extract_first_package(packing) or {"wbn": awb}
                order_id = extract_order_id_from_pkg(pkg, packing)
                if order_id:
                    print(f"  Order ID (Delhivery): {order_id}")
                else:
                    print("  [WARN] Order ID missing in packing slip.")

        # Fallback: find the order in sheet by tracking/awb number
        # Fallback: find the order in sheet by tracking/awb number
        row: Optional[List[str]] = None
        if not order_id and awb_key and sheet.tracking_index:
            row = sheet.tracking_index.get(awb_key)
            if row:
                order_id = normalize_order_id(row[sheet.order_col]) if sheet.order_col < len(row) else ""
                if order_id:
                    print(f"  Order ID (Sheet via tracking): {order_id}")
                    pkg["oid"] = order_id

        entered_manual_str = ""
        # If still no order id, ask user
        if not order_id:
            manual = input("  Enter Order ID or SKU manually (blank to skip): ").strip()
            if not manual:
                continue
            entered_manual_str = manual
            order_id = normalize_order_id(manual)
            pkg["oid"] = order_id

        # Find row by order id (primary)
        if row is None and order_id:
            row = sheet.index.get(order_id)

        # If row still not found, try tracking again (sometimes order-id mismatch)
        if not row and awb_key and sheet.tracking_index:
            row = sheet.tracking_index.get(awb_key)

        if not row:
            if entered_manual_str:
                print("  [WARN] Order not found in Google Sheet. Proceeding with Delhivery details.")
            else:
                print("  [ERR] Order not found in Google Sheet.")
                manual = input("  Enter Order ID or SKU (blank to skip): ").strip()
                if not manual:
                    continue
                entered_manual_str = manual
                row = sheet.index.get(normalize_order_id(manual))
                if not row:
                    print("  [WARN] Still not found in sheet. Proceeding with Delhivery details.")
                else:
                    order_id = normalize_order_id(manual)
                    pkg["oid"] = order_id

        pkg2, sku, title = build_pkg_from_sheet_and_delhivery(pkg, row or [], sheet.headers)
        
        # Override SKU if provided in sku_map or entered_manual_str
        if awb in sku_map:
            sku = sku_map[awb]
        elif not row and entered_manual_str:
            sku = entered_manual_str

        if not sku and not title:
            sku = input("  SKU not found. Enter SKU manually (blank to skip): ").strip()

        render_label_page(c, pkg2, awb, sku=sku, title=title)
        c.showPage()
        ok += 1

    if ok == 0:
        raise RuntimeError("No labels were generated.")

    c.save()
    return os.path.abspath(out_path)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--awb", action="append", default=[], help="Delhivery AWB / waybill number (repeatable)")
    p.add_argument("--sku_map", action="append", default=[], help="Format AWB=SKU")
    p.add_argument("--out", default="", help="Output PDF file path")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    awbs: List[str] = []
    for a in args.awb:
        awbs.extend(split_awbs(a))

    if not awbs:
        print("Enter AWB(s) one per line. Blank line to finish:")
        while True:
            try:
                line = input().strip()
            except EOFError:
                break
            if not line:
                break
            awbs.extend(split_awbs(line))

    if not awbs:
        print("[ERR] No AWB provided.")
        return 2

    sku_map_dict = {}
    for sm in args.sku_map:
        if "=" in sm:
            k, v = sm.split("=", 1)
            sku_map_dict[k.strip()] = v.strip()

    try:
        out = generate_labels(awbs, out_path=(args.out.strip() or None), sku_map=sku_map_dict)
        print(f"\n[DONE] PDF saved: {out}")
        return 0
    except Exception as e:
        print(f"\n[ERR] {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
