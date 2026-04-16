"""
db.py — SQLite ORM for Order Management System
Tables: orders, order_items, audit_logs, sync_logs
"""
import sqlite3
import json
from datetime import datetime, timedelta
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oms.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id          TEXT PRIMARY KEY,
            source_channel    TEXT DEFAULT 'SHOPIFY',
            customer_name     TEXT,
            customer_phone    TEXT,
            address_line1     TEXT,
            address_line2     TEXT,
            address_line3     TEXT,
            city              TEXT,
            state_code        TEXT,
            pincode           TEXT,
            total_amount      REAL DEFAULT 0,
            is_cod            INTEGER DEFAULT 1,
            status            TEXT DEFAULT 'NEW',
            fulfillment_channel TEXT,
            tracking_company  TEXT,
            tracking_number   TEXT,
            tracking_url      TEXT,
            shopify_fulfilled INTEGER DEFAULT 0,
            row_number        INTEGER DEFAULT 0,
            seller_sku        TEXT,
            title             TEXT,
            qty               INTEGER DEFAULT 1,
            created_at        TEXT,
            updated_at        TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id   TEXT,
            seller_sku TEXT,
            title      TEXT,
            quantity   INTEGER DEFAULT 1,
            price      REAL DEFAULT 0,
            FOREIGN KEY(order_id) REFERENCES orders(order_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id   TEXT,
            old_status TEXT,
            new_status TEXT,
            actor      TEXT DEFAULT 'SYSTEM',
            reason     TEXT,
            timestamp  TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY(order_id) REFERENCES orders(order_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sync_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            status     TEXT,
            details    TEXT,
            timestamp  TEXT DEFAULT (datetime('now','localtime'))
        )
    """)

    conn.commit()

    # Schema migrations — add columns that may not exist in older DB
    _migrate(conn)

    conn.close()


def _migrate(conn):
    """Add any missing columns for backward compatibility."""
    c = conn.cursor()
    migrations = [
        ("orders", "shopify_fulfilled", "INTEGER DEFAULT 0"),
        ("orders", "address_line3",     "TEXT"),
        ("orders", "seller_sku",        "TEXT"),
        ("orders", "title",             "TEXT"),
        ("orders", "qty",               "INTEGER DEFAULT 1"),
        ("orders", "row_number",        "INTEGER DEFAULT 0"),
    ]
    for table, col, col_def in migrations:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
        except Exception:
            pass  # column already exists
    conn.commit()


# ─────────────────────────────────────────────
# AUDIT & SYNC LOGGING
# ─────────────────────────────────────────────
def log_audit(order_id, old_status, new_status, actor="SYSTEM", reason="", existing_conn=None):
    conn = existing_conn or get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO audit_logs (order_id, old_status, new_status, actor, reason) VALUES (?, ?, ?, ?, ?)",
        (order_id, old_status, new_status, actor, reason),
    )
    if not existing_conn:
        conn.commit()
        conn.close()


def log_sync(event_type, status, details=""):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO sync_logs (event_type, status, details) VALUES (?, ?, ?)",
        (event_type, status, str(details)[:500]),
    )
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# ORDER CRUD
# ─────────────────────────────────────────────
def save_order(order_data):
    """Insert new order (skip if already exists)."""
    conn = get_connection()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("SELECT order_id FROM orders WHERE order_id = ?", (order_data["order_id"],))
    if c.fetchone():
        conn.close()
        return False  # already exists

    is_cod = 1 if "cod" in str(order_data.get("is_cod", "")).lower() else 0

    c.execute("""
        INSERT INTO orders (
            order_id, source_channel, customer_name, customer_phone,
            address_line1, address_line2, address_line3,
            city, state_code, pincode,
            total_amount, is_cod, status,
            row_number, seller_sku, title, qty,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        order_data["order_id"],
        order_data.get("source_channel", "SHOPIFY"),
        order_data.get("customer", ""),
        order_data.get("phone", ""),
        order_data.get("addr_line1", ""),
        order_data.get("addr_line2", ""),
        order_data.get("addr_line3", ""),
        order_data.get("city", ""),
        order_data.get("state_code", ""),
        order_data.get("pincode", ""),
        float(order_data.get("amount", 0) or 0),
        is_cod,
        "NEW",
        order_data.get("row_number", 0),
        order_data.get("seller_sku", ""),
        order_data.get("title", ""),
        int(order_data.get("qty", 1) or 1),
        order_data.get("date", now),
        now,
    ))

    # Insert items if provided
    for item in order_data.get("items", []):
        c.execute(
            "INSERT INTO order_items (order_id, seller_sku, title, quantity, price) VALUES (?, ?, ?, ?, ?)",
            (
                order_data["order_id"],
                item.get("seller_sku", item.get("sellerSku", "")),
                item.get("title", ""),
                int(item.get("quantity", 1)),
                float(item.get("price", 0) or 0),
            ),
        )

    log_audit(order_data["order_id"], None, "NEW", "SYSTEM", "Order synced from source", existing_conn=conn)
    conn.commit()
    conn.close()
    return True


def update_order_status(order_id, new_status, actor="SYSTEM", reason="", fulfillment_channel=None):
    """Update order status and log the transition."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT status FROM orders WHERE order_id = ?", (order_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return

    old_status = row["status"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updates = ["status = ?", "updated_at = ?"]
    params = [new_status, now]

    if fulfillment_channel:
        updates.append("fulfillment_channel = ?")
        params.append(fulfillment_channel)

    params.append(order_id)
    c.execute(f"UPDATE orders SET {', '.join(updates)} WHERE order_id = ?", params)
    log_audit(order_id, old_status, new_status, actor, reason, existing_conn=conn)
    conn.commit()
    conn.close()


def update_order_tracking(order_id, tracking_company, tracking_number, tracking_url=""):
    """Attach tracking info and move order to SHIPPED."""
    conn = get_connection()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        UPDATE orders
        SET tracking_company = ?, tracking_number = ?, tracking_url = ?,
            status = 'SHIPPED', updated_at = ?
        WHERE order_id = ?
    """, (tracking_company, tracking_number, tracking_url, now, order_id))
    log_audit(order_id, "PROCESSING", "SHIPPED", "SYSTEM", f"Tracking: {tracking_number}", existing_conn=conn)
    conn.commit()
    conn.close()


def mark_shopify_fulfilled(order_id):
    """Mark order as Shopify-fulfilled."""
    conn = get_connection()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE orders SET shopify_fulfilled = 1, updated_at = ? WHERE order_id = ?", (now, order_id))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# QUERIES
# ─────────────────────────────────────────────
def get_orders(filters=None):
    """Fetch orders with optional dict filters. Returns list of dicts."""
    conn = get_connection()
    c = conn.cursor()
    query = "SELECT * FROM orders WHERE 1=1"
    params = []
    if filters:
        for k, v in filters.items():
            query += f" AND {k} = ?"
            params.append(v)
    query += " ORDER BY created_at DESC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_orders_filtered(
    status=None,
    channel=None,
    date_from=None,
    date_to=None,
    search=None,
    limit=500,
):
    """Advanced filtered order query for Reports page."""
    conn = get_connection()
    c = conn.cursor()
    query = "SELECT * FROM orders WHERE 1=1"
    params = []

    if status and status != "All":
        if isinstance(status, list):
            placeholders = ",".join("?" * len(status))
            query += f" AND status IN ({placeholders})"
            params.extend(status)
        else:
            query += " AND status = ?"
            params.append(status)

    if channel and channel != "All":
        if isinstance(channel, list):
            placeholders = ",".join("?" * len(channel))
            query += f" AND fulfillment_channel IN ({placeholders})"
            params.extend(channel)
        else:
            query += " AND fulfillment_channel = ?"
            params.append(channel)

    if date_from:
        query += " AND created_at >= ?"
        params.append(str(date_from))

    if date_to:
        query += " AND created_at <= ?"
        params.append(str(date_to) + " 23:59:59")

    if search:
        query += " AND (order_id LIKE ? OR customer_name LIKE ? OR tracking_number LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s])

    query += f" ORDER BY created_at DESC LIMIT {int(limit)}"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_order_history(order_id):
    """Get audit trail for one order."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM audit_logs WHERE order_id = ? ORDER BY timestamp ASC", (order_id,))
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_sync_logs(limit=100):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM sync_logs ORDER BY timestamp DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_phones():
    """Fetch all customer phones to check for repeated customers."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT DISTINCT customer_phone FROM orders WHERE customer_phone IS NOT NULL AND customer_phone != ''")
    rows = c.fetchall()
    conn.close()
    return {r["customer_phone"] for r in rows}


def get_stats():
    """Return dashboard statistics dict."""
    conn = get_connection()
    c = conn.cursor()

    def scalar(sql, params=()):
        c.execute(sql, params)
        row = c.fetchone()
        return row[0] if row and row[0] is not None else 0

    today = datetime.now().strftime("%Y-%m-%d")
    week_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    month_start = datetime.now().strftime("%Y-%m-01")

    stats = {
        "total":           scalar("SELECT COUNT(*) FROM orders"),
        "pending":         scalar("SELECT COUNT(*) FROM orders WHERE status = 'NEW'"),
        "processing":      scalar("SELECT COUNT(*) FROM orders WHERE status = 'PROCESSING'"),
        "shipped":         scalar("SELECT COUNT(*) FROM orders WHERE status = 'SHIPPED'"),
        "fulfilled":       scalar("SELECT COUNT(*) FROM orders WHERE status IN ('SHIPPED','DELIVERED') OR shopify_fulfilled = 1"),
        "failed":          scalar("SELECT COUNT(*) FROM orders WHERE status = 'FAILED'"),
        "mcf_count":       scalar("SELECT COUNT(*) FROM orders WHERE fulfillment_channel = 'MCF'"),
        "delhivery_count": scalar("SELECT COUNT(*) FROM orders WHERE fulfillment_channel = 'DELHIVERY'"),
        "cod_count":       scalar("SELECT COUNT(*) FROM orders WHERE is_cod = 1"),
        "prepaid_count":   scalar("SELECT COUNT(*) FROM orders WHERE is_cod = 0"),
        "with_tracking":   scalar("SELECT COUNT(*) FROM orders WHERE tracking_number IS NOT NULL AND tracking_number != ''"),
        "without_tracking":scalar("SELECT COUNT(*) FROM orders WHERE (tracking_number IS NULL OR tracking_number = '') AND status = 'PROCESSING'"),
        "shopify_fulfilled":scalar("SELECT COUNT(*) FROM orders WHERE shopify_fulfilled = 1"),
        "today_orders":    scalar("SELECT COUNT(*) FROM orders WHERE created_at LIKE ?", (today + "%",)),
        "week_orders":     scalar("SELECT COUNT(*) FROM orders WHERE created_at >= ?", (week_start,)),
        "month_orders":    scalar("SELECT COUNT(*) FROM orders WHERE created_at >= ?", (month_start,)),
    }

    # Average processing time (created_at → shipped)
    c.execute("""
        SELECT AVG(
            (julianday(updated_at) - julianday(created_at)) * 24
        ) FROM orders WHERE status = 'SHIPPED'
    """)
    row = c.fetchone()
    stats["avg_processing_hours"] = round(row[0], 1) if row and row[0] else 0

    conn.close()
    return stats


# Init on import
init_db()
