"""
Flipkart Seller API — increase listing price (single shot or 3-step gradual).

Docs: https://seller.flipkart.com/api-docs/listing-api-docs/LMAPIRef.html
"""
import base64
import json
import sys
import time
from typing import Any

import requests

# ── Toggle sandbox vs production ─────────────────────────────────────────────
USE_SANDBOX = False  # True = sandbox.flipkart.net, False = api.flipkart.net

# ── Credentials (fill before running) ────────────────────────────────────────
CLIENT_ID = "paste_your_client_id_here"
CLIENT_SECRET = "paste_your_client_secret_here"

# ── Product identifiers ───────────────────────────────────────────────────────
FLIPKART_SKU = "CU_ 2x_Twist+2x_Zigzag"
FLIPKART_PRODUCT_ID = "TOCHFMWW9HZGJ3VW"  # MFASIN / FSN

# ── Pricing (INR integers; MRP must be > selling price at every step) ────────
CURRENT_PRICE = 0   # fill current selling price
TARGET_PRICE = 0    # fill desired selling price
MRP = 0             # fill MRP (must be >= TARGET_PRICE)

# ── Gradual increase: 3 steps, 1 hour apart ──────────────────────────────────
USE_GRADUAL_INCREASE = True
GRADUAL_STEPS = 3
STEP_WAIT_SECONDS = 3600  # 1 hour; set to 60 for quick local testing

# ── API hosts ─────────────────────────────────────────────────────────────────
PROD_API_HOST = "https://api.flipkart.net"
SANDBOX_API_HOST = "https://sandbox.flipkart.net"
OAUTH_SCOPE = "Seller_Api,Default"
MAX_SKUS_PER_BATCH = 10


def api_host() -> str:
    return SANDBOX_API_HOST if USE_SANDBOX else PROD_API_HOST


def sellers_base_url() -> str:
    return f"{api_host()}/sellers"


def oauth_token_url() -> str:
    return f"{api_host()}/oauth-service/oauth/token"


def validate_config() -> None:
    missing = []
    if CLIENT_ID.startswith("paste_") or not CLIENT_ID.strip():
        missing.append("CLIENT_ID")
    if CLIENT_SECRET.startswith("paste_") or not CLIENT_SECRET.strip():
        missing.append("CLIENT_SECRET")
    if not FLIPKART_SKU.strip():
        missing.append("FLIPKART_SKU")
    if not FLIPKART_PRODUCT_ID.strip():
        missing.append("FLIPKART_PRODUCT_ID")
    if CURRENT_PRICE <= 0:
        missing.append("CURRENT_PRICE")
    if TARGET_PRICE <= 0:
        missing.append("TARGET_PRICE")
    if MRP <= 0:
        missing.append("MRP")
    if missing:
        raise ValueError(f"Fill required config: {', '.join(missing)}")
    if TARGET_PRICE <= CURRENT_PRICE:
        raise ValueError("TARGET_PRICE must be greater than CURRENT_PRICE")
    if MRP < TARGET_PRICE:
        raise ValueError("MRP must be >= TARGET_PRICE")
    if MRP <= CURRENT_PRICE:
        raise ValueError("MRP must be greater than CURRENT_PRICE")


def get_access_token(client_id: str, client_secret: str) -> str:
    """Client Credentials flow → Bearer access token."""
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    params = {
        "grant_type": "client_credentials",
        "scope": OAUTH_SCOPE,
    }
    url = oauth_token_url()
    print(f"[AUTH] POST {url}")
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    print(f"[AUTH] HTTP {resp.status_code}")
    try:
        data = resp.json()
    except Exception:
        print(resp.text[:500])
        resp.raise_for_status()
        raise

    if resp.status_code != 200:
        print(json.dumps(data, indent=2))
        raise RuntimeError(f"Token request failed: HTTP {resp.status_code}")

    token = data.get("access_token")
    if not token:
        print(json.dumps(data, indent=2))
        raise RuntimeError("No access_token in OAuth response")

    expires = data.get("expires_in", "?")
    print(f"[AUTH] Token OK (expires_in={expires}s)\n")
    return token


def build_price_payload(sku: str, product_id: str, selling_price: int, mrp: int) -> dict:
    if mrp <= selling_price:
        raise ValueError(f"MRP ({mrp}) must be > selling_price ({selling_price})")
    return {
        sku: {
            "product_id": product_id,
            "price": {
                "mrp": int(mrp),
                "selling_price": int(selling_price),
                "currency": "INR",
            },
        }
    }


def print_price_response(resp: requests.Response, sku: str) -> bool:
    """Print API response; return True if SKU update succeeded."""
    print(f"HTTP {resp.status_code}")
    try:
        body = resp.json()
    except Exception:
        print(resp.text[:1000])
        return False

    print(json.dumps(body, indent=2, ensure_ascii=False))

    if resp.status_code not in (200, 201):
        return False

    # Response keys are SKU ids; match exact or first entry
    sku_result = body.get(sku)
    if sku_result is None and len(body) == 1:
        sku_result = next(iter(body.values()))
    if sku_result is None:
        print(f"[WARN] No per-SKU block for '{sku}' in response")
        return False

    status = str(sku_result.get("status", "")).lower()
    errors = sku_result.get("errors") or []
    attr_errors = sku_result.get("attribute_errors") or []

    if errors:
        print("[ERRORS]")
        for e in errors:
            print(f"  - [{e.get('severity')}] {e.get('code')}: {e.get('description')}")
    if attr_errors:
        print("[ATTRIBUTE ERRORS]")
        for e in attr_errors:
            print(
                f"  - [{e.get('severity')}] {e.get('attribute')} "
                f"{e.get('code')}: {e.get('description')}"
            )

    ok = status == "success"
    print(f"[RESULT] SKU '{sku}' → {status.upper()}\n")
    return ok


def update_price(
    token: str,
    sku: str,
    product_id: str,
    selling_price: int,
    mrp: int,
) -> bool:
    """POST /listings/v3/update/price for one SKU."""
    url = f"{sellers_base_url()}/listings/v3/update/price"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = build_price_payload(sku, product_id, selling_price, mrp)

    print(f"[PRICE] {sku} | selling_price=₹{selling_price} | mrp=₹{mrp}")
    print(f"[PRICE] POST {url}")
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    return print_price_response(resp, sku)


def calc_gradual_prices(current: int, target: int, steps: int) -> list[int]:
    """Linear ramp in `steps` steps, last step = exact target."""
    if steps < 1:
        raise ValueError("steps must be >= 1")
    if target <= current:
        raise ValueError("target must be > current")
    out = []
    for i in range(1, steps + 1):
        price = current + round((target - current) * i / steps)
        out.append(price)
    out[-1] = target
    # dedupe consecutive identical prices
    deduped = [out[0]]
    for p in out[1:]:
        if p > deduped[-1]:
            deduped.append(p)
    return deduped


def run_gradual_increase(token: str) -> None:
    prices = calc_gradual_prices(CURRENT_PRICE, TARGET_PRICE, GRADUAL_STEPS)
    print(
        f"[PLAN] Gradual increase: {CURRENT_PRICE} → {TARGET_PRICE} "
        f"in {len(prices)} step(s): {prices}\n"
    )
    for idx, price in enumerate(prices, start=1):
        print(f"{'=' * 60}\nSTEP {idx}/{len(prices)}\n{'=' * 60}")
        ok = update_price(token, FLIPKART_SKU, FLIPKART_PRODUCT_ID, price, MRP)
        if not ok:
            print("[ABORT] Step failed — stopping gradual increase.")
            sys.exit(1)
        if idx < len(prices):
            print(f"[WAIT] Sleeping {STEP_WAIT_SECONDS}s before next step...")
            time.sleep(STEP_WAIT_SECONDS)
    print("[DONE] All gradual price steps completed successfully.")


def run_single_update(token: str) -> None:
    print(f"[PLAN] Single update: {CURRENT_PRICE} → {TARGET_PRICE}\n")
    ok = update_price(token, FLIPKART_SKU, FLIPKART_PRODUCT_ID, TARGET_PRICE, MRP)
    if not ok:
        sys.exit(1)
    print("[DONE] Price updated successfully.")


def main() -> None:
    validate_config()
    env = "SANDBOX" if USE_SANDBOX else "PRODUCTION"
    print(f"Flipkart Price Update | {env} | SKU: {FLIPKART_SKU}\n")

    token = get_access_token(CLIENT_ID, CLIENT_SECRET)

    if USE_GRADUAL_INCREASE:
        run_gradual_increase(token)
    else:
        run_single_update(token)


if __name__ == "__main__":
    main()
