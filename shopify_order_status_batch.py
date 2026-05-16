#!/usr/bin/env python3
"""Lookup Shopify order status by order name (number) via Admin GraphQL."""
import json
import sys

import requests

from utils import get_shopify_config, read_secret

ORDER_NAMES = [
    "3296", "3349", "3424", "3427", "3541", "3546", "3604", "3639", "3657", "3658",
    "3665", "3670", "3687", "3691", "3696", "3716", "3717", "3718", "3720", "3728",
    "3732", "3848", "3849", "3895", "4258", "4263", "4425",
]

QUERY = """
query OrderLookup($q: String!) {
  orders(first: 1, query: $q) {
    nodes {
      name
      legacyResourceId
      displayFulfillmentStatus
      displayFinancialStatus
      cancelledAt
      closed
    }
  }
}
"""


def main():
    secrets = read_secret()
    cfg = get_shopify_config(secrets)
    shop = cfg["shop_url"].replace("https://", "").replace("http://", "").rstrip("/")
    token = cfg["headers"]["X-Shopify-Access-Token"]
    if not shop or not token:
        print("Missing shop_url or shop_assesstoken in secret.txt", file=sys.stderr)
        sys.exit(1)

    gql_url = f"https://{shop}/admin/api/2024-01/graphql.json"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }

    results = []
    for raw in ORDER_NAMES:
        clean = str(raw).replace("#", "").strip()
        found = None
        last_err = None
        for qterm in (f"name:{clean}", f"name:#{clean}"):
            r = requests.post(
                gql_url,
                headers=headers,
                json={"query": QUERY, "variables": {"q": qterm}},
                timeout=45,
            )
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                continue
            data = r.json()
            if data.get("errors"):
                last_err = json.dumps(data["errors"])[:200]
                continue
            nodes = ((data.get("data") or {}).get("orders") or {}).get("nodes") or []
            if nodes:
                found = nodes[0]
                break
        if found:
            results.append({
                "requested": raw,
                "shopify_name": found.get("name"),
                "order_id": found.get("legacyResourceId"),
                "fulfillment_status": found.get("displayFulfillmentStatus"),
                "financial_status": found.get("displayFinancialStatus"),
                "cancelled_at": found.get("cancelledAt"),
                "closed": found.get("closed"),
            })
        else:
            results.append({
                "requested": raw,
                "error": last_err or "not_found",
            })

    print(json.dumps(results, indent=2))
    print("\n--- summary ---")
    for row in results:
        if "error" in row:
            print(f"{row['requested']}\tNOT FOUND / {row['error']}")
        else:
            cn = "cancelled" if row.get("cancelled_at") else "active"
            print(
                f"{row['requested']}\t{row.get('shopify_name')}\t"
                f"{row.get('fulfillment_status')}\t{row.get('financial_status')}\t{cn}"
            )


if __name__ == "__main__":
    main()
