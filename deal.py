#!/usr/bin/env python3
import json
import sys

import requests
from lxml import html

DEFAULT_PRODUCT_URL = "https://www.amazon.in/dp/B07MMF8P1K"
MAIN_DEAL_XPATH = "//*[@id='dealBadge_feature_div']/span"


def check_deal_main_xpath(url, xpath_expr=MAIN_DEAL_XPATH):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-IN,en;q=0.9",
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    tree = html.fromstring(resp.text)
    nodes = tree.xpath(xpath_expr)

    values = []
    for node in nodes:
        if isinstance(node, str):
            text = node.strip()
        else:
            text = (node.text_content() or "").strip()
        if text and len(text) <= 160:
            values.append(text)

    values = list(dict.fromkeys(values))
    has_limited_time_xpath = any("limited time deal" in v.lower() for v in values) or len(nodes) > 0

    return {
        "url": url,
        "xpath": xpath_expr,
        "status_code": resp.status_code,
        "xpath_match_count": len(nodes),
        "xpath_values": values,
        "deal_hai": has_limited_time_xpath,
    }


if __name__ == "__main__":
    url = sys.argv[1].strip() if len(sys.argv) > 1 else DEFAULT_PRODUCT_URL
    xpath_expr = sys.argv[2].strip() if len(sys.argv) > 2 else MAIN_DEAL_XPATH

    try:
        result = check_deal_main_xpath(url, xpath_expr)
        print("\n=== AMAZON MAIN DEAL CHECK ===")
        print(json.dumps(result, indent=2))
        print("\nResult:", "HAI" if result["deal_hai"] else "NAHI")
    except Exception as e:
        print("Error:", str(e))
