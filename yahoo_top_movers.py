import json
import logging
import os
from datetime import datetime, time
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

MOVERS_FILE = os.path.join(os.path.dirname(__file__), "movers.json")


def _fetch_soup(url: str, headers: Dict[str, str] | None = None, timeout: int = 10) -> BeautifulSoup:
    """Fetch a page and return a BeautifulSoup object."""
    headers = headers or {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
    logger.info("Fetching: %s", url)
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def _parse_top_rows(soup: BeautifulSoup, top_n: int = 5) -> List[Dict[str, str]]:
    """Parse the first top_n rows from the first table on the page into dicts.

    The function is defensive because site markup can change.
    """
    table = soup.find("table")
    if not table:
        logger.warning("No table found on page")
        return []

    tbody = table.find("tbody") or table
    rows = tbody.find_all("tr")
    results: List[Dict[str, str]] = []

    for tr in rows[:top_n]:
        cols = [c.get_text(strip=True) for c in tr.find_all("td")]
        if not cols:
            continue
        item = {
            "symbol": cols[0] if len(cols) > 0 else "",
            "name": cols[1] if len(cols) > 1 else "",
            "price": cols[2] if len(cols) > 2 else "",
            "change": cols[4] if len(cols) > 4 else "",
            "pct_change": cols[5] if len(cols) > 5 else "",
        }
        results.append(item)

    return results


def _parse_float(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        cleaned = s.replace(",", "").replace("%", "").replace("\u2212", "-").strip()
        if cleaned.startswith("(") and cleaned.endswith(")"):
            cleaned = "-" + cleaned[1:-1]
        return float(cleaned)
    except Exception:
        return None


def _find_table_value(soup: BeautifulSoup, label: str) -> str:
    el = soup.find(lambda tag: tag.name in ("td", "th") and tag.get_text(strip=True) == label)
    if el:
        nxt = el.find_next_sibling("td")
        if nxt:
            return nxt.get_text(" ", strip=True)
    return ""


def fetch_symbol_details(symbol: str) -> Dict[str, Optional[float]]:
    url = f"https://finance.yahoo.com/quote/{symbol}"
    try:
        soup = _fetch_soup(url)
    except Exception:
        logger.exception("Failed to fetch symbol page: %s", symbol)
        return {"open": None, "close": None, "change_pts": None}

    open_s = _find_table_value(soup, "Open")
    prev_close_s = _find_table_value(soup, "Previous Close")

    price_el = soup.find("fin-streamer", {"data-field": "regularMarketPrice"})
    price_s = price_el.get_text(strip=True) if price_el else ""

    open_v = _parse_float(open_s)
    close_v = _parse_float(prev_close_s) or _parse_float(price_s)

    change_pts = None
    if open_v is not None and close_v is not None:
        change_pts = round(close_v - open_v, 6)

    return {"open": open_v, "close": close_v, "change_pts": change_pts}


def load_movers(path: str = MOVERS_FILE) -> Dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.exception("Failed to load movers.json; starting fresh")
    return {}


def save_movers(data: Dict, path: str = MOVERS_FILE) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    os.replace(tmp, path)
    logger.info("Saved movers to %s", path)


def is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5


def is_market_day(dt: datetime) -> bool:
    try:
        import pandas_market_calendars as mcal
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(start_date=dt.date(), end_date=dt.date())
        return not schedule.empty
    except Exception:
        return not is_weekend(dt)


def after_cutoff_et(dt: datetime, cutoff_hour: int = 20) -> bool:
    try:
        from zoneinfo import ZoneInfo
        dt = dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        pass
    return dt.time() >= time(hour=cutoff_hour)


def scrape_top_gainers_and_losers(top_n: int = 5) -> Dict[str, List[Dict[str, str]]]:
    """Scrape top gainers and losers from Yahoo Finance and log results.

    Returns a dict with keys 'gainers' and 'losers'.
    """
    gainers_url = "https://finance.yahoo.com/markets/stocks/gainers/"
    losers_url = "https://finance.yahoo.com/markets/stocks/losers/"

    try:
        gainers_soup = _fetch_soup(gainers_url)
        losers_soup = _fetch_soup(losers_url)
    except Exception:
        logger.exception("Failed to fetch pages")
        return {"gainers": [], "losers": []}

    gainers = _parse_top_rows(gainers_soup, top_n=top_n)
    losers = _parse_top_rows(losers_soup, top_n=top_n)

    logger.info("Top %d Gainers:", top_n)
    for i, g in enumerate(gainers, 1):
        logger.info("%d: %s | %s | %s | %s | %s", i, g["symbol"], g["name"], g["price"], g["change"], g["pct_change"])

    logger.info("Top %d Losers:", top_n)
    for i, l in enumerate(losers, 1):
        logger.info("%d: %s | %s | %s | %s | %s", i, l["symbol"], l["name"], l["price"], l["change"], l["pct_change"])

    return {"gainers": gainers, "losers": losers}


def enrich_pool(pool: List[Dict[str, str]]) -> None:
    for item in pool:
        sym = item.get("symbol", "").split()[0] if item.get("symbol") else ""
        if not sym:
            item.update({"open": None, "close": None, "change_pts": None})
            continue
        details = fetch_symbol_details(sym)
        item["open"] = details["open"]
        item["close"] = details["close"]
        item["change_pts"] = details["change_pts"]


def run_and_persist(top_n: int = 5) -> None:
    try:
        from zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        now_et = datetime.utcnow()

    date_str = now_et.date().isoformat()

    if not is_market_day(now_et):
        logger.info("Not a market day (%s); skipping persist.", date_str)
        return

    if not after_cutoff_et(now_et, cutoff_hour=20):
        logger.info("Before cutoff (20:00 ET). Current ET time: %s — skipping.", now_et.time())
        return

    data = scrape_top_gainers_and_losers(top_n=top_n)
    candidate_items = data.get("gainers", []) + data.get("losers", [])
    if not candidate_items:
        logger.info("No movers found; skipping persist.")
        return

    enrich_pool(data.get("gainers", []))
    enrich_pool(data.get("losers", []))

    has_data = any((item.get("close") is not None) or item.get("pct_change") for item in candidate_items)
    if not has_data:
        logger.info("No valid numeric data found after enrichment; skipping persist.")
        return

    movers = load_movers()
    if date_str in movers:
        logger.info("Entry for %s already exists; not overwriting.", date_str)
        return

    movers[date_str] = {"scraped_at": now_et.isoformat(), "gainers": data.get("gainers", []), "losers": data.get("losers", [])}
    save_movers(movers)

    # update CSV after saving movers.json
    try:
        from populate_movers_csv import populate_from_movers
        populate_from_movers()
    except Exception:
        logger.exception("Failed to update CSV from movers.json")

    # fetch one subsequent day per prior mover and update CSV
    appended = 0
    try:
        from fetch_subsequent import fetch_one_round
        appended = fetch_one_round()
        logger.info("fetch_subsequent appended %s new subsequent day(s)", appended)
    except Exception:
        logger.exception("Failed to run fetch_subsequent")

    # regenerate CSV to reflect any changes (cache may have been updated)
    try:
        populate_from_movers()
    except Exception:
        logger.exception("Failed to regenerate CSV after fetching subsequent days")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    # Attempt to persist today's movers (may skip on non-market days or before cutoff)
    try:
        run_and_persist(5)
    except Exception:
        logger.exception("run_and_persist failed")

    # Regardless of whether we persisted new movers, try to fetch one subsequent day per mover
    try:
        from fetch_subsequent import fetch_one_round
        appended = fetch_one_round()
        logger.info("fetch_subsequent appended %s new subsequent day(s)", appended)
    except Exception:
        logger.exception("Failed to run fetch_subsequent")

    # Regenerate CSV to reflect latest movers.json and subsequent cache
    try:
        from populate_movers_csv import populate_from_movers
        populate_from_movers()
    except Exception:
        logger.exception("Failed to populate CSV from movers.json")
