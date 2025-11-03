"""Fetch one subsequent trading day per saved mover using yfinance and append it to subsequent_cache.json.

Behavior:
- For each entry in movers.json (sorted by date) and each mover (gainers+losers), compute cache key TICKER|EVENT_DATE.
- If cached days < max_days, fetch daily history after event_date and append the next available trading day if that day has completed (now ET >= candidate_date 20:00 ET).
- Append only one new day per key per run. Write subsequent_cache.json atomically.
"""

import json
import logging
import os
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

ROOT = os.path.dirname(__file__)
MOVERS_FILE = os.path.join(ROOT, "movers.json")
CACHE_FILE = os.path.join(ROOT, "subsequent_cache.json")

# how many subsequent days to keep
MAX_DAYS_DEFAULT = 11
# cutoff time ET for considering a trading day complete
CUTOFF_HOUR_ET = 20


def _now_et() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return datetime.utcnow()


def _is_day_complete(candidate_date: datetime) -> bool:
    # candidate_date is a date (midnight) in local NY date. Consider it complete after CUTOFF_HOUR_ET ET
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        now = datetime.utcnow()
    cutoff = datetime.combine(candidate_date.date(), time(hour=CUTOFF_HOUR_ET))
    try:
        cutoff = cutoff.replace(tzinfo=ZoneInfo("America/New_York"))
    except Exception:
        pass
    return now >= cutoff


def _load_json(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("Failed to load JSON %s", path)
        return {}


def _save_json_atomic(data: Dict, path: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    os.replace(tmp, path)


def _clean_sym(s: Optional[str]) -> str:
    if not s:
        return ""
    return str(s).split()[0].upper().strip()


def fetch_one_round(movers_path: str = MOVERS_FILE, cache_path: str = CACHE_FILE, max_days: int = MAX_DAYS_DEFAULT) -> int:
    """Try to append at most one new subsequent trading day per mover. Returns number of appended days."""
    try:
        import yfinance as yf
    except Exception:
        logger.exception("yfinance is required. Install with: pip install yfinance")
        return 0

    movers = _load_json(movers_path)
    cache = _load_json(cache_path)

    appended = 0
    now_et = _now_et()

    # iterate deterministic order
    for event_date in sorted(k for k in movers.keys() if k):
        entry = movers.get(event_date, {})
        for mover_type in ("gainers", "losers"):
            for it in entry.get(mover_type, []) or []:
                ticker = _clean_sym(it.get("symbol", ""))
                if not ticker:
                    continue
                key = f"{ticker}|{event_date}"
                days_list = cache.get(key, []) if isinstance(cache, dict) else []
                if len(days_list) >= max_days:
                    continue

                # fetch history after event_date (exclusive)
                # request a window from event_date +1 to now (plus 1 day) to ensure we have candidate rows
                try:
                    event_dt = datetime.fromisoformat(event_date)
                except Exception:
                    # try parsing date only
                    try:
                        event_dt = datetime.strptime(event_date, "%Y-%m-%d")
                    except Exception:
                        logger.warning("Invalid event_date format for %s: %s", key, event_date)
                        continue

                start_dt = (event_dt + timedelta(days=1)).date()
                # yfinance expects strings for start/end
                start_str = start_dt.isoformat()
                end_str = (now_et.date() + timedelta(days=1)).isoformat()

                try:
                    tk = yf.Ticker(ticker)
                    hist = tk.history(start=start_str, end=end_str, interval="1d", auto_adjust=False)
                except Exception:
                    logger.exception("Failed to fetch history for %s", ticker)
                    continue

                # hist index is DatetimeIndex; build list of rows with date in NY timezone
                rows = []
                try:
                    for idx, row in hist.iterrows():
                        # idx is Timestamp (tz-aware or naive in UTC); convert to date in America/New_York
                        try:
                            from zoneinfo import ZoneInfo
                            if idx.tzinfo is None:
                                # assume UTC
                                idx_utc = idx.replace(tzinfo=ZoneInfo("UTC"))
                            else:
                                idx_utc = idx
                            idx_ny = idx_utc.astimezone(ZoneInfo("America/New_York"))
                            row_date = idx_ny.date()
                        except Exception:
                            row_date = idx.date()

                        # only consider rows strictly after event_date
                        if row_date <= event_dt.date():
                            continue

                        open_v = None if row.get("Open") is None else float(row.get("Open"))
                        close_v = None if row.get("Close") is None else float(row.get("Close"))
                        rows.append({"date": row_date.isoformat(), "open": open_v, "close": close_v})
                except Exception:
                    logger.exception("Failed to iterate history for %s", ticker)
                    continue

                # pick candidate index = len(days_list)
                idx = len(days_list)
                if idx >= len(rows):
                    # no new trading day available yet
                    continue

                candidate = rows[idx]
                # candidate['date'] is ISO date string
                try:
                    candidate_dt = datetime.strptime(candidate["date"], "%Y-%m-%d")
                except Exception:
                    try:
                        candidate_dt = datetime.fromisoformat(candidate["date"])  # fallback
                    except Exception:
                        logger.warning("Invalid candidate date for %s: %s", key, candidate.get("date"))
                        continue

                # check day complete: now_et >= candidate_date 20:00 ET
                # use candidate_dt as midnight ET on that date
                try:
                    from zoneinfo import ZoneInfo
                    candidate_mid = datetime.combine(candidate_dt.date(), time())
                    candidate_mid = candidate_mid.replace(tzinfo=ZoneInfo("America/New_York"))
                except Exception:
                    candidate_mid = datetime.combine(candidate_dt.date(), time())

                cutoff = datetime.combine(candidate_mid.date(), time(hour=CUTOFF_HOUR_ET))
                try:
                    from zoneinfo import ZoneInfo
                    cutoff = cutoff.replace(tzinfo=ZoneInfo("America/New_York"))
                except Exception:
                    pass

                if now_et < cutoff:
                    # not yet complete
                    continue

                open_v = candidate.get("open")
                close_v = candidate.get("close")
                if open_v is None or close_v is None:
                    logger.info("No OHLC for %s on %s; skipping", key, candidate.get("date"))
                    continue

                # compute pct_long
                try:
                    pct_long = round((close_v - open_v) / open_v * 100, 2) if open_v != 0 else None
                except Exception:
                    pct_long = None

                day_obj = {"date": candidate.get("date"), "open": round(open_v, 2), "close": round(close_v, 2)}
                if pct_long is not None:
                    day_obj["pct_long"] = pct_long

                # append and save to cache structure
                if key not in cache or not isinstance(cache.get(key), list):
                    cache[key] = []
                cache[key].append(day_obj)
                appended += 1
                logger.info("Appended subsequent day for %s: %s", key, day_obj)

    if appended:
        try:
            _save_json_atomic(cache, cache_path)
        except Exception:
            logger.exception("Failed to save cache %s", cache_path)
    else:
        logger.info("No new subsequent days appended")

    return appended


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    cnt = fetch_one_round()
    print(f"Appended: {cnt}")
