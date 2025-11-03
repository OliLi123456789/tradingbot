import csv
import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

ROOT = os.path.dirname(__file__)
MOVERS_FILE = os.path.join(ROOT, "movers.json")
OUT_CSV = os.path.join(ROOT, "movers_spreadsheet.csv")
SUBSEQUENT_CACHE = os.path.join(ROOT, "subsequent_cache.json")

HEADER = [
    "ticker",
    "event_date",
    "mover_type",
    "direction",
    "Points Change",
    "Event Day %Change",
    "Day 1 %",
    "Day 1 Open",
    "Day 1 Close",
    "Day 2 %",
    "Day 2 Open",
    "Day 2 Close",
    "Day 3 %",
    "Day 3 Open",
    "Day 3 Close",
    "Day 4 %",
    "Day 4 Open",
    "Day 4 Close",
    "Day 5 %",
    "Day 5 Open",
    "Day 5 Close",
    "Day 6 %",
    "Day 6 Open",
    "Day 6 Close",
    "Day 7 %",
    "Day 7 Open",
    "Day 7 Close",
    "Day 8 %",
    "Day 8 Open",
    "Day 8 Close",
    "Day 9 %",
    "Day 9 Open",
    "Day 9 Close",
    "Day 10 %",
    "Day 10 Open",
    "Day 10 Close",
    "Day 11 %",
    "Day 11 Open",
    "Day 11 Close",
]

HEADER_INDEX = {h: i for i, h in enumerate(HEADER)}


def _parse_pct(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if s == "":
            return None
        s = s.replace("%", "").replace("\u2212", "-").replace(",", "")
        # sometimes the stored value is like '+24.91' or '+24.91%'
        return float(s)
    except Exception:
        return None


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{v:+.2f}%"


def _fmt_points(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{v:+.2f}"


def _fmt_price(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{v:.2f}"


def _clean_sym(s: Any) -> str:
    if not s:
        return ""
    return str(s).split()[0].upper().strip()


def _ensure_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _load_subsequent_cache(path: str = SUBSEQUENT_CACHE) -> Dict[str, List[Dict[str, Any]]]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("Failed to load subsequent cache %s", path)
        return {}


def _compute_summary_rows(buckets: Dict[str, List[List[float]]]) -> List[List[str]]:
    """Compute summary rows for the CSV footer from buckets.

    Buckets keys: 'gainer_long','gainer_short','loser_long','loser_short'
    Each bucket is a list of 11 lists containing percent returns (floats).
    Returns rows to append to CSV (including a spacer row).
    """
    import math

    rows: List[List[str]] = []
    # spacer row
    rows.append([""] * len(HEADER))

    groups = [
        ("gainer_long", "Gainer Long"),
        ("gainer_short", "Gainer Short"),
        ("loser_long", "Loser Long"),
        ("loser_short", "Loser Short"),
    ]

    for key, label in groups:
        lists = buckets.get(key, [[] for _ in range(11)])
        # prepare three rows: total, win rate, avg
        total_row = [""] * len(HEADER)
        total_row[0] = f"Total {label} Return (%)"
        win_row = [""] * len(HEADER)
        win_row[0] = f"Win Rate {label} (%)"
        avg_row = [""] * len(HEADER)
        avg_row[0] = f"Avg Daily Return {label} (%)"

        for i in range(11):
            R = lists[i] if i < len(lists) else []
            N = len(R)
            if N == 0:
                continue
            # total compounded return
            prod = 1.0
            for r in R:
                prod *= (1.0 + r / 100.0)
            total_pct = (prod - 1.0) * 100.0
            win_rate = 100.0 * sum(1 for r in R if r > 0) / N
            avg = sum(R) / N
            idx = HEADER_INDEX[f"Day {i+1} %"]
            total_row[idx] = _fmt_pct(round(total_pct, 2))
            win_row[idx] = _fmt_pct(round(win_rate, 2))
            avg_row[idx] = _fmt_pct(round(avg, 2))

        rows.extend([total_row, win_row, avg_row])

    return rows


def populate_from_movers(path: str = MOVERS_FILE, out_csv: str = OUT_CSV) -> int:
    """Read movers.json and write a flattened CSV, including Day 1..Day 11 columns from subsequent_cache.json.

    Also computes and appends summary statistic rows at the bottom of the CSV.
    """
    if not os.path.exists(path):
        logger.warning("movers.json not found at %s", path)
        # still write header to CSV
        try:
            tmp = out_csv + ".tmp"
            with open(tmp, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(HEADER)
            os.replace(tmp, out_csv)
        except Exception:
            logger.exception("Failed to write header to CSV %s", out_csv)
        return 0

    with open(path, "r", encoding="utf-8") as f:
        try:
            movers = json.load(f)
        except Exception:
            logger.exception("Failed to parse movers.json")
            return 0

    cache = _load_subsequent_cache()

    rows: List[List[str]] = []

    # initialize buckets for summary calculations
    buckets: Dict[str, List[List[float]]] = {
        "gainer_long": [[] for _ in range(11)],
        "gainer_short": [[] for _ in range(11)],
        "loser_long": [[] for _ in range(11)],
        "loser_short": [[] for _ in range(11)],
    }

    # sort dates for deterministic output
    for date_key in sorted(k for k in movers.keys() if k):
        entry = movers.get(date_key, {})
        for mover_type in ("gainers", "losers"):
            items = entry.get(mover_type, []) or []
            for it in items:
                ticker = _clean_sym(it.get("symbol", ""))
                pct_raw = it.get("pct_change") or it.get("pct") or ""
                pct_f = _parse_pct(pct_raw)

                # points change: prefer numeric change_pts, fallback to change text parsing
                pts = _ensure_number(it.get("change_pts"))
                if pts is None:
                    # try to extract from 'change' field which sometimes contains 'price+change(+%)'
                    change_field = it.get("change", "")
                    # attempt to find a +/- number inside
                    try:
                        import re

                        m = re.search(r"([+-]?\d+[\d,]*\.?\d*)", str(change_field))
                        if m:
                            pts = _ensure_number(m.group(1))
                    except Exception:
                        pts = None

                mover_label = "gainer" if mover_type == "gainers" else "loser"

                # build a full-length empty row and fill fields
                base_row: List[str] = [""] * len(HEADER)
                base_row[HEADER_INDEX["ticker"]] = ticker
                base_row[HEADER_INDEX["event_date"]] = date_key
                base_row[HEADER_INDEX["mover_type"]] = mover_label
                # we'll write both directions separately below
                base_row[HEADER_INDEX["Points Change"]] = _fmt_points(pts)
                base_row[HEADER_INDEX["Event Day %Change"]] = _fmt_pct(pct_f)

                # load cache for this ticker+event
                cache_key = f"{ticker}|{date_key}"
                days_list = cache.get(cache_key, []) if isinstance(cache, dict) else []

                # populate Day N columns from cache and collect for buckets
                for i in range(11):
                    col_pct = f"Day {i+1} %"
                    col_open = f"Day {i+1} Open"
                    col_close = f"Day {i+1} Close"
                    if i < len(days_list):
                        d = days_list[i]
                        open_v = _ensure_number(d.get("open"))
                        close_v = _ensure_number(d.get("close"))
                        pct_long = _ensure_number(d.get("pct_long"))
                        # fallback compute if not present
                        if pct_long is None and open_v is not None and close_v is not None and open_v != 0:
                            pct_long = round((close_v - open_v) / open_v * 100, 2)

                        base_row[HEADER_INDEX[col_open]] = _fmt_price(open_v)
                        base_row[HEADER_INDEX[col_close]] = _fmt_price(close_v)

                        # record into buckets for summary
                        if pct_long is not None:
                            if mover_label == "gainer":
                                buckets["gainer_long"][i].append(pct_long)
                                buckets["gainer_short"][i].append(-pct_long)
                            else:
                                buckets["loser_long"][i].append(pct_long)
                                buckets["loser_short"][i].append(-pct_long)
                    else:
                        base_row[HEADER_INDEX[col_open]] = ""
                        base_row[HEADER_INDEX[col_close]] = ""
                        base_row[HEADER_INDEX[col_pct]] = ""

                # now create two rows (long and short) from base_row, setting Day N % appropriately
                for direction in ("long", "short"):
                    row = list(base_row)  # copy
                    row[HEADER_INDEX["direction"]] = direction

                    # fill Day N % from cache for this direction
                    for i in range(11):
                        col_pct = f"Day {i+1} %"
                        if i < len(days_list):
                            d = days_list[i]
                            open_v = _ensure_number(d.get("open"))
                            close_v = _ensure_number(d.get("close"))
                            pct_long = _ensure_number(d.get("pct_long"))
                            if pct_long is None and open_v is not None and close_v is not None and open_v != 0:
                                pct_long = round((close_v - open_v) / open_v * 100, 2)
                            if pct_long is None:
                                row[HEADER_INDEX[col_pct]] = ""
                            else:
                                val = pct_long if direction == "long" else -pct_long
                                row[HEADER_INDEX[col_pct]] = _fmt_pct(val)
                        else:
                            row[HEADER_INDEX[col_pct]] = ""

                    rows.append(row)

    # compute summary rows from buckets and append
    try:
        summary_rows = _compute_summary_rows(buckets)
        rows.extend(summary_rows)
    except Exception:
        logger.exception("Failed to compute summary rows")

    # atomic write
    tmp = out_csv + ".tmp"
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HEADER)
            writer.writerows(rows)
        os.replace(tmp, out_csv)
    except Exception:
        logger.exception("Failed to write CSV %s", out_csv)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return 0

    logger.info("Wrote %d rows to %s", len(rows), out_csv)
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    populate_from_movers()
