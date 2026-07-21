"""
Funding Rate Arbitrage Scanner — single file
Run:  pip install flask requests
      python app.py
Open: http://127.0.0.1:5000
"""

import bisect
import os
import statistics
import time

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# --- global rate limiter: Startup plan allows 80 req/min; stay under ~70 ---
_rl_lock_ts = {"t": 0.0}
MIN_GAP_S = 0.86

def _throttled_get(url, **kw):
    wait = _rl_lock_ts["t"] + MIN_GAP_S - time.time()
    if wait > 0:
        time.sleep(wait)
    _rl_lock_ts["t"] = time.time()
    return requests.get(url, **kw)



COINGLASS_URL = "https://open-api-v4.coinglass.com/api/futures/funding-rate/arbitrage"

# Env var overrides the baked-in key if you ever rotate it.
# Do NOT share this file or push it to a public repo with the key inside.
API_KEY = os.environ.get("COINGLASS_API_KEY", "81025b7d4be54b21831af0b36ff5f34b")

_cache = {"ts": 0, "usd": None, "payload": None}
CACHE_SECONDS = 20

_ob_cache = {}          # (exchange, coin) -> {ts, payload}
OB_CACHE_SECONDS = 60

_pairs_cache = {"ts": 0, "data": None}
PAIRS_CACHE_SECONDS = 3600

_markets_cache = {}          # base symbol -> {ts, list}
MARKETS_CACHE_SECONDS = 30


def _get_markets_for(base):
    """Fetch pairs-markets for one coin (?symbol=BASE), cached per coin."""
    now = time.time()
    hit = _markets_cache.get(base)
    if hit and now - hit["ts"] < MARKETS_CACHE_SECONDS:
        return hit["list"]
    resp = _throttled_get(
        "https://open-api-v4.coinglass.com/api/futures/pairs-markets",
        params={"symbol": base},
        headers={"accept": "application/json", "CG-API-KEY": API_KEY},
        timeout=20,
    )
    data = resp.json()
    if str(data.get("code")) != "0":
        raise RuntimeError(data.get("msg", "pairs-markets failed"))
    lst = data.get("data", []) or []
    _markets_cache[base] = {"ts": now, "list": lst}
    return lst


def _pick_market(lst, exchange, base):
    """Pick this exchange's USDT-quoted entry from the coin's market list."""
    exn = _norm(exchange)
    cands = []
    for it in lst:
        e = _norm(it.get("exchange_name", ""))
        if e == exn or exn in e or e in exn:
            cands.append(it)
    if not cands:
        return None
    # prefer USDT-quoted pair (symbol like "BTC/USDT")
    def rank(it):
        s = str(it.get("symbol", "")).upper()
        return 0 if s.endswith("/USDT") else (1 if "USD" in s else 2)
    cands.sort(key=rank)
    it = cands[0]
    return {
        "price": it.get("current_price"),
        "index_price": it.get("index_price"),
        "instrument": it.get("instrument_id"),
        "volume_usd": it.get("volume_usd"),
        "oi_usd": it.get("open_interest_usd"),
        "oi_chg_24h": it.get("open_interest_change_percent_24h"),
    }


@app.route("/api/coin-info")
def coin_info():
    """Full pairs-markets rows for one coin across every exchange — reuses the
    same CoinGlass endpoint/cache as /api/prices, just returns everything."""
    base = request.args.get("symbol", "BTC").upper().strip()
    if not base:
        return jsonify({"code": "error", "msg": "symbol required"}), 400
    try:
        lst = _get_markets_for(base)
    except Exception as exc:
        return jsonify({"code": "error", "msg": str(exc)}), 502
    if not lst:
        return jsonify({"code": "0", "symbol": base, "data": [],
                        "msg": f"No markets found for {base} — check the symbol (e.g. BTC, not BTCUSDT)."})
    out = []
    for it in lst:
        out.append({
            "exchange": it.get("exchange_name"),
            "instrument_id": it.get("instrument_id"),
            "symbol": it.get("symbol"),
            "price": it.get("current_price"),
            "index_price": it.get("index_price"),
            "price_chg_24h": it.get("price_change_percent_24h"),
            "volume_usd": it.get("volume_usd"),
            "volume_chg_24h": it.get("volume_usd_change_percent_24h"),
            "oi_usd": it.get("open_interest_usd"),
            "oi_chg_24h": it.get("open_interest_change_percent_24h"),
            "funding_rate": it.get("funding_rate"),
            "next_funding_time": it.get("next_funding_time"),
            "long_liq_24h": it.get("long_liquidation_usd_24h"),
            "short_liq_24h": it.get("short_liquidation_usd_24h"),
            "oi_vol_ratio": it.get("open_interest_volume_radio"),
        })
    out.sort(key=lambda x: -(x["oi_usd"] or 0))
    return jsonify({"code": "0", "symbol": base, "data": out})


@app.route("/api/prices")
def prices():
    base = request.args.get("symbol", "BTC")
    long_ex = request.args.get("long", "")
    short_ex = request.args.get("short", "")
    try:
        lst = _get_markets_for(base)
    except Exception as exc:
        return jsonify({"code": "error", "msg": str(exc)}), 502
    lp = _pick_market(lst, long_ex, base)
    sp = _pick_market(lst, short_ex, base)
    out = {"code": "0", "long": lp, "short": sp}
    if lp and sp and lp.get("price") and sp.get("price"):
        mid = (lp["price"] + sp["price"]) / 2
        out["mid"] = mid
        # + means the short venue trades richer than the long venue:
        # you sell high / buy low on entry (favorable); you pay it back on exit.
        out["discrepancy_pct"] = (sp["price"] - lp["price"]) / mid * 100
    return jsonify(out)


# ======================= BACKTEST + STRATEGY ENGINE =======================

_hist_cache = {}     # historical OHLC fetches
_obh_cache = {}      # historical orderbook fetches
_bt_cache = {}       # finished backtests, keyed by params

FR_PATHS = ["/api/futures/funding-rate/history", "/api/futures/fundingRate/ohlc-history"]
PX_PATHS = ["/api/futures/price/history", "/api/futures/price/ohlc-history"]
H = 3600_000  # one hour in ms


def _fetch_any_interval(paths, exchange, symbol, intervals_h, start_ms, end_ms):
    """Try intervals in order (plan tiers restrict small intervals).
    Returns (data, used_interval_h, err)."""
    last = "no data"
    for ih in intervals_h:
        label = f"{ih}h" if ih < 24 else "1d"
        data, err = _fetch_ohlc(paths, exchange, symbol, label, start_ms, end_ms)
        if data:
            return data, ih, None
        last = err
    return None, None, last


def _fetch_ohlc(paths, exchange, symbol, interval, start_ms, end_ms):
    key = (paths[0], exchange, symbol, interval, start_ms // H, end_ms // H)
    if key in _hist_cache:
        return _hist_cache[key]
    last = "no data"
    for p in paths:
        try:
            r = _throttled_get("https://open-api-v4.coinglass.com" + p,
                             params={"exchange": exchange, "symbol": symbol,
                                     "interval": interval, "start_time": start_ms,
                                     "end_time": end_ms, "limit": 1000},
                             headers={"accept": "application/json", "CG-API-KEY": API_KEY},
                             timeout=25)
            data = r.json()
        except Exception as exc:
            last = str(exc)
            continue
        if str(data.get("code")) == "0" and data.get("data"):
            rows = sorted(data["data"], key=lambda x: x["time"])
            def f(row, k):
                v = row.get(k, row.get("close", 0))
                try: return float(v or 0)
                except (TypeError, ValueError): return 0.0
            out = ([int(r_["time"]) for r_ in rows],
                   [f(r_, "high") for r_ in rows],
                   [f(r_, "close") for r_ in rows])
            _hist_cache[key] = (out, None)
            return out, None
        last = data.get("msg", last)
    _hist_cache[key] = (None, last)
    return None, last


def _ffill(times, vals, t):
    i = bisect.bisect_right(times, t) - 1
    return vals[i] if i >= 0 else None


def _sum_between(times, vals, t0, t1):
    """Sum settlement values with t0 < time <= t1."""
    lo = bisect.bisect_right(times, t0)
    hi = bisect.bisect_right(times, t1)
    return sum(vals[lo:hi])


def _depth_hist(exchange, symbol, t_ms):
    bucket = t_ms // (6 * H)
    key = (exchange, symbol, bucket)
    if key in _obh_cache:
        rows = _obh_cache[key]
    else:
        try:
            r = _throttled_get("https://open-api-v4.coinglass.com/api/futures/orderbook/ask-bids-history",
                             params={"exchange": exchange, "symbol": symbol, "interval": "1h",
                                     "range": "1", "start_time": t_ms - 6 * H,
                                     "end_time": t_ms + H, "limit": 10},
                             headers={"accept": "application/json", "CG-API-KEY": API_KEY},
                             timeout=20)
            data = r.json()
            rows = data.get("data") if str(data.get("code")) == "0" else None
        except Exception:
            rows = None
        _obh_cache[key] = rows
    if not rows:
        return None
    ok = [x for x in rows if x["time"] <= t_ms + H]
    return ok[-1] if ok else None


def _instrument_info(exchange, coin):
    """(canonical exchange, instrument_id, funding_interval_h)."""
    ex, cands, _diag = _resolve_instruments(exchange, coin)
    inst = cands[0] if cands else f"{coin}USDT"
    fi = 8
    try:
        pairs = _get_supported_pairs()
        for k, lst in pairs.items():
            if _norm(k) == _norm(ex):
                for p in lst:
                    if str(p.get("instrument_id")) == inst:
                        fi = int(p.get("funding_interval") or 8)
    except Exception:
        pass
    return ex, inst, fi


def _pctl(xs, p):
    if not xs:
        return 0.0
    s = sorted(xs)
    i = min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))
    return s[i]


def run_backtest(coin, long_ex, short_ex, days, usd, fee_rt, entry_th, exit_th, max_hold, depth_x, use_depth=True):
    now = int(time.time() * 1000) // H * H
    start = now - days * 24 * H
    exL, instL, fiL = _instrument_info(long_ex, coin)
    exS, instS, fiS = _instrument_info(short_ex, coin)

    def fr_ints(fi):
        seen, out = set(), []
        for x in [fi, 4, 8, 24]:
            if x >= 1 and x not in seen:
                seen.add(x); out.append(x)
        return out

    frL, ihL, e1 = _fetch_any_interval(FR_PATHS, exL, instL, fr_ints(fiL), start, now)
    frS, ihS, e2 = _fetch_any_interval(FR_PATHS, exS, instS, fr_ints(fiS), start, now)
    pxL, _, e3 = _fetch_any_interval(PX_PATHS, exL, instL, [1, 4, 8], start, now)
    pxS, _, e4 = _fetch_any_interval(PX_PATHS, exS, instS, [1, 4, 8], start, now)
    # if plan limits forced a coarser candle than the funding interval,
    # each candle contains (candle/fi) settlements — scale payment sums
    scaleL = (ihL / fiL) if frL else 1.0
    scaleS = (ihS / fiS) if frS else 1.0
    missing = [n for n, d, e in [("funding:" + exL, frL, e1), ("funding:" + exS, frS, e2),
                                 ("price:" + exL, pxL, e3), ("price:" + exS, pxS, e4)] if d is None]
    if missing:
        return {"code": "error",
                "msg": "Missing historical data (plan tier or coverage): " +
                       "; ".join(f"{n} ({e})" for n, d, e in
                                 [("funding " + exL, frL, e1), ("funding " + exS, frS, e2),
                                  ("price " + exL, pxL, e3), ("price " + exS, pxS, e4)] if d is None)}

    t0 = max(frL[0][0], frS[0][0], pxL[0][0], pxS[0][0])
    t1 = min(frL[0][-1], frS[0][-1], pxL[0][-1], pxS[0][-1])
    if t1 - t0 < 3 * 24 * H:
        return {"code": "error", "msg": "Under 3 days of overlapping history — not enough to backtest."}

    cyc = fiS * H
    ratio = fiS / fiL
    trades, decay = [], {}
    rejected, nodata = 0, 0
    t = t0
    while t + max_hold * cyc <= t1:
        fS = _ffill(frS[0], frS[2], t)
        fL = _ffill(frL[0], frL[2], t)
        if fS is None or fL is None:
            t += H; continue
        diff = fS - fL * ratio
        if diff < entry_th:
            t += H; continue

        obL = obS = None
        if use_depth:
            obL = _depth_hist(exL, instL, t)
            obS = _depth_hist(exS, instS, t)
            if obL is None or obS is None:
                nodata += 1; t += cyc; continue
            if min(obL["asks_usd"], obS["bids_usd"]) < depth_x * usd:
                rejected += 1; t += cyc; continue

        pS0 = _ffill(pxS[0], pxS[2], t)
        pL0 = _ffill(pxL[0], pxL[2], t)
        if not pS0 or not pL0:
            t += H; continue
        entry_gap = (pS0 - pL0) / pS0 * 100
        if use_depth:
            slip = min(0.5, 100 * usd / max(obL["asks_usd"], 1) * 0.5) + \
               min(0.5, 100 * usd / max(obS["bids_usd"], 1) * 0.5)
        else:
            slip = 0.0

        funding, held, max_up, te = 0.0, 0, 0.0, t
        while held < max_hold:
            te_next = te + cyc
            if te_next > t1: break
            held += 1
            funding += _sum_between(frS[0], frS[2], te, te_next) * scaleS   # short collects
            funding -= _sum_between(frL[0], frL[2], te, te_next) * scaleL   # long pays (earns if neg)
            lo = bisect.bisect_right(pxS[0], te); hi = bisect.bisect_right(pxS[0], te_next)
            if hi > lo:
                max_up = max(max_up, (max(pxS[1][lo:hi]) - pS0) / pS0 * 100)
            dnow_s = _ffill(frS[0], frS[2], te_next)
            dnow_l = _ffill(frL[0], frL[2], te_next)
            dnow = (dnow_s - dnow_l * ratio) if dnow_s is not None and dnow_l is not None else None
            decay.setdefault(held, []).append(dnow if dnow is not None else 0.0)
            te = te_next
            if dnow is not None and dnow < exit_th:
                break

        pS1 = _ffill(pxS[0], pxS[2], te) or pS0
        pL1 = _ffill(pxL[0], pxL[2], te) or pL0
        exit_gap = (pS1 - pL1) / pS1 * 100
        gap_pnl = entry_gap - exit_gap
        net = funding + gap_pnl - fee_rt - slip
        trades.append({"entry": t, "cycles": held, "funding": round(funding, 3),
                       "gap_entry": round(entry_gap, 3), "gap_pnl": round(gap_pnl, 3),
                       "slip": round(slip, 3), "net": round(net, 3),
                       "max_pump": round(max_up, 1),
                       "min_depth": round(min(obL["asks_usd"], obS["bids_usd"])) if use_depth else 0})
        t = te + H

    if not trades:
        return {"code": "0", "trades": [], "summary": None, "rec": None,
                "depth": {"rejected": rejected, "nodata": nodata},
                "msg": "No qualifying entries in the window — diff never crossed the threshold with adequate depth."}

    nets = [x["net"] for x in trades]
    pumps = [x["max_pump"] for x in trades]
    depths = [x["min_depth"] for x in trades]
    liq = {lv: sum(1 for p in pumps if p >= 100.0 / lv * 0.9) / len(pumps) for lv in (1, 2, 3, 5, 10)}
    dec = [{"cycle": k, "avg": round(statistics.mean(v), 3)} for k, v in sorted(decay.items()) if k <= 10]

    # ----- strategy recommendation -----
    rec_cycles = 1
    for row in dec:
        if row["avg"] >= exit_th: rec_cycles = row["cycle"]
        else: break
    rec_cycles = max(1, min(rec_cycles, 8))
    exp_funding = sum(r["avg"] for r in dec[:rec_cycles])
    avg_slip = statistics.mean([x["slip"] for x in trades])
    costs = fee_rt + avg_slip
    rec_entry = round(max(entry_th, costs / max(rec_cycles, 1) * 1.5), 2)
    p95_pump = _pctl(pumps, 95)
    rec_lev = 1
    for lv in (5, 3, 2, 1):
        lr = liq.get(lv)
        if lr is None:
            continue
        if lr == 0 and (100.0 / lv) > 1.6 * p95_pump:
            rec_lev = lv; break
    med_depth = _pctl(depths, 50)
    if use_depth:
        rec_pos = int(min(usd, max(500, med_depth / 10)) // 100 * 100)
        pos_why = f"Median executable depth at signals was ${med_depth:,.0f}; stay ≤ ~10% of it."
    else:
        rec_pos = int(usd)
        pos_why = "Depth check OFF — size unvalidated against the book; verify liquidity before trading."
    avg_gap = statistics.mean([x["gap_pnl"] for x in trades])
    exp_net = round(exp_funding + avg_gap - costs, 3)
    warnings = []
    if not use_depth: warnings.append("Depth check OFF — this is the PURE NUMERICAL edge: no orderbook gating, zero slippage assumed. Real fills will be worse; re-run with depth ON before trading.")
    if exp_net <= 0: warnings.append("Expected net ≤ 0 after fees and slippage at the recommended parameters — historically this pair was NOT worth trading. Skip it, or wait for diffs well above the recommended entry threshold.")
    if len(trades) < 5: warnings.append(f"Only {len(trades)} historical trades — low sample, treat every number as rough.")
    if rejected + nodata > len(trades): warnings.append("More signals failed the depth check than passed — real capacity is the binding constraint.")
    if p95_pump > 25: warnings.append(f"95th-pct pump during holds was {p95_pump:.0f}% — this coin squeezes; keep spare margin on {short_ex}.")
    if avg_gap < -0.05: warnings.append("Price gap historically moved AGAINST entries — don't count on convergence profit.")

    rec = {"entry_diff": rec_entry,
           "entry_timing": "Enter 30–60 min before a settlement, once the accruing diff ≥ threshold. Never enter in the final minutes (pre-settlement scrum) or right after (rate already paid).",
           "position_usd": rec_pos,
           "position_why": pos_why,
           "leverage": rec_lev,
           "leverage_why": f"Worst pump seen {max(pumps):.0f}%, 95th pct {p95_pump:.0f}%. At {rec_lev}x liquidation sits ≈ {100/rec_lev:.0f}% away (0 historical liquidations).",
           "hold_cycles": rec_cycles,
           "hold_why": f"Edge decays below the {exit_th}% hurdle after ~{rec_cycles} cycle(s); expected funding over that hold ≈ {exp_funding:.2f}%.",
           "take_profit": f"Time/decay-based: exit after {rec_cycles} cycle(s) OR the moment the next predicted diff < {exit_th}%. If the price gap converges beyond your entry gap early, harvest it — that profit is already banked.",
           "stop_loss": [
               "NEVER a single-leg price stop — a hedged move isn't a loss; a stop converts it into one and leaves you naked.",
               f"Price ALERT (not order) at +{int(100/rec_lev*0.4)}% on the coin: top up short-leg margin on {short_ex} or unwind BOTH legs together.",
               "Unwind both legs if the cross-venue gap moves against you by more than the funding you still expect to collect.",
               "Unwind immediately on structural news: delisting notice, token unlock, funding flipping sign."],
           "capital": f"${int(2*rec_pos/rec_lev):,} margin ({rec_lev}x per leg) + spare collateral parked on {short_ex} (the fragile leg).",
           "expected_net_pct": exp_net,
           "expected_net_usd": round(exp_net / 100 * rec_pos, 2),
           "warnings": warnings}

    summary = {"trades": len(trades), "win_rate": round(sum(1 for x in nets if x > 0) / len(nets) * 100),
               "avg_net": round(statistics.mean(nets), 3), "total_net": round(sum(nets), 2),
               "avg_funding": round(statistics.mean([x["funding"] for x in trades]), 3),
               "avg_gap_pnl": round(avg_gap, 3), "avg_cycles": round(statistics.mean([x["cycles"] for x in trades]), 1),
               "worst": round(min(nets), 3), "best": round(max(nets), 3), "days": days}
    return {"code": "0", "trades": trades[-40:], "summary": summary, "decay": dec,
            "liq": {str(k): round(v * 100) for k, v in liq.items()},
            "depth": {"rejected": rejected, "nodata": nodata}, "rec": rec}


@app.route("/api/backtest")
def api_backtest():
    coin = request.args.get("symbol", "BTC")
    long_ex = request.args.get("long", "Binance")
    short_ex = request.args.get("short", "OKX")
    days = min(60, max(7, int(request.args.get("days", 21))))
    usd = max(100, float(request.args.get("usd", 10000)))
    fee = float(request.args.get("fee", 0.12))
    use_depth = request.args.get("depth", "1") != "0"
    key = (coin, long_ex, short_ex, days, int(usd), fee, use_depth)
    now = time.time()
    hit = _bt_cache.get(key)
    if hit and now - hit["ts"] < 600:
        return jsonify(hit["payload"])
    try:
        res = run_backtest(coin, long_ex, short_ex, days, usd, fee,
                           entry_th=0.30, exit_th=0.05, max_hold=12, depth_x=5,
                           use_depth=use_depth)
    except Exception as exc:
        return jsonify({"code": "error", "msg": f"backtest crashed: {exc}"}), 500
    if str(res.get("code")) == "0":
        res["use_depth"] = use_depth
        _bt_cache[key] = {"ts": now, "payload": res}
    return jsonify(res)


def _norm(s):
    return "".join(ch for ch in str(s).lower() if ch.isalnum())


def _get_supported_pairs():
    """Fetch & cache the exchange -> [instrument] mapping (1h cache)."""
    now = time.time()
    if _pairs_cache["data"] and now - _pairs_cache["ts"] < PAIRS_CACHE_SECONDS:
        return _pairs_cache["data"]
    resp = _throttled_get(
        "https://open-api-v4.coinglass.com/api/futures/supported-exchange-pairs",
        headers={"accept": "application/json", "CG-API-KEY": API_KEY},
        timeout=15,
    )
    data = resp.json()
    if str(data.get("code")) != "0":
        raise RuntimeError(data.get("msg", "supported-exchange-pairs failed"))
    _pairs_cache.update({"ts": now, "data": data.get("data", {})})
    return _pairs_cache["data"]


def _resolve_instruments(exchange, coin):
    """Return (canonical_exchange, candidate instrument_ids, diagnosis).
    diagnosis: 'ok' | 'exchange_unknown' | 'coin_unlisted' | 'pairs_api_failed'"""
    candidates = []
    ex_key = None
    diagnosis = "ok"
    try:
        pairs = _get_supported_pairs()
        # exchange names differ slightly ("Gate.io" vs "Gate") — normalized match
        ex_key = next((k for k in pairs if _norm(k) == _norm(exchange)), None)
        if ex_key is None:
            ex_key = next((k for k in pairs if _norm(exchange) in _norm(k)
                           or _norm(k) in _norm(exchange)), None)
        if ex_key is None:
            diagnosis = "exchange_unknown"
        else:
            matches = [p for p in pairs[ex_key]
                       if _norm(p.get("base_asset", "")) == _norm(coin)]
            if not matches:
                diagnosis = "coin_unlisted"
            # prefer USDT-quoted, USDT-settled perps; dated futures last
            def rank(p):
                iid = str(p.get("instrument_id", ""))
                usdt_q = 0 if "usdt" in _norm(p.get("quote_asset", "")) + _norm(iid) else 1
                usdt_s = 0 if "usdt" in _norm(p.get("settlement_currency", "usdt")) else 1
                dated = 1 if iid[-4:].isdigit() else 0
                has_funding = 0 if p.get("funding_interval") else 1
                return (usdt_q, dated, usdt_s, has_funding, len(iid))
            candidates = [str(p["instrument_id"]) for p in sorted(matches, key=rank)
                          if p.get("instrument_id")]
    except Exception:
        diagnosis = "pairs_api_failed"
    fallback = f"{coin}USDT"
    if fallback not in candidates:
        candidates.append(fallback)
    return ex_key or exchange, candidates[:3], diagnosis


DIAGNOSIS_TEXT = {
    "exchange_unknown": "This exchange isn't in CoinGlass's instrument list at all — "
                        "no CoinGlass data exists for it.",
    "coin_unlisted": "CoinGlass doesn't list this coin on this exchange.",
    "pairs_api_failed": "Couldn't load CoinGlass's instrument list to resolve the pair.",
    "coverage": "Instrument exists, but CoinGlass doesn't track orderbook depth for it "
                "(depth coverage is limited to major venues/pairs).",
}


@app.route("/api/orderbook")
def orderbook():
    exchange = request.args.get("exchange", "Binance")
    coin = request.args.get("coin") or request.args.get("symbol", "BTC")
    rng = request.args.get("range", "1")
    if rng not in ("0.25", "0.5", "0.75", "1", "2", "3", "5", "10"):
        rng = "1"

    key = (exchange, coin, rng)
    now = time.time()
    hit = _ob_cache.get(key)
    if hit and now - hit["ts"] < OB_CACHE_SECONDS:
        return jsonify(hit["payload"])

    tried = []
    last_msg = "no data"
    ex_canonical, instruments, diagnosis = _resolve_instruments(exchange, coin)
    for symbol in instruments:
        for interval in ("30m", "1h", "4h", "1d"):
            tried.append(f"{symbol}@{interval}")
            try:
                resp = _throttled_get(
                    "https://open-api-v4.coinglass.com/api/futures/orderbook/ask-bids-history",
                    params={"exchange": ex_canonical, "symbol": symbol,
                            "interval": interval, "limit": 10, "range": rng},
                    headers={"accept": "application/json", "CG-API-KEY": API_KEY},
                    timeout=15,
                )
                data = resp.json()
            except requests.RequestException as exc:
                last_msg = str(exc)
                continue
            except ValueError:
                last_msg = "non-JSON response"
                continue
            if str(data.get("code")) == "0" and data.get("data"):
                payload = {"code": "0", "data": data["data"],
                           "symbol_used": symbol, "interval": interval,
                           "exchange": ex_canonical, "range": rng}
                _ob_cache[key] = {"ts": now, "payload": payload}
                return jsonify(payload)
            last_msg = data.get("msg", last_msg)
    reason = DIAGNOSIS_TEXT.get(diagnosis if diagnosis != "ok" else "coverage")
    return jsonify({"code": "no_data", "reason": reason,
                    "msg": last_msg, "tried": tried}), 404


@app.route("/api/arb")
def arb():
    usd = request.args.get("usd", "10000")
    try:
        usd_val = max(1, int(float(usd)))
    except ValueError:
        usd_val = 10000

    now = time.time()
    if _cache["payload"] and _cache["usd"] == usd_val and now - _cache["ts"] < CACHE_SECONDS:
        return jsonify(_cache["payload"])

    try:
        resp = _throttled_get(
            COINGLASS_URL,
            params={"usd": usd_val},
            headers={"accept": "application/json", "CG-API-KEY": API_KEY},
            timeout=15,
        )
        data = resp.json()
    except requests.RequestException as exc:
        return jsonify({"code": "network_error", "msg": str(exc)}), 502
    except ValueError:
        return jsonify({"code": "bad_response", "msg": "CoinGlass returned non-JSON."}), 502

    if str(data.get("code")) != "0":
        return jsonify(data), 502

    _cache.update({"ts": now, "usd": usd_val, "payload": data})
    return jsonify(data)


@app.route("/api/keepalive")
def keepalive():
    """Ping target for an external cron service (e.g. cron-job.org).
    Keeps the free instance awake AND refreshes the arb scan cache,
    so data is current even if nobody has the page open."""
    try:
        resp = _throttled_get(
            COINGLASS_URL, params={"usd": 10000},
            headers={"accept": "application/json", "CG-API-KEY": API_KEY}, timeout=15)
        data = resp.json()
        if str(data.get("code")) == "0":
            _cache.update({"ts": time.time(), "usd": 10000, "payload": data})
            return jsonify({"code": "0", "status": "awake", "scan_refreshed": True,
                            "pairs": len(data.get("data", [])), "time": time.time()})
        return jsonify({"code": "0", "status": "awake", "scan_refreshed": False, "msg": data.get("msg")})
    except Exception as exc:
        return jsonify({"code": "0", "status": "awake", "scan_refreshed": False, "msg": str(exc)})


@app.route("/")
def index():
    return PAGE


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Funding Capture — Arbitrage Scanner</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#0C0F16; --panel:#131927; --panel-2:#0F1420; --edge:#222C47;
    --text:#E8ECF4; --muted:#7E89A3; --faint:#4A5570;
    --long:#35D9BC; --long-dim:rgba(53,217,188,.12);
    --short:#FF6470; --short-dim:rgba(255,100,112,.12);
    --gold:#F2C14E; --gold-dim:rgba(242,193,78,.14);
    --mono:'IBM Plex Mono',monospace; --disp:'Space Grotesk',sans-serif;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:var(--mono);font-size:13px;min-height:100vh}

  header{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end;justify-content:space-between;
    padding:28px 32px 20px;border-bottom:1px solid var(--edge)}
  .brand h1{font-family:var(--disp);font-weight:700;font-size:22px;letter-spacing:.5px}
  .brand h1 span{color:var(--gold)}
  .brand p{color:var(--muted);margin-top:4px;font-size:12px}
  .legend{display:flex;gap:14px;margin-top:8px;font-size:11px;color:var(--muted)}
  .legend i{display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:5px}
  .legend .l i{background:var(--long)} .legend .s i{background:var(--short)}

  .controls{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end}
  .ctl{display:flex;flex-direction:column;gap:5px}
  .ctl label{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--faint)}
  .ctl input,.ctl select{background:var(--panel-2);border:1px solid var(--edge);color:var(--text);
    font-family:var(--mono);font-size:13px;padding:8px 10px;border-radius:6px;width:130px}
  .ctl input:focus,.ctl select:focus{outline:2px solid var(--gold);outline-offset:1px}
  button{background:var(--gold);color:#141200;border:0;font-family:var(--disp);font-weight:700;
    font-size:13px;padding:9px 18px;border-radius:6px;cursor:pointer}
  button.ghost{background:transparent;color:var(--muted);border:1px solid var(--edge)}
  button.ghost[aria-pressed="true"]{color:var(--gold);border-color:var(--gold)}
  button:focus-visible{outline:2px solid var(--text);outline-offset:2px}
  .seg{display:flex;border:1px solid var(--edge);border-radius:6px;overflow:hidden}
  .seg button{border-radius:0;background:transparent;color:var(--muted);border:0;padding:9px 14px}
  .seg button[aria-pressed="true"]{background:var(--panel);color:var(--gold)}

  .statusbar{padding:10px 32px;font-size:11px;color:var(--muted);display:flex;gap:18px;flex-wrap:wrap}
  .statusbar .err{color:var(--short)}

  main{padding:8px 32px 60px;max-width:1280px;margin:0 auto}

  /* ---- table view ---- */
  .tablewrap{overflow-x:auto;border:1px solid var(--edge);border-radius:10px;background:var(--panel)}
  table{width:100%;border-collapse:collapse;min-width:1320px}
  thead th{position:sticky;top:0;background:var(--panel-2);color:var(--faint);font-size:10px;
    letter-spacing:.1em;text-transform:uppercase;font-weight:600;text-align:right;
    padding:11px 12px;border-bottom:1px solid var(--edge);cursor:pointer;user-select:none;white-space:nowrap}
  thead th:hover{color:var(--text)}
  thead th.txt{text-align:left}
  thead th .dir{color:var(--gold)}
  tbody td{padding:10px 12px;border-bottom:1px solid var(--edge);text-align:right;white-space:nowrap}
  tbody tr:last-child td{border-bottom:0}
  tbody tr:hover{background:var(--panel-2)}
  td.txt{text-align:left}
  td .ex{display:block;font-size:10px;color:var(--muted);margin-top:2px}
  .sym-cell{font-family:var(--disp);font-weight:700;font-size:14px;letter-spacing:.03em}
  .r-long{color:var(--long);font-weight:600}
  .r-short{color:var(--short);font-weight:600}
  .r-diff{color:var(--gold);font-weight:600}
  .r-apr{color:var(--gold)}
  .r-dim{color:var(--muted)}
  .warn-cell{color:var(--short);font-size:10px;letter-spacing:.05em}
  .countdown b{color:var(--text);font-weight:600}

  /* ---- ticket view ---- */
  .ticket{background:var(--panel);border:1px solid var(--edge);border-radius:10px;margin-bottom:14px;overflow:hidden}
  .ticket-head{display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:space-between;
    padding:12px 18px;border-bottom:1px solid var(--edge);background:var(--panel-2)}
  .sym{font-family:var(--disp);font-weight:700;font-size:17px;letter-spacing:.04em}
  .apr{color:var(--gold);background:var(--gold-dim);padding:3px 10px;border-radius:5px;font-weight:600}
  .flags{display:flex;gap:6px;flex-wrap:wrap}
  .flag{font-size:10px;letter-spacing:.06em;padding:3px 8px;border-radius:4px;border:1px solid var(--edge);color:var(--muted)}
  .flag.warn{color:var(--short);border-color:rgba(255,100,112,.4)}
  .countdown{color:var(--muted)}
  .ticket-body{display:grid;grid-template-columns:1fr 200px 1fr}
  .leg{padding:16px 18px}
  .leg.long{border-left:3px solid var(--long)}
  .leg.short{border-right:3px solid var(--short);text-align:right}
  .leg .side{font-size:10px;letter-spacing:.14em;font-weight:600;margin-bottom:6px}
  .leg.long .side{color:var(--long)} .leg.short .side{color:var(--short)}
  .leg .ex2{font-family:var(--disp);font-size:15px;font-weight:500}
  .leg .rate{font-size:20px;font-weight:600;margin:6px 0 2px}
  .leg.long .rate{color:var(--long)} .leg.short .rate{color:var(--short)}
  .leg .sub{color:var(--muted);font-size:11px;line-height:1.7}
  .capture{background:var(--panel-2);border-left:1px solid var(--edge);border-right:1px solid var(--edge);
    padding:14px;display:flex;flex-direction:column;justify-content:center;text-align:center;gap:3px}
  .capture .net{font-size:19px;font-weight:600;color:var(--gold)}
  .capture .cap-label{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--faint)}
  .capture .be{font-size:11px;color:var(--muted);margin-top:6px}

  .empty{padding:60px 20px;text-align:center;color:var(--muted);line-height:1.9}
  .empty b{color:var(--text)}

  /* ---- route summary ---- */
  .routes{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:2px 0 14px}
  .routes-label{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--faint);margin-right:4px}
  .route{background:var(--panel);border:1px solid var(--edge);color:var(--muted);
    font-family:var(--mono);font-size:11px;font-weight:400;padding:6px 10px;border-radius:6px;cursor:pointer}
  .route:hover{color:var(--text);border-color:var(--faint)}
  .route.on{border-color:var(--gold);color:var(--text);background:var(--gold-dim)}
  .route .rl{color:var(--long);font-weight:600}
  .route .rs{color:var(--short);font-weight:600}
  .route span{color:var(--faint)}

  /* ---- orderbook depth panel ---- */
  tr.datarow{cursor:pointer}
  .chev{color:var(--faint);font-size:10px}
  tr.depthrow td{background:var(--panel-2);padding:0;border-bottom:1px solid var(--edge);
    white-space:normal;overflow-wrap:anywhere;word-break:break-word}
  .depthwrap{padding:14px 16px;max-width:1200px}
  tr.depthrow:hover{background:transparent}
  .depth-loading{padding:14px 16px;color:var(--muted)}
  .dverdict{font-family:var(--disp);font-weight:700;font-size:13px;margin-bottom:10px}
  .dverdict span{font-family:var(--mono);font-weight:400;font-size:11px;color:var(--muted)}
  .dverdict.ok{color:var(--long)} .dverdict.mid{color:var(--gold)} .dverdict.bad{color:var(--short)}
  .dgrid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px}
  @media(max-width:820px){.dgrid{grid-template-columns:1fr}}
  .dleg{background:var(--panel);border:1px solid var(--edge);border-radius:8px;padding:12px 14px}
  .dleg.dl{border-left:3px solid var(--long)} .dleg.ds{border-left:3px solid var(--short)}
  .dhead{font-size:11px;letter-spacing:.1em;font-weight:600;margin-bottom:8px;color:var(--text)}
  .dtag{font-size:10px;padding:2px 7px;border-radius:4px;margin-left:6px}
  .dtag.ok{color:var(--long);background:var(--long-dim)}
  .dtag.mid{color:var(--gold);background:var(--gold-dim)}
  .dtag.bad{color:var(--short);background:var(--short-dim)}
  .drow{display:flex;justify-content:space-between;gap:10px;padding:3px 0;font-size:12px}
  .drow span{color:var(--muted)}
  .dbar{height:6px;border-radius:3px;background:var(--short-dim);overflow:hidden;margin:8px 0 4px}
  .dbar i{display:block;height:100%;background:var(--long)}
  .dsub{font-size:10px;color:var(--faint)}
  .dna{color:var(--muted);font-size:12px;line-height:1.7}
  .derr{margin-top:6px;font-size:10px;color:var(--faint);overflow-wrap:anywhere}
  .dnote{margin-top:10px;font-size:10px;color:var(--faint)}
  .pxgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin-bottom:12px}
  @media(max-width:900px){.pxgrid{grid-template-columns:repeat(2,minmax(0,1fr))}}
  .pxcard{background:var(--panel);border:1px solid var(--edge);border-radius:8px;padding:12px 14px}
  .pxcard.long{border-top:3px solid var(--long)}
  .pxcard.short{border-top:3px solid var(--short)}
  .pxcard.gap.gok{border-top:3px solid var(--long)}
  .pxcard.gap.gmid{border-top:3px solid var(--gold)}
  .pxcard.gap.gbad{border-top:3px solid var(--short);background:rgba(255,100,112,.05)}
  .pxlabel{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--faint);margin-bottom:6px}
  .pxval{font-size:21px;font-weight:600;color:var(--text)}
  .pxcard.gap.gok .pxval{color:var(--long)}
  .pxcard.gap.gmid .pxval{color:var(--gold)}
  .pxcard.gap.gbad .pxval{color:var(--short)}
  .pxsub{font-size:11px;color:var(--muted);margin-top:5px;line-height:1.5}
  .pxwarn{font-size:10px;letter-spacing:.06em;color:var(--short);margin-top:6px;font-weight:600;text-transform:uppercase}
  .pxna{color:var(--faint);font-size:11px}

  /* ---- backtest & playbook ---- */
  .btbar{display:flex;align-items:center;gap:12px;margin-top:12px}
  .btbtn{background:var(--gold);color:#141200;font-family:var(--disp);font-weight:700;font-size:12px;
    padding:9px 16px;border-radius:6px;border:0;cursor:pointer;letter-spacing:.04em}
  .btbtn:disabled{opacity:.6;cursor:wait}
  .bthint{font-size:10px;color:var(--faint)}
  .btload{margin-top:12px;padding:12px 14px;background:var(--panel);border:1px solid var(--edge);
    border-radius:8px;color:var(--muted);font-size:12px;line-height:1.7}
  .btgrid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px;margin-top:12px}
  @media(max-width:1000px){.btgrid{grid-template-columns:repeat(3,minmax(0,1fr))}}
  .btstat{background:var(--panel);border:1px solid var(--edge);border-radius:8px;padding:10px 12px}
  .btval{font-size:16px;font-weight:600;margin-top:4px;color:var(--text)}
  .btrow2{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.4fr);gap:12px;margin-top:12px}
  @media(max-width:1000px){.btrow2{grid-template-columns:1fr}}
  .btpanel{background:var(--panel);border:1px solid var(--edge);border-radius:8px;padding:12px 14px}
  .decay{display:flex;align-items:flex-end;gap:8px;height:64px;margin-top:8px}
  .dcol{display:flex;flex-direction:column;align-items:center;gap:3px}
  .dcol i{display:block;width:20px;background:var(--gold);border-radius:2px 2px 0 0}
  .dcol i.neg{background:var(--short)}
  .dcol span{font-size:9px;color:var(--faint)}
  .liqrow{display:flex;gap:12px;flex-wrap:wrap;margin-top:6px;font-size:12px;font-weight:600}
  .playbook{border-left:3px solid var(--gold)}
  .pbtitle{font-family:var(--disp);font-weight:700;font-size:13px;letter-spacing:.06em;margin-bottom:10px}
  .pbtitle span{font-family:var(--mono);font-weight:400;font-size:11px;color:var(--muted)}
  .pbrow{display:grid;grid-template-columns:92px minmax(0,1fr);gap:10px;padding:7px 0;
    border-top:1px solid var(--edge);font-size:12px;line-height:1.65;color:var(--muted)}
  .pbrow>b:first-child{color:var(--faint);font-size:10px;letter-spacing:.1em;text-transform:uppercase;padding-top:2px}
  .pbrow b{color:var(--text)}
  .btwarn{margin-top:8px;padding:8px 10px;background:var(--short-dim);border-radius:6px;
    color:var(--short);font-size:11px;line-height:1.6}

  /* ---- ranking board ---- */
  #btboard{max-width:1280px;margin:0 auto;padding:8px 32px 0}
  @media(max-width:820px){#btboard{padding:8px 16px 0}}
  .boardwrap{background:var(--panel-2);border:1px solid var(--gold);border-radius:10px;
    padding:14px 16px;margin-bottom:16px}
  .boardhead{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;margin-bottom:10px}
  .boardstatus{font-size:11px;color:var(--gold)}
  .boardclose{font-size:11px;padding:5px 10px}
  .boardwrap thead th{cursor:default}

  /* ---- data availability badges ---- */
  .datacell{white-space:nowrap}
  .dchk{display:inline-block;font-size:10px;font-weight:600;padding:2px 5px;border-radius:4px;
    margin-right:3px;cursor:help;border:1px solid var(--edge)}
  .dchk.ok{color:var(--long);border-color:rgba(53,217,188,.35)}
  .dchk.bad{color:var(--short);border-color:rgba(255,100,112,.4);background:var(--short-dim)}
  .dchk.unk{color:var(--faint)}

  /* ---- profit calculator ---- */
  .calcgrid{display:grid;grid-template-columns:340px minmax(0,1fr);gap:16px;align-items:start}
  @media(max-width:900px){.calcgrid{grid-template-columns:1fr}}
  .cfield{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:5px 0}
  .cfield label{font-size:11px;color:var(--muted);flex:1}
  .cfield input{background:var(--panel-2);border:1px solid var(--edge);color:var(--text);
    font-family:var(--mono);font-size:12px;padding:6px 8px;border-radius:5px;width:120px;text-align:right}
  .cfield input:focus{outline:2px solid var(--gold);outline-offset:1px}
  .cstep{background:var(--panel);border:1px solid var(--edge);border-radius:10px;padding:16px 18px;margin-bottom:14px}
  .cstep .pbtitle{margin-bottom:10px}
  .cline{display:flex;justify-content:space-between;gap:14px;padding:6px 0;border-top:1px solid var(--edge);
    font-size:12px;align-items:baseline}
  .cline:first-of-type{border-top:0}
  .cline .clabel{color:var(--muted)}
  .cline .cformula{color:var(--faint);font-size:10px;display:block;margin-top:2px}
  .cline .cval{color:var(--text);font-weight:600;white-space:nowrap}
  .ctotal{display:flex;justify-content:space-between;align-items:center;padding-top:10px;margin-top:6px;
    border-top:2px solid var(--edge)}
  .ctotal .clabel{font-family:var(--disp);font-weight:700;font-size:13px;color:var(--text)}
  .ctotal .cval{font-family:var(--disp);font-weight:700;font-size:22px}
  .flowrow{display:flex;align-items:center;gap:0;margin-bottom:14px;flex-wrap:wrap}
  .flowcard{flex:1;min-width:160px;background:var(--panel-2);border:1px solid var(--edge);border-radius:8px;padding:12px 14px}
  .flowcard.l{border-top:3px solid var(--long)}
  .flowcard.s{border-top:3px solid var(--short)}
  .flowarrow{flex:0 0 40px;text-align:center;color:var(--gold);font-size:18px}
  .flowcard .fttl{font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--faint);margin-bottom:6px}
  .flowcard .fval{font-size:16px;font-weight:600}
  tr.browrank{cursor:pointer}
  tr.browdetail td{background:var(--panel-2);white-space:normal;overflow-wrap:anywhere}
  .browdetailwrap{padding:10px 14px;display:flex;flex-direction:column;gap:6px}
  .ddat{font-size:11px;color:var(--muted);line-height:1.7}
  .ddat b{color:var(--text)}


  /* ---- app shell / sidebar nav ---- */
  .applayout{display:flex;min-height:100vh}
  .sidenav{width:190px;flex:0 0 190px;background:var(--panel);border-right:1px solid var(--edge);
    padding:20px 0;position:sticky;top:0;height:100vh;overflow-y:auto}
  .sidebrand{padding:0 18px 18px;border-bottom:1px solid var(--edge);margin-bottom:10px}
  .sidebrand b{font-family:var(--disp);font-weight:700;font-size:15px;letter-spacing:.03em}
  .sidebrand b span{color:var(--gold)}
  .navitem{display:flex;align-items:center;gap:10px;padding:11px 18px;color:var(--muted);
    font-size:12px;cursor:pointer;border-left:3px solid transparent;line-height:1.4}
  .navitem .navnum{font-family:var(--disp);font-weight:700;font-size:11px;color:var(--faint);
    width:16px;flex:0 0 16px}
  .navitem:hover{color:var(--text);background:var(--panel-2)}
  .navitem.active{color:var(--gold);border-left-color:var(--gold);background:var(--panel-2)}
  .navitem.active .navnum{color:var(--gold)}
  .pagearea{flex:1;min-width:0}
  .page{display:none}
  .page.active{display:block}
  @media(max-width:820px){
    .applayout{flex-direction:column}
    .sidenav{width:100%;flex:none;height:auto;position:relative;padding:10px 0;
      display:flex;overflow-x:auto;overflow-y:visible}
    .sidebrand{display:none}
    .navitem{border-left:0;border-bottom:3px solid transparent;white-space:nowrap;padding:10px 14px}
    .navitem.active{border-left:0;border-bottom-color:var(--gold)}
  }

  @media(max-width:820px){
    header,main,.statusbar{padding-left:16px;padding-right:16px}
    .ticket-body{grid-template-columns:1fr}
    .leg.short{text-align:left;border-right:0;border-left:3px solid var(--short)}
    .capture{border:0;border-top:1px solid var(--edge);border-bottom:1px solid var(--edge)}
  }
</style>
</head>
<body>
<div class="applayout">
<nav class="sidenav">
  <div class="sidebrand"><b>FUNDING <span>CAPTURE</span></b></div>
  <div class="navitem active" data-page="page-arb"><span class="navnum">01</span>Live Arbitrage Table</div>
  <div class="navitem" data-page="page-backtest"><span class="navnum">02</span>Run Backtest</div>
  <div class="navitem" data-page="page-history"><span class="navnum">03</span>Coin Research</div>
  <div class="navitem" data-page="page-calc"><span class="navnum">04</span>Profit Calculator</div>
  <div class="navitem" data-page="page-explain"><span class="navnum">05</span>How This Trade Works</div>
  <div class="navitem" data-page="page-btexplain"><span class="navnum">06</span>How Backtesting Works</div>
</nav>
<div class="pagearea">

<div class="page active" id="page-arb">
<header>
  <div class="brand">
    <h1>FUNDING <span>CAPTURE</span></h1>
    <p>Cross-exchange funding-rate arbitrage · both legs perpetual futures · delta-neutral</p>
    <div class="legend">
      <span class="l"><i></i>LONG rate — cheap-funding leg</span>
      <span class="s"><i></i>SHORT rate — rich-funding leg</span>
      <span><i style="background:var(--gold)"></i>DIFF — what you capture per cycle</span>
    </div>
  </div>
  <div class="controls">
    <div class="ctl"><label for="usd">Position / leg (USD)</label><input id="usd" type="number" value="10000" min="100" step="500"></div>
    <div class="ctl"><label for="lev">Leverage / leg</label><input id="lev" type="number" value="3" min="1" max="50" step="1"></div>
    <div class="ctl"><label for="hold">Hold (cycles)</label><input id="hold" type="number" value="6" min="1" step="1"></div>
    <div class="ctl"><label for="minNet">Min diff %/cycle</label><input id="minNet" type="number" value="0.05" step="0.05"></div>
    <div class="ctl"><label for="exLong">Long venue</label>
      <select id="exLong"><option value="">All exchanges</option></select></div>
    <div class="ctl"><label for="exShort">Short venue</label>
      <select id="exShort"><option value="">All exchanges</option></select></div>
    <div class="ctl"><label>Rates shown as</label>
      <div class="seg">
        <button id="mCycle" aria-pressed="true">Per cycle</button>
        <button id="mAnnual" aria-pressed="false">Annual</button>
      </div>
    </div>
    <div class="ctl"><label>View</label>
      <div class="seg">
        <button id="vTable" aria-pressed="true">Table</button>
        <button id="vTickets" aria-pressed="false">Tickets</button>
      </div>
    </div>
    <div class="ctl"><label for="hideThin">&nbsp;</label>
      <button id="hideThin" class="ghost" aria-pressed="false">Hide thin liquidity</button></div>
    <div class="ctl"><label for="auto">&nbsp;</label>
      <button id="auto" class="ghost" aria-pressed="false">Auto-refresh 60s</button></div>
    <div class="ctl"><label for="chkN">Check top</label><input id="chkN" type="number" value="6" min="2" max="15" style="width:60px"></div>
    <div class="ctl"><label>&nbsp;</label><button id="chkData" class="ghost">✓ Check data</button></div>
    <div class="ctl"><label>&nbsp;</label><button id="refresh">Scan</button></div>
  </div>
</header>

<div class="statusbar">
  <span id="status">Loading arbitrage data…</span>
  <span id="count"></span>
  <span id="capinfo"></span>
  <span class="r-dim">Click any column header to sort</span>
</div>

<main id="list">
  <div class="empty">
    <b>Loading…</b><br>
    Fetching cross-exchange funding arbitrage pairs from CoinGlass.<br>
    If this never resolves, check the status bar above for the exact error.
  </div>
</main>
</div><!-- /page-arb -->

<div class="page" id="page-backtest">
  <header style="border-bottom:1px solid var(--edge);padding:28px 32px 20px">
  <div class="brand">
    <h1>RUN <span>BACKTEST</span></h1>
    <p>Replay funding, price & orderbook history for any pair, then rank the strongest candidates.</p>
  </div>
</header>
<main style="padding:20px 32px 60px;max-width:1280px;margin:0 auto">

  <div class="btpanel" style="margin-bottom:16px">
    <div class="pxlabel" style="margin-bottom:10px">Single pair</div>
    <div class="controls" style="gap:10px">
      <div class="ctl"><label for="bt2Pick">Pick from current scan</label>
        <select id="bt2Pick" style="width:230px"><option value="">— choose a live pair —</option></select></div>
      <div class="ctl"><label for="bt2Sym">or Symbol</label><input id="bt2Sym" placeholder="SUPRA" style="width:100px"></div>
      <div class="ctl"><label for="bt2Long">Long exchange</label><input id="bt2Long" placeholder="MEXC" style="width:110px"></div>
      <div class="ctl"><label for="bt2Short">Short exchange</label><input id="bt2Short" placeholder="Gate.io" style="width:110px"></div>
      <div class="ctl"><label for="bt2Usd">Position (USD)</label><input id="bt2Usd" type="number" value="10000" style="width:100px"></div>
      <div class="ctl"><label for="bt2Fee">Round-trip fee %</label><input id="bt2Fee" type="number" value="0.12" step="0.01" style="width:90px"></div>
      <div class="ctl"><label for="bt2Days">Lookback (days)</label><input id="bt2Days" type="number" value="21" min="7" max="60" style="width:80px"></div>
      <div class="ctl"><label>&nbsp;</label><button id="bt2Run">▶ RUN BACKTEST</button></div>
    </div>
  </div>

  <div id="bt2out"></div>

  <div class="btpanel" style="margin-top:20px">
    <div class="pxlabel" style="margin-bottom:10px">Rank many pairs</div>
    <div class="controls" style="gap:10px">
      <div class="ctl"><label>Depth check (applies to all backtests on this page)</label>
        <button id="depthTog" class="ghost" aria-pressed="false">OFF · numerical only</button></div>
      <div class="ctl"><label for="topN">Rank top</label><input id="topN" type="number" value="6" min="2" max="15" style="width:70px"></div>
      <div class="ctl"><label>&nbsp;</label><button id="btAll" class="ghost">🏆 Backtest & rank top N (from current scan)</button></div>
      <div class="ctl"><label>&nbsp;</label><button id="btAllShown" class="ghost">🏆 Backtest ALL shown pairs</button></div>
    </div>
    <div class="dsub" style="margin-top:6px">Uses the filters &amp; scan from page 01 — go there first if you haven't scanned yet.</div>
  </div>

  <section id="btboard" style="margin-top:16px"></section>
</main>
</div>

<div class="page" id="page-history">
  <header style="border-bottom:1px solid var(--edge);padding:28px 32px 20px">
  <div class="brand">
    <h1>COIN <span>RESEARCH</span></h1>
    <p>Search a coin to see its price, funding rate, open interest, volume and liquidations across every exchange CoinGlass tracks.</p>
  </div>
</header>
<main style="padding:20px 32px 60px;max-width:1280px;margin:0 auto">
  <div class="controls" style="gap:10px;margin-bottom:16px">
    <div class="ctl"><label for="hSym">Coin symbol</label><input id="hSym" placeholder="BTC" style="width:120px" value="BTC"></div>
    <div class="ctl"><label>&nbsp;</label><button id="hSearch">🔍 Search</button></div>
  </div>
  <div id="hOut"><div class="empty">Enter a coin symbol (e.g. BTC, ETH, SUPRA — not the trading pair, just the base asset) and press Search.</div></div>
</main>
</div>

<div class="page" id="page-explain">
  <header style="border-bottom:1px solid var(--edge);padding:28px 32px 20px">
  <div class="brand">
    <h1>HOW THIS <span>TRADE</span> WORKS</h1>
    <p>No crypto background needed — here's the whole idea in plain language.</p>
  </div>
</header>
<main style="padding:20px 32px 70px;max-width:900px;margin:0 auto;font-size:14px;line-height:1.8;color:var(--muted)">

  <div class="btpanel" style="margin-bottom:20px">
    <div class="pbtitle" style="margin-bottom:14px">THE BASIC IDEA</div>
    <p style="color:var(--text)">Some crypto exchanges let people bet on a coin's price without owning it — this is called a
    <b>"perpetual future"</b> or "perp." Because so many people want to bet the same direction at the same time, exchanges
    charge a small recurring fee — the <b>funding rate</b> — to whichever side is more crowded, and pay it to the other side.
    This resets the balance every few hours.</p>
    <p style="margin-top:10px">The trick: <b class="r-long">the SAME coin can have a different funding rate on two different exchanges</b>
    at the same time. If you're on the side that <i>gets paid</i> on both exchanges at once, you collect money from both —
    with the price risk cancelled out.</p>
  </div>

  <svg viewBox="0 0 820 260" style="width:100%;height:auto;background:var(--panel);border:1px solid var(--edge);border-radius:10px;margin-bottom:20px">
    <text x="410" y="28" text-anchor="middle" fill="#E8ECF4" font-family="Space Grotesk" font-size="15" font-weight="700">ONE COIN, TWO EXCHANGES, TWO BETS</text>
    <rect x="40" y="60" width="280" height="150" rx="10" fill="#0F1420" stroke="#35D9BC" stroke-width="2"/>
    <text x="180" y="88" text-anchor="middle" fill="#35D9BC" font-family="Space Grotesk" font-weight="700" font-size="13">EXCHANGE A</text>
    <text x="180" y="108" text-anchor="middle" fill="#7E89A3" font-size="11">funding rate: NEGATIVE</text>
    <text x="180" y="130" text-anchor="middle" fill="#E8ECF4" font-size="12">You go LONG (bet price rises)</text>
    <text x="180" y="152" text-anchor="middle" fill="#35D9BC" font-size="12" font-weight="600">→ you GET PAID to hold this</text>
    <text x="180" y="185" text-anchor="middle" fill="#4A5570" font-size="10">(the crowded side pays you)</text>
    <rect x="500" y="60" width="280" height="150" rx="10" fill="#0F1420" stroke="#FF6470" stroke-width="2"/>
    <text x="640" y="88" text-anchor="middle" fill="#FF6470" font-family="Space Grotesk" font-weight="700" font-size="13">EXCHANGE B</text>
    <text x="640" y="108" text-anchor="middle" fill="#7E89A3" font-size="11">funding rate: POSITIVE</text>
    <text x="640" y="130" text-anchor="middle" fill="#E8ECF4" font-size="12">You go SHORT (bet price falls)</text>
    <text x="640" y="152" text-anchor="middle" fill="#FF6470" font-size="12" font-weight="600">→ you GET PAID to hold this</text>
    <text x="640" y="185" text-anchor="middle" fill="#4A5570" font-size="10">(the crowded side pays you)</text>
    <path d="M320 135 L500 135" stroke="#F2C14E" stroke-width="2" stroke-dasharray="5,4" marker-end="url(#arrow)"/>
    <path d="M500 155 L320 155" stroke="#F2C14E" stroke-width="2" stroke-dasharray="5,4" marker-end="url(#arrow2)"/>
    <text x="410" y="225" text-anchor="middle" fill="#F2C14E" font-size="12" font-weight="600">Same coin, opposite bets → price moves CANCEL OUT. You just collect both payments.</text>
    <defs>
      <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" fill="none" stroke="#F2C14E"/></marker>
      <marker id="arrow2" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" fill="none" stroke="#F2C14E"/></marker>
    </defs>
  </svg>

  <div class="btpanel" style="margin-bottom:20px">
    <div class="pbtitle" style="margin-bottom:14px">THE TRADE LIFECYCLE</div>
    <svg viewBox="0 0 820 130" style="width:100%;height:auto">
      <text x="70" y="30" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="13">1. FIND</text>
      <text x="70" y="50" fill="#7E89A3" font-size="11">Scanner spots a coin with</text>
      <text x="70" y="65" fill="#7E89A3" font-size="11">different funding on 2 venues</text>
      <text x="270" y="30" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="13">2. CHECK</text>
      <text x="270" y="50" fill="#7E89A3" font-size="11">Is there enough order book</text>
      <text x="270" y="65" fill="#7E89A3" font-size="11">depth to actually fill both legs?</text>
      <text x="470" y="30" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="13">3. ENTER</text>
      <text x="470" y="50" fill="#7E89A3" font-size="11">Open long + short together,</text>
      <text x="470" y="65" fill="#7E89A3" font-size="11">30-60 min before settlement</text>
      <text x="670" y="30" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="13">4. HOLD & EXIT</text>
      <text x="670" y="50" fill="#7E89A3" font-size="11">Collect payments each cycle;</text>
      <text x="670" y="65" fill="#7E89A3" font-size="11">close both legs together</text>
      <line x1="10" y1="95" x2="810" y2="95" stroke="#222C47" stroke-width="2"/>
      <circle cx="70" cy="95" r="6" fill="#F2C14E"/><circle cx="270" cy="95" r="6" fill="#F2C14E"/>
      <circle cx="470" cy="95" r="6" fill="#F2C14E"/><circle cx="670" cy="95" r="6" fill="#F2C14E"/>
    </svg>
  </div>

  <div class="btpanel" style="margin-bottom:20px">
    <div class="pbtitle" style="margin-bottom:14px">WHY IT ISN'T FREE MONEY</div>
    <p><b class="r-short">The coin can still move a lot.</b> If it suddenly jumps up 30%, your "short" bet loses on paper —
    and because your long and short sit on <i>different</i> exchanges, that loss can trigger a forced exit
    (called <b>liquidation</b>) on one side before the matching profit on the other side can help.</p>
    <p style="margin-top:10px"><b class="r-short">The rate can flip or fade.</b> The payment you're earning isn't fixed —
    it can shrink, or even reverse, the very next cycle.</p>
    <p style="margin-top:10px"><b class="r-short">Getting in and out costs money.</b> Trading fees, and — on smaller coins —
    the risk that there isn't enough of a market to buy or sell your full size at a fair price.</p>
    <p style="margin-top:10px">This dashboard's job is to measure all three of those before you risk real money:
    the <b>Live Arbitrage Table</b> finds candidates, <b>Run Backtest</b> tests how they'd have performed historically
    including those costs, and <b>Coin Research</b> lets you sanity-check any coin by hand.</p>
  </div>

</main>
</div>

<div class="page" id="page-calc">
<header style="border-bottom:1px solid var(--edge);padding:28px 32px 20px">
  <div class="brand">
    <h1>PROFIT <span>CALCULATOR</span></h1>
    <p>Every line of the math shown explicitly — funding on each leg, the price-gap trade, fees, and what's left over.</p>
  </div>
</header>
<main style="padding:20px 32px 70px;max-width:1280px;margin:0 auto">

  <div class="controls" style="gap:10px;margin-bottom:16px">
    <div class="ctl"><label for="cPick">Load a live pair</label>
      <select id="cPick" style="width:260px"><option value="">— manual entry —</option></select></div>
    <div class="ctl"><label>&nbsp;</label><button id="cFetchPx" class="ghost">📡 Fetch live prices &amp; OI for this pair</button></div>
    <div class="ctl"><label>&nbsp;</label><span id="cFetchStatus" class="dsub"></span></div>
  </div>

  <div class="calcgrid">
    <div class="btpanel">
      <div class="pxlabel" style="margin-bottom:10px">1 · POSITION &amp; LEVERAGE</div>
      <div class="cfield"><label>Symbol</label><input id="cSym" value="COIN"></div>
      <div class="cfield"><label>Position size per leg (USD)</label><input id="cUsd" type="number" value="10000" step="500"></div>
      <div class="cfield"><label>Leverage per leg</label><input id="cLev" type="number" value="3" min="1" step="1"></div>

      <div class="pxlabel" style="margin:16px 0 10px">2 · FUNDING RATES</div>
      <div class="cfield"><label>Long venue name</label><input id="cLongEx" value="Exchange A"></div>
      <div class="cfield"><label>Long funding rate (%/settlement)</label><input id="cLongRate" type="number" value="-0.50" step="0.01"></div>
      <div class="cfield"><label>Long settles every (hours)</label><input id="cLongInt" type="number" value="4" min="1" step="1"></div>
      <div class="cfield"><label>Short venue name</label><input id="cShortEx" value="Exchange B"></div>
      <div class="cfield"><label>Short funding rate (%/settlement)</label><input id="cShortRate" type="number" value="0.02" step="0.01"></div>
      <div class="cfield"><label>Short settles every (hours)</label><input id="cShortInt" type="number" value="8" min="1" step="1"></div>

      <div class="pxlabel" style="margin:16px 0 10px">3 · PRICES — FUTURES (per venue) &amp; UNDERLYING/INDEX</div>
      <div class="cfield"><label>Long price at entry</label><input id="cPxLE" type="number" value="1.0000" step="0.0001"></div>
      <div class="cfield"><label>Short price at entry</label><input id="cPxSE" type="number" value="1.0000" step="0.0001"></div>
      <div class="cfield"><label>Index/spot price at entry</label><input id="cPxIE" type="number" value="1.0000" step="0.0001"></div>
      <div class="cfield"><label>Long price at exit</label><input id="cPxLX" type="number" value="1.0000" step="0.0001"></div>
      <div class="cfield"><label>Short price at exit</label><input id="cPxSX" type="number" value="1.0000" step="0.0001"></div>
      <div class="cfield"><label>Index/spot price at exit</label><input id="cPxIX" type="number" value="1.0000" step="0.0001"></div>
      <div class="cfield"><label>Long venue open interest (USD)</label><input id="cOiL" type="number" value="" placeholder="auto or manual"></div>
      <div class="cfield"><label>Short venue open interest (USD)</label><input id="cOiS" type="number" value="" placeholder="auto or manual"></div>

      <div class="pxlabel" style="margin:16px 0 10px">4 · HOLD &amp; COSTS</div>
      <div class="cfield"><label>Hold duration (hours)</label><input id="cHold" type="number" value="24" min="1" step="1"></div>
      <div class="cfield"><label>Round-trip fee, both legs (%)</label><input id="cFee" type="number" value="0.12" step="0.01"></div>
    </div>

    <div id="cOut"></div>
  </div>
</main>
</div><!-- /page-calc -->

<div class="page" id="page-btexplain">
<header style="border-bottom:1px solid var(--edge);padding:28px 32px 20px">
  <div class="brand">
    <h1>HOW <span>BACKTESTING</span> WORKS</h1>
    <p>No trading or coding background needed — here's exactly what happens when you click "Run Backtest."</p>
  </div>
</header>
<main style="padding:20px 32px 70px;max-width:900px;margin:0 auto;font-size:14px;line-height:1.8;color:var(--muted)">

  <div class="btpanel" style="margin-bottom:20px">
    <div class="pbtitle" style="margin-bottom:14px">THE BASIC IDEA</div>
    <p style="color:var(--text)">A backtest is a <b>rehearsal using the past</b>. Instead of risking real money to find out
    whether a trade idea works, the computer replays several weeks of real history — what the funding rate actually was,
    what the price actually did, hour by hour — and pretends to take the trade every time the opportunity would have
    appeared. At the end, it tallies up what would have happened.</p>
    <p style="margin-top:10px">Think of it like a flight simulator: you find out how the plane handles turbulence
    <b>before</b> you're actually in the air.</p>
  </div>

  <div class="btpanel" style="margin-bottom:20px">
    <div class="pbtitle" style="margin-bottom:14px">THE FIVE STEPS</div>
    <svg viewBox="0 0 820 300" style="width:100%;height:auto">
      <text x="410" y="24" text-anchor="middle" fill="#E8ECF4" font-family="Space Grotesk" font-weight="700" font-size="14">WALKING THROUGH THE PAST, ONE HOUR AT A TIME</text>

      <rect x="10" y="45" width="150" height="120" rx="8" fill="#0F1420" stroke="#F2C14E" stroke-width="1.5"/>
      <text x="85" y="65" text-anchor="middle" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="12">1. GATHER</text>
      <text x="85" y="88" text-anchor="middle" fill="#7E89A3" font-size="10">Download weeks of real</text>
      <text x="85" y="102" text-anchor="middle" fill="#7E89A3" font-size="10">funding rates &amp; prices</text>
      <text x="85" y="116" text-anchor="middle" fill="#7E89A3" font-size="10">from both exchanges</text>

      <rect x="175" y="45" width="150" height="120" rx="8" fill="#0F1420" stroke="#F2C14E" stroke-width="1.5"/>
      <text x="250" y="65" text-anchor="middle" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="12">2. WALK FORWARD</text>
      <text x="250" y="88" text-anchor="middle" fill="#7E89A3" font-size="10">Step through history</text>
      <text x="250" y="102" text-anchor="middle" fill="#7E89A3" font-size="10">hour by hour, checking:</text>
      <text x="250" y="116" text-anchor="middle" fill="#7E89A3" font-size="10">"is the gap big enough</text>
      <text x="250" y="130" text-anchor="middle" fill="#7E89A3" font-size="10">right now?"</text>

      <rect x="340" y="45" width="150" height="120" rx="8" fill="#0F1420" stroke="#F2C14E" stroke-width="1.5"/>
      <text x="415" y="65" text-anchor="middle" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="12">3. PRETEND TRADE</text>
      <text x="415" y="88" text-anchor="middle" fill="#7E89A3" font-size="10">Found a good moment?</text>
      <text x="415" y="102" text-anchor="middle" fill="#7E89A3" font-size="10">"Open" both legs there,</text>
      <text x="415" y="116" text-anchor="middle" fill="#7E89A3" font-size="10">collect the real historical</text>
      <text x="415" y="130" text-anchor="middle" fill="#7E89A3" font-size="10">payments, then "close"</text>

      <rect x="505" y="45" width="150" height="120" rx="8" fill="#0F1420" stroke="#F2C14E" stroke-width="1.5"/>
      <text x="580" y="65" text-anchor="middle" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="12">4. GRADE IT</text>
      <text x="580" y="88" text-anchor="middle" fill="#7E89A3" font-size="10">Money collected, minus</text>
      <text x="580" y="102" text-anchor="middle" fill="#7E89A3" font-size="10">fees and any price moves</text>
      <text x="580" y="116" text-anchor="middle" fill="#7E89A3" font-size="10">against you = win or loss</text>
      <text x="580" y="130" text-anchor="middle" fill="#7E89A3" font-size="10">for that one attempt</text>

      <rect x="660" y="45" width="150" height="120" rx="8" fill="#0F1420" stroke="#F2C14E" stroke-width="1.5"/>
      <text x="735" y="65" text-anchor="middle" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="12">5. SUM IT UP</text>
      <text x="735" y="88" text-anchor="middle" fill="#7E89A3" font-size="10">Repeat for every moment</text>
      <text x="735" y="102" text-anchor="middle" fill="#7E89A3" font-size="10">found, then average the</text>
      <text x="735" y="116" text-anchor="middle" fill="#7E89A3" font-size="10">results into a report</text>
      <text x="735" y="130" text-anchor="middle" fill="#7E89A3" font-size="10">and a recommendation</text>

      <path d="M160 105 L175 105" stroke="#4A5570" stroke-width="2" marker-end="url(#a1)"/>
      <path d="M325 105 L340 105" stroke="#4A5570" stroke-width="2" marker-end="url(#a2)"/>
      <path d="M490 105 L505 105" stroke="#4A5570" stroke-width="2" marker-end="url(#a3)"/>
      <path d="M655 105 L660 105" stroke="#4A5570" stroke-width="2" marker-end="url(#a4)"/>
      <defs>
        <marker id="a1" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" fill="none" stroke="#4A5570"/></marker>
        <marker id="a2" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" fill="none" stroke="#4A5570"/></marker>
        <marker id="a3" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" fill="none" stroke="#4A5570"/></marker>
        <marker id="a4" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" fill="none" stroke="#4A5570"/></marker>
      </defs>

      <line x1="20" y1="220" x2="800" y2="220" stroke="#222C47" stroke-width="2"/>
      <circle cx="120" cy="220" r="5" fill="#4A5570"/>
      <circle cx="260" cy="220" r="8" fill="#35D9BC"/>
      <circle cx="400" cy="220" r="5" fill="#4A5570"/>
      <circle cx="520" cy="220" r="8" fill="#35D9BC"/>
      <circle cx="700" cy="220" r="5" fill="#4A5570"/>
      <text x="260" y="245" text-anchor="middle" fill="#35D9BC" font-size="11" font-weight="600">gap was wide → trade taken</text>
      <text x="520" y="245" text-anchor="middle" fill="#35D9BC" font-size="11" font-weight="600">gap was wide → trade taken</text>
      <text x="410" y="270" text-anchor="middle" fill="#7E89A3" font-size="11">Timeline of real history — most hours nothing happens; the backtest only "trades" at the moments the rule would have fired</text>
    </svg>
  </div>

  <div class="btpanel" style="margin-bottom:20px">
    <div class="pbtitle" style="margin-bottom:14px">READING THE REPORT — IN PLAIN WORDS</div>
    <p><b style="color:var(--text)">Trades</b> — how many separate times, over the whole history window, the opportunity
    appeared and was actually taken. A pair that only qualified twice tells you very little; one that qualified
    15 times is a pattern you can trust more.</p>
    <p style="margin-top:8px"><b style="color:var(--text)">Win rate</b> — out of those attempts, what percentage came out
    ahead after every cost was subtracted. 100% win rate on 2 trades might just be luck; 70% on 15 trades is a real edge.</p>
    <p style="margin-top:8px"><b style="color:var(--text)">Edge decay chart</b> — a set of bars showing how big the
    opportunity typically was 1 cycle after entering, 2 cycles after, 3 cycles after… Falling bars mean the free money
    fades fast, which tells the computer (and you) how long to actually hold before it's not worth it anymore.</p>
    <p style="margin-top:8px"><b style="color:var(--text)">Liquidation frequency</b> — out of all those historical
    moments, how often would the price have swung far enough to wipe out your position at a given leverage. This is
    measured from what the price <i>actually did</i> historically, not guessed.</p>
    <p style="margin-top:8px">All of this feeds one final output: a <b style="color:var(--text)">recommended playbook</b> —
    what size to trade, how much leverage, how long to hold, and where to set alerts — built directly from what
    actually happened in the past, not a guess.</p>
  </div>

  <div class="btpanel">
    <div class="pbtitle" style="margin-bottom:14px">WHY A BACKTEST ISN'T A GUARANTEE</div>
    <svg viewBox="0 0 780 90" style="width:100%;height:auto;margin-bottom:12px">
      <text x="15" y="20" fill="#F2C14E" font-family="Space Grotesk" font-weight="700" font-size="12">The decay curve — a real example</text>
      <g>
        <rect x="30" y="30" width="34" height="45" fill="#F2C14E"/>
        <rect x="90" y="45" width="34" height="30" fill="#F2C14E"/>
        <rect x="150" y="58" width="34" height="17" fill="#F2C14E"/>
        <rect x="210" y="66" width="34" height="9" fill="#FF6470"/>
        <rect x="270" y="70" width="34" height="5" fill="#FF6470"/>
        <text x="47" y="88" text-anchor="middle" fill="#7E89A3" font-size="9">cycle 1</text>
        <text x="107" y="88" text-anchor="middle" fill="#7E89A3" font-size="9">cycle 2</text>
        <text x="167" y="88" text-anchor="middle" fill="#7E89A3" font-size="9">cycle 3</text>
        <text x="227" y="88" text-anchor="middle" fill="#7E89A3" font-size="9">cycle 4</text>
        <text x="287" y="88" text-anchor="middle" fill="#7E89A3" font-size="9">cycle 5</text>
      </g>
      <text x="340" y="55" fill="#7E89A3" font-size="11">← the opportunity shrinks the longer you hold it,</text>
      <text x="340" y="72" fill="#7E89A3" font-size="11">   which is exactly what the backtest measures for you</text>
    </svg>
    <p>A backtest can only ever tell you what <b class="r-short">would have happened</b> under the exact past conditions
    it was given. A few honest limits worth knowing:</p>
    <p style="margin-top:8px">• <b style="color:var(--text)">It only sees coins that still exist today.</b> If a coin
    crashed or got delisted after a wild funding spike, it's gone from the list of things being tested — so the very
    worst outcomes are quietly missing from the history.</p>
    <p style="margin-top:6px">• <b style="color:var(--text)">The future can differ from the past.</b> A pattern that
    repeated 10 times in the last month might not repeat next month — markets change.</p>
    <p style="margin-top:6px">• <b style="color:var(--text)">Some numbers are estimates, not certainties</b> — like how
    much the price would move when your order tries to fill (slippage), which is modeled mathematically rather than
    lived through for real.</p>
    <p style="margin-top:10px">Treat the backtest as a <b style="color:var(--text)">well-informed guide, not a promise</b>.
    It's far better than trading on a gut feeling or a single flashy number — but it's still describing the past.</p>
  </div>

</main>
</div><!-- /page-btexplain -->

</div><!-- /pagearea -->
</div><!-- /applayout -->

<script>
const $=id=>document.getElementById(id);
let rows=[], timer=null, view="table", rateMode="cycle";   // "cycle" | "annual"
let lastList=[], lastUsd=10000;
let sortState={key:"apr", dir:-1};   // dir -1 = descending

// Annualize a per-cycle funding rate given its settlement interval in hours.
// rate %/cycle × cycles/day × 365. Assumes the rate persists — it usually won't,
// so treat annualized figures as a ranking signal, not a forecast.
const annualize = (rate, intervalH) => rate * (24/(intervalH||8)) * 365;
const fmtBig = n => Number(n).toLocaleString(undefined,{maximumFractionDigits:0});
const pctA = n => (n>=0?"+":"")+fmtBig(n)+"%";

const fmt$ = n => "$"+Number(n).toLocaleString(undefined,{maximumFractionDigits:0});
const fmt$2 = n => "$"+Number(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
const pct = n => (n>=0?"+":"")+Number(n).toFixed(3)+"%";
function oiShort(n){
  if(n>=1e9) return "$"+(n/1e9).toFixed(2)+"B";
  if(n>=1e6) return "$"+(n/1e6).toFixed(2)+"M";
  return "$"+(n/1e3).toFixed(0)+"k";
}

async function scan(){
  const usd = Math.max(100, Number($("usd").value)||10000);
  $("status").textContent = "Scanning…"; $("status").className="";
  try{
    const r = await fetch("/api/arb?usd="+usd);
    const j = await r.json();
    if(String(j.code)!=="0") throw new Error(j.msg||"API error (code "+j.code+")");
    rows = j.data||[];
    populateExchanges();
    $("status").textContent = "Last scan "+new Date().toLocaleTimeString();
    render();
  }catch(e){
    let hint = "";
    const m = String(e.message).toLowerCase();
    if(m.includes("401")||m.includes("unauthorized")||m.includes("api key")) hint = " — check the API key";
    else if(m.includes("plan")||m.includes("upgrade")||m.includes("permission")) hint = " — this endpoint may need a higher CoinGlass plan";
    else if(m.includes("limit")) hint = " — rate limit hit, wait a minute";
    $("status").textContent = "Error: "+e.message+hint;
    $("status").className="err";
    $("list").innerHTML = '<div class="empty"><b>Could not load data.</b><br>'+e.message+hint+'</div>';
  }
}

function derived(d, usd){
  const lev = Math.max(1, Number($("lev").value)||3);
  const holdN = Math.max(1, Math.round(Number($("hold").value)||6));
  const minOI = Math.min(d.buy?.open_interest_usd??0, d.sell?.open_interest_usd??0);
  const perCycleUsd = usd * d.funding/100;
  const entryCostPct = d.fee + Math.max(0, d.spread);
  const breakeven = d.funding>0 ? entryCostPct/d.funding : Infinity;
  const mismatch = d.buy?.funding_rate_interval !== d.sell?.funding_rate_interval;
  const thin = minOI < usd*20;
  // --- expected profit over the holding period ---
  // capital: margin posted on BOTH exchanges
  const capital = 2 * usd / lev;
  // gross funding collected over N cycles (assumes current diff persists)
  const grossN = usd * d.funding/100 * holdN;
  // round-trip costs: open + close fees (2 × API fee) plus adverse spread if positive
  const costs = usd * (2*d.fee + Math.max(0, d.spread)) / 100;
  const profit = grossN - costs;
  const roc = capital>0 ? profit/capital*100 : 0;
  return {minOI, perCycleUsd, breakeven, mismatch, thin, lev, holdN, capital, profit, roc};
}

const SORTERS = {
  symbol:(a,b)=> a.d.symbol.localeCompare(b.d.symbol),
  longRate:(a,b)=> rateMode==="annual"
    ? annualize(a.d.buy.funding_rate,a.d.buy.funding_rate_interval) - annualize(b.d.buy.funding_rate,b.d.buy.funding_rate_interval)
    : a.d.buy.funding_rate - b.d.buy.funding_rate,
  shortRate:(a,b)=> rateMode==="annual"
    ? annualize(a.d.sell.funding_rate,a.d.sell.funding_rate_interval) - annualize(b.d.sell.funding_rate,b.d.sell.funding_rate_interval)
    : a.d.sell.funding_rate - b.d.sell.funding_rate,
  funding:(a,b)=> a.d.funding - b.d.funding,
  fee:(a,b)=> a.d.fee - b.d.fee,
  spread:(a,b)=> a.d.spread - b.d.spread,
  apr:(a,b)=> a.d.apr - b.d.apr,
  oi:(a,b)=> a.x.minOI - b.x.minOI,
  cycle:(a,b)=> a.x.perCycleUsd - b.x.perCycleUsd,
  profit:(a,b)=> a.x.profit - b.x.profit,
  roc:(a,b)=> a.x.roc - b.x.roc,
  time:(a,b)=> a.d.next_funding_time - b.d.next_funding_time,
};

function populateExchanges(){
  const fill=(sel, vals)=>{
    const cur = sel.value;
    sel.innerHTML = '<option value="">All exchanges</option>'+
      vals.map(v=>`<option value="${v}">${v}</option>`).join("");
    if(vals.includes(cur)) sel.value = cur;
  };
  fill($("exLong"), [...new Set(rows.map(d=>d.buy?.exchange).filter(Boolean))].sort());
  fill($("exShort"), [...new Set(rows.map(d=>d.sell?.exchange).filter(Boolean))].sort());
}

function getList(){
  const usd = Math.max(100, Number($("usd").value)||10000);
  const minNet = Number($("minNet").value)||0;
  const hideThin = $("hideThin").getAttribute("aria-pressed")==="true";
  const exL = $("exLong").value, exS = $("exShort").value;
  let list = rows.filter(d=>d.funding>=minNet)
    .filter(d=>!exL || d.buy?.exchange===exL)
    .filter(d=>!exS || d.sell?.exchange===exS)
    .map(d=>({d, x:derived(d,usd)}));
  if(hideThin) list = list.filter(o=>!o.x.thin);
  const cmp = SORTERS[sortState.key]||SORTERS.apr;
  list.sort((a,b)=> sortState.dir * cmp(a,b));
  return {list, usd};
}

// Summary of which long→short exchange routes appear, with counts and best diff
function routeSummary(){
  const usd = Math.max(100, Number($("usd").value)||10000);
  const minNet = Number($("minNet").value)||0;
  const map = new Map();
  rows.filter(d=>d.funding>=minNet).forEach(d=>{
    const k = d.buy.exchange+"|"+d.sell.exchange;
    const e = map.get(k)||{long:d.buy.exchange, short:d.sell.exchange, n:0, best:0, bestSym:""};
    e.n++;
    if(d.funding>e.best){ e.best=d.funding; e.bestSym=d.symbol; }
    map.set(k,e);
  });
  const routes=[...map.values()].sort((a,b)=>b.n-a.n).slice(0,12);
  if(!routes.length) return "";
  const active = $("exLong").value+"|"+$("exShort").value;
  return `<div class="routes">
    <span class="routes-label">Routes (long → short)</span>
    ${routes.map(r=>{
      const on = active===(r.long+"|"+r.short);
      return `<button class="route${on?" on":""}" data-l="${r.long}" data-s="${r.short}"
        title="best: ${r.bestSym} +${r.best.toFixed(3)}%/cycle">
        <b class="rl">${r.long}</b> → <b class="rs">${r.short}</b> <span>×${r.n}</span></button>`;
    }).join("")}
  </div>`;
}

const COLS = () => [
  {key:"symbol", label:"Symbol", cls:"txt"},
  {key:"longRate", label: rateMode==="annual" ? "Long APR / venue" : "Long rate / venue"},
  {key:"shortRate", label: rateMode==="annual" ? "Short APR / venue" : "Short rate / venue"},
  {key: rateMode==="annual" ? "apr" : "funding", label: rateMode==="annual" ? "Diff APR %" : "Diff %/cycle"},
  {key:"cycle", label: rateMode==="annual" ? "$ / year" : "$ / cycle"},
  {key:"profit", label:"Est. profit"},
  {key:"roc", label:"ROC %"},
  {key:"fee", label:"Fee %"},
  {key:"spread", label:"Spread %"},
  {key: rateMode==="annual" ? "funding" : "apr", label: rateMode==="annual" ? "Diff %/cycle" : "APR %"},
  {key:"oi", label:"Min OI"},
  {key:"time", label:"Next funding"},
  {key:null, label:"Data"},
  {key:null, label:"Flags"},
];

function render(){
  const {list, usd} = getList();
  $("count").textContent = list.length+" of "+rows.length+" pairs shown";
  const lev = Math.max(1, Number($("lev").value)||3);
  const holdN = Math.max(1, Math.round(Number($("hold").value)||6));
  $("capinfo").textContent = "Capital: "+fmt$(2*usd/lev)+" ("+lev+"x per leg) · profit est. over "
    +holdN+" cycles incl. open+close fees · liq buffer ≈ ±"+(100/lev).toFixed(0)+"% price move";
  const el = $("list");
  if(!rows.length){ return; }
  if(!list.length){
    el.innerHTML = '<div class="empty"><b>Nothing matches your filters.</b><br>Lower the minimum diff or allow thin liquidity.</div>';
    return;
  }
  el.innerHTML = routeSummary() + (view==="table" ? renderTable(list, usd) : renderTickets(list, usd));
  lastList = list; lastUsd = usd;
  el.querySelectorAll("tbody tr.datarow").forEach(tr=>{
    tr.addEventListener("click",()=>toggleDepth(tr));
  });
  el.querySelectorAll(".route").forEach(btn=>{
    btn.addEventListener("click",()=>{
      const l=btn.dataset.l, s=btn.dataset.s;
      const already = $("exLong").value===l && $("exShort").value===s;
      $("exLong").value = already ? "" : l;
      $("exShort").value = already ? "" : s;
      render();
    });
  });
  if(view==="table"){
    el.querySelectorAll("thead th[data-key]").forEach(th=>{
      th.addEventListener("click",()=>{
        const k = th.dataset.key;
        if(sortState.key===k) sortState.dir *= -1;
        else sortState = {key:k, dir: k==="symbol"||k==="time" ? 1 : -1};
        render();
      });
    });
  }
  tickCountdowns();
}

function flagsFor(d,x){
  const f=[];
  if(x.thin) f.push("THIN LIQ");
  if(x.mismatch) f.push("INTERVAL");
  if(Math.abs(d.spread) > d.funding) f.push("SPREAD");
  return f;
}

function renderTable(list, usd){
  const cols = COLS();
  const head = cols.map(c=>{
    if(!c.key) return `<th class="txt">${c.label}</th>`;
    const arrow = sortState.key===c.key ? `<span class="dir">${sortState.dir<0?"▼":"▲"}</span>` : "";
    return `<th data-key="${c.key}" class="${c.cls||""}">${c.label} ${arrow}</th>`;
  }).join("");

  const A = rateMode==="annual";
  const body = list.map(({d,x})=>{
    const fl = flagsFor(d,x);
    const longVal  = A ? pctA(annualize(d.buy.funding_rate,  d.buy.funding_rate_interval))  : pct(d.buy.funding_rate);
    const shortVal = A ? pctA(annualize(d.sell.funding_rate, d.sell.funding_rate_interval)) : pct(d.sell.funding_rate);
    const diffMain = A ? pctA(d.apr) : pct(d.funding);
    const usdVal   = A ? "$"+fmtBig(usd*d.apr/100)+"/yr" : fmt$2(x.perCycleUsd);
    const diffAlt  = A ? pct(d.funding) : fmtBig(d.apr);
    return `<tr class="datarow" data-sym="${d.symbol}" title="Click to check orderbook depth for both legs">
      <td class="txt sym-cell"><span class="chev">▸</span> ${d.symbol}</td>
      <td><span class="r-long">${longVal}</span>
          <span class="ex">${d.buy.exchange} · ${d.buy.funding_rate_interval}h</span></td>
      <td><span class="r-short">${shortVal}</span>
          <span class="ex">${d.sell.exchange} · ${d.sell.funding_rate_interval}h</span></td>
      <td class="r-diff">${diffMain}</td>
      <td>${usdVal}</td>
      <td class="${x.profit>=0?'r-long':'r-short'}">${x.profit>=0?"+":"−"}$${fmtBig(Math.abs(x.profit))}</td>
      <td class="${x.roc>=0?'r-long':'r-short'}">${(x.roc>=0?"+":"")+x.roc.toFixed(1)}</td>
      <td class="r-dim">${d.fee}</td>
      <td class="r-dim">${pct(d.spread)}</td>
      <td class="r-apr">${diffAlt}</td>
      <td class="r-dim">${oiShort(x.minOI)}</td>
      <td class="countdown" data-t="${d.next_funding_time}"><b>—</b></td>
      <td class="txt datacell">${dataBadge(d.symbol)}</td>
      <td class="txt warn-cell">${fl.join(" · ")}</td>
    </tr>`;
  }).join("");

  return `<div class="tablewrap"><table>
    <thead><tr>${head}</tr></thead>
    <tbody>${body}</tbody>
  </table></div>`;
}

function renderTickets(list, usd){
  const A = rateMode==="annual";
  return list.map(({d,x})=>{
    const fl = flagsFor(d,x).map(t=>`<span class="flag warn">${t}</span>`).join("");
    const longR  = A ? pctA(annualize(d.buy.funding_rate,  d.buy.funding_rate_interval))+" APR"  : pct(d.buy.funding_rate);
    const shortR = A ? pctA(annualize(d.sell.funding_rate, d.sell.funding_rate_interval))+" APR" : pct(d.sell.funding_rate);
    const capMain = A ? pctA(d.apr)+" APR" : pct(d.funding);
    const capUsd  = A ? "$"+fmtBig(usd*d.apr/100)+"/yr on "+fmt$(usd)+"/leg" : fmt$2(x.perCycleUsd)+" on "+fmt$(usd)+"/leg";
    return `
    <article class="ticket">
      <div class="ticket-head">
        <span class="sym">${d.symbol}</span>
        <span class="apr">APR ${Number(d.apr).toLocaleString(undefined,{maximumFractionDigits:0})}%</span>
        <span class="flags">${fl}</span>
        <span class="countdown" data-t="${d.next_funding_time}">next funding <b>—</b></span>
      </div>
      <div class="ticket-body">
        <div class="leg long">
          <div class="side">LONG · PAY LESS / GET PAID</div>
          <div class="ex2">${d.buy.exchange}</div>
          <div class="rate">${longR}</div>
          <div class="sub">every ${d.buy.funding_rate_interval}h · OI ${oiShort(d.buy.open_interest_usd)}</div>
        </div>
        <div class="capture">
          <div class="cap-label">Net capture${A?" (annualized)":" / cycle"}</div>
          <div class="net">${capMain}</div>
          <div>${capUsd}</div>
          <div class="be">fees ${d.fee}% · spread ${pct(d.spread)}<br>
            breakeven ${isFinite(x.breakeven)?x.breakeven.toFixed(1)+" cycles":"—"}<br>
            est. ${x.holdN} cycles @ ${x.lev}x: <b style="color:${x.profit>=0?'var(--long)':'var(--short)'}">
            ${x.profit>=0?"+":"−"}$${fmtBig(Math.abs(x.profit))}</b> (${(x.roc>=0?"+":"")+x.roc.toFixed(1)}% on ${fmt$(x.capital)})</div>
        </div>
        <div class="leg short">
          <div class="side">COLLECT FUNDING · SHORT</div>
          <div class="ex2">${d.sell.exchange}</div>
          <div class="rate">${shortR}</div>
          <div class="sub">every ${d.sell.funding_rate_interval}h · OI ${oiShort(d.sell.open_interest_usd)}</div>
        </div>
      </div>
    </article>`;
  }).join("");
}

// ---- orderbook depth check (click a table row) ----
const NCOLS = 14;

async function fetchDepth(exchange, coin){
  // The backend looks up this exchange's real instrument_id from
  // /api/futures/supported-exchange-pairs, then queries the orderbook
  // (1h granularity first, then 1d).
  try{
    const r = await fetch(`/api/orderbook?exchange=${encodeURIComponent(exchange)}&coin=${encodeURIComponent(coin)}`);
    const j = await r.json();
    if(String(j.code)==="0" && Array.isArray(j.data) && j.data.length){
      return {latest: j.data[j.data.length-1], interval: j.interval, pair: j.symbol_used};
    }
    return {error: (j.msg||"no data")+(j.tried?" · tried: "+j.tried.join(", "):"")};
  }catch(e){
    return {error: e.message};
  }
}

function toggleDepth(tr){
  const next = tr.nextElementSibling;
  if(next && next.classList.contains("depthrow")){ next.remove(); tr.querySelector(".chev").textContent="▸"; return; }
  // close any other open one
  document.querySelectorAll(".depthrow").forEach(x=>x.remove());
  document.querySelectorAll(".chev").forEach(c=>c.textContent="▸");
  tr.querySelector(".chev").textContent="▾";

  const sym = tr.dataset.sym;
  const item = lastList.find(o=>o.d.symbol===sym);
  if(!item) return;
  const d = item.d;

  const row = document.createElement("tr");
  row.className = "depthrow";
  row.innerHTML = `<td colspan="${NCOLS}"><div class="depth-loading">Checking orderbook depth on ${d.buy.exchange} and ${d.sell.exchange}…</div></td>`;
  tr.after(row);
  loadDepth(row, d);
}

async function loadDepth(row, d){
  const [longOb, shortOb, px] = await Promise.all([
    fetchDepth(d.buy.exchange, d.symbol),
    fetchDepth(d.sell.exchange, d.symbol),
    fetch(`/api/prices?symbol=${encodeURIComponent(d.symbol)}&long=${encodeURIComponent(d.buy.exchange)}&short=${encodeURIComponent(d.sell.exchange)}`)
      .then(r=>r.json()).catch(()=>null),
  ]);
  row.querySelector("td").innerHTML = depthPanel(d, longOb, shortOb, px);
  setDataStatus(d, longOb, shortOb, px);
}

function fmtPx(p){
  if(p==null) return "—";
  const dp = p>=1000?2 : p>=1?4 : 6;
  return "$"+Number(p).toLocaleString(undefined,{maximumFractionDigits:dp});
}

function priceStrip(d, px){
  if(!px || String(px.code)!=="0" || !px.long || !px.short || px.mid==null){
    return `<div class="pxgrid"><div class="pxcard na" style="grid-column:1/-1">
      <div class="pxlabel">Venue prices</div>
      <div class="pxna">Unavailable — check top-of-book on both exchanges before firing.</div>
    </div></div>`;
  }
  const disc = px.discrepancy_pct;
  const favorable = disc >= 0;
  const holdN = Math.max(1, Math.round(Number($("hold").value)||6));
  const fundingOverHold = d.funding*holdN;
  const eats = Math.abs(disc) > fundingOverHold;
  const idx = px.long.index_price ?? px.short.index_price;
  const devL = idx ? (px.long.price-idx)/idx*100 : null;
  const devS = idx ? (px.short.price-idx)/idx*100 : null;
  const vol = v => v!=null ? oiShort(v)+" 24h vol" : "vol n/a";
  const dev = v => v==null ? "" : ` · ${(v>=0?"+":"")+v.toFixed(2)}% vs index`;
  const oiL = px.long.oi_usd, oiS = px.short.oi_usd;
  const pctL = oiL ? lastUsd/oiL*100 : null;
  const pctS = oiS ? lastUsd/oiS*100 : null;
  const worstPct = [pctL, pctS].filter(x=>x!=null).length ? Math.max(...[pctL,pctS].filter(x=>x!=null)) : null;
  const impactTag = p => p==null ? ['na','OI N/A'] : p<0.5 ? ['ok','LOW ('+p.toFixed(2)+'%)'] : p<2 ? ['mid','MODERATE ('+p.toFixed(2)+'%)'] : ['bad','HIGH ('+p.toFixed(2)+'%)'];
  const [itCls, itTxt] = impactTag(worstPct);
  return `<div class="pxgrid">
    <div class="pxcard long">
      <div class="pxlabel">Long · buy here</div>
      <div class="pxval r-long">${fmtPx(px.long.price)}</div>
      <div class="pxsub">${d.buy.exchange} · ${vol(px.long.volume_usd)}${dev(devL)}</div>
      <div class="pxsub">OI ${oiL?oiShort(oiL):"n/a"}${pctL!=null?` · your size = ${pctL.toFixed(2)}% of OI`:""}</div>
    </div>
    <div class="pxcard short">
      <div class="pxlabel">Short · sell here</div>
      <div class="pxval r-short">${fmtPx(px.short.price)}</div>
      <div class="pxsub">${d.sell.exchange} · ${vol(px.short.volume_usd)}${dev(devS)}</div>
      <div class="pxsub">OI ${oiS?oiShort(oiS):"n/a"}${pctS!=null?` · your size = ${pctS.toFixed(2)}% of OI`:""}</div>
    </div>
    <div class="pxcard">
      <div class="pxlabel">Mid ${idx?"/ Index":""}</div>
      <div class="pxval">${fmtPx(px.mid)}</div>
      <div class="pxsub">${idx?`index ${fmtPx(idx)}`:"index n/a"}</div>
    </div>
    <div class="pxcard gap ${eats?'gbad':(favorable?'gok':'gmid')}">
      <div class="pxlabel">Price gap (short − long)</div>
      <div class="pxval">${(disc>=0?"+":"")+disc.toFixed(3)}%</div>
      <div class="pxsub">${favorable?"sell high / buy low on entry":"you buy the expensive side"}</div>
      ${eats?`<div class="pxwarn">gap exceeds ${holdN}-cycle funding (${fundingOverHold.toFixed(2)}%)</div>`:""}
    </div>
    <div class="pxcard gap ${itCls==='ok'?'gok':itCls==='mid'?'gmid':itCls==='bad'?'gbad':''}">
      <div class="pxlabel">Market impact (your size vs OI)</div>
      <div class="pxval">${itTxt}</div>
      <div class="pxsub">Worst-case leg — entering can push price AND compress funding on this venue.</div>
    </div>
  </div>`;
}

function coverageTag(mult){
  if(mult>=20) return ['ok','DEEP ×'+mult.toFixed(0)];
  if(mult>=5)  return ['mid','OK ×'+mult.toFixed(1)];
  if(mult>=1)  return ['bad','TIGHT ×'+mult.toFixed(1)];
  return ['bad','TOO THIN ×'+mult.toFixed(2)];
}

function volProxyTag(volUsd){
  // rough executability from 24h volume when the book isn't tracked:
  // your order should be a small fraction of daily flow
  const mult = volUsd/lastUsd;
  if(mult>=2000) return ['mid','VOL PROXY ×'+fmtBig(mult)];
  if(mult>=500)  return ['mid','VOL PROXY ×'+fmtBig(mult)];
  return ['bad','VOL TOO LOW ×'+fmtBig(mult)];
}

function depthLeg(title, cls, exchange, action, sideLabel, sideUsd, sideQty, otherLabel, otherUsd, ob, mkt){
  if(!ob || !ob.latest){
    let err = ob && ob.error ? String(ob.error) : "";
    if(err.length>160) err = err.slice(0,160)+"…";
    const vol = mkt && mkt.volume_usd;
    if(vol){
      const [tagCls, tagTxt] = volProxyTag(vol);
      return `<div class="dleg ${cls}">
      <div class="dhead">${title} · ${exchange} <span class="dtag ${tagCls}">${tagTxt}</span></div>
      <div class="drow"><span>Book not tracked by CoinGlass — falling back to volume</span></div>
      <div class="drow"><span>24h volume</span><b>${oiShort(vol)}</b></div>
      <div class="drow"><span>your order vs daily flow</span><b>${(lastUsd/vol*100).toFixed(3)}%</b></div>
      <div class="dsub">Volume is a weaker signal than book depth: it says the pair trades actively,
      not that resting liquidity exists at your moment of execution. Use limit orders and verify the
      live book on ${exchange} before firing.${err?`<div class="derr">${err}</div>`:""}</div></div>`;
    }
    return `<div class="dleg ${cls}">
    <div class="dhead">${title} · ${exchange} <span class="dtag bad">NO DATA</span></div>
    <div class="dna">No orderbook depth and no volume data for this pair on this venue —
    a serious liquidity red flag. Treat as not executable at size.
    ${err?`<div class="derr">${err}</div>`:""}</div></div>`;
  }
  const t = new Date(ob.latest.time).toLocaleString();
  const mult = sideUsd>0 ? sideUsd/lastUsd : 0;
  const [tagCls, tagTxt] = coverageTag(mult);
  const total = sideUsd + otherUsd;
  const imb = total>0 ? (ob.latest.bids_usd/total*100) : 50;
  return `<div class="dleg ${cls}">
    <div class="dhead">${title} · ${exchange} <span class="dtag ${tagCls}">${tagTxt}</span></div>
    <div class="drow"><span>${action} — you consume <b>${sideLabel}</b></span><b>${oiShort(sideUsd)}</b></div>
    <div class="drow"><span>${sideLabel} quantity</span><b>${Number(sideQty).toLocaleString()}</b></div>
    <div class="drow"><span>${otherLabel} (other side)</span><b>${oiShort(otherUsd)}</b></div>
    <div class="dbar"><i style="width:${imb.toFixed(1)}%"></i></div>
    <div class="dsub">instrument ${ob.pair||"?"} · bids ${imb.toFixed(0)}% / asks ${(100-imb).toFixed(0)}% · snapshot ${t} (${ob.interval})</div>
  </div>`;
}

function depthPanel(d, longOb, shortOb, px){
  const hasL = longOb && longOb.latest, hasS = shortOb && shortOb.latest;
  const okL = hasL ? longOb.latest.asks_usd/lastUsd : 0;
  const okS = hasS ? shortOb.latest.bids_usd/lastUsd : 0;
  const mktL = px && px.long ? px.long : null;
  const mktS = px && px.short ? px.short : null;
  // is a missing-book leg rescued by a strong volume proxy?
  const proxyOK = leg => leg && leg.volume_usd && leg.volume_usd >= lastUsd*500;
  const worst = Math.min(hasL?okL:Infinity, hasS?okS:Infinity);
  let verdict, vcls;
  if(hasL && hasS){
    if(worst>=20){ verdict="EXECUTABLE — both books ≥20× your size within ±1% of mid"; vcls="ok"; }
    else if(worst>=5){ verdict="MARGINAL — expect some slippage, consider splitting the order"; vcls="mid"; }
    else { verdict="NOT EXECUTABLE AT SIZE — thinnest book under 5× your position"; vcls="bad"; }
  } else {
    const missOK = (hasL || proxyOK(mktL)) && (hasS || proxyOK(mktS));
    const trackedBad = (hasL && okL<5) || (hasS && okS<5);
    if(missOK && !trackedBad){
      verdict="PROXY ONLY — book untracked on one leg; 24h volume looks sufficient, verify the live book before firing"; vcls="mid";
    } else {
      verdict="UNVERIFIED — missing depth and weak volume on at least one leg"; vcls="bad";
    }
  }
  return `
  <div class="depthwrap">
    <div class="dverdict ${vcls}">${verdict} <span>· position ${fmt$(lastUsd)}/leg</span></div>
    ${priceStrip(d, px)}
    <div class="dgrid">
      ${depthLeg("LONG LEG","dl", d.buy.exchange, "BUY "+d.symbol, "asks",
        hasL?longOb.latest.asks_usd:0, hasL?longOb.latest.asks_quantity:0,
        "bids", hasL?longOb.latest.bids_usd:0, longOb, mktL)}
      ${depthLeg("SHORT LEG","ds", d.sell.exchange, "SELL "+d.symbol, "bids",
        hasS?shortOb.latest.bids_usd:0, hasS?shortOb.latest.bids_quantity:0,
        "asks", hasS?shortOb.latest.asks_usd:0, shortOb, mktS)}
    </div>
    <div class="dnote">Depth is CoinGlass's aggregated liquidity within ±1% of mid — not live top-of-book quotes. Verify final prices on the exchange before firing. Buying consumes asks; selling consumes bids.</div>
    <div class="btbar">
      <button class="btbtn" data-sym="${d.symbol}">▶ BACKTEST & RECOMMEND STRATEGY</button>
      <span class="bthint">replays ~21 days of funding, price & depth history for this pair</span>
    </div>
    <div class="btout"></div>
  </div>`;
}

// ---- data availability tracking ----
const dataStatus = {};   // symbol -> {obL, obS, px, errL, errS, errP}

function setDataStatus(d, longOb, shortOb, px){
  dataStatus[d.symbol] = {
    obL: !!(longOb && longOb.latest),
    obS: !!(shortOb && shortOb.latest),
    px:  !!(px && String(px.code)==="0" && px.long && px.short && px.mid!=null),
    errL: longOb && longOb.error ? String(longOb.error).slice(0,140) : "",
    errS: shortOb && shortOb.error ? String(shortOb.error).slice(0,140) : "",
    errP: px && px.msg ? String(px.msg).slice(0,140) : "",
  };
  // live-update the cell without re-rendering (keeps open panels intact)
  const tr = document.querySelector(`tr.datarow[data-sym="${CSS.escape(d.symbol)}"]`);
  if(tr){ const c = tr.querySelector(".datacell"); if(c) c.innerHTML = dataBadge(d.symbol); }
}

function dataBadge(sym){
  const s = dataStatus[sym];
  if(!s) return '<span class="dchk unk" title="Not checked yet — expand the row or use ✓ Check data">·&nbsp;·&nbsp;·</span>';
  const b = (ok, label, err)=>`<span class="dchk ${ok?'ok':'bad'}" title="${label} ${ok?'available':'MISSING'}${err?': '+err.replace(/"/g,'&quot;'):''}">${label}${ok?'✓':'✗'}</span>`;
  return b(s.obL,"L",s.errL) + b(s.obS,"S",s.errS) + b(s.px,"P",s.errP);
}

let checking = false;
$("chkData").onclick = async ()=>{
  if(checking || !rows.length) return;
  checking = true;
  const n = Math.min(15, Math.max(2, Number($("chkN").value)||6));
  const {list} = getList();
  const picks = list.filter(o=>!dataStatus[o.d.symbol]).slice(0, n);
  const btn = $("chkData");
  for(let i=0;i<picks.length;i++){
    const d = picks[i].d;
    btn.textContent = `Checking ${i+1}/${picks.length}…`;
    try{
      const [obL, obS, px] = await Promise.all([
        fetchDepth(d.buy.exchange, d.symbol),
        fetchDepth(d.sell.exchange, d.symbol),
        fetch(`/api/prices?symbol=${encodeURIComponent(d.symbol)}&long=${encodeURIComponent(d.buy.exchange)}&short=${encodeURIComponent(d.sell.exchange)}`)
          .then(r=>r.json()).catch(e=>({code:"error", msg:String(e)})),
      ]);
      setDataStatus(d, obL, obS, px);
    }catch(e){ /* leave unchecked */ }
  }
  btn.textContent = "✓ Check data";
  checking = false;
};

// ---- leaderboard detail rows (warnings & data status) ----
document.addEventListener("click", e=>{
  const tr = e.target.closest("tr.browrank");
  if(!tr) return;
  const next = tr.nextElementSibling;
  if(next && next.classList.contains("browdetail")){ next.remove(); return; }
  tr.closest("tbody").querySelectorAll(".browdetail").forEach(x=>x.remove());
  const res = btResultsMap[tr.dataset.sym];
  if(!res) return;
  const det = document.createElement("tr");
  det.className = "browdetail";
  det.innerHTML = `<td colspan="9">${browDetail(res)}</td>`;
  tr.after(det);
});

function browDetail(res){
  const {d, j} = res;
  const parts = [];
  if(String(j.code)!=="0" || !j.summary){
    parts.push(`<div class="btwarn">FAILED / NO TRADES — ${j.msg||"unknown"}</div>`);
    if(j.depth) parts.push(`<div class="ddat">Depth-rejected signals: ${j.depth.rejected} · signals with NO depth data: ${j.depth.nodata}</div>`);
  } else {
    parts.push(`<div class="ddat"><b>Data loaded:</b> funding history ✓ (both venues) · price history ✓ (both venues) · `+
      `depth check ${j.use_depth?`ON — rejected ${j.depth.rejected}, no-data ${j.depth.nodata}`:"OFF (numerical only — no book data used)"}</div>`);
    const w = (j.rec && j.rec.warnings) || [];
    if(w.length) parts.push(w.map(x=>`<div class="btwarn">⚠ ${x}</div>`).join(""));
    else parts.push(`<div class="ddat r-long">No warnings for this pair.</div>`);
  }
  parts.push(`<div class="ddat"><b>Live data check:</b> ${dataBadge(d.symbol)} <span class="r-dim">(L=long book, S=short book, P=prices — hover a red badge for the exact error; dots = not checked yet, use ✓ Check data)</span></div>`);
  return `<div class="browdetailwrap">${parts.join("")}</div>`;
}

// ---- backtest & rank top pairs ----
$("depthTog").onclick = e=>{
  const b=e.currentTarget, on=b.getAttribute("aria-pressed")==="true";
  b.setAttribute("aria-pressed",String(!on));
  b.textContent = !on ? "ON · execution-gated" : "OFF · numerical only";
};
function depthFlag(){ return $("depthTog").getAttribute("aria-pressed")==="true" ? "1" : "0"; }

let ranking = false, btCancel = false;
const btResultsMap = {};

async function runRanking(picks, label){
  if(ranking) return;
  ranking = true; btCancel = false;
  const board = $("btboard");
  const results = [];
  for(let i=0;i<picks.length;i++){
    if(btCancel){ break; }
    const d = picks[i].d;
    board.innerHTML = boardShell(`${label} ${i+1}/${picks.length}: ${d.symbol} (${d.buy.exchange} → ${d.sell.exchange})… `+
      `cached pairs are instant; new ones take seconds (depth OFF) to a minute (depth ON).`, results, false, true);
    try{
      const q = new URLSearchParams({symbol:d.symbol, long:d.buy.exchange, short:d.sell.exchange,
        usd:lastUsd, fee:(2*d.fee).toFixed(3), days:21, depth:depthFlag()});
      const r = await fetch("/api/backtest?"+q);
      const j = await r.json();
      results.push({d, j}); btResultsMap[d.symbol] = {d, j};
    }catch(err){
      results.push({d, j:{code:"error", msg:String(err.message)}});
      btResultsMap[d.symbol] = results[results.length-1];
    }
  }
  board.innerHTML = boardShell(btCancel?`Stopped — ${results.length} of ${picks.length} done.`:"", results, true);
  ranking = false;
}

$("btAll").onclick = ()=>{
  if(!rows.length){ alert("Scan first."); return; }
  const n = Math.min(15, Math.max(2, Number($("topN").value)||6));
  runRanking(getList().list.slice(0, n), "Backtesting");
};

$("btAllShown").onclick = ()=>{
  if(!rows.length){ alert("Scan first."); return; }
  const picks = getList().list;
  const per = depthFlag()==="1" ? "20–60" : "4–6";
  if(!confirm(`Backtest ALL ${picks.length} shown pairs?\n\n≈ ${per} API calls per uncached pair `+
      `(depth check ${depthFlag()==="1"?"ON":"OFF"}). This can use a lot of your CoinGlass quota. `+
      `You can stop it any time with the Stop button.`)) return;
  runRanking(picks, "Backtesting all");
};

function scoreOf(res){
  const j = res.j;
  if(String(j.code)!=="0" || !j.summary) return -1e9;
  let s = j.summary.total_net;
  if(j.rec && j.rec.expected_net_pct<=0) s -= 5;          // penalize "skip" verdicts
  s -= (j.rec && j.rec.warnings ? j.rec.warnings.length*0.5 : 0);
  return s;
}

function boardShell(status, results, done, running){
  const ranked = [...results].sort((a,b)=>scoreOf(b)-scoreOf(a));
  const rows_ = ranked.map((res,i)=>{
    const d=res.d, j=res.j;
    if(String(j.code)!=="0" || !j.summary){
      return `<tr class="browrank" data-sym="${d.symbol}" title="Click for full error"><td class="txt r-dim">—</td><td class="txt sym-cell">${d.symbol}</td>
        <td class="txt r-dim">${d.buy.exchange} → ${d.sell.exchange}</td>
        <td colspan="6" class="txt warn-cell">${(j.msg||"no data").slice(0,110)} ▾</td></tr>`;
    }
    const s=j.summary, r=j.rec;
    const ok = r.expected_net_pct>0 && s.trades>=3;
    const wtip = (r.warnings||[]).join("  •  ").replace(/"/g,"&quot;");
    return `<tr class="browrank" data-sym="${d.symbol}" title="Click for warnings & data status">
      <td class="txt ${i===0&&ok?'r-apr':'r-dim'}">#${i+1}</td>
      <td class="txt sym-cell">${d.symbol}</td>
      <td class="txt r-dim">${d.buy.exchange} → ${d.sell.exchange}</td>
      <td>${s.trades} <span class="r-dim">(${s.win_rate}% win)</span></td>
      <td class="${s.total_net>=0?'r-long':'r-short'}">${(s.total_net>=0?"+":"")+s.total_net}%</td>
      <td class="${r.expected_net_pct>=0?'r-long':'r-short'}">${(r.expected_net_pct>=0?"+":"")+r.expected_net_pct}%</td>
      <td class="r-dim">$${r.position_usd.toLocaleString()} · ${r.leverage}x · ${r.hold_cycles}c</td>
      <td class="txt">${ok?'<span class="dtag ok">TRADE</span>':'<span class="dtag bad">SKIP</span>'}</td>
      <td class="txt warn-cell" title="${wtip}">${r.warnings&&r.warnings.length? r.warnings.length+"⚠ ▾":"▾"}</td>
    </tr>`;
  }).join("");
  return `<div class="boardwrap">
    <div class="boardhead">
      <span class="pbtitle">PAIR RANKING <span>· 21-day backtest, net of fees, slippage & depth checks · sorted by realized total net</span></span>
      ${status?`<span class="boardstatus">${status}</span>`:""}
      ${running?`<button class="ghost boardclose" onclick="btCancel=true; this.textContent='Stopping…'">■ Stop</button>`:""}
      ${done?`<button class="ghost boardclose" onclick="this.closest('#btboard').innerHTML=''">✕ close</button>`:""}
    </div>
    ${results.length?`<div class="tablewrap"><table style="min-width:900px">
      <thead><tr><th class="txt">Rank</th><th class="txt">Symbol</th><th class="txt">Route (long → short)</th>
      <th>Trades</th><th>Total net</th><th>Exp / trade</th><th>Rec size·lev·hold</th>
      <th class="txt">Verdict</th><th class="txt">Warn</th></tr></thead>
      <tbody>${rows_}</tbody></table></div>`:""}
    <div class="dsub" style="margin-top:8px">TRADE = positive expected net at recommended parameters with ≥3 historical trades.
    Expand a pair's row in the table below and hit its backtest button for the full playbook.</div>
  </div>`;
}

// ---- backtest & strategy ----
async function runSingleBacktest(d, out, btn, opts){
  opts = opts || {};
  const days = opts.days || 21;
  btn.disabled = true; const oldTxt = btn.textContent; btn.textContent = "RUNNING…";
  out.innerHTML = '<div class="btload">Backtesting '+d.symbol+' ('+d.buy.exchange+' → '+d.sell.exchange+') over '+days+' days — '+
    'first run makes several API calls and can take up to a minute (more with depth check ON). Results cache for 10 minutes.</div>';
  try{
    const q = new URLSearchParams({symbol:d.symbol, long:d.buy.exchange, short:d.sell.exchange,
      usd: opts.usd||lastUsd, fee:(opts.fee!=null?opts.fee:2*d.fee).toFixed(3), days, depth:depthFlag()});
    const r = await fetch("/api/backtest?"+q);
    const j = await r.json();
    if(String(j.code)!=="0") throw new Error(j.msg||"backtest failed");
    out.innerHTML = btReport(j, d);
  }catch(err){
    out.innerHTML = '<div class="btload r-short">Backtest error: '+String(err.message).slice(0,300)+'</div>';
  }
  btn.disabled = false; btn.textContent = oldTxt.startsWith("↻")?oldTxt:"↻ RE-RUN BACKTEST";
}

document.addEventListener("click", e=>{
  const b = e.target.closest(".btbtn");
  if(!b) return;
  const sym = b.dataset.sym;
  const item = lastList.find(o=>o.d.symbol===sym);
  if(!item) return;
  const out = b.closest(".depthwrap").querySelector(".btout");
  runSingleBacktest(item.d, out, b);
});

function fmtPct4v(n){ return (n>=0?"+":"")+Number(n).toFixed(4)+"%"; }

function btReport(j, d){
  if(!j.summary){
    return `<div class="btload">${j.msg||"No trades found."}<br>
      Depth-rejected signals: ${j.depth?j.depth.rejected:0} · no depth data: ${j.depth?j.depth.nodata:0}</div>`;
  }
  const s = j.summary, r = j.rec;
  const avgEntryGap = j.trades && j.trades.length ?
    j.trades.reduce((a,t)=>a+(t.gap_entry||0),0)/j.trades.length : null;
  const stat = (label,val,cls)=>`<div class="btstat"><div class="pxlabel">${label}</div><div class="btval ${cls||""}">${val}</div></div>`;
  const maxd = Math.max(...j.decay.map(x=>Math.abs(x.avg)), 0.01);
  const decayBars = j.decay.map(x=>`<div class="dcol" title="cycle ${x.cycle}: ${x.avg>=0?"+":""}${x.avg}%">
      <i style="height:${Math.max(3, Math.abs(x.avg)/maxd*46)}px" class="${x.avg>=0?'':'neg'}"></i><span>c${x.cycle}</span></div>`).join("");
  const liqRow = Object.entries(j.liq).map(([lv,p])=>
      `<span class="liqcell ${p>0?'r-short':'r-long'}">${lv}x: ${p}%</span>`).join("");
  const warn = (r.warnings||[]).map(w=>`<div class="btwarn">⚠ ${w}</div>`).join("");
  return `
  <div class="btgrid">
    ${stat("Trades / "+s.days+"d", s.trades)}
    ${stat("Win rate", s.win_rate+"%", s.win_rate>=60?"r-long":(s.win_rate>=45?"r-apr":"r-short"))}
    ${stat("Avg net / trade", (s.avg_net>=0?"+":"")+s.avg_net+"%", s.avg_net>=0?"r-long":"r-short")}
    ${stat("Total net", (s.total_net>=0?"+":"")+s.total_net+"%", s.total_net>=0?"r-long":"r-short")}
    ${stat("Funding vs gap", (s.avg_funding>=0?"+":"")+s.avg_funding+"% / "+(s.avg_gap_pnl>=0?"+":"")+s.avg_gap_pnl+"%")}
    ${stat("Best / worst", "+"+s.best+"% / "+s.worst+"%")}
    ${stat("Avg entry price gap", avgEntryGap!=null?fmtPct4v(avgEntryGap):"—", avgEntryGap>0?"r-long":"r-short")}
  </div>
  <div class="dsub" style="margin:6px 0 -4px">Entry price gap = (short-venue futures price − long-venue futures price) ÷ short price, averaged across historical entries — the futures-price discrepancy between your two legs at the moment each trade opened (see the Profit Calculator for the full underlying/index breakdown on any single trade).</div>
  <div class="btrow2">
    <div class="btpanel">
      <div class="pxlabel">Edge decay after entry (avg diff %/cycle)</div>
      <div class="decay">${decayBars}</div>
      <div class="pxlabel" style="margin-top:10px">Short-leg liquidation frequency by leverage</div>
      <div class="liqrow">${liqRow}</div>
      <div class="dsub" style="margin-top:8px">Depth-rejected signals: ${j.depth.rejected} · no depth data: ${j.depth.nodata} —
        opportunities that existed on paper but not in the book.</div>
    </div>
    <div class="btpanel playbook">
      <div class="pbtitle">RECOMMENDED PLAYBOOK <span>· ${d.symbol} · long ${d.buy.exchange} / short ${d.sell.exchange}</span></div>
      <div class="pbrow"><b>Enter</b>when per-cycle diff ≥ <b class="r-apr">${r.entry_diff}%</b> and both books ≥5× size. ${r.entry_timing}</div>
      <div class="pbrow"><b>Position</b><b class="r-apr">$${r.position_usd.toLocaleString()}</b>/leg. ${r.position_why}</div>
      <div class="pbrow"><b>Leverage</b><b class="r-apr">${r.leverage}x</b> per leg. ${r.leverage_why}</div>
      <div class="pbrow"><b>Hold</b><b class="r-apr">${r.hold_cycles} cycle(s)</b>. ${r.hold_why}</div>
      <div class="pbrow"><b>Take profit</b>${r.take_profit}</div>
      <div class="pbrow"><b>Stops</b>${r.stop_loss.map(x=>"· "+x).join("<br>")}</div>
      <div class="pbrow"><b>Capital</b>${r.capital}</div>
      <div class="pbrow"><b>Expected</b><b class="${r.expected_net_pct>=0?'r-long':'r-short'}">${(r.expected_net_pct>=0?"+":"")+r.expected_net_pct}%</b>
        (≈ $${r.expected_net_usd.toLocaleString()}) per trade at the recommended size, after fees & modeled slippage.</div>
      ${warn}
      <div class="dsub" style="margin-top:8px">Derived from ${s.trades} historical trades over ${s.days} days. Settled rates only (no intra-period prediction), hourly ±1% depth snapshots, survivorship bias applies. A guide, not a guarantee.</div>
    </div>
  </div>`;
}

function tickCountdowns(){
  document.querySelectorAll(".countdown").forEach(el=>{
    const t = Number(el.dataset.t)-Date.now();
    if(!el.dataset.t) return;
    if(t<=0){ el.innerHTML="<b>settling…</b>"; return; }
    const h=Math.floor(t/3.6e6), m=Math.floor(t%3.6e6/6e4), s=Math.floor(t%6e4/1e3);
    const str = (h?h+"h ":"")+String(m).padStart(2,"0")+"m "+String(s).padStart(2,"0")+"s";
    el.innerHTML = view==="table" ? "<b>"+str+"</b>" : "next funding <b>"+str+"</b>";
  });
}

$("refresh").onclick = scan;
["minNet","exLong","exShort","lev","hold"].forEach(id=>{
  $(id).addEventListener("change",render);
  $(id).addEventListener("input",()=>{ if(rows.length) render(); });
});
$("usd").addEventListener("change",()=>{ if(rows.length) render(); });
$("usd").addEventListener("input",()=>{ if(rows.length) render(); });
$("hideThin").onclick = e=>{
  const b=e.currentTarget, on=b.getAttribute("aria-pressed")==="true";
  b.setAttribute("aria-pressed",String(!on)); render();
};
$("auto").onclick = e=>{
  const b=e.currentTarget, on=b.getAttribute("aria-pressed")==="true";
  b.setAttribute("aria-pressed",String(!on));
  clearInterval(timer);
  if(!on) timer=setInterval(scan,60000);
};
function setView(v){
  view=v;
  $("vTable").setAttribute("aria-pressed",String(v==="table"));
  $("vTickets").setAttribute("aria-pressed",String(v==="tickets"));
  if(rows.length) render();
}
$("vTable").onclick=()=>setView("table");
$("vTickets").onclick=()=>setView("tickets");
function setMode(m){
  rateMode=m;
  $("mCycle").setAttribute("aria-pressed",String(m==="cycle"));
  $("mAnnual").setAttribute("aria-pressed",String(m==="annual"));
  if(rows.length) render();
}
$("mCycle").onclick=()=>setMode("cycle");
$("mAnnual").onclick=()=>setMode("annual");
setInterval(tickCountdowns,1000);
scan();   // fetch arbitrage data immediately on page load
// ==================== PAGE NAVIGATION ====================
document.querySelectorAll(".navitem").forEach(nav=>{
  nav.addEventListener("click", ()=>{
    document.querySelectorAll(".page").forEach(p=>p.classList.remove("active"));
    document.querySelectorAll(".navitem").forEach(n=>n.classList.remove("active"));
    document.getElementById(nav.dataset.page).classList.add("active");
    nav.classList.add("active");
    if(nav.dataset.page==="page-backtest") populateBt2Pick();
    if(nav.dataset.page==="page-calc") populateCPick();
  });
});

// ==================== PAGE 5: profit calculator ====================
function populateCPick(){
  const sel = $("cPick");
  const cur = sel.value;
  sel.innerHTML = '<option value="">— manual entry —</option>' +
    rows.map((d,i)=>`<option value="${i}">${d.symbol} · ${d.buy.exchange} → ${d.sell.exchange}</option>`).join("");
  if(cur) sel.value = cur;
}
$("cPick").addEventListener("change", ()=>{
  const i = $("cPick").value;
  if(i==="") return;
  const d = rows[Number(i)];
  $("cSym").value = d.symbol;
  $("cLongEx").value = d.buy.exchange;
  $("cLongRate").value = d.buy.funding_rate;
  $("cLongInt").value = d.buy.funding_rate_interval;
  $("cShortEx").value = d.sell.exchange;
  $("cShortRate").value = d.sell.funding_rate;
  $("cShortInt").value = d.sell.funding_rate_interval;
  $("cFee").value = (2*d.fee).toFixed(3);
  fetchCalcLivePrices();
});

const CFIELDS = ["cSym","cUsd","cLev","cLongEx","cLongRate","cLongInt","cShortEx","cShortRate","cShortInt",
                 "cPxLE","cPxSE","cPxIE","cPxLX","cPxSX","cPxIX","cOiL","cOiS","cHold","cFee"];
CFIELDS.forEach(id=>{ const el=$(id); if(el) el.addEventListener("input", calcRender); });

async function fetchCalcLivePrices(){
  const sym = $("cSym").value.trim().toUpperCase();
  const lx = $("cLongEx").value.trim();
  const sx = $("cShortEx").value.trim();
  if(!sym || !lx || !sx){ $("cFetchStatus").textContent = "Need symbol + both exchanges first."; return; }
  $("cFetchStatus").textContent = "Fetching…";
  try{
    const r = await fetch(`/api/prices?symbol=${encodeURIComponent(sym)}&long=${encodeURIComponent(lx)}&short=${encodeURIComponent(sx)}`);
    const j = await r.json();
    if(String(j.code)!=="0" || !j.long || !j.short){ $("cFetchStatus").textContent = "No live price data for this pair/venue combo."; return; }
    if(j.long.price!=null) $("cPxLE").value = j.long.price;
    if(j.short.price!=null) $("cPxSE").value = j.short.price;
    const idx = j.long.index_price ?? j.short.index_price;
    if(idx!=null) $("cPxIE").value = idx;
    // exit defaults to entry (no forecast) — edit manually to model a scenario
    if(j.long.price!=null) $("cPxLX").value = j.long.price;
    if(j.short.price!=null) $("cPxSX").value = j.short.price;
    if(idx!=null) $("cPxIX").value = idx;
    if(j.long.oi_usd!=null) $("cOiL").value = Math.round(j.long.oi_usd);
    if(j.short.oi_usd!=null) $("cOiS").value = Math.round(j.short.oi_usd);
    $("cFetchStatus").textContent = "Live prices loaded — exit fields default to entry, edit them to model a scenario.";
    calcRender();
  }catch(err){
    $("cFetchStatus").textContent = "Fetch error: "+String(err.message).slice(0,120);
  }
}
$("cFetchPx").onclick = fetchCalcLivePrices;


function fmtMoney(n){ const s = n<0?"−":""; return s+"$"+Math.abs(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}); }
function fmtPct4(n){ return (n>=0?"+":"")+Number(n).toFixed(4)+"%"; }

function calcRender(){
  const sym = $("cSym").value.trim() || "COIN";
  const usd = Math.max(0, Number($("cUsd").value)||0);
  const lev = Math.max(1, Number($("cLev").value)||1);
  const lEx = $("cLongEx").value.trim() || "Long venue";
  const sEx = $("cShortEx").value.trim() || "Short venue";
  const lRate = Number($("cLongRate").value)||0;
  const sRate = Number($("cShortRate").value)||0;
  const lInt = Math.max(1, Number($("cLongInt").value)||8);
  const sInt = Math.max(1, Number($("cShortInt").value)||8);
  const pxLE = Number($("cPxLE").value)||1, pxSE = Number($("cPxSE").value)||1;
  const pxLX = Number($("cPxLX").value)||1, pxSX = Number($("cPxSX").value)||1;
  const pxIE = Number($("cPxIE").value)||1, pxIX = Number($("cPxIX").value)||1;
  const hold = Math.max(1, Number($("cHold").value)||24);
  const feeRT = Math.max(0, Number($("cFee").value)||0);
  const oiL = Number($("cOiL").value)||0, oiS = Number($("cOiS").value)||0;
  const pctOiL = oiL>0 ? usd/oiL*100 : null;
  const pctOiS = oiS>0 ? usd/oiS*100 : null;
  const impactTag = p => p==null ? ['na','NO OI DATA — fetch or enter manually'] :
    p<0.5 ? ['ok','LOW — unlikely to move price or funding'] :
    p<2   ? ['mid','MODERATE — expect some price impact & funding compression'] :
            ['bad','HIGH — you are likely to move both the price and the funding rate'];
  const [itClsL, itTxtL] = impactTag(pctOiL);
  const [itClsS, itTxtS] = impactTag(pctOiS);

  // deviation of each futures price from the underlying/index at entry & exit
  const devLE = pxIE ? (pxLE-pxIE)/pxIE*100 : 0, devSE = pxIE ? (pxSE-pxIE)/pxIE*100 : 0;
  const devLX = pxIX ? (pxLX-pxIX)/pxIX*100 : 0, devSX = pxIX ? (pxSX-pxIX)/pxIX*100 : 0;

  // --- step A: capital ---
  const capital = 2*usd/lev;

  // --- step B: funding, leg by leg, counting real settlements over the hold window ---
  const longN = Math.floor(hold / lInt);
  const shortN = Math.floor(hold / sInt);
  const longFundingUsd = -longN * usd * lRate/100;   // long PAYS the rate (earns if negative)
  const shortFundingUsd = shortN * usd * sRate/100;  // short COLLECTS the rate
  const netFundingUsd = longFundingUsd + shortFundingUsd;

  // --- step C: price gap trade ---
  const entryGap = pxSE!==0 ? (pxSE - pxLE)/pxSE*100 : 0;
  const exitGap = pxSX!==0 ? (pxSX - pxLX)/pxSX*100 : 0;
  const gapPnlPct = entryGap - exitGap;
  const gapPnlUsd = usd * gapPnlPct/100;

  // --- step D: costs ---
  const feeUsd = usd * feeRT/100;

  // --- step E: total ---
  const netUsd = netFundingUsd + gapPnlUsd - feeUsd;
  const roc = capital>0 ? netUsd/capital*100 : 0;

  const out = $("cOut");
  out.innerHTML = `
  <div class="cstep">
    <div class="pbtitle">TRADE FLOW</div>
    <div class="flowrow">
      <div class="flowcard l">
        <div class="fttl">Long · ${lEx}</div>
        <div class="fval r-long">${fmtPct4(lRate)} / ${lInt}h</div>
        <div class="dsub">${longN} settlement(s) over ${hold}h</div>
        <div class="dsub">futures: $${pxLE} → $${pxLX}</div>
        <div class="dsub">vs index: ${fmtPct4(devLE)} → ${fmtPct4(devLX)}</div>
      </div>
      <div class="flowarrow">＋</div>
      <div class="flowcard s">
        <div class="fttl">Short · ${sEx}</div>
        <div class="fval r-short">${fmtPct4(sRate)} / ${sInt}h</div>
        <div class="dsub">${shortN} settlement(s) over ${hold}h</div>
        <div class="dsub">futures: $${pxSE} → $${pxSX}</div>
        <div class="dsub">vs index: ${fmtPct4(devSE)} → ${fmtPct4(devSX)}</div>
      </div>
      <div class="flowarrow">＝</div>
      <div class="flowcard" style="border-top:3px solid var(--gold)">
        <div class="fttl">Net result, ${sym}</div>
        <div class="fval ${netUsd>=0?'r-long':'r-short'}">${fmtMoney(netUsd)}</div>
        <div class="dsub">${roc.toFixed(1)}% ROC on ${fmtMoney(capital)} capital</div>
      </div>
    </div>
    <div class="dsub">Capital = 2 × position ÷ leverage = 2 × $${usd.toLocaleString()} ÷ ${lev} = ${fmtMoney(capital)} (margin posted on both exchanges).</div>
  </div>

  <div class="cstep">
    <div class="pbtitle">STEP 0 — MARKET IMPACT CHECK (will entering move this yourself?)</div>
    <div class="cline"><div><div class="clabel">Long leg — position vs open interest</div>
      <span class="cformula">$${usd.toLocaleString()} ÷ ${oiL?fmtMoney(oiL):"?"} OI ${pctOiL!=null?"= "+pctOiL.toFixed(3)+"%":""}</span></div>
      <div class="cval dchk ${itClsL}" style="border:none;background:none;padding:0">${itTxtL}</div></div>
    <div class="cline"><div><div class="clabel">Short leg — position vs open interest</div>
      <span class="cformula">$${usd.toLocaleString()} ÷ ${oiS?fmtMoney(oiS):"?"} OI ${pctOiS!=null?"= "+pctOiS.toFixed(3)+"%":""}</span></div>
      <div class="cval dchk ${itClsS}" style="border:none;background:none;padding:0">${itTxtS}</div></div>
    <div class="dsub" style="margin-top:6px">Rule of thumb: under ~0.5% of a venue's OI, your own order is unlikely to be the reason the price or funding rate moves. Above ~2%, assume you ARE the marginal flow — your entry price will likely be worse than the quote you're calculating from, and the funding rate can compress toward zero as you enter. Use "Fetch live prices &amp; OI" above, or enter OI manually from the Coin Research or Live Table pages.</div>
  </div>

  <div class="cstep">
    <div class="pbtitle">STEP 1 — FUNDING P&amp;L (the recurring income)</div>
    <div class="cline"><div><div class="clabel">Long leg — ${lRate<0?'<b class="r-long">you RECEIVE</b> this rate (negative funding pays the long side)':'<b class="r-short">you PAY</b> this rate (positive funding: longs pay shorts)'}</div>
      <span class="cformula">−${longN} × $${usd.toLocaleString()} × (${lRate}%) = ${fmtMoney(longFundingUsd)}</span></div>
      <div class="cval ${longFundingUsd>=0?'r-long':'r-short'}">${fmtMoney(longFundingUsd)}</div></div>
    <div class="cline"><div><div class="clabel">Short leg — ${sRate>=0?'<b class="r-long">you RECEIVE</b> this rate (positive funding: longs pay shorts, you are short)':'<b class="r-short">you PAY</b> this rate (negative funding pays the long side, you are short)'}</div>
      <span class="cformula">+${shortN} × $${usd.toLocaleString()} × (${sRate}%) = ${fmtMoney(shortFundingUsd)}</span></div>
      <div class="cval ${shortFundingUsd>=0?'r-long':'r-short'}">${fmtMoney(shortFundingUsd)}</div></div>
    <div class="ctotal"><div class="clabel">Net funding collected</div>
      <div class="cval ${netFundingUsd>=0?'r-long':'r-short'}">${fmtMoney(netFundingUsd)}</div></div>
  </div>

  <div class="cstep">
    <div class="pbtitle">STEP 2 — PRICE DISCREPANCY P&amp;L (one-shot, pays only if the gap converges)</div>
    <div class="cline"><div><div class="clabel">Underlying/index price — the reference both futures track</div>
      <span class="cformula">entry $${pxIE} → exit $${pxIX}</span></div>
      <div class="cval r-dim">reference</div></div>
    <div class="cline"><div><div class="clabel">Long futures vs index (entry → exit)</div>
      <span class="cformula">($${pxLE}−$${pxIE})÷$${pxIE} = ${fmtPct4(devLE)}  →  ($${pxLX}−$${pxIX})÷$${pxIX} = ${fmtPct4(devLX)}</span></div>
      <div class="cval">${fmtPct4(devLE)} → ${fmtPct4(devLX)}</div></div>
    <div class="cline"><div><div class="clabel">Short futures vs index (entry → exit)</div>
      <span class="cformula">($${pxSE}−$${pxIE})÷$${pxIE} = ${fmtPct4(devSE)}  →  ($${pxSX}−$${pxIX})÷$${pxIX} = ${fmtPct4(devSX)}</span></div>
      <div class="cval">${fmtPct4(devSE)} → ${fmtPct4(devSX)}</div></div>
    <div class="cline"><div><div class="clabel">Entry gap (short price vs long price)</div>
      <span class="cformula">(${pxSE} − ${pxLE}) ÷ ${pxSE} × 100 = ${fmtPct4(entryGap)}</span></div>
      <div class="cval">${fmtPct4(entryGap)}</div></div>
    <div class="cline"><div><div class="clabel">Exit gap (short price vs long price)</div>
      <span class="cformula">(${pxSX} − ${pxLX}) ÷ ${pxSX} × 100 = ${fmtPct4(exitGap)}</span></div>
      <div class="cval">${fmtPct4(exitGap)}</div></div>
    <div class="cline"><div><div class="clabel">Gap P&amp;L % — you sold the rich side, bought the cheap side at entry</div>
      <span class="cformula">entry gap − exit gap = ${fmtPct4(entryGap)} − ${fmtPct4(exitGap)} = ${fmtPct4(gapPnlPct)}</span></div>
      <div class="cval ${gapPnlPct>=0?'r-long':'r-short'}">${fmtPct4(gapPnlPct)}</div></div>
    <div class="ctotal"><div class="clabel">Gap P&amp;L in dollars</div>
      <span class="cformula" style="margin-right:auto"></span>
      <div class="cval ${gapPnlUsd>=0?'r-long':'r-short'}">${fmtMoney(gapPnlUsd)}</div></div>
    <div class="dsub" style="margin-top:6px">If the gap doesn't move, this is $0 — it only pays you when the dislocation that created the opportunity normalizes while you're positioned for it. If the gap widens against you, this line goes negative.</div>
  </div>

  <div class="cstep">
    <div class="pbtitle">STEP 3 — COSTS</div>
    <div class="cline"><div><div class="clabel">Round-trip fees, both legs, open + close</div>
      <span class="cformula">$${usd.toLocaleString()} × ${feeRT}% = ${fmtMoney(feeUsd)}</span></div>
      <div class="cval r-short">−${fmtMoney(feeUsd)}</div></div>
  </div>

  <div class="cstep" style="border-color:var(--gold)">
    <div class="pbtitle">STEP 4 — NET RESULT</div>
    <div class="cline"><div class="clabel">Funding P&amp;L</div><div class="cval ${netFundingUsd>=0?'r-long':'r-short'}">${fmtMoney(netFundingUsd)}</div></div>
    <div class="cline"><div class="clabel">+ Gap P&amp;L</div><div class="cval ${gapPnlUsd>=0?'r-long':'r-short'}">${fmtMoney(gapPnlUsd)}</div></div>
    <div class="cline"><div class="clabel">− Fees</div><div class="cval r-short">−${fmtMoney(feeUsd)}</div></div>
    <div class="ctotal"><div class="clabel">NET PROFIT</div><div class="cval ${netUsd>=0?'r-long':'r-short'}">${fmtMoney(netUsd)}</div></div>
    <div class="cline" style="border-top:1px dashed var(--edge);margin-top:8px;padding-top:10px">
      <div class="clabel">Return on capital (ROC)</div>
      <span class="cformula">${fmtMoney(netUsd)} ÷ ${fmtMoney(capital)} × 100</span></div>
      <div class="cval ${roc>=0?'r-long':'r-short'}" style="font-size:20px">${roc.toFixed(2)}%</div>
    <div class="dsub" style="margin-top:8px">This is arithmetic on the numbers you entered, not a forecast — real funding rates, prices and fills will differ. Use it to sanity-check a scenario, or plug in numbers from the Backtest or Live Table pages.</div>
  </div>`;
}
calcRender();


function populateBt2Pick(){
  const sel = $("bt2Pick");
  const cur = sel.value;
  sel.innerHTML = '<option value="">— choose a live pair —</option>' +
    rows.map((d,i)=>`<option value="${i}">${d.symbol} · ${d.buy.exchange} → ${d.sell.exchange} (${d.funding>=0?"+":""}${d.funding.toFixed(3)}%/cycle)</option>`).join("");
  if(cur) sel.value = cur;
}
$("bt2Pick").addEventListener("change", ()=>{
  const i = $("bt2Pick").value;
  if(i==="") return;
  const d = rows[Number(i)];
  $("bt2Sym").value = d.symbol;
  $("bt2Long").value = d.buy.exchange;
  $("bt2Short").value = d.sell.exchange;
  $("bt2Fee").value = (2*d.fee).toFixed(3);
});
$("bt2Run").onclick = ()=>{
  const sym = $("bt2Sym").value.trim().toUpperCase();
  const lx = $("bt2Long").value.trim();
  const sx = $("bt2Short").value.trim();
  if(!sym || !lx || !sx){ alert("Fill in symbol, long exchange and short exchange."); return; }
  const d = {symbol: sym, buy:{exchange:lx}, sell:{exchange:sx}, fee: Number($("bt2Fee").value)/2 || 0.06};
  const usd = Math.max(100, Number($("bt2Usd").value)||10000);
  const days = Math.min(60, Math.max(7, Number($("bt2Days").value)||21));
  runSingleBacktest(d, $("bt2out"), $("bt2Run"), {usd, days, fee: Number($("bt2Fee").value)||0.12});
};

// ==================== PAGE 3: coin research ====================
function fmtSigned(n, dp){ dp = dp==null?2:dp; if(n==null) return "—"; return (n>=0?"+":"")+Number(n).toFixed(dp)+"%"; }
function fmtUsdShort(n){ return n==null ? "—" : oiShort(n); }

async function searchCoin(){
  const sym = $("hSym").value.trim().toUpperCase();
  if(!sym) return;
  $("hOut").innerHTML = '<div class="btload">Loading '+sym+' across all tracked exchanges…</div>';
  try{
    const r = await fetch("/api/coin-info?symbol="+encodeURIComponent(sym));
    const j = await r.json();
    if(String(j.code)!=="0") throw new Error(j.msg||"lookup failed");
    if(!j.data.length){ $("hOut").innerHTML = `<div class="empty">${j.msg||"No data found for "+sym+"."}</div>`; return; }
    $("hOut").innerHTML = renderCoinInfo(sym, j.data);
    tickCountdowns();
  }catch(err){
    $("hOut").innerHTML = '<div class="btload r-short">Error: '+String(err.message).slice(0,300)+'</div>';
  }
}
$("hSearch").onclick = searchCoin;
$("hSym").addEventListener("keydown", e=>{ if(e.key==="Enter") searchCoin(); });

function renderCoinInfo(sym, list){
  const prices = list.map(x=>x.price).filter(x=>x!=null);
  const spread = prices.length>1 ? (Math.max(...prices)-Math.min(...prices))/((Math.max(...prices)+Math.min(...prices))/2)*100 : 0;
  const fundings = list.map(x=>x.funding_rate).filter(x=>x!=null);
  const summary = `<div class="btgrid" style="grid-template-columns:repeat(4,minmax(0,1fr))">
    <div class="btstat"><div class="pxlabel">Exchanges tracked</div><div class="btval">${list.length}</div></div>
    <div class="btstat"><div class="pxlabel">Price range</div><div class="btval ${spread>0.3?'r-apr':''}">${spread.toFixed(3)}%</div></div>
    <div class="btstat"><div class="pxlabel">Funding range</div><div class="btval">${fundings.length?(Math.min(...fundings)>=0?"+":"")+Math.min(...fundings).toFixed(3)+"% / "+(Math.max(...fundings)>=0?"+":"")+Math.max(...fundings).toFixed(3)+"%":"—"}</div></div>
    <div class="btstat"><div class="pxlabel">Total OI tracked</div><div class="btval">${fmtUsdShort(list.reduce((a,x)=>a+(x.oi_usd||0),0))}</div></div>
  </div>`;
  const rows_ = list.map(x=>`<tr>
    <td class="txt sym-cell">${x.exchange}</td>
    <td class="txt r-dim">${x.instrument_id||""}</td>
    <td>${fmtPx(x.price)}</td>
    <td class="r-dim">${fmtPx(x.index_price)}</td>
    <td class="${(x.price_chg_24h||0)>=0?'r-long':'r-short'}">${fmtSigned(x.price_chg_24h)}</td>
    <td class="${(x.funding_rate||0)>=0?'r-short':'r-long'}">${x.funding_rate!=null?fmtSigned(x.funding_rate,4):"—"}</td>
    <td class="countdown" data-t="${x.next_funding_time||0}">${x.next_funding_time?'<b>—</b>':'<span class="r-dim">—</span>'}</td>
    <td class="r-dim">${fmtUsdShort(x.oi_usd)}</td>
    <td class="${(x.oi_chg_24h||0)>=0?'r-long':'r-short'}">${fmtSigned(x.oi_chg_24h)}</td>
    <td class="r-dim">${fmtUsdShort(x.volume_usd)}</td>
    <td class="${(x.volume_chg_24h||0)>=0?'r-long':'r-short'}">${fmtSigned(x.volume_chg_24h)}</td>
    <td class="r-short">${fmtUsdShort(x.long_liq_24h)}</td>
    <td class="r-long">${fmtUsdShort(x.short_liq_24h)}</td>
  </tr>`).join("");
  return summary + `
  <div class="tablewrap" style="margin-top:14px"><table style="min-width:1400px">
    <thead><tr>
      <th class="txt">Exchange</th><th class="txt">Instrument</th><th>Price</th><th>Index</th><th>24h %</th>
      <th>Funding</th><th>Next funding</th><th>OI</th><th>OI 24h %</th><th>Volume 24h</th><th>Vol 24h %</th>
      <th>Long liq 24h</th><th>Short liq 24h</th>
    </tr></thead>
    <tbody>${rows_}</tbody>
  </table></div>
  <div class="dsub" style="margin-top:8px">Data: CoinGlass pairs-markets, all exchanges tracking ${sym}. "Long liq" = longs force-closed in the last 24h (a squeeze-down signal); "Short liq" = shorts force-closed (a squeeze-up signal). Sorted by open interest.</div>`;
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
