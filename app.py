"""
Nnanna Michael - single-file trading robot
Strategy: 30m major BOS -> liquidity (last OB/wick in the BOS leg) -> entry at TOP of the OB below it
(bullish; bottom of the OB above it for bearish) -> SL at protected low/high -> TP at 1:RR.
Everything (liquidity + entry OB) is taken from INSIDE the leg of the latest BOS.
Env vars: ADMIN_PASSWORD, SECRET_KEY, WEBHOOK_SECRET (optional), AUTOSCAN=1, ANTHROPIC_API_KEY (optional, image analysis)
"""
import os, re, json, hmac, hashlib, time, datetime, threading, uuid, base64, secrets
import urllib.request, urllib.parse, urllib.error
from decimal import Decimal, ROUND_DOWN
from flask import Flask, request, jsonify, redirect, session, send_from_directory, abort, Response
from markupsafe import escape
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)      # correct https address behind Render's proxy
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24).hex())
app.config.update(SESSION_COOKIE_SAMESITE="Lax", SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SECURE=bool(os.environ.get("RENDER")),
                  PERMANENT_SESSION_LIFETIME=datetime.timedelta(hours=12), MAX_CONTENT_LENGTH=8 * 1024 * 1024)
DATA_DIR = os.environ.get("DATA_DIR", ".")      # on Render: mount a disk at /var/data and set DATA_DIR=/var/data
DATA_FILE, UPLOADS = os.path.join(DATA_DIR, "bot_data.json"), os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOADS, exist_ok=True)
EA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ISIG_EA.mq5")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD") or "Mick@2008"   # first-login password; change it inside Admin > Security
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
LOCK = threading.Lock()

DEFAULTS = {
    "site_name": "Nnanna Michael", "tagline": "Automated Trading Robot",
    "lead": "A smart-money trading bot that reads market structure for you: it spots the break of structure, finds the liquidity, and places the entry, stop loss and take profit automatically. Every trade is recorded so you can see the real results.",
    "max_accounts": 500,
    "platforms": ["BINANCE", "BYBIT", "OKX", "BITGET", "MT5", "MT4", "EA"],
    "symbols": ["BTCUSDT", "ETHUSDT"], "timeframe": "30m",   # crypto = ends with USDT; anything else (EURUSD, XAUUSD...) is traded through MT4/MT5
    "swing_n": 2, "min_bos": 2, "rr": 2.0, "ob_entry": "top",   # top | mid
    "risk_pct": 1.0,
    "mode": "paper",                                              # paper | live
    "filters": {"liquidity": [], "session": [], "side": [], "bos_run": []},   # learned skips, e.g. {"session": ["Asia"]}
    "strategy_text": "30m trend by consecutive major BOS. Liquidity = last order block (or wick) before the latest BOS. "
                     "Entry = top of the OB below the liquidity (bullish), bottom of the OB above it (bearish). "
                     "SL at protected low/high of the BOS leg. TP = 1:2.",
}

try:
    from cryptography.fernet import Fernet, MultiFernet
    CRYPTO = True
except Exception:
    CRYPTO = False
SENSITIVE = ("login", "password", "extra")          # API key, secret, passphrase / MetaApi id: stored encrypted (AES + HMAC via Fernet)

def _h(k): return hashlib.sha256(k.encode()).hexdigest()   # EA keys are random 32-char secrets: only their hash is stored

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
_TEXT, _SIG, _DB_LOADED, _DB_RETRY = None, None, False, 0.0
DB_STATUS = {"ok": None, "err": ""}
LAST_SEEN = {}                                   # EA last-contact times live in memory only (so they never trigger a database write)

def _db():
    import pg8000.native, ssl
    u = urllib.parse.urlparse(DATABASE_URL)
    con = pg8000.native.Connection(user=urllib.parse.unquote(u.username or ""), password=urllib.parse.unquote(u.password or ""), host=u.hostname,
                                   port=u.port or 5432, database=(u.path or "/").lstrip("/"), ssl_context=ssl.create_default_context(), timeout=20)
    con.run("CREATE TABLE IF NOT EXISTS bot_state (id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
    return con

def _db_read():
    con = _db()
    try:
        rows = con.run("SELECT data FROM bot_state WHERE id = 1")
        return rows[0][0] if rows else None
    finally: con.close()

def _db_write(text):
    con = _db()
    try: con.run("INSERT INTO bot_state (id, data) VALUES (1, :d) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data", d=text)
    finally: con.close()

def _file_text():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f: return f.read()
    return None

def _raw():
    """The stored data as text. With a database: read it once at start-up, never overwrite it with a stale local copy."""
    global _TEXT, _DB_LOADED, _DB_RETRY
    if DATABASE_URL and not _DB_LOADED and time.time() >= _DB_RETRY:
        try:
            t = _db_read()
            if t is None:                                  # brand-new database: carry over whatever the local file has
                t = _file_text()
                if t: _db_write(t)
            _TEXT, _DB_LOADED = t or "", True; DB_STATUS.update(ok=True, err="")
        except Exception as e:
            DB_STATUS.update(ok=False, err=str(e)[:200]); _DB_RETRY = time.time() + 30; print("database read failed:", e)
    if _TEXT is None: _TEXT = _file_text() or ""      # no database (or it is unreachable): use the local file for now
    return _TEXT

def crypto_ready():
    """Encrypt only if the key will still exist after a restart (DATA_KEY/SECRET_KEY), or the data lives on a persistent local disk."""
    return CRYPTO and (bool(os.environ.get("DATA_KEY") or os.environ.get("SECRET_KEY")) or not DATABASE_URL)

def _fkeys():
    ks = []
    for name in ("DATA_KEY", "SECRET_KEY"):
        raw = os.environ.get(name)
        if raw: ks.append(base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest()))
    kf = os.path.join(DATA_DIR, ".datakey")
    if os.path.exists(kf):
        with open(kf, "rb") as fh: ks.append(fh.read().strip())
    elif not ks and not DATABASE_URL:                 # no env key at all: create a key file as a last resort
        k = Fernet.generate_key(); fd = os.open(kf, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600); os.write(fd, k); os.close(fd); ks.append(k)
    return ks

_FER = None
def _enc(v):
    global _FER
    if _FER is None: _FER = MultiFernet([Fernet(k) for k in _fkeys()])
    return _FER.encrypt(v.encode()).decode()
def _dec(v):
    global _FER
    try:
        if _FER is None: _FER = MultiFernet([Fernet(k) for k in _fkeys()])
        return _FER.decrypt(v.encode()).decode()
    except Exception: return ""

def _protect(accs):
    out = []
    for a in accs:
        b = dict(a)
        if b.get("platform") == "EA": b.pop("login", None)          # never write an EA key to disk
        elif crypto_ready():
            for k in SENSITIVE:
                v = b.get(k)
                if v and not str(v).startswith("enc:"): b[k] = "enc:" + _enc(str(v))
        out.append(b)
    return out

BRAND_KEYS = ("site_name", "tagline", "lead")

# ---------------- storage ----------------
def load():
    d = {"settings": {}, "accounts": [], "orders": [], "seen": [], "journal": [], "lessons": "", "admin_hash": None}
    try: d.update(json.loads(_raw() or "{}"))
    except Exception: pass
    d["settings"] = {**DEFAULTS, **d["settings"]}
    if d["settings"].get("site_name") in ("", "ISIG BOT"): d["settings"]["site_name"] = DEFAULTS["site_name"]   # rename old saved brand
    if "EA" not in d["settings"]["platforms"]: d["settings"]["platforms"] = d["settings"]["platforms"] + ["EA"]
    for a in d["accounts"]:
        for k in SENSITIVE:
            v = a.get(k)
            if isinstance(v, str) and v.startswith("enc:"): a[k] = _dec(v[4:]) if CRYPTO else ""
        if a.get("platform") == "EA" and a.get("login") and not a.get("key_hash"): a["key_hash"] = _h(a.pop("login"))   # upgrade older plaintext EA keys
    return d

def save(d):
    global _TEXT, _SIG
    sig = json.dumps(d, sort_keys=True, default=str)
    if sig == _SIG: return                           # nothing changed: no write (keeps the free database asleep)
    text = json.dumps({**d, "accounts": _protect(d["accounts"])}, indent=2)
    stored = not DATABASE_URL
    if DATABASE_URL:
        _raw()                                       # make sure the saved data was read before we write anything
        if _DB_LOADED:
            try: _db_write(text); DB_STATUS.update(ok=True, err=""); stored = True
            except Exception as e: DB_STATUS.update(ok=False, err=str(e)[:200]); print("database write failed:", e)
    tmp = DATA_FILE + ".tmp"                         # local copy too (a backup; on Render's free plan it disappears on restart)
    try:
        with open(tmp, "w") as f: f.write(text)
        try: os.chmod(tmp, 0o600)
        except Exception: pass
        os.replace(tmp, DATA_FILE)
    except OSError as e: print("local save failed:", e)
    _TEXT = text
    if stored: _SIG = sig

# ---------------- strategy engine ----------------
def fetch_candles(symbol, interval="30m", limit=300):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    with urllib.request.urlopen(url, timeout=10) as r: rows = json.loads(r.read())[:-1]   # drop forming candle
    return [{"t": x[0], "o": float(x[1]), "h": float(x[2]), "l": float(x[3]), "c": float(x[4])} for x in rows]

def _swing(c, i, n, kind):
    k = "h" if kind == "high" else "l"
    w = [x[k] for x in c[i - n:i + n + 1]]
    return c[i][k] == (max(w) if kind == "high" else min(w))

def bos_events(c, n):
    ev, hi, lo = [], None, None
    for i in range(2 * n, len(c)):
        j = i - n
        if _swing(c, j, n, "high"): hi = (j, c[j]["h"])
        if _swing(c, j, n, "low"): lo = (j, c[j]["l"])
        if hi and c[i]["c"] > hi[1]: ev.append({"dir": "bull", "i": i, "broken": hi}); hi = None
        elif lo and c[i]["c"] < lo[1]: ev.append({"dir": "bear", "i": i, "broken": lo}); lo = None
    return ev

def find_setup(c, s):
    n, rr, min_bos = s["swing_n"], s["rr"], s["min_bos"]
    ev = bos_events(c, n)
    if len(ev) < min_bos: return None
    last = ev[-1]
    if any(e["dir"] != last["dir"] for e in ev[-min_bos:]): return None      # no clear trend
    run = 0
    for e in reversed(ev):
        if e["dir"] != last["dir"]: break
        run += 1
    bull, bos_i, broken_i = last["dir"] == "bull", last["i"], last["broken"][0]

    prior = [j for j in range(n, broken_i) if _swing(c, j, n, "low" if bull else "high")]
    start = prior[-1] if prior else max(0, broken_i - 10)
    leg = range(start, bos_i + 1)                                              # the BOS leg
    origin = min(leg, key=lambda k: c[k]["l"]) if bull else max(leg, key=lambda k: c[k]["h"])
    protected = c[origin]["l"] if bull else c[origin]["h"]

    is_ob = (lambda x: x["c"] < x["o"]) if bull else (lambda x: x["c"] > x["o"])
    obs = [k for k in range(origin, bos_i) if is_ob(c[k])]                     # only candles inside the leg
    if obs:
        liq = obs[-1]; kind = "order_block"
        lower = [k for k in obs if k < liq and (c[k]["l"] < c[liq]["l"] if bull else c[k]["h"] > c[liq]["h"])]
        ent = lower[-1] if lower else origin
    else:
        wick = (lambda x: min(x["o"], x["c"]) - x["l"]) if bull else (lambda x: x["h"] - max(x["o"], x["c"]))
        liq = max(range(origin, bos_i), key=lambda k: wick(c[k]), default=origin); kind, ent = "wick", origin
    ob = c[ent]
    if bull: entry, sl = (ob["h"] if s["ob_entry"] == "top" else (ob["h"] + ob["l"]) / 2), protected
    else:    entry, sl = (ob["l"] if s["ob_entry"] == "top" else (ob["h"] + ob["l"]) / 2), protected
    risk = (entry - sl) if bull else (sl - entry)
    if risk <= 0: return None
    tp = entry + rr * risk if bull else entry - rr * risk

    after, price = c[bos_i + 1:], c[-1]["c"]
    if bull and (any(x["l"] <= entry for x in after) or price <= entry): return None   # already tapped / stale
    if not bull and (any(x["h"] >= entry for x in after) or price >= entry): return None
    h = datetime.datetime.utcnow().hour
    sess = "Asia" if h < 7 else "London" if h < 13 else "NY" if h < 21 else "Late"
    return {"side": "BUY" if bull else "SELL", "entry": entry, "sl": sl, "tp": tp, "rr": rr, "liquidity": kind,
            "session": sess, "bos_run": min(run, 5), "key": f"{last['dir']}-{c[bos_i]['t']}", "last_t": c[-1]["t"]}

def resolve(o, candles):
    """Walk candles since the order was created: PENDING -> OPEN -> WIN/LOSS/EXPIRED. SL wins ties (conservative)."""
    if o.get("outcome") not in ("PENDING", "OPEN"): return False
    bull, ch = o["side"] == "BUY", False
    for k in candles:
        if k["t"] <= o["last_t"]: continue
        o["last_t"] = k["t"]; o["bars"] = o.get("bars", 0) + 1
        if o["outcome"] == "PENDING":
            if (k["l"] <= o["entry"]) if bull else (k["h"] >= o["entry"]): o["outcome"] = "OPEN"; ch = True
            elif o["bars"] > 150: o["outcome"] = "EXPIRED"; return True
            else: continue
        sl_hit = (k["l"] <= o["sl"]) if bull else (k["h"] >= o["sl"])
        tp_hit = (k["h"] >= o["tp"]) if bull else (k["l"] <= o["tp"])
        if sl_hit: o["outcome"], o["r"] = "LOSS", -1.0; return True
        if tp_hit: o["outcome"], o["r"] = "WIN", o["rr"]; return True
    return ch

def stats(orders):
    seen, rows = set(), []
    for o in orders:
        if o.get("outcome") in ("WIN", "LOSS") and o.get("key") not in seen: seen.add(o["key"]); rows.append(o)
    out = {}
    for f in ("side", "liquidity", "session", "bos_run"):
        g = {}
        for o in rows: g.setdefault(str(o.get(f)), []).append(o["r"])
        out[f] = {k: {"n": len(v), "win": round(100 * sum(x > 0 for x in v) / len(v)), "exp": round(sum(v) / len(v), 2)} for k, v in g.items()}
    return out, len(rows)

def perf(orders, view="all"):
    """Track record. R-stats count each signal once; money sums every placed order (risk_amount x R)."""
    paper = lambda o: str(o.get("status", "")).startswith("PAPER")
    sel = [o for o in orders if o.get("outcome") in ("WIN", "LOSS") and (view == "all" or (view == "paper") == paper(o))]
    sel.sort(key=lambda o: o.get("time", ""))
    seen, sig = set(), []
    for o in sel:
        if o.get("key") not in seen: seen.add(o.get("key")); sig.append(o)
    wins, losses = [o["r"] for o in sig if o["r"] > 0], [o["r"] for o in sig if o["r"] <= 0]
    curve, tot, peak, dd = [0.0], 0.0, 0.0, 0.0
    for o in sig:
        tot += o["r"]; curve.append(round(tot, 2)); peak = max(peak, tot); dd = max(dd, peak - tot)
    n = len(sig)
    return {"trades": n, "wins": len(wins), "losses": len(losses), "win_rate": round(100 * len(wins) / n, 1) if n else 0,
            "total_r": round(tot, 2), "avg_r": round(tot / n, 2) if n else 0, "avg_win_r": round(sum(wins) / len(wins), 2) if wins else 0,
            "avg_loss_r": round(sum(losses) / len(losses), 2) if losses else 0,
            "profit_factor": round(sum(wins) / abs(sum(losses)), 2) if losses and sum(losses) else None,
            "max_drawdown_r": round(dd, 2), "money": round(sum(o.get("risk_amount", 0) * o["r"] for o in sel), 2), "curve": curve}

# ---------------- execution ----------------
def bn(acc, method, path, params=None, signed=True):
    base = "https://testnet.binancefuture.com" if acc.get("server", "").lower() == "testnet" else "https://fapi.binance.com"
    p, hdr = dict(params or {}), {}
    if signed:
        p.update(timestamp=int(time.time() * 1000), recvWindow=5000)
        q = urllib.parse.urlencode(p)
        q += "&signature=" + hmac.new(acc["password"].encode(), q.encode(), hashlib.sha256).hexdigest()
        hdr["X-MBX-APIKEY"] = acc["login"]
    else: q = urllib.parse.urlencode(p)
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{base}{path}?{q}", method=method, headers=hdr), timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e: raise RuntimeError(e.read().decode()[:300])

_filters = {}
def sym_filters(acc, sym):
    k = (acc.get("server", ""), sym)
    if k not in _filters:
        info = bn(acc, "GET", "/fapi/v1/exchangeInfo", signed=False)
        for s_ in info["symbols"]:
            f = {x["filterType"]: x for x in s_["filters"]}
            _filters[(acc.get("server", ""), s_["symbol"])] = {"tick": f["PRICE_FILTER"]["tickSize"], "step": f["LOT_SIZE"]["stepSize"], "min": f["LOT_SIZE"]["minQty"]}
    return _filters[k]

def q_floor(x, step): return str((Decimal(str(x)) / Decimal(step)).to_integral_value(ROUND_DOWN) * Decimal(step))

def live_binance(acc, o):
    f = sym_filters(acc, o["symbol"])
    qty, side = q_floor(o["size"], f["step"]), o["side"]
    if Decimal(qty) < Decimal(f["min"]): raise RuntimeError(f"size {qty} below min {f['min']}")
    opp = "SELL" if side == "BUY" else "BUY"
    bn(acc, "POST", "/fapi/v1/order", {"symbol": o["symbol"], "side": side, "type": "LIMIT", "timeInForce": "GTC", "quantity": qty, "price": q_floor(o["entry"], f["tick"])})
    bn(acc, "POST", "/fapi/v1/order", {"symbol": o["symbol"], "side": opp, "type": "STOP_MARKET", "stopPrice": q_floor(o["sl"], f["tick"]), "closePosition": "true", "workingType": "MARK_PRICE"})
    bn(acc, "POST", "/fapi/v1/order", {"symbol": o["symbol"], "side": opp, "type": "TAKE_PROFIT_MARKET", "stopPrice": q_floor(o["tp"], f["tick"]), "closePosition": "true", "workingType": "MARK_PRICE"})

def by(acc, method, path, params=None, body=None):
    base = "https://api-testnet.bybit.com" if acc.get("server", "").lower() == "testnet" else "https://api.bybit.com"
    ts, rw = str(int(time.time() * 1000)), "5000"
    if method == "GET": payload = urllib.parse.urlencode(params or {}); url, data = f"{base}{path}?{payload}", None
    else: payload = json.dumps(body or {}); url, data = base + path, payload.encode()
    sign = hmac.new(acc["password"].encode(), (ts + acc["login"] + rw + payload).encode(), hashlib.sha256).hexdigest()
    hdr = {"X-BAPI-API-KEY": acc["login"], "X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign, "X-BAPI-RECV-WINDOW": rw, "Content-Type": "application/json"}
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, method=method, headers=hdr), timeout=15) as r: j = json.loads(r.read())
    except urllib.error.HTTPError as e: raise RuntimeError(e.read().decode()[:300])
    if j.get("retCode") != 0: raise RuntimeError(str(j.get("retMsg")))
    return j["result"]

def live_bybit(acc, o):
    """USDT perpetuals (category=linear), one-way mode. Entry limit order with SL/TP attached to it."""
    r = by(acc, "GET", "/v5/market/instrument-info", {"category": "linear", "symbol": o["symbol"]})["list"][0]
    step, mn, tick = r["lotSizeFilter"]["qtyStep"], r["lotSizeFilter"]["minOrderQty"], r["priceFilter"]["tickSize"]
    qty = q_floor(o["size"], step)
    if Decimal(qty) < Decimal(mn): raise RuntimeError(f"size {qty} below min {mn}")
    by(acc, "POST", "/v5/order/create", body={"category": "linear", "symbol": o["symbol"], "side": "Buy" if o["side"] == "BUY" else "Sell",
        "orderType": "Limit", "qty": qty, "price": q_floor(o["entry"], tick), "timeInForce": "GTC", "positionIdx": 0,
        "stopLoss": q_floor(o["sl"], tick), "takeProfit": q_floor(o["tp"], tick), "slTriggerBy": "MarkPrice", "tpTriggerBy": "MarkPrice"})

def _http(url, method="GET", data=None, hdr=None):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, method=method, headers=hdr or {}), timeout=20) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e: raise RuntimeError(e.read().decode()[:300])

def _b64sign(secret, msg): return base64.b64encode(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()).decode()

def okx(acc, method, path, body=None, params=None):
    if params: path += "?" + urllib.parse.urlencode(params)
    now = datetime.datetime.utcnow(); ts = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    payload = json.dumps(body) if body else ""
    hdr = {"OK-ACCESS-KEY": acc["login"], "OK-ACCESS-SIGN": _b64sign(acc["password"], ts + method + path + payload), "OK-ACCESS-TIMESTAMP": ts,
           "OK-ACCESS-PASSPHRASE": acc.get("extra", ""), "Content-Type": "application/json"}
    if acc.get("server", "").lower() == "testnet": hdr["x-simulated-trading"] = "1"
    j = _http("https://www.okx.com" + path, method, payload.encode() if payload else None, hdr)
    if j.get("code") != "0": raise RuntimeError(j.get("msg") or str(j)[:200])
    return j["data"]

def live_okx(acc, o):
    inst = o["symbol"][:-4] + "-USDT-SWAP"
    i = okx(acc, "GET", "/api/v5/public/instruments", params={"instType": "SWAP", "instId": inst})[0]
    sz = q_floor(Decimal(str(o["size"])) / Decimal(i["ctVal"]), i["lotSz"])
    if Decimal(sz) < Decimal(i["minSz"]): raise RuntimeError(f"size {sz} below min {i['minSz']}")
    tick = i["tickSz"]
    r = okx(acc, "POST", "/api/v5/trade/order", body={"instId": inst, "tdMode": "cross", "side": o["side"].lower(), "ordType": "limit", "sz": sz,
        "px": q_floor(o["entry"], tick), "attachAlgoOrds": [{"tpTriggerPx": q_floor(o["tp"], tick), "tpOrdPx": "-1", "slTriggerPx": q_floor(o["sl"], tick), "slOrdPx": "-1"}]})
    if r[0].get("sCode") != "0": raise RuntimeError(r[0].get("sMsg"))
    return r[0]["ordId"]

def bitget(acc, method, path, body=None, params=None):
    if params: path += "?" + urllib.parse.urlencode(params)
    ts = str(int(time.time() * 1000)); payload = json.dumps(body) if body else ""
    hdr = {"ACCESS-KEY": acc["login"], "ACCESS-SIGN": _b64sign(acc["password"], ts + method + path + payload), "ACCESS-TIMESTAMP": ts,
           "ACCESS-PASSPHRASE": acc.get("extra", ""), "Content-Type": "application/json", "locale": "en-US"}
    j = _http("https://api.bitget.com" + path, method, payload.encode() if payload else None, hdr)
    if j.get("code") != "00000": raise RuntimeError(j.get("msg") or str(j)[:200])
    return j["data"]

def live_bitget(acc, o):
    c = bitget(acc, "GET", "/api/v2/mix/market/contracts", params={"productType": "USDT-FUTURES", "symbol": o["symbol"]})[0]
    vp, pp = int(c["volumePlace"]), int(c["pricePlace"])
    step = max(Decimal(str(c.get("sizeMultiplier") or 0)), Decimal(1).scaleb(-vp))
    qty = q_floor(o["size"], str(step))
    if Decimal(qty) < Decimal(str(c["minTradeNum"])): raise RuntimeError(f"size {qty} below min {c['minTradeNum']}")
    tick = str(Decimal(1).scaleb(-pp))
    r = bitget(acc, "POST", "/api/v2/mix/order/place-order", body={"symbol": o["symbol"], "productType": "USDT-FUTURES", "marginMode": "crossed", "marginCoin": "USDT",
        "size": qty, "price": q_floor(o["entry"], tick), "side": o["side"].lower(), "orderType": "limit", "force": "gtc",
        "presetStopLossPrice": q_floor(o["sl"], tick), "presetStopSurplusPrice": q_floor(o["tp"], tick)})
    return r.get("orderId")

TF_MS = lambda tf: int(tf[:-1]) * {"m": 60000, "h": 3600000, "d": 86400000}[tf[-1]]

def meta(acc, method, path, body=None, market=False):
    """MetaApi.cloud REST: one connection for ANY MT4/MT5 broker (Exness, FBS, FTMO, IC Markets...). login=API token, extra=accountId, server=region."""
    host = ("mt-market-data-client-api-v1." if market else "mt-client-api-v1.") + (acc.get("server") or "new-york") + ".agiliumtrade.ai"
    return _http(f"https://{host}/users/current/accounts/{acc['extra']}{path}", method, json.dumps(body).encode() if body else None,
                 {"auth-token": acc["login"], "Content-Type": "application/json"})

def fetch_candles_meta(acc, symbol, tf):
    now = datetime.datetime.utcnow()
    st = urllib.parse.quote(now.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
    rows = meta(acc, "GET", f"/historical-market-data/symbols/{symbol}/timeframes/{tf}/candles?startTime={st}&limit=300", market=True)
    out = [{"t": int(datetime.datetime.strptime(k["time"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000),
            "o": k["open"], "h": k["high"], "l": k["low"], "c": k["close"]} for k in rows]
    out.sort(key=lambda x: x["t"])
    if out and out[-1]["t"] + TF_MS(tf) > time.time() * 1000: out.pop()        # drop forming candle
    return out

def live_meta(acc, o):
    sym = o["symbol"]; core = re.sub(r"[a-z._#]+$", "", sym)                  # EURUSDm -> EURUSD
    try: sp = meta(acc, "GET", f"/symbols/{sym}/specification")
    except Exception: sp = {}
    cs = float(sp.get("contractSize") or (100 if core.startswith("XAU") else 100000))
    step, mn, digits = float(sp.get("volumeStep") or 0.01), float(sp.get("minVolume") or 0.01), int(sp.get("digits") or 5)
    dist = abs(o["entry"] - o["sl"])
    if core.endswith("USD"): per_lot = dist * cs
    elif core.startswith("USD"): per_lot = dist * cs / o["entry"]
    else: raise RuntimeError("cross pairs not supported for auto lot sizing (USD account)")
    lots = int(o["risk_amount"] / per_lot / step + 1e-9) * step
    if lots < mn: raise RuntimeError(f"lot size {lots:.3f} below broker minimum {mn} (balance too small for this stop)")
    r = meta(acc, "POST", "/trade", {"actionType": "ORDER_TYPE_BUY_LIMIT" if o["side"] == "BUY" else "ORDER_TYPE_SELL_LIMIT", "symbol": sym,
        "volume": round(lots, 3), "openPrice": round(o["entry"], digits), "stopLoss": round(o["sl"], digits), "takeProfit": round(o["tp"], digits), "comment": "ISIGBOT"})
    if r.get("stringCode") not in ("TRADE_RETCODE_DONE", "TRADE_RETCODE_PLACED"): raise RuntimeError(str(r.get("message") or r)[:200])
    return r.get("orderId")

LIVE = {"BINANCE": live_binance, "BYBIT": live_bybit, "OKX": live_okx, "BITGET": live_bitget, "MT4": live_meta, "MT5": live_meta}

def cancel_orders(a, o):
    p, sym = a["platform"], o["symbol"]
    if p == "BINANCE": bn(a, "DELETE", "/fapi/v1/allOpenOrders", {"symbol": sym})
    elif p == "BYBIT": by(a, "POST", "/v5/order/cancel-all", body={"category": "linear", "symbol": sym})
    elif p == "OKX": okx(a, "POST", "/api/v5/trade/cancel-order", body={"instId": sym[:-4] + "-USDT-SWAP", "ordId": o["ext_id"]})
    elif p == "BITGET": bitget(a, "POST", "/api/v2/mix/order/cancel-order", body={"symbol": sym, "productType": "USDT-FUTURES", "orderId": o["ext_id"]})
    elif p in ("MT4", "MT5"): meta(a, "POST", "/trade", {"actionType": "ORDER_CANCEL", "orderId": o["ext_id"]})

def balance(acc):
    p = acc["platform"]
    if p == "BINANCE": return float(next(x for x in bn(acc, "GET", "/fapi/v2/balance") if x["asset"] == "USDT")["balance"])
    if p == "BYBIT": return float(by(acc, "GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"})["list"][0]["totalEquity"])
    if p == "OKX": return float(okx(acc, "GET", "/api/v5/account/balance")[0]["totalEq"])
    if p == "BITGET": return float(next(x for x in bitget(acc, "GET", "/api/v2/mix/account/accounts", params={"productType": "USDT-FUTURES"}) if x["marginCoin"] == "USDT")["accountEquity"])
    if p in ("MT4", "MT5"): return float(meta(acc, "GET", "/account-information")["balance"])
    if p == "EA": return float(acc.get("balance") or 0)                       # the EA reports its own balance
    raise RuntimeError("unsupported platform")

def is_fx(sym): return not sym.endswith("USDT")

def place_order(acc, sig, sym, s):
    bal = float(acc.get("balance") or 0)
    if s["mode"] == "live":
        try: bal = balance(acc)                                                # use the real balance for sizing
        except Exception: pass
    risk_amt = bal * s["risk_pct"] / 100
    dist = abs(sig["entry"] - sig["sl"])
    o = {"id": uuid.uuid4().hex[:8], "time": str(datetime.datetime.utcnow()), "account": acc["id"], "platform": acc["platform"],
         "symbol": sym, "size": round(risk_amt / dist, 6) if dist else 0, "risk_amount": round(risk_amt, 2),
         "outcome": "PENDING", "bars": 0, **sig}
    if s["mode"] == "paper": o["status"] = "PAPER"
    elif acc["platform"] == "EA": o["status"], o["ea_ack"] = "LIVE: queued for EA", False
    elif acc["platform"] in LIVE:
        try: o["ext_id"] = LIVE[acc["platform"]](acc, o); o["status"] = "LIVE: orders sent"
        except Exception as e: o["status"], o["outcome"] = f"LIVE FAILED: {e}", "REJECTED"
    else: o["status"], o["outcome"] = f"REJECTED: no live adapter for {acc['platform']}", "REJECTED"
    return o

def run_scan():
    with LOCK:
        d = load(); s = d["settings"]; new, cache = [], {}
        def candles(sym):
            if sym not in cache:
                if is_fx(sym):
                    ma = next((a for a in d["accounts"] if a["platform"] in ("MT4", "MT5")), None)
                    if not ma: raise RuntimeError(f"{sym}: add an MT4/MT5 account (via MetaApi) to get forex/gold candles")
                    cache[sym] = fetch_candles_meta(ma, sym, s["timeframe"])
                else: cache[sym] = fetch_candles(sym, s["timeframe"])
            return cache[sym]
        for o in d["orders"]:                                                  # 1) resolve open trades
            if o.get("outcome") in ("PENDING", "OPEN") and o.get("platform") != "EA":      # EA orders are resolved from the EA's own candles
                try:
                    resolve(o, candles(o["symbol"]))
                    if o["outcome"] == "EXPIRED" and o["status"].startswith("LIVE"):
                        a = next((a for a in d["accounts"] if a["id"] == o["account"]), None)
                        if a: cancel_orders(a, o)
                except Exception: pass
        for sym in s["symbols"]:                                               # 2) look for new setups
            try: sig = find_setup(candles(sym), s)
            except Exception as e:
                new.append({"id": uuid.uuid4().hex[:8], "time": str(datetime.datetime.utcnow()), "symbol": sym, "status": f"DATA ERROR: {e}", "outcome": "ERROR"}); continue
            if not sig or f"{sym}-{sig['key']}" in d["seen"]: continue
            d["seen"] = (d["seen"] + [f"{sym}-{sig['key']}"])[-500:]
            if any(str(sig[f]) in s["filters"].get(f, []) for f in s["filters"] if f in sig): continue   # learned filters
            accs = [a for a in d["accounts"] if a["platform"] != "EA" and (a["platform"] in ("MT4", "MT5")) == is_fx(sym)] or [{"id": "-", "platform": "SIGNAL", "balance": 0}]
            for a in accs:
                busy = any(o.get("account") == a["id"] and o.get("symbol") == sym and o.get("outcome") in ("PENDING", "OPEN") for o in d["orders"] + new)
                if not busy: new.append(place_order(a, sig, sym, s))
        d["orders"] = (d["orders"] + new)[-5000:]; save(d); return new

def autoscan():
    while True:
        try: run_scan()
        except Exception as e: print("scan error", e)
        time.sleep(300)
if os.environ.get("AUTOSCAN") == "1": threading.Thread(target=autoscan, daemon=True).start()

# ---------------- Claude (optional) for screenshot analysis ----------------
def claude(content, system):
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key: return "Set ANTHROPIC_API_KEY to enable analysis."
    body = json.dumps({"model": os.environ.get("ANALYSIS_MODEL", "claude-sonnet-5-5"), "max_tokens": 1500, "system": system,
                       "messages": [{"role": "user", "content": content}]}).encode()
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=body, method="POST",
                                 headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=90) as r: j = json.loads(r.read())
        return "".join(b.get("text", "") for b in j["content"])
    except Exception as e: return f"Analysis failed: {e}"

# ---------------- UI ----------------
CSS = """
:root{--bg:#f4f7fb;--ink:#0f172a;--mut:#64748b;--line:#e2e8f0;--brand:#2563eb;--brand2:#7c3aed}
*{margin:0;padding:0;box-sizing:border-box;font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}
html{scroll-behavior:smooth}body{background:var(--bg);color:var(--ink);line-height:1.5}
a{color:var(--brand)}
.nav{position:sticky;top:0;z-index:50;background:rgba(255,255,255,.88);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border-bottom:1px solid var(--line);padding:12px 24px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
.logo{display:flex;align-items:center;gap:10px;font-weight:800;font-size:18px;color:var(--ink);text-decoration:none;letter-spacing:-.01em}
.mark{width:34px;height:34px;border-radius:10px;background:linear-gradient(135deg,var(--brand),var(--brand2));display:grid;place-items:center;box-shadow:0 6px 16px rgba(37,99,235,.35)}
.links a{margin-left:4px;text-decoration:none;color:#475569;font-weight:700;font-size:12px;letter-spacing:.04em;padding:8px 12px;border-radius:999px;transition:.2s}
.links a:hover{background:#eef2ff;color:var(--brand)}
.card{background:#fff;border:1px solid var(--line);border-radius:18px;padding:22px;margin:16px auto;max-width:1100px;box-shadow:0 8px 30px rgba(15,23,42,.05);animation:fade .5s ease both}
@keyframes fade{from{opacity:0;translate:0 10px}to{opacity:1;translate:0 0}}
.btn{background:linear-gradient(135deg,var(--brand),var(--brand2));color:#fff;padding:11px 18px;border-radius:12px;border:none;font-weight:700;cursor:pointer;text-decoration:none;display:inline-block;transition:.2s;box-shadow:0 8px 20px rgba(37,99,235,.25)}
.btn:hover{transform:translateY(-2px);box-shadow:0 12px 28px rgba(37,99,235,.35)}
.input{width:100%;padding:11px;border:1px solid var(--line);border-radius:12px;margin-top:4px;background:#fff;transition:.2s}
.input:focus{outline:none;border-color:var(--brand);box-shadow:0 0 0 4px rgba(37,99,235,.12)}
label{font-weight:600;font-size:12px;color:#334155}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px}.muted{color:var(--mut);font-size:13px}
pre{white-space:pre-wrap;background:#f1f5f9;padding:12px;border-radius:10px;font-size:12px;margin-top:6px}h2,h3{margin-bottom:8px}
table th{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.05em}
table tr:hover td{background:#f8fafc}
.foot{text-align:center;color:var(--mut);font-size:12px;padding:30px 20px 40px}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
"""

# ---------------- installable app (PWA): manifest, service worker, icons ----------------
import zlib, struct, functools
@functools.lru_cache(maxsize=4)
def _icon_png(size):
    bars = [(.24, .47, .34, .76), (.45, .26, .55, .76), (.66, .38, .76, .76)]
    raw = bytearray()
    for yy in range(size):
        raw.append(0); fy = yy / size
        for xx in range(size):
            fx = xx / size
            if any(x0 <= fx <= x1 and y0 <= fy <= y1 for x0, y0, x1, y1 in bars): raw += b"\xff\xff\xff"
            else:
                t = (fx + fy) / 2; raw += bytes((int(37 + 87 * t), int(99 - 41 * t), int(235 + 2 * t)))
    chunk = lambda tag, data: struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xffffffff)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(bytes(raw), 9)) + chunk(b"IEND", b"")

@app.route("/icon-<int:size>.png")
def icon(size):
    if size not in (192, 512): abort(404)
    return _icon_png(size), 200, {"Content-Type": "image/png", "Cache-Control": "public, max-age=86400"}

@app.route("/manifest.webmanifest")
def manifest():
    name = load()["settings"]["site_name"]
    return jsonify({"name": name, "short_name": name[:12], "start_url": "/", "scope": "/", "display": "standalone", "background_color": "#0b1220", "theme_color": "#2563eb",
                    "icons": [{"src": f"/icon-{z}.png", "sizes": f"{z}x{z}", "type": "image/png", "purpose": "any maskable"} for z in (192, 512)]}), 200, {"Content-Type": "application/manifest+json"}

@app.route("/sw.js")
def service_worker():
    js = "self.addEventListener('install',function(){self.skipWaiting()});self.addEventListener('activate',function(e){e.waitUntil(self.clients.claim())});" \
         "self.addEventListener('fetch',function(e){e.respondWith(fetch(e.request).catch(function(){return new Response('You are offline. Reconnect to open the app.',{headers:{'Content-Type':'text/plain'}})}))});"
    return js, 200, {"Content-Type": "application/javascript", "Service-Worker-Allowed": "/", "Cache-Control": "no-cache"}

PWA_JS = "<script>if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js');var dp;addEventListener('beforeinstallprompt',function(e){e.preventDefault();dp=e;var b=document.getElementById('inst');b.style.display='inline-block';b.onclick=function(x){x.preventDefault();dp.prompt();b.style.display='none'}});</script>"

LOGO_SVG = "<svg width='18' height='18' viewBox='0 0 18 18'><rect x='2' y='8' width='3' height='8' rx='1' fill='#fff'/><rect x='7.5' y='3' width='3' height='13' rx='1' fill='#fff'/><rect x='13' y='6' width='3' height='10' rx='1' fill='#fff' opacity='.85'/></svg>"

def layout(title, body):
    name = escape(load()["settings"]["site_name"])
    links = "".join(f"<a href='{h}'>{t}</a>" for h, t in (("/", "HOME"), ("/setup", "SETUP"), ("/performance", "PERFORMANCE"), ("/orders", "ORDERS"), ("/status", "MY ACCOUNT"), ("/admin", "ADMIN")))
    return (f"<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>{escape(title)} · {name}</title>"
            f"<link rel='preconnect' href='https://fonts.googleapis.com'><link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&display=swap' rel='stylesheet'>"
            f"<link rel='manifest' href='/manifest.webmanifest'><meta name='theme-color' content='#2563eb'><link rel='apple-touch-icon' href='/icon-192.png'><style>{CSS}</style></head><body><div class='nav'><a class='logo' href='/'><span class='mark'>{LOGO_SVG}</span>{name}</a><div class='links'>{links}<a href='#' id='inst' style='display:none'>⬇ INSTALL APP</a></div></div>{body}"
            f"<div class='foot'>© {datetime.datetime.utcnow().year} {name} · Trading involves risk. Past results do not guarantee future results. Not financial advice.</div>{PWA_JS}</body></html>")

@app.after_request
def secure_headers(r):
    r.headers.update({"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer",
        "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
        "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'; object-src 'none'"})
    if os.environ.get("RENDER"): r.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    if request.path.startswith(("/admin", "/status", "/ea/download", "/save_account")): r.headers["Cache-Control"] = "no-store"
    return r

@app.before_request
def csrf_guard():
    """Browsers always send Origin on form posts: refuse posts that come from another website."""
    if request.method == "POST" and request.path != "/ea/sync":
        o = request.headers.get("Origin")
        if o and urllib.parse.urlparse(o).netloc != request.host: abort(403)

FAILS = {}
def throttled(name, limit=10, window=600, add=False):
    now = time.time(); k = f"{name}:{_ip()}"
    if len(FAILS) > 5000: FAILS.clear()
    FAILS[k] = [t for t in FAILS.get(k, []) if now - t < window]
    if add: FAILS[k].append(now)
    return len(FAILS[k]) >= limit
def _ip(): return (request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0]).strip()
def _pw_hash(pw, salt): return hashlib.pbkdf2_hmac("sha256", pw.encode(), bytes.fromhex(salt), 200000).hex()
def check_admin(pw):
    """Password changed in the admin panel (stored hashed) wins; otherwise ADMIN_PASSWORD env var / built-in default."""
    h = load().get("admin_hash")
    if h: return hmac.compare_digest(_pw_hash(pw, h["salt"]).encode(), h["hash"].encode())
    return hmac.compare_digest(pw.encode(), ADMIN_PASSWORD.encode())

def _warnings(d):
    w = []
    if not d.get("admin_hash"): w.append("You are still using the built-in admin password. Change it under Security below.")
    if not CRYPTO: w.append("Encryption library missing: add 'cryptography' to requirements.txt and redeploy. New exchange keys are refused until then.")
    if not DATABASE_URL: w.append("Render's free plan erases this app's files every time it restarts. Add a free Neon database (environment variable DATABASE_URL) so accounts, orders and results survive.")
    elif DB_STATUS["ok"] is False: w.append("The database is not reachable right now: " + DB_STATUS["err"] + " (the app keeps running; it retries automatically)")
    if DATABASE_URL and not (os.environ.get("DATA_KEY") or os.environ.get("SECRET_KEY")): w.append("With a database you MUST set DATA_KEY on Render, otherwise new exchange accounts are refused.")
    if not os.environ.get("DATA_KEY"): w.append("Set a DATA_KEY environment variable on Render (any long random text) so stored keys are encrypted with a key that is not on the disk.")
    ok = "<div class='card' style='background:#ecfdf5;border-color:#a7f3d0;font-weight:600'>✅ Data is stored in your external database, so restarts will not erase it.</div>" if DATABASE_URL and DB_STATUS["ok"] else ""
    return ok + "".join(f"<div class='card' style='background:#fef2f2;border-color:#fecaca;font-weight:600;color:#991b1b'>⚠️ {escape(x)}</div>" for x in w)

def admin_only(f):
    def w(*a, **k):
        if session.get("admin") is not True: return redirect("/admin")
        return f(*a, **k)
    w.__name__ = f.__name__; return w

def _hero_chart(rr=2.0):
    """Animated illustration of ONE bullish setup, drawn with the bot's real rules:
    SL = protected low (bottom of the leg) -> liquidity = last order block before the BOS ->
    ENTRY = top of the order block BELOW the liquidity (so entry sits between liquidity and SL) -> TP = 1:RR."""
    T, X0, DX, W = 16.0, 20, 26, 14
    SL_Y, EN_Y = 250, 200
    TP_Y = max(EN_Y - rr * (SL_Y - EN_Y), 22)
    # candles as (open_y, close_y, high_y, low_y); smaller y = higher price
    C = [(238, 228, 224, 250), (202, 222, 200, 226), (222, 186, 182, 226), (188, 166, 162, 192), (166, 178, 162, 182), (176, 146, 142, 180),
         (148, 124, 112, 152), (124, 132, 118, 136), (130, 98, 94, 134), (98, 128, 96, 132), (128, 164, 126, 168), (164, 198, 162, 204),
         (198, 150, 146, 202), (152, 108, 104, 156), (110, 82, 78, 114), (84, 58, 52, 88)]
    css, n = [], [0]

    def show(t, scale=False):
        n[0] += 1; k = n[0]; a = t / T * 100; b = a + 1.5
        f, g = ("transform:scaleY(.2);", "transform:scaleY(1);") if scale else ("", "")
        css.append(f"@keyframes k{k}{{0%,{a:.2f}%{{opacity:0;{f}}}{b:.2f}%,92%{{opacity:1;{g}}}100%{{opacity:0;{g}}}}}")
        return f"class='sq{' cd' if scale else ''}' style='animation-name:k{k}'"

    def level(y, col, txt, t):
        return (f"<g {show(t)}><line x1='0' x2='520' y1='{y}' y2='{y}' stroke='{col}' stroke-width='1.5' stroke-dasharray='6 5'/>"
                f"<rect x='434' y='{y - 10}' width='78' height='20' rx='10' fill='{col}'/>"
                f"<text x='473' y='{y + 4}' text-anchor='middle' font-size='10' font-weight='800' fill='#fff'>{txt}</text></g>")

    def pill(x, y, w, txt, col, t, tc="#fff"):
        return (f"<g {show(t)}><rect x='{x}' y='{y}' width='{w}' height='18' rx='9' fill='{col}'/>"
                f"<text x='{x + w / 2}' y='{y + 12.5}' text-anchor='middle' font-size='10' font-weight='800' fill='{tc}'>{txt}</text></g>")

    cx = lambda i: X0 + i * DX
    p = [f"<rect width='520' height='300' fill='#0b1220'/>"]
    p.append("".join(f"<line x1='0' x2='520' y1='{gy}' y2='{gy}' stroke='#1e293b'/>" for gy in range(40, 300, 50)))
    p.append(level(round(TP_Y), "#22c55e", f"TP 1:{rr:g}", 7.4))
    p.append(level(EN_Y, "#3b82f6", "ENTRY", 5.6))
    p.append(level(SL_Y, "#ef4444", "SL", 6.6))
    # BOS line (the swing high that gets broken)
    p.append(f"<g {show(3.7)}><line x1='{cx(6) - 6}' x2='{cx(8) + 40}' y1='112' y2='112' stroke='#eab308' stroke-width='1.5' stroke-dasharray='4 4'/></g>")
    p.append(pill(cx(6) - 50, 86, 44, "BOS", "#eab308", 3.7, "#0b1220"))
    # liquidity = last order block before the BOS (purple); order block = entry zone below it (blue)
    p.append(f"<g {show(4.6)}><rect x='{cx(4) - 4}' y='160' width='22' height='24' rx='3' fill='rgba(168,85,247,.28)' stroke='#a855f7' stroke-width='1.5'/>"
             f"<line x1='98' y1='137' x2='{cx(4) - 2}' y2='162' stroke='#a855f7' stroke-width='1.2'/></g>")
    p.append(pill(24, 128, 76, "LIQUIDITY", "#a855f7", 4.6))
    p.append(f"<g {show(5.6)}><rect x='{cx(1) - 4}' y='{EN_Y}' width='22' height='26' rx='3' fill='rgba(59,130,246,.3)' stroke='#3b82f6' stroke-width='1.5'/>"
             f"<line x1='{cx(1) + 7}' y1='226' x2='{cx(1) + 7}' y2='268' stroke='#3b82f6' stroke-width='1.2'/></g>")
    p.append(pill(18, 268, 98, "ORDER BLOCK", "#3b82f6", 5.6))
    for i, (o, c, h, l) in enumerate(C):
        t = 0.3 + i * 0.4 if i < 9 else 8.4 + (i - 9) * 0.5
        col = "#22c55e" if c < o else "#ef4444"; top, bot = min(o, c), max(o, c); x = cx(i)
        p.append(f"<g {show(t, True)}><line x1='{x + W / 2}' x2='{x + W / 2}' y1='{h}' y2='{l}' stroke='{col}' stroke-width='2'/>"
                 f"<rect x='{x}' y='{top}' width='{W}' height='{max(bot - top, 2)}' rx='2' fill='{col}'/></g>")
    # price taps the entry, then reaches the target
    p.append(f"<g {show(9.6)}><circle class='ring' cx='{cx(11) + 7}' cy='{EN_Y}' r='7' fill='none' stroke='#3b82f6' stroke-width='2.5'/></g>")
    p.append(f"<g {show(10.9)}><circle class='ring' cx='{cx(14) + 7}' cy='{round(TP_Y)}' r='7' fill='none' stroke='#22c55e' stroke-width='2.5'/></g>")
    style = (f"<style>.sq{{animation-duration:{T:g}s;animation-iteration-count:infinite;animation-timing-function:linear}}"
             ".cd{transform-box:fill-box;transform-origin:50% 100%}.ring{transform-box:fill-box;transform-origin:center;animation:rg 1.5s ease-out infinite}"
             "@keyframes rg{from{transform:scale(.6);opacity:1}to{transform:scale(2.4);opacity:0}}" + "".join(css) + "</style>")
    return f"<svg viewBox='0 0 520 300'>{style}{''.join(p)}</svg>"

HOME_TPL = """
<style>
.hero{position:relative;overflow:hidden;background:radial-gradient(1100px 560px at 85% -10%,#3730a3 0,transparent 60%),linear-gradient(135deg,#0b1220,#101a36 60%,#0b1220);color:#fff;padding:64px 24px 80px}
.hero .in{max-width:1100px;margin:auto;display:grid;grid-template-columns:1.05fr .95fr;gap:40px;align-items:center;position:relative;z-index:2}
@media(max-width:860px){.hero .in{grid-template-columns:1fr}.hero{padding-top:44px}}
.orb{position:absolute;border-radius:50%;filter:blur(70px);opacity:.5;animation:float 10s ease-in-out infinite}
.o1{width:340px;height:340px;background:#2563eb;left:-90px;top:-70px}.o2{width:300px;height:300px;background:#7c3aed;right:-70px;bottom:-90px;animation-delay:-5s}
@keyframes float{50%{transform:translate(34px,-28px) scale(1.1)}}
.badge{display:inline-flex;gap:8px;align-items:center;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.18);padding:6px 14px;border-radius:999px;font-size:12px;font-weight:700;letter-spacing:.04em}
.dot{width:8px;height:8px;border-radius:50%;background:#22c55e;animation:pulse 1.8s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(34,197,94,.7)}70%,100%{box-shadow:0 0 0 10px rgba(34,197,94,0)}}
.hero h1{font-size:clamp(36px,6.4vw,60px);font-weight:900;line-height:1.04;margin:18px 0 14px;letter-spacing:-.02em}
.grad{background:linear-gradient(90deg,#60a5fa,#c084fc,#22d3ee,#60a5fa);background-size:250% auto;-webkit-background-clip:text;background-clip:text;color:transparent;animation:shine 7s linear infinite}
@keyframes shine{to{background-position:250% center}}
.lead{color:#cbd5e1;font-size:17px;max-width:520px}
.cta{display:flex;gap:10px;flex-wrap:wrap;margin-top:24px}
.btn.ghost{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.28);box-shadow:none}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:32px}
@media(max-width:520px){.kpis{grid-template-columns:repeat(2,1fr)}}
.kpi{background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.14);border-radius:14px;padding:12px 14px}
.kpi b{display:block;font-size:24px;font-weight:800}.kpi span{font-size:11px;color:#94a3b8}
.chartbox{position:relative;background:rgba(15,23,42,.7);border:1px solid rgba(255,255,255,.14);border-radius:22px;padding:14px;box-shadow:0 30px 80px rgba(0,0,0,.5);animation:bobbox 7s ease-in-out infinite}
@keyframes bobbox{50%{transform:translateY(-8px)}}
.chartbox svg{width:100%;height:auto;display:block;border-radius:14px}
.chart-top{display:flex;justify-content:space-between;font-size:12px;font-weight:700;color:#94a3b8;margin:0 4px 10px}
.chip{position:absolute;background:#fff;color:#0f172a;font-weight:800;font-size:11px;padding:7px 12px;border-radius:999px;box-shadow:0 10px 30px rgba(0,0,0,.4);animation:bob 4s ease-in-out infinite;white-space:nowrap}
@keyframes bob{50%{transform:translateY(-12px)}}
.c1{left:-12px;top:54px}.c2{right:-10px;top:120px;animation-delay:-1s}.c3{left:24px;bottom:-14px;animation-delay:-2s}.c4{right:34px;bottom:28px;animation-delay:-3s}
.scroll{animation:scrollx 18s linear infinite}
@keyframes scrollx{to{transform:translateX(-880px)}}
.lvl{animation:blink 2.6s ease-in-out infinite}
@keyframes blink{50%{opacity:.5}}
.marq{overflow:hidden;background:#fff;border-bottom:1px solid var(--line);padding:16px 0}
.tr{display:flex;width:max-content;animation:mq 28s linear infinite}
.tr span{font-weight:800;color:#475569;letter-spacing:.14em;font-size:13px;margin-right:48px}
@keyframes mq{to{transform:translateX(-50%)}}
.sec{max-width:1100px;margin:52px auto;padding:0 20px}
.sec h2{font-size:30px;font-weight:800;text-align:center;letter-spacing:-.01em}.sec .sub{text-align:center;color:var(--mut);margin:6px auto 26px;max-width:560px}
.steps{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:16px}
.step{background:#fff;border:1px solid var(--line);border-radius:20px;padding:22px;box-shadow:0 8px 24px rgba(15,23,42,.05);transition:transform .25s,box-shadow .25s;animation:up .7s both}
@keyframes up{from{opacity:0;translate:0 22px}to{opacity:1;translate:0 0}}
.step:hover{transform:translateY(-6px);box-shadow:0 18px 40px rgba(37,99,235,.18)}
.step .n{width:42px;height:42px;border-radius:12px;background:linear-gradient(135deg,var(--brand),var(--brand2));color:#fff;display:grid;place-items:center;font-weight:800;margin-bottom:14px}
.step h3{font-size:17px}.step p{color:var(--mut);font-size:14px}
.band{max-width:1060px;margin:56px auto;border-radius:24px;padding:42px 28px;text-align:center;color:#fff;background:linear-gradient(135deg,#1d4ed8,#7c3aed);position:relative;overflow:hidden}
.band h2{font-size:28px;font-weight:800}.band p{opacity:.9;margin:8px 0 20px}
.band .btn{background:#fff;color:#1d4ed8;box-shadow:none}
.band::before{content:'';position:absolute;width:260px;height:260px;border-radius:50%;background:rgba(255,255,255,.12);right:-60px;top:-80px;animation:float 9s ease-in-out infinite}
</style>
<section class='hero'><div class='orb o1'></div><div class='orb o2'></div>
<div class='in'>
 <div>
  <span class='badge'><span class='dot'></span>@@MODE@@ MODE · SCANNING THE MARKET</span>
  <h1><span class='grad'>@@NAME@@</span><br>@@TAGLINE@@</h1>
  <p class='lead'>@@LEAD@@</p>
  <div class='cta'><a class='btn' href='/setup'>CONNECT ACCOUNT →</a><a class='btn ghost' href='/performance'>VIEW PERFORMANCE</a></div>
  <div class='kpis'>
   <div class='kpi'><b data-to='@@TRADES@@'>0</b><span>Closed trades</span></div>
   <div class='kpi'><b data-to='@@WIN@@' data-dec='1' data-suf='%'>0</b><span>Win rate</span></div>
   <div class='kpi'><b data-to='@@TOTALR@@' data-dec='2' data-pre='@@PRE@@' data-suf='R'>0</b><span>Total result</span></div>
   <div class='kpi'><b data-to='@@NPLAT@@'>0</b><span>Platforms supported</span></div>
  </div>
 </div>
 <div class='chartbox'>
  <div class='chart-top'><span>@@META@@</span><span style='color:#22c55e'>● LIVE SETUP</span></div>
  @@CHART@@
  <span class='chip c1'>✔ Break of Structure</span><span class='chip c2'>💧 Liquidity</span><span class='chip c3'>🎯 Order Block Entry</span><span class='chip c4'>🛡 Protected SL</span>
 </div>
</div></section>
<div class='marq'><div class='tr'>@@PLATS@@@@PLATS@@</div></div>
<div class='sec'><h2>How the robot trades</h2><p class='sub'>Four clear steps, applied the same way every time, with no emotion and no guessing.</p>
<div class='steps'>
 <div class='step' style='animation-delay:.05s'><div class='n'>01</div><h3>Break of Structure</h3><p>Reads the @@TF@@ chart and confirms the trend from consecutive major breaks of structure.</p></div>
 <div class='step' style='animation-delay:.15s'><div class='n'>02</div><h3>Finds the liquidity</h3><p>Marks the last order block or wick inside the latest break, where the big money sits.</p></div>
 <div class='step' style='animation-delay:.25s'><div class='n'>03</div><h3>Precise entry</h3><p>Places a limit order at the order block below (or above) that liquidity zone.</p></div>
 <div class='step' style='animation-delay:.35s'><div class='n'>04</div><h3>Risk managed</h3><p>Stop loss at the protected low or high, take profit at 1:@@RR@@, sized to risk only @@RISK@@% per trade.</p></div>
</div></div>
<div class='card'><h3>Strategy rules</h3><pre>@@STRAT@@</pre></div>
<div class='band'><h2>Start with paper trading, go live when you trust it</h2><p>Connect your account, watch every trade get logged with its result, and judge the numbers yourself.</p><a class='btn' href='/setup'>CONNECT ACCOUNT →</a></div>
<script>
(function(){var reduce=window.matchMedia&&matchMedia('(prefers-reduced-motion: reduce)').matches;
document.querySelectorAll('[data-to]').forEach(function(el){var to=parseFloat(el.dataset.to)||0,dec=+(el.dataset.dec||0),suf=el.dataset.suf||'',pre=el.dataset.pre||'';
function show(v){el.textContent=pre+v.toFixed(dec)+suf}
if(reduce){show(to);return}var t0=null;function f(t){if(!t0)t0=t;var k=Math.min((t-t0)/1500,1);show(to*(1-Math.pow(1-k,3)));if(k<1)requestAnimationFrame(f)}requestAnimationFrame(f)})})();
</script>
"""

@app.route("/")
def home():
    d = load(); s = d["settings"]; p = perf(d["orders"])
    rep = {"@@NAME@@": str(escape(s["site_name"])), "@@MODE@@": str(escape(s["mode"])).upper(), "@@RR@@": str(s["rr"]), "@@RISK@@": str(s["risk_pct"]),
           "@@TF@@": str(escape(s["timeframe"])), "@@TRADES@@": str(p["trades"]), "@@WIN@@": str(p["win_rate"]), "@@TOTALR@@": str(p["total_r"]),
           "@@PRE@@": "+" if p["total_r"] >= 0 else "", "@@NPLAT@@": str(len(s["platforms"])), "@@CHART@@": _hero_chart(s["rr"]),
           "@@META@@": str(escape(f"{s['timeframe']} · {', '.join(s['symbols'])}")), "@@PLATS@@": "".join(f"<span>{escape(x)}</span>" for x in s["platforms"]) * 3,
           "@@STRAT@@": str(escape(s["strategy_text"])), "@@TAGLINE@@": str(escape(s["tagline"])), "@@LEAD@@": str(escape(s["lead"]))}
    html = HOME_TPL
    for k, v in rep.items(): html = html.replace(k, v)
    return layout("Home", html)

@app.route("/setup")
def setup():
    d = load(); s = d["settings"]
    if session.get("admin") is True:
        saved = "".join(f"<div style='border:1px solid #e2e8f0;padding:10px;border-radius:10px;margin:6px 0'><b>#{escape(a['id'])} {escape(a['platform'])}</b> · key {escape('hidden' if a['platform'] == 'EA' else str(a['login'])[:4] + '****')}{' · EA last heard ' + escape(LAST_SEEN.get(a['id'], 'never')) if a['platform'] == 'EA' else ''} · balance {escape(str(a.get('balance','0')))} · {escape(a.get('server',''))} <form action='/test_account/{escape(a['id'])}' method='post' style='display:inline'><button class='btn' style='padding:4px 10px;font-size:12px;margin-left:8px'>Test connection</button></form></div>" for a in d["accounts"]) or "<p class='muted'>No account yet.</p>"
    else: saved = "<p class='muted'>Fill the form to connect your account. Saved accounts are visible to the admin only.</p>"
    plats = "".join(f"<option>{escape(p)}</option>" for p in s["platforms"])
    return layout("Setup", f"""<div class='card'><h2>Accounts</h2>{saved}<hr style='margin:14px 0;border:none;border-top:1px solid #e2e8f0'>
<form action='/save_account' method='post' class='grid'><div><label>Account #</label><input class='input' name='id' type='number' min='1' max='{s['max_accounts']}' required></div>
<div><label>Platform</label><select class='input' name='platform'>{plats}</select></div>
<div><label>API key / login (platform EA: leave blank, a key is created for you)</label><input class='input' name='login'></div><div><label>API secret (MT4/MT5: type any text)</label><input class='input' name='password' type='password'></div>
<div><label>Extra: passphrase (OKX, Bitget) · MetaApi account ID (MT4/MT5)</label><input class='input' name='extra'></div>
<div><label>Server (Binance: type testnet for test money, blank for real)</label><input class='input' name='server'></div>
<div><label>Account balance (USDT) used for sizing</label><input class='input' name='balance' type='number' step='any'></div><div><button class='btn'>Save</button></div></form>
<div class='muted' style='margin-top:12px;line-height:1.7'><b>What to enter</b><br>
<b>BINANCE / BYBIT:</b> API key + secret. Server: blank = real, <code>testnet</code> = test money.<br>
<b>OKX / BITGET:</b> API key + secret + passphrase (in Extra). OKX Server <code>testnet</code> = demo keys.<br>
<b>MT5 / MT4</b> (Exness, FBS, FTMO, IC Markets, any broker): create a free account at metaapi.cloud, add your MT login there, then paste its <b>API token</b> into "API key", its <b>account ID</b> into Extra, and your region (e.g. <code>new-york</code>, <code>london</code>) into Server.<br>
<b>EA</b> (free, runs inside MT5 on your laptop, any MT5 broker): choose Platform EA, leave the key blank and press Save. The next page gives you the EA file with your key already inside, and a link to check your connection.<br>Use trade-only keys with withdrawals disabled.</div></div>""")

def _hidden(aid, key): return f"<input type='hidden' name='id' value='{escape(aid)}'><input type='hidden' name='key' value='{escape(key)}'>"

@app.route("/save_account", methods=["POST"])
def save_account():
    is_admin = session.get("admin") is True
    if not is_admin and throttled("mk", 8, 3600, add=True): return "Too many accounts created from your network. Try again later.", 429
    with LOCK:
        d = load(); f = request.form
        acc = {k: f.get(k, "").strip() for k in ("id", "platform", "login", "password", "server", "balance", "extra")}
        if not acc["id"].isdigit() or len(acc["id"]) > 6 or acc["platform"] not in d["settings"]["platforms"]: return "Invalid account", 400
        if acc["platform"] == "EA":
            acc["login"] = secrets.token_urlsafe(24); acc["key_hash"] = _h(acc["login"]); acc["password"] = acc["extra"] = ""
        elif not acc["login"]: return "API key / login is required", 400
        elif not crypto_ready(): return "Secure storage is not ready on this server yet. Please try again later.", 503
        exists = any(a["id"] == acc["id"] for a in d["accounts"])
        if exists and not is_admin: return "Account number already used", 403
        if not exists and len(d["accounts"]) >= int(d["settings"]["max_accounts"]): return "This app has reached its account limit", 403
        d["accounts"] = [a for a in d["accounts"] if a["id"] != acc["id"]] + [acc]; save(d)
    if acc["platform"] == "EA":
        return layout("EA key", f"<div class='card' style='max-width:640px'><h2>Account saved</h2><p class='muted'>Your EA key. Write it down: with your account number it lets you check your connection later.</p><pre style='font-size:14px;word-break:break-all'>Account #{escape(acc['id'])}\nKey: {escape(acc['login'])}</pre>"
                f"<form method='post' action='/ea/download' style='margin-top:12px'>{_hidden(acc['id'], acc['login'])}<button class='btn'>1. Download the EA (your key is already inside)</button></form>"
                f"<form method='post' action='/status' style='margin-top:10px'>{_hidden(acc['id'], acc['login'])}<button class='btn' style='background:#0f172a'>2. Open my account page</button></form></div>")
    if is_admin: return redirect("/setup")
    return layout("Saved", f"<div class='card' style='max-width:640px'><h2>Account saved</h2><p class='muted'>Your details are stored encrypted. Check that the connection works:</p>"
                  f"<form method='post' action='/status' style='margin-top:12px'>{_hidden(acc['id'], acc['login'])}<input type='hidden' name='action' value='test'><button class='btn'>Test my connection</button></form></div>")

@app.route("/test_account/<aid>", methods=["POST"])
@admin_only
def test_account(aid):
    with LOCK:
        d = load(); a = next((x for x in d["accounts"] if x["id"] == aid), None)
        if not a: abort(404)
        if a["platform"] == "EA":
            msg = (f"✅ EA connected. Last heard from it: {escape(LAST_SEEN[a['id']])} UTC. Balance: {escape(str(a.get('balance', '0')))}" if LAST_SEEN.get(a["id"])
                   else "⚠️ The EA has not contacted the app yet. Attach it to a chart in MT5 and read the Experts tab.")
        else:
            try: b = balance(a); a["balance"] = str(round(b, 2)); save(d); msg = f"✅ Connected. Balance synced: {b:,.2f}"
            except Exception as e: msg = f"❌ Could not connect: {escape(str(e))}"
    return layout("Test", f"<div class='card'><h2>Connection test</h2><p style='margin:10px 0'>{msg}</p><a class='btn' href='/setup'>Back</a></div>")

# ---------------- EA bridge: MT5 on the user's own laptop talks to the app (no MetaApi needed) ----------------
def ea_account(d, key):
    h = _h(key)
    for a in d["accounts"]:
        if a["platform"] == "EA" and a.get("key_hash") and hmac.compare_digest(a["key_hash"], h): return a

@app.route("/ea/sync", methods=["POST"])
def ea_sync():
    """Plain-text protocol. The EA sends: KEY= SYMBOL= TF= BAL= [PLACED=id] [FAILED=id~reason] then '---' and candle lines t,o,h,l,c (closed bars, oldest first).
    The app answers: 'OK <mode>' then ORDER|id|side|entry|sl|tp|risk and CANCEL|id lines."""
    head, rows, placed, failed = {}, [], [], []
    for ln in request.get_data(as_text=True).splitlines():
        ln = ln.strip()
        if not ln or ln == "---": continue
        k, _, v = ln.partition("=")
        if _ and k in ("KEY", "SYMBOL", "TF", "BAL", "PLACED", "FAILED"):
            if k == "PLACED": placed.append(v)
            elif k == "FAILED": failed.append(v)
            else: head[k] = v
        elif ln.count(",") == 4:
            try: t, o_, h, l, c = ln.split(","); rows.append({"t": int(t) * 1000, "o": float(o_), "h": float(h), "l": float(l), "c": float(c)})
            except ValueError: return "ERR bad candle data", 400
    sym = re.sub(r"[^A-Za-z0-9._#]", "", head.get("SYMBOL", ""))[:20]
    if not sym or len(rows) > 1000: return "ERR bad request", 400
    if throttled("ea", 30): return "ERR too many wrong keys. Wait a few minutes.", 429
    with LOCK:
        d = load(); s = d["settings"]; a = ea_account(d, head.get("KEY", ""))
        if not a:
            throttled("ea", 30, add=True)
            return "ERR unknown EA key. Check the key in the EA inputs.", 401
        if rows and head.get("TF") != s["timeframe"]: return f"ERR the chart is {head.get('TF')} but the app trades {s['timeframe']}. Change the chart timeframe.", 400
        if rows and len(rows) < 30: return "ERR not enough candles on the chart yet", 400
        try: bal = float(head.get("BAL", 0))
        except ValueError: bal = 0
        if bal > 0: a["balance"] = str(round(bal, 2))
        LAST_SEEN[a["id"]] = str(datetime.datetime.utcnow())[:19]
        mine = [o for o in d["orders"] if o.get("account") == a["id"] and o.get("symbol") == sym]
        for o in mine:
            if o["id"] in placed: o["ea_ack"] = True
        for f in failed:
            oid, _, err = f.partition("~")
            for o in mine:
                if o["id"] == oid and o.get("outcome") == "PENDING" and not o.get("ea_ack"): o["status"], o["outcome"] = f"LIVE FAILED: {err[:150]}", "REJECTED"
        if rows:
            for o in mine: resolve(o, rows)
            sig = find_setup(rows, s)
            seen_key = f"ea:{a['id']}:{sym}-{sig['key']}" if sig else None
            if sig and seen_key not in d["seen"]:
                d["seen"] = (d["seen"] + [seen_key])[-500:]
                skip = any(str(sig[f]) in s["filters"].get(f, []) for f in s["filters"] if f in sig)
                busy = any(o.get("outcome") in ("PENDING", "OPEN") for o in mine)
                if not skip and not busy:
                    new = place_order(a, sig, sym, s); d["orders"].append(new); mine.append(new)
        out = [f"OK {s['mode']}"]
        if s["mode"] == "live":
            for o in mine:
                if str(o.get("status", "")).startswith("LIVE: queued") and not o.get("ea_ack") and o.get("outcome") == "PENDING":
                    out.append(f"ORDER|{o['id']}|{o['side']}|{o['entry']!r}|{o['sl']!r}|{o['tp']!r}|{o['risk_amount']}")
        cutoff = str(datetime.datetime.utcnow() - datetime.timedelta(days=3))
        for o in mine:
            if o.get("outcome") == "EXPIRED" and str(o.get("status", "")).startswith("LIVE") and o.get("time", "") > cutoff: out.append(f"CANCEL|{o['id']}")
        d["orders"] = d["orders"][-5000:]; save(d)
    return "\n".join(out), 200, {"Content-Type": "text/plain"}

def find_account(d, aid, key):
    if not key: return None
    for a in d["accounts"]:
        if a["id"] != str(aid).strip(): continue
        if a["platform"] == "EA": ok = bool(a.get("key_hash")) and hmac.compare_digest(a["key_hash"], _h(key))
        else: ok = bool(a.get("login")) and hmac.compare_digest(str(a["login"]).encode(), key.encode())
        if ok: return a

@app.route("/status", methods=["GET", "POST"])
def my_status():
    form = ("<div class='card' style='max-width:520px'><h2>My account</h2><p class='muted' style='margin-bottom:10px'>Check that your connection works and see your own results. "
            "Enter your account number and your key: the EA key you were given, or your exchange API key.</p>"
            "<form method='post'><label>Account #</label><input class='input' name='id' required><br><br><label>Key</label><input class='input' name='key' type='password' required><br><br><button class='btn'>Open my account</button></form></div>")
    if request.method == "GET": return layout("My account", form)
    aid, key, act = request.form.get("id", "").strip(), request.form.get("key", "").strip(), request.form.get("action", "")
    if throttled("st", 10): return layout("My account", "<div class='card' style='max-width:520px'>Too many attempts. Try again in 10 minutes.</div>"), 429
    d = load(); a = find_account(d, aid, key)
    if not a:
        throttled("st", 10, add=True)
        return layout("My account", "<div class='card' style='max-width:520px;color:#dc2626;font-weight:600'>Account number or key not recognised.</div>" + form), 403
    s, ee, note, tested = d["settings"], a["platform"] == "EA", "", None
    if act == "test" and not ee:
        try:
            b = balance(a); tested = True; note = f"Connected. Your balance: {b:,.2f}"; a["balance"] = str(round(b, 2))
            with LOCK:
                dd = load(); x = next((z for z in dd["accounts"] if z["id"] == a["id"]), None)
                if x: x["balance"] = a["balance"]; save(dd)
        except Exception as e: tested = False; note = "Could not connect: " + str(e)[:200]
    if ee:
        ls = LAST_SEEN.get(a["id"], "")
        try: on = bool(ls) and (datetime.datetime.utcnow() - datetime.datetime.strptime(ls, "%Y-%m-%d %H:%M:%S")).total_seconds() < 180
        except ValueError: on = False
        badge = (f"<b style='color:#16a34a'>● Connected</b> <span class='muted'>(EA last heard {escape(ls)} UTC)</span>" if on else
                 f"<b style='color:#dc2626'>● Not connected</b> <span class='muted'>({'last heard ' + escape(ls) + ' UTC' if ls else 'the EA has not contacted the app yet'})</span>")
    elif tested is True: badge = f"<b style='color:#16a34a'>● {escape(note)}</b>"
    elif tested is False: badge = f"<b style='color:#dc2626'>● {escape(note)}</b>"
    else: badge = "<b style='color:#64748b'>● Saved. Press Test connection to check your keys.</b>"
    mode = ("LIVE: orders are sent to your account." if s["mode"] == "live" else "PAPER: practice mode. Trades are recorded here but not sent to your account.")
    mine = [o for o in d["orders"] if o.get("account") == a["id"]]; p = perf(mine)
    box = lambda l, v: f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px'><div class='muted'>{l}</div><div style='font-size:22px;font-weight:800'>{v}</div></div>"
    rows = "".join(f"<div style='padding:8px;border-bottom:1px solid #f1f5f9;font-size:12px'>{escape(o.get('time', '')[:16])} · {escape(o.get('symbol', ''))} {escape(o.get('side', ''))} · E {o['entry']:.6g} SL {o['sl']:.6g} TP {o['tp']:.6g} · <b>{escape(o.get('outcome', ''))}</b> {escape(str(o.get('status', '')))}</div>"
                   for o in mine[::-1][:10] if "entry" in o) or "<p class='muted'>No trades yet. The robot only trades when its pattern appears.</p>"
    tr = f"{p['total_r']:+}R"
    hid = _hidden(a["id"], key)
    acts = f"<form method='post' style='display:inline'>{hid}<button class='btn'>Refresh</button></form> "
    if ee: acts += f"<form method='post' action='/ea/download' style='display:inline'>{hid}<button class='btn' style='background:#0f172a'>Download EA with my key</button></form>"
    else: acts += f"<form method='post' style='display:inline'>{hid}<input type='hidden' name='action' value='test'><button class='btn' style='background:#0f172a'>Test connection</button></form>"
    guide = ("<details style='margin-top:14px'><summary style='cursor:pointer;font-weight:700'>How to connect the EA (about 5 minutes)</summary><ol class='muted' style='line-height:1.9;margin:8px 0 0 18px'>"
             "<li>Install MetaTrader 5 and log in to your broker account (demo or real).</li>"
             "<li>Press Copy EA code below. In MT5 press F4 to open MetaEditor, choose File, New, Expert Advisor (template), name it ISIG_EA and press Finish. Select all (Ctrl+A), paste (Ctrl+V), then press F7. It should say 0 errors.</li>"
             f"<li>In MT5: Tools, Options, Expert Advisors. Tick Allow WebRequest and add <b>{escape(request.host_url.rstrip('/'))}</b></li>"
             "<li>Open a 30-minute chart of your pair. In Navigator, right-click Expert Advisors, Refresh, drag ISIG_EA onto the chart, tick Allow Algo Trading, press OK.</li>"
             "<li>Turn on the Algo Trading button in MT5. This page will show Connected within a minute.</li></ol></details>") if ee else ""
    src = _ea_source(key) if ee else None
    code = ("<div class='card' style='max-width:760px'><h3>Copy the EA code</h3><p class='muted' style='margin:4px 0 10px'>Press the button, then paste (Ctrl+V) into a new file in MetaEditor. Your key is already inside. Keep it private.</p>"
            "<button class='btn' type='button' id='cpbtn'>Copy EA code</button> <span id='cpmsg' class='muted'></span>"
            f"<textarea id='eacode' readonly rows='6' style='width:100%;margin-top:10px;font-family:monospace;font-size:11px;border:1px solid #e2e8f0;border-radius:10px;padding:8px'>{escape(src or '')}</textarea>"
            "<script>document.getElementById('cpbtn').onclick=function(){var t=document.getElementById('eacode'),m=document.getElementById('cpmsg');function ok(){m.textContent='Copied. Now paste it in MetaEditor.'}"
            "if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(t.value).then(ok,function(){t.select();document.execCommand('copy');ok()})}else{t.select();document.execCommand('copy');ok()}}</script></div>") if src else ""
    body = (f"<div class='card' style='max-width:760px'><h2>Account #{escape(a['id'])} · {escape(a['platform'])}</h2><p style='margin:8px 0'>{badge}</p>"
            f"<p class='muted'>Balance: {escape(str(a.get('balance', '0')))} · risk per trade {s['risk_pct']}% · target 1:{s['rr']}</p><p class='muted' style='margin:6px 0 12px'>Mode: {escape(mode)}</p>{acts}{guide}</div>{code}"
            f"<div class='card' style='max-width:760px'><h3>My results</h3><div class='grid'>{box('Closed trades', p['trades'])}{box('Win rate', str(p['win_rate']) + '%')}{box('Total result', tr)}</div>"
            f"<h3 style='margin-top:14px'>My latest trades</h3>{rows}</div>")
    return layout("My account", body)

def _ea_source(key):
    try:
        with open(EA_FILE, encoding="utf-8") as fh: src = fh.read()
    except OSError: return None
    server = request.host_url.rstrip("/")
    src = re.sub(r'(input\s+string\s+InpServer\s*=\s*)"[^"]*"', lambda m: m.group(1) + '"' + server + '"', src)
    return re.sub(r'(input\s+string\s+InpKey\s*=\s*)"[^"]*"', lambda m: m.group(1) + '"' + key + '"', src)

@app.route("/ea/download", methods=["POST"])
def ea_download():
    if throttled("st", 10): return "Too many attempts. Try again later.", 429
    key = request.form.get("key", "").strip()
    a = find_account(load(), request.form.get("id", ""), key)
    if not a or a["platform"] != "EA": throttled("st", 10, add=True); return "Account number or key not recognised.", 403
    src = _ea_source(key)
    if src is None: return "The EA file (ISIG_EA.mq5) is not on the server yet. Ask the owner to upload it next to app.py.", 404
    return Response(src, mimetype="text/plain", headers={"Content-Disposition": "attachment; filename=ISIG_EA.mq5"})

@app.route("/webhook", methods=["POST", "GET"])
def webhook():
    if not WEBHOOK_SECRET or not hmac.compare_digest(request.args.get("key", ""), WEBHOOK_SECRET): return jsonify({"error": "unauthorized"}), 401
    return jsonify({"status": "scanned", "new": len(run_scan())})

def _curve_svg(c):
    if len(c) < 2: return "<p class='muted'>Chart appears after 2+ closed trades.</p>"
    w, h, lo, hi = 640, 180, min(c + [0]), max(c + [0]); span = (hi - lo) or 1
    pts = " ".join(f"{i * w / (len(c) - 1):.1f},{h - 10 - (v - lo) / span * (h - 20):.1f}" for i, v in enumerate(c))
    zero = h - 10 - (0 - lo) / span * (h - 20)
    col = "#16a34a" if c[-1] >= 0 else "#dc2626"
    return f"<svg viewBox='0 0 {w} {h}' style='width:100%;height:auto'><line x1='0' x2='{w}' y1='{zero:.1f}' y2='{zero:.1f}' stroke='#cbd5e1' stroke-dasharray='4'/><polyline fill='none' stroke='{col}' stroke-width='2.5' points='{pts}'/></svg>"

# ---------------- trade log (closed trades table + CSV) ----------------
def closed_trades(orders, view="all"):
    paper = lambda o: str(o.get("status", "")).startswith("PAPER")
    rows = [o for o in orders if o.get("outcome") in ("WIN", "LOSS") and (view == "all" or (view == "paper") == paper(o))]
    return sorted(rows, key=lambda o: o.get("time", ""), reverse=True)

def trade_table(trs):
    if not trs: return "<div class='card'><h2>Trade log</h2><p class='muted'>No closed trades yet.</p></div>"
    h = "<tr>" + "".join(f"<th style='text-align:left;padding:6px'>{x}</th>" for x in ("Time", "Symbol", "Side", "Entry", "SL", "TP", "Result", "Money", "Type")) + "</tr>"
    rows = ""
    for o in trs:
        col = "#16a34a" if o["r"] > 0 else "#dc2626"
        typ = "PAPER" if str(o.get("status", "")).startswith("PAPER") else "LIVE"
        cells = [escape(o["time"][:16]), escape(o["symbol"]), o["side"], f"{o['entry']:.6g}", f"{o['sl']:.6g}", f"{o['tp']:.6g}",
                 f"<b style='color:{col}'>{o['outcome']} {o['r']:+}R</b>", f"{o.get('risk_amount', 0) * o['r']:+,.2f}", typ]
        rows += "<tr style='border-top:1px solid #f1f5f9'>" + "".join(f"<td style='padding:6px'>{c}</td>" for c in cells) + "</tr>"
    return f"<div class='card'><h2>Trade log ({len(trs)})</h2><p class='muted'><a href='/trades.csv'>Download CSV</a></p><div style='overflow-x:auto;font-size:12px'><table style='width:100%;border-collapse:collapse'>{h}{rows}</table></div></div>"

@app.route("/trades.csv")
def trades_csv():
    import csv, io
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["time", "symbol", "side", "entry", "sl", "tp", "outcome", "r", "money", "type"])
    for o in closed_trades(load()["orders"]):
        w.writerow([o["time"], o["symbol"], o["side"], o["entry"], o["sl"], o["tp"], o["outcome"], o["r"],
                    round(o.get("risk_amount", 0) * o["r"], 2), "PAPER" if str(o.get("status", "")).startswith("PAPER") else "LIVE"])
    return buf.getvalue(), 200, {"Content-Type": "text/csv", "Content-Disposition": "attachment; filename=trades.csv"}

@app.route("/performance")
def performance():
    view = request.args.get("view", "all"); view = view if view in ("all", "paper", "live") else "all"
    orders_all = load()["orders"]
    p = perf(orders_all, view)
    good = lambda v: "#16a34a" if v >= 0 else "#dc2626"
    box = lambda label, val, col="#0f172a": f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:14px'><div class='muted'>{label}</div><div style='font-size:24px;font-weight:800;color:{col}'>{val}</div></div>"
    tabs = "".join(f"<a class='btn' style='padding:6px 14px;margin-right:6px;{'' if v == view else 'background:#e2e8f0;color:#0f172a'}' href='/performance?view={v}'>{v.upper()}</a>" for v in ("all", "paper", "live"))
    pf = p["profit_factor"] if p["profit_factor"] is not None else "-"
    money = f"{p['money']:+,.2f}" if p["money"] else "n/a (paper results carry no money)"
    body = f"""<div class='card'><h2>Bot performance</h2><div style='margin:10px 0'>{tabs}</div>
<div class='grid'>{box('Closed trades', p['trades'])}{box('Win rate', str(p['win_rate']) + '%', good(p['win_rate'] - 33.4))}{box('Total result', f"{p['total_r']:+}R", good(p['total_r']))}
{box('Profit / loss (money)', money, good(p['money']))}{box('Avg win / avg loss', f"{p['avg_win_r']:+}R / {p['avg_loss_r']:+}R")}{box('Profit factor', pf)}{box('Max drawdown', f"-{p['max_drawdown_r']}R", '#dc2626')}{box('Avg per trade', f"{p['avg_r']:+}R", good(p['avg_r']))}</div>
<h3 style='margin-top:16px'>Equity curve (cumulative R)</h3>{_curve_svg(p['curve'])}
<p class='muted' style='margin-top:12px'>1R = the amount risked on one trade. Results are measured from market price data after each signal, not from broker statements. PAPER trades are simulated. Past results do not guarantee future results; this is not financial advice.</p></div>"""
    body += trade_table(closed_trades(orders_all, view))
    return layout("Performance", body)

@app.route("/api/performance")
def api_performance():
    v = request.args.get("view", "all"); return jsonify(perf(load()["orders"], v if v in ("all", "paper", "live") else "all"))

@app.route("/orders")
def orders():
    d = load(); st, n = stats(d["orders"])
    def row(r):
        if "entry" not in r: return f"<div style='padding:8px;font-size:12px'>{escape(r.get('status',''))}</div>"
        return f"<div style='padding:8px;border-bottom:1px solid #f1f5f9;font-size:12px'>{escape(r.get('time','')[:16])} · {escape(r.get('symbol',''))} {escape(r.get('side',''))} · E {r['entry']:.6g} SL {r['sl']:.6g} TP {r['tp']:.6g} · acct {escape(str(r.get('account','-')))} · <b>{escape(r.get('outcome',''))}</b> {escape(r.get('status',''))}</div>"
    rows = "".join(row(r) for r in d["orders"][-40:][::-1])
    return layout("Orders", f"<div class='card'><h2>Orders</h2><p class='muted'>{n} resolved trades</p>{rows or '<p class=muted>No signals yet.</p>'}</div>")

@app.route("/admin", methods=["GET", "POST"])
def admin():
    if request.method == "POST":
        ip, now = _ip(), time.time()
        FAILS[ip] = [t for t in FAILS.get(ip, []) if now - t < 600]
        FAILS["admin*"] = [t for t in FAILS.get("admin*", []) if now - t < 600]          # global cap, even if someone fakes their address
        if len(FAILS[ip]) >= 5 or len(FAILS["admin*"]) >= 20: session["msg"] = "Too many wrong attempts. Try again in 10 minutes."
        elif check_admin(request.form.get("pw", "")): session["admin"] = True; session.permanent = True; FAILS.pop(ip, None)
        else: FAILS[ip].append(now); FAILS["admin*"].append(now); session["msg"] = "Wrong password."
        return redirect("/admin")
    if session.get("admin") is not True:
        m = session.pop("msg", None)
        note = f"<p style='color:#dc2626;margin:8px 0;font-weight:600'>{escape(m)}</p>" if m else ""
        return layout("Admin", f"<div class='card' style='max-width:420px'><h2>Admin login</h2><p class='muted'>Only the owner can open this panel.</p>{note}<form method='post'><input class='input' name='pw' type='password' placeholder='Password' autofocus><br><br><button class='btn'>Unlock</button></form></div>")
    d = load(); s = d["settings"]; st, n = stats(d["orders"])
    fields = ""
    for k in DEFAULTS:
        if k in BRAND_KEYS: continue
        cur = s[k]; val = json.dumps(cur) if isinstance(cur, (list, dict)) else cur
        fields += f"<div><label>{k}</label><input class='input' name='{k}' value='{escape(str(val))}'></div>"
    m = session.pop("msg", None)
    msg = f"<div class='card' style='background:#ecfdf5;border-color:#a7f3d0;font-weight:600'>{escape(m)}</div>" if m else ""
    brand = f"""<div class='card'><h2>Branding</h2><p class='muted' style='margin-bottom:10px'>Changes show on the whole site immediately.</p><form action='/admin/save' method='post' class='grid'>
<div><label>App name</label><input class='input' name='site_name' maxlength='60' value='{escape(s['site_name'])}'></div>
<div><label>Headline under the name</label><input class='input' name='tagline' maxlength='80' value='{escape(s['tagline'])}'></div>
<div style='grid-column:1/-1'><label>Homepage description</label><textarea class='input' name='lead' rows='3' maxlength='400'>{escape(s['lead'])}</textarea></div>
<div><button class='btn'>Save branding</button></div></form></div>"""
    security = """<div class='card'><h2>Security</h2><p class='muted' style='margin-bottom:10px'>Change the admin password (at least 8 characters). Only someone with the password can open this panel.</p><form action='/admin/password' method='post' class='grid'>
<div><label>Current password</label><input class='input' name='cur' type='password' required></div>
<div><label>New password</label><input class='input' name='new' type='password' minlength='8' required></div>
<div><label>Repeat new password</label><input class='input' name='new2' type='password' minlength='8' required></div>
<div><button class='btn'>Change password</button></div></form>
<form action='/admin/logout' method='post' style='margin-top:12px'><button class='btn' style='background:#0f172a'>Log out</button></form></div>"""
    tbl, tips = "", []
    for f, g in st.items():
        for k, m in g.items():
            tbl += f"<div class='muted'>{f}={escape(k)}: {m['n']} trades · win {m['win']}% · avg {m['exp']}R</div>"
            if m["n"] >= 10 and m["exp"] < 0: tips.append(f"Consider filtering {f}={k} ({m['n']} trades, {m['exp']}R avg)")
    jr = "".join(f"<div style='border-top:1px solid #e2e8f0;padding:10px 0'><img src='/uploads/{escape(j['file'])}' style='max-width:100%;max-height:260px;border-radius:8px'><div class='muted'>{escape(j['symbol'])} · {escape(j['result'])} · {escape(j['note'])}</div><pre>{escape(j.get('analysis','(not analysed)'))}</pre><form action='/admin/analyze/{j['id']}' method='post'><button class='btn' style='padding:6px 12px'>Analyse with Claude</button></form></div>" for j in d["journal"][::-1][:15])
    return layout("Admin", f"""{msg}{_warnings(d)}{brand}<div class='card'><h2>1. Settings</h2><form action='/admin/save' method='post' class='grid'>{fields}
<div><label>Type LIVE here to allow mode=live</label><input class='input' name='confirm'></div><div><button class='btn'>Save settings</button></div></form>
<p class='muted' style='margin-top:8px'>Lists/filters are JSON. Example filter: {{"session": ["Asia"]}}</p>
<form action='/admin/scan' method='post' style='margin-top:10px'><button class='btn' style='background:#0f172a'>Run scan now</button></form></div>
<div class='card'><h2>2. Learning from results ({n} resolved trades)</h2>{tbl or '<p class=muted>Needs resolved trades.</p>'}<pre>{escape(chr(10).join(tips) or 'No suggestions yet (needs 10+ trades per group).')}</pre>
<form action='/admin/synthesize' method='post' style='margin-top:8px'><button class='btn'>Ask Claude for improvements</button></form><pre>{escape(d['lessons'])}</pre></div>
<div class='card'><h2>3. Screenshot journal</h2><form action='/admin/upload' method='post' enctype='multipart/form-data' class='grid'>
<div><label>Chart screenshot (entry, SL, TP drawn)</label><input class='input' type='file' name='img' accept='image/*' required></div>
<div><label>Symbol</label><input class='input' name='symbol'></div><div><label>Result</label><select class='input' name='result'><option>WIN</option><option>LOSS</option><option>BREAKEVEN</option></select></div>
<div><label>Notes (what happened)</label><input class='input' name='note'></div><div><button class='btn'>Upload</button></div></form>{jr}</div>{security}""")

@app.route("/admin/save", methods=["POST"])
@admin_only
def admin_save():
    with LOCK:
        d = load(); s = d["settings"]
        for k, v in DEFAULTS.items():
            raw = request.form.get(k)
            if raw is None: continue
            try:
                new = json.loads(raw) if isinstance(v, (list, dict)) else type(v)(raw)
                if isinstance(v, (list, dict)) and not isinstance(new, type(v)): continue
                if k == "mode" and (new not in ("paper", "live") or (new == "live" and request.form.get("confirm") != "LIVE")): continue
                if k == "ob_entry" and new not in ("top", "mid"): continue
                if k in BRAND_KEYS: new = new.strip()[:400 if k == "lead" else 80] or DEFAULTS[k]
                s[k] = new
            except Exception: pass
        save(d)
    session["msg"] = "Saved."
    return redirect("/admin")

@app.route("/admin/password", methods=["POST"])
@admin_only
def admin_password():
    f = request.form
    if not check_admin(f.get("cur", "")): session["msg"] = "Current password is wrong."
    elif len(f.get("new", "")) < 8 or f.get("new") != f.get("new2"): session["msg"] = "New passwords must match and be at least 8 characters."
    else:
        with LOCK:
            d = load(); salt = os.urandom(16).hex(); d["admin_hash"] = {"salt": salt, "hash": _pw_hash(f["new"], salt)}; save(d)
        session["msg"] = "Password changed."
    return redirect("/admin")

@app.route("/admin/logout", methods=["POST"])
def admin_logout(): session.clear(); return redirect("/admin")

@app.route("/admin/scan", methods=["POST"])
@admin_only
def admin_scan(): run_scan(); return redirect("/orders")

@app.route("/admin/upload", methods=["POST"])
@admin_only
def upload():
    f = request.files.get("img")
    ext = os.path.splitext(secure_filename(f.filename or ""))[1].lower() if f else ""
    if ext not in (".png", ".jpg", ".jpeg", ".webp"): return "Unsupported file", 400
    name = uuid.uuid4().hex + ext; f.save(os.path.join(UPLOADS, name))
    with LOCK:
        d = load(); fm = request.form
        d["journal"].append({"id": uuid.uuid4().hex[:8], "file": name, "symbol": fm.get("symbol", ""), "result": fm.get("result", ""), "note": fm.get("note", "")[:500], "time": str(datetime.datetime.utcnow())}); save(d)
    return redirect("/admin")

@app.route("/uploads/<name>")
@admin_only
def uploads(name): return send_from_directory(UPLOADS, secure_filename(name))

@app.route("/admin/analyze/<jid>", methods=["POST"])
@admin_only
def analyze(jid):
    d = load(); j = next((x for x in d["journal"] if x["id"] == jid), None)
    if not j: abort(404)
    ext = j["file"].rsplit(".", 1)[-1]; mt = {"png": "image/png", "webp": "image/webp"}.get(ext, "image/jpeg")
    with open(os.path.join(UPLOADS, j["file"]), "rb") as fh: b64 = base64.b64encode(fh.read()).decode()
    sysmsg = "You review trading chart screenshots against the user's rules: " + d["settings"]["strategy_text"]
    txt = f"Symbol {j['symbol']}, result {j['result']}. Notes: {j['note']}. Identify the BOS, the liquidity, the entry order block, SL and TP shown. Say whether each follows the rules, what went wrong or right, and one concrete rule/filter change to test. Be specific; say if the image is unclear."
    out = claude([{"type": "image", "source": {"type": "base64", "media_type": mt, "data": b64}}, {"type": "text", "text": txt}], sysmsg)
    with LOCK:
        d = load()
        for x in d["journal"]:
            if x["id"] == jid: x["analysis"] = out
        save(d)
    return redirect("/admin")

@app.route("/admin/synthesize", methods=["POST"])
@admin_only
def synthesize():
    d = load(); st, n = stats(d["orders"])
    notes = "\n---\n".join(f"{j['symbol']} {j['result']}: {j.get('analysis','')[:700]}" for j in d["journal"][-10:])
    out = claude(f"Strategy rules: {d['settings']['strategy_text']}\nBot stats ({n} trades): {json.dumps(st)}\nScreenshot reviews:\n{notes}\n\nFind repeating patterns in the losses and propose at most 3 testable changes (settings or filters). Flag small sample sizes honestly.", "You are a careful quantitative trading reviewer. Do not overfit; recommend paper-testing every change.")
    with LOCK: d = load(); d["lessons"] = out; save(d)
    return redirect("/admin")

@app.route("/api/status")
def status(): d = load(); return jsonify({"status": "live", "mode": d["settings"]["mode"], "accounts": len(d["accounts"]), "storage": ("database" if DATABASE_URL and DB_STATUS["ok"] else "database unreachable" if DATABASE_URL else "local file")})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
