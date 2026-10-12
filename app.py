"""
Nnanna Michael - trading robot, scalable edition.

What changed compared with the single-file/JSON version:
  * Storage: real database tables (users, accounts, orders, jobs...) instead of ONE JSON blob that every request re-read and re-wrote.
    PostgreSQL in production (DATABASE_URL), SQLite automatically when DATABASE_URL is empty (local testing / tiny installs).
  * Web processes are stateless (no in-memory state that matters) so you can run many of them behind a load balancer.
  * Heavy work (market scan, placing orders for thousands of accounts) runs in worker processes fed by a database job queue.
  * Sign-up / login for users (admin switch "Require sign-up").
  * Every account has a TIER: demo or real. The tier is decided by the SERVER from evidence, never typed in by the user. Unknown = real (fail closed).
  * Monetisation switch: demo accounts are free forever, real accounts pay monthly (after the optional free trial).

Run:   web    ->  gunicorn app:app -k gthread -w 4 --threads 8 -b 0.0.0.0:$PORT
       worker ->  python app.py worker          (run 1 or more; they share the job queue safely)
Env:   DATABASE_URL, SECRET_KEY, DATA_KEY, ADMIN_PASSWORD, WEBHOOK_SECRET (optional), REDIS_URL (optional, shared rate limits),
       AUTOSCAN=1 (tiny installs only: runs the worker inside the web process), TRUSTED_PROXIES (default 1), DB_POOL (default 8 per process),
       META_TOKEN / META_ACCOUNT / META_REGION (optional dedicated MetaApi feed for forex/gold candles), ANTHROPIC_API_KEY (optional).
"""
import os, re, json, hmac, hashlib, time, datetime, threading, uuid, base64, secrets, struct, queue, random, ssl, contextlib, copy, collections
import urllib.request, urllib.parse, urllib.error
from decimal import Decimal, ROUND_DOWN
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, redirect, session, send_from_directory, abort, Response, g
from markupsafe import escape
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DATA_DIR = os.environ.get("DATA_DIR", ".")
UPLOADS = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOADS, exist_ok=True)
EA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ISIG_EA.mq5")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD") or ""
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
TRUSTED = int(os.environ.get("TRUSTED_PROXIES", "1"))

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=TRUSTED, x_proto=1, x_host=1)      # real client address + https behind Render's proxy
app.config.update(SESSION_COOKIE_SAMESITE="Lax", SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SECURE=bool(os.environ.get("RENDER")),
                  PERMANENT_SESSION_LIFETIME=datetime.timedelta(days=14), MAX_CONTENT_LENGTH=8 * 1024 * 1024)

DEFAULTS = {
    "site_name": "Nnanna Michael", "tagline": "Automated Trading Robot",
    "lead": "A smart-money trading bot that reads market structure for you: it spots the break of structure, finds the liquidity, and places the entry, stop loss and take profit automatically. Every trade is recorded so you can see the real results.",
    "platforms": ["BINANCE", "BYBIT", "OKX", "BITGET", "MT5", "MT4", "EA"],
    "symbols": ["BTCUSDT", "ETHUSDT"], "timeframe": "30m", "timeframes": ["30m", "1h"],
    "swing_n": 2, "min_bos": 2, "rr": 2.0, "ob_entry": "top",
    "risk_pct": 1.0, "max_mitigation_pct": 30.0, "min_leg_atr": 3.0,
    # --- access
    "require_signup": False,        # ON = nobody can add an account without signing up / logging in first
    "gmail_only": False, "max_users": 0, "max_accounts_per_user": 5, "block_unlinked": False,
    # --- monetisation (demo = free forever; real = paid monthly once paid_mode is ON)
    "paid_mode": False, "trial_days": 7, "plan_days": 30, "price_text": "", "payment_link": "", "pay_note": "",
    # --- demo / real trust
    "ea_demo_trust": "strict",      # strict = EA may be demo only if it reports DEMO *and* its server is on the demo list | off = EA accounts are always real
    "demo_servers": "demo,trial,contest,practice,paper", "demo_blocklist": "", "demo_max_balance": 0.0,
    # --- scale
    "ea_poll_seconds": 30, "max_inflight": 200,
    "mode": "paper",                                              # paper | live
    "filters": {"liquidity": [], "session": [], "side": [], "bos_run": [], "tf": []},
    "strategy_text": "30m trend by consecutive major BOS. Liquidity = last order block (or wick) before the latest BOS. "
                     "Entry = top of the OB below the liquidity (bullish), bottom of the OB above it (bearish). "
                     "SL at protected low/high of the BOS leg. TP = 1:2.",
}
BRAND_KEYS = ("site_name", "tagline", "lead")
ACCESS_KEYS = ("require_signup", "gmail_only", "max_users", "max_accounts_per_user", "block_unlinked", "paid_mode", "trial_days", "plan_days", "price_text", "payment_link", "pay_note")
TRUST_KEYS = ("ea_demo_trust", "demo_servers", "demo_blocklist", "demo_max_balance")
SCALE_KEYS = ("ea_poll_seconds", "max_inflight")
TESTNET_OK = ("BINANCE", "BYBIT", "OKX")        # platforms whose own exchange offers a separate demo (testnet) endpoint
FX_PLATS = ("MT4", "MT5")

def now_s(): return str(datetime.datetime.utcnow())
def today(): return datetime.datetime.utcnow().date()
def _h(k): return hashlib.sha256(k.encode()).hexdigest()

# ======================================================================================================================
# database layer: connection pool, same code for PostgreSQL (pg8000) and SQLite. Named parameters (:name) work in both.
# ======================================================================================================================
class _T:
    """A connection in use (inside db.tx() every statement runs in one transaction)."""
    def __init__(self, d, c): self.d, self.c = d, c
    def q(self, sql, **p): return self.d._exec(self.c, sql, p)[0]
    def one(self, sql, **p):
        r = self.q(sql, **p); return r[0] if r else None
    def x(self, sql, **p): return self.d._exec(self.c, sql, p)[1]

class DB:
    def __init__(self):
        self.pg = bool(DATABASE_URL)
        self.pool, self.max = queue.LifoQueue(), int(os.environ.get("DB_POOL", "8"))
        self.sem = threading.BoundedSemaphore(self.max)
        self.path = os.path.join(DATA_DIR, "bot.db")
        self.skip = " FOR UPDATE SKIP LOCKED" if self.pg else ""
        self.lock = " FOR UPDATE" if self.pg else ""

    def _new(self):
        if self.pg:
            import pg8000.native
            u = urllib.parse.urlparse(DATABASE_URL)
            local = (u.hostname or "") in ("localhost", "127.0.0.1")
            return pg8000.native.Connection(user=urllib.parse.unquote(u.username or ""), password=urllib.parse.unquote(u.password or ""), host=u.hostname,
                                            port=u.port or 5432, database=(u.path or "/").lstrip("/"), ssl_context=None if local else ssl.create_default_context(), timeout=20)
        import sqlite3
        c = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
        c.row_factory = sqlite3.Row
        for pragma in ("journal_mode=WAL", "synchronous=NORMAL", "busy_timeout=30000"): c.execute("PRAGMA " + pragma)
        return c

    @contextlib.contextmanager
    def conn(self):
        self.sem.acquire()
        c = None
        try:
            try: c = self.pool.get_nowait()
            except queue.Empty: c = self._new()
            yield c
            self.pool.put(c); c = None
        except BaseException:
            if c is not None:                                   # never reuse a connection that just failed
                try: c.close()
                except Exception: pass
            raise
        finally: self.sem.release()

    def _exec(self, c, sql, p):
        if self.pg:
            rows = c.run(sql, **p)
            cols = [x["name"] for x in (c.columns or [])]
            return ([dict(zip(cols, r)) for r in rows] if rows else []), (c.row_count if c.row_count is not None else 0)
        cur = c.execute(sql, p)
        return ([dict(r) for r in cur.fetchall()] if cur.description else []), cur.rowcount

    def q(self, sql, **p):
        with self.conn() as c: return self._exec(c, sql, p)[0]
    def one(self, sql, **p):
        r = self.q(sql, **p); return r[0] if r else None
    def x(self, sql, **p):
        with self.conn() as c: return self._exec(c, sql, p)[1]

    @contextlib.contextmanager
    def tx(self):
        with self.conn() as c:
            run = (lambda s: c.run(s)) if self.pg else (lambda s: c.execute(s))
            run("START TRANSACTION" if self.pg else "BEGIN IMMEDIATE")
            try: yield _T(self, c)
            except BaseException:
                try: run("ROLLBACK")
                except Exception: pass
                raise
            else: run("COMMIT")

db = DB()

def init_db():
    pk = "BIGSERIAL PRIMARY KEY" if db.pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
    stmts = [
        "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)",
        f"CREATE TABLE IF NOT EXISTS users (id {pk}, email TEXT NOT NULL UNIQUE, pw_salt TEXT NOT NULL, pw_hash TEXT NOT NULL, created TEXT NOT NULL, trial_end TEXT, blocked INTEGER NOT NULL DEFAULT 0, ip TEXT, last_login TEXT)",
        f"""CREATE TABLE IF NOT EXISTS accounts (id {pk}, ref TEXT NOT NULL UNIQUE, user_id BIGINT, legacy_id TEXT, platform TEXT NOT NULL, tf TEXT NOT NULL,
            env TEXT NOT NULL DEFAULT '', server TEXT NOT NULL DEFAULT '', secrets TEXT NOT NULL DEFAULT '{{}}', key_hash TEXT, balance DOUBLE PRECISION NOT NULL DEFAULT 0,
            created TEXT NOT NULL, paid_until TEXT, trial_end TEXT, blocked INTEGER NOT NULL DEFAULT 0,
            tier TEXT NOT NULL DEFAULT 'real', tier_src TEXT NOT NULL DEFAULT 'unverified', tier_note TEXT NOT NULL DEFAULT '', tier_override TEXT NOT NULL DEFAULT '',
            ever_real INTEGER NOT NULL DEFAULT 0, fp TEXT, fp_changes INTEGER NOT NULL DEFAULT 0, attest TEXT NOT NULL DEFAULT '{{}}', last_seen TEXT, flags TEXT NOT NULL DEFAULT '', label TEXT NOT NULL DEFAULT '')""",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_acc_key ON accounts(key_hash) WHERE key_hash IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_acc_fp ON accounts(fp) WHERE fp IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_acc_user ON accounts(user_id)",
        "CREATE INDEX IF NOT EXISTS ix_acc_legacy ON accounts(legacy_id)",
        "CREATE INDEX IF NOT EXISTS ix_acc_sched ON accounts(platform, tf)",
        """CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, account_id BIGINT NOT NULL, platform TEXT NOT NULL, symbol TEXT NOT NULL, tf TEXT NOT NULL DEFAULT '',
            sig_key TEXT NOT NULL DEFAULT '', created TEXT NOT NULL, outcome TEXT NOT NULL, status TEXT NOT NULL DEFAULT '', ack INTEGER NOT NULL DEFAULT 0,
            r DOUBLE PRECISION, last_t BIGINT NOT NULL DEFAULT 0, bars INTEGER NOT NULL DEFAULT 0, ext_id TEXT, data TEXT NOT NULL)""",
        "CREATE INDEX IF NOT EXISTS ix_ord_acc ON orders(account_id, outcome)",
        "CREATE INDEX IF NOT EXISTS ix_ord_sig ON orders(sig_key, symbol, tf)",
        "CREATE INDEX IF NOT EXISTS ix_ord_open ON orders(platform, outcome)",
        "CREATE INDEX IF NOT EXISTS ix_ord_created ON orders(created)",
        "CREATE TABLE IF NOT EXISTS track (tkey TEXT PRIMARY KEY, created TEXT NOT NULL, tf TEXT, paper INTEGER NOT NULL, data TEXT NOT NULL)",
        "CREATE INDEX IF NOT EXISTS ix_track ON track(tf, created)",
        "CREATE TABLE IF NOT EXISTS seen (k TEXT PRIMARY KEY, t TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS sigs (skey TEXT PRIMARY KEY, data TEXT NOT NULL, created TEXT NOT NULL)",
        f"CREATE TABLE IF NOT EXISTS jobs (id {pk}, kind TEXT NOT NULL, account_id BIGINT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL, started TEXT, worker TEXT)",
        "CREATE INDEX IF NOT EXISTS ix_jobs ON jobs(status, id)",
        "CREATE TABLE IF NOT EXISTS leases (name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires DOUBLE PRECISION NOT NULL)",
        "CREATE TABLE IF NOT EXISTS journal (id TEXT PRIMARY KEY, created TEXT NOT NULL, data TEXT NOT NULL)",
    ]
    for s_ in stmts: db.x(s_)

# ---------------- key/value + settings ----------------
def kv_get(k, default=None):
    r = db.one("SELECT v FROM kv WHERE k=:k", k=k)
    return r["v"] if r else default
def kv_set(k, v): db.x("INSERT INTO kv (k, v) VALUES (:k, :v) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", k=k, v=v)

_SC = {"t": 0.0, "v": None}
def load_settings(fresh=False):
    if fresh or _SC["v"] is None or time.time() - _SC["t"] > 5:
        try: stored = json.loads(kv_get("settings", "{}") or "{}")
        except Exception: stored = {}
        v = {**copy.deepcopy(DEFAULTS), **stored}
        if v.get("site_name") in ("", "ISIG BOT"): v["site_name"] = DEFAULTS["site_name"]
        if "EA" not in v["platforms"]: v["platforms"] = v["platforms"] + ["EA"]
        _SC.update(v=v, t=time.time())
    return _SC["v"]
S = load_settings
def save_settings(s): kv_set("settings", json.dumps(s)); _SC.update(v=None)

# ---------------- encryption of exchange keys (AES-GCM, key derived with scrypt from DATA_KEY / SECRET_KEY) ----------------
try:
    from cryptography.fernet import Fernet, MultiFernet
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    CRYPTO = True
except Exception:
    CRYPTO = False
SENSITIVE = ("login", "password", "extra")

def kdf_salt():
    s_ = kv_get("kdf_salt")
    if not s_:
        db.x("INSERT INTO kv (k, v) VALUES ('kdf_salt', :v) ON CONFLICT (k) DO NOTHING", v=secrets.token_hex(16)); s_ = kv_get("kdf_salt")
    return s_

def _materials():
    ms = [os.environ[n].encode() for n in ("DATA_KEY", "SECRET_KEY") if os.environ.get(n)]
    kf = os.path.join(DATA_DIR, ".datakey")
    if os.path.exists(kf):
        with open(kf, "rb") as fh: ms.append(fh.read().strip())
    elif not ms and not DATABASE_URL:
        k = Fernet.generate_key(); fd = os.open(kf, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600); os.write(fd, k); os.close(fd); ms.append(k)
    return ms

def crypto_ready():
    """Encrypt only if the key will still exist after a restart (DATA_KEY/SECRET_KEY), or the data lives on a persistent local disk."""
    return CRYPTO and (bool(os.environ.get("DATA_KEY") or os.environ.get("SECRET_KEY")) or not DATABASE_URL)

_KC = {}
def _k2(salt):
    out = []
    for m in _materials():
        if (m, salt) not in _KC: _KC[(m, salt)] = Scrypt(salt=bytes.fromhex(salt), length=32, n=2 ** 15, r=8, p=1).derive(m)
        out.append(_KC[(m, salt)])
    return out
def _enc2(value, aad, salt):
    nonce = os.urandom(12)
    return base64.urlsafe_b64encode(nonce + AESGCM(_k2(salt)[0]).encrypt(nonce, value.encode(), aad.encode())).decode()
def _dec2(token, aad, salt):
    try: raw = base64.urlsafe_b64decode(token.encode())
    except Exception: return ""
    for k in _k2(salt):
        try: return AESGCM(k).decrypt(raw[:12], raw[12:], aad.encode()).decode()
        except Exception: continue
    return ""
def _dec_fernet(v):                                  # only for migrating the very oldest saved data
    try: return MultiFernet([Fernet(base64.urlsafe_b64encode(hashlib.sha256(os.environ[n].encode()).digest())) for n in ("DATA_KEY", "SECRET_KEY") if os.environ.get(n)]).decrypt(v.encode()).decode()
    except Exception: return ""

def seal(ref, fields):
    """dict of secret fields -> JSON text with each value encrypted (bound to this account)."""
    out = {}
    for k in SENSITIVE:
        v = fields.get(k)
        if v: out[k] = "enc2:" + _enc2(str(v), f"{ref}:{k}", kdf_salt())
    return json.dumps(out)
def unseal(ref, text):
    try: raw = json.loads(text or "{}")
    except Exception: return {}
    return {k: (_dec2(v[5:], f"{ref}:{k}", kdf_salt()) if str(v).startswith("enc2:") else "") for k, v in raw.items()}

# ---------------- rate limiting (shared through Redis when REDIS_URL is set, otherwise per process) ----------------
class Limiter:
    def __init__(self):
        self.mem, self.r = {}, None
        if os.environ.get("REDIS_URL"):
            try: import redis; self.r = redis.Redis.from_url(os.environ["REDIS_URL"], socket_timeout=1.5)
            except Exception as e: print("redis unavailable, using per-process limits:", e)
    def hit(self, name, ident, limit, window, add=False):
        """True = this caller is over the limit. add=True records one more attempt."""
        k = f"rl:{name}:{ident}"
        if self.r is not None:
            try:
                if add:
                    p = self.r.pipeline(); p.incr(k); p.expire(k, window); n = p.execute()[0]
                else: n = int(self.r.get(k) or 0)
                return n >= limit
            except Exception: pass
        now = time.time()
        if len(self.mem) > 50000: self.mem.clear()
        lst = [t for t in self.mem.get(k, []) if now - t < window]
        if add: lst.append(now)
        self.mem[k] = lst
        return len(lst) >= limit
RL = Limiter()
def _ip(): return request.remote_addr or "?"
def throttled(name, limit=10, window=600, add=False, ident=None): return RL.hit(name, ident or _ip(), limit, window, add)
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

def mitigation(c, k, bull):
    """How deep price has traded into order block c[k] since it formed: 0.0 = never touched, 1.0 = completely filled."""
    hi, lo = c[k]["h"], c[k]["l"]
    after = c[k + 1:]
    if hi <= lo or not after: return 0.0
    depth = (hi - min(x["l"] for x in after)) if bull else (max(x["h"] for x in after) - lo)
    return max(0.0, min(1.0, depth / (hi - lo)))

def find_setup(c, s, tf=None):
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
    ranges = [x["h"] - x["l"] for x in c[max(0, start - 14):start]]
    leg_h = (max(c[k]["h"] for k in leg) - protected) if bull else (protected - min(c[k]["l"] for k in leg))
    if s.get("min_leg_atr", 0) and len(ranges) >= 5 and leg_h < s["min_leg_atr"] * (sum(ranges) / len(ranges)): return None    # the BOS must be a big, clear move

    is_ob = (lambda x: x["c"] < x["o"]) if bull else (lambda x: x["c"] > x["o"])
    obs = [k for k in range(origin, bos_i) if is_ob(c[k])]                     # only candles inside the leg
    if obs:
        liq = obs[-1]; kind = "order_block"
        lower = [k for k in obs if k < liq and (c[k]["l"] < c[liq]["l"] if bull else c[k]["h"] > c[liq]["h"])]
        cands = lower[::-1] + [origin]                 # nearest block below the liquidity first
    else:
        wick = (lambda x: min(x["o"], x["c"]) - x["l"]) if bull else (lambda x: x["h"] - max(x["o"], x["c"]))
        liq = max(range(origin, bos_i), key=lambda k: wick(c[k]), default=origin); kind = "wick"; cands = [origin]
    if (c[liq]["h"] > c[bos_i]["h"]) if bull else (c[liq]["l"] < c[bos_i]["l"]): return None      # the BOS must reach at least the liquidity line
    maxm = float(s.get("max_mitigation_pct", 30)) / 100
    ent = next((k for k in cands if mitigation(c, k, bull) <= maxm + 1e-9), None)                  # entry block must be unmitigated (or barely touched)
    if ent is None: return None
    ob = c[ent]; mit = mitigation(c, ent, bull)
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
            "session": sess, "bos_run": min(run, 5), "key": (f"{tf}-" if tf and tf != s["timeframe"] else "") + f"{last['dir']}-{c[bos_i]['t']}", "tf": tf or s["timeframe"], "mitigated_pct": round(100 * mit), "last_t": c[-1]["t"]}

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
    for f in ("side", "liquidity", "session", "bos_run", "tf"):
        g = {}
        for o in rows: g.setdefault(str(o.get(f) or "30m") if f == "tf" else str(o.get(f)), []).append(o["r"])
        out[f] = {k: {"n": len(v), "win": round(100 * sum(x > 0 for x in v) / len(v)), "exp": round(sum(v) / len(v), 2)} for k, v in g.items()}
    return out, len(rows)

def perf(orders, view="all", tf=None):
    """Track record. R-stats count each signal once; money sums every placed order (risk_amount x R)."""
    paper = lambda o: str(o.get("status", "")).startswith("PAPER")
    sel = [o for o in orders if o.get("outcome") in ("WIN", "LOSS") and (view == "all" or (view == "paper") == paper(o)) and (tf is None or (o.get("tf") or "30m") == tf)]
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
    base = "https://testnet.binancefuture.com" if acc.get("env") == "testnet" else "https://fapi.binance.com"
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
    k = (acc.get("env", ""), sym)
    if k not in _filters:
        info = bn(acc, "GET", "/fapi/v1/exchangeInfo", signed=False)
        for s_ in info["symbols"]:
            f = {x["filterType"]: x for x in s_["filters"]}
            _filters[(acc.get("env", ""), s_["symbol"])] = {"tick": f["PRICE_FILTER"]["tickSize"], "step": f["LOT_SIZE"]["stepSize"], "min": f["LOT_SIZE"]["minQty"]}
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
    base = "https://api-testnet.bybit.com" if acc.get("env") == "testnet" else "https://api.bybit.com"
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
    if acc.get("env") == "testnet": hdr["x-simulated-trading"] = "1"
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

# ======================================================================================================================
# DEMO vs REAL: the tier is decided HERE, from evidence the server can check. It is never a field the user fills in.
#   proven by the exchange   BINANCE / BYBIT / OKX : the account only ever talks to the exchange's testnet when env=testnet,
#                            and real API keys do NOT work on a testnet, so "demo" cannot be used to trade real money for free.
#   proven by the broker     MT4 / MT5 through MetaApi : MetaApi logs in to the broker itself and reports the real server name;
#                            demo = that server is on the owner's demo list.
#   self-reported            EA : the EA reports account type + server + login. Accepted as demo only when ALL checks pass
#                            (see eval_ea). Anything unknown, unverified, inconsistent or reported REAL once is REAL (fail closed).
# ======================================================================================================================
def csv_list(x): return [p.strip().lower() for p in str(x or "").split(",") if p.strip()]

def demo_server(name, s):
    n = (name or "").strip().lower()
    if not n or any(b in n for b in csv_list(s.get("demo_blocklist"))): return False
    return any(p in n for p in csv_list(s.get("demo_servers")))

def _att(a):
    v = a.get("attest")
    if isinstance(v, dict): return v
    try: return json.loads(v or "{}")
    except Exception: return {}

def eval_ea(a, s):
    att, mode = _att(a), _att(a).get("mode", "")
    if mode == "REAL": return "real", "ea", "MetaTrader reports a REAL account"
    if s.get("ea_demo_trust") != "strict": return "real", "ea", "Free demo through the EA is switched off by the owner"
    if mode not in ("DEMO", "CONTEST"): return "real", "unverified", "Waiting for the EA to report the account type"
    if a.get("ever_real"): return "real", "ea", "This EA key has reported a real account before"
    if not att.get("login") or not att.get("server"): return "real", "ea", "The EA did not report the account login/server"
    if not demo_server(att["server"], s): return "real", "ea", f"Server '{att['server']}' is not on the demo server list"
    cap = float(s.get("demo_max_balance") or 0)
    if cap and float(a.get("balance") or 0) > cap: return "real", "ea", "Balance is above the maximum allowed for a demo account"
    return "demo", "ea", f"Demo account on '{att['server']}' (reported by the EA)"

def auto_tier(a, s):
    p = a["platform"]
    if a.get("tier_override") in ("demo", "real"): return a["tier_override"], "admin", "Set by the owner"
    if p in TESTNET_OK:
        return ("demo", "endpoint", "Connected to the exchange's own demo (testnet) servers") if a.get("env") == "testnet" else ("real", "endpoint", "Real exchange account")
    if p in FX_PLATS:
        sv = _att(a).get("server", "")
        if sv and demo_server(sv, s): return "demo", "metaapi", f"Broker server '{sv}' (checked through MetaApi) is a demo server"
        return "real", ("metaapi" if sv else "unverified"), (f"Broker server '{sv}' is not on the demo list" if sv else "Not verified yet")
    if p == "EA": return eval_ea(a, s)
    return "real", "default", "Real account"

def tier_cols(a, s):
    t, src, note = auto_tier(a, s); return {"tier": t, "tier_src": src, "tier_note": note[:200]}

def parse_date(x):
    try: return datetime.date.fromisoformat(str(x)[:10])
    except Exception: return None

def sub_state(a, s, u=None):
    """(allowed to trade?, text). Demo is free forever. Real needs a plan once the owner switches 'Charge for real accounts' on."""
    if a.get("blocked") or (u and u.get("blocked")): return False, "Paused by the owner"
    if s.get("block_unlinked") and not a.get("user_id"): return False, "Sign up and link this account to keep using the robot"
    if a.get("tier") == "demo": return True, "Demo account: free forever"
    if not s.get("paid_mode"): return True, "Active (free for now)"
    t = today(); pu = parse_date(a.get("paid_until"))
    if pu and pu >= t: return True, "Free access" if pu.year >= 2099 else f"Active until {pu}"
    te = parse_date((u or {}).get("trial_end") or a.get("trial_end"))
    if te and te >= t: return True, f"Free trial until {te}"
    return False, "Real account: monthly plan needed"

def elig_sql(s):
    """Same rules as sub_state, as SQL, so thousands of accounts are selected with ONE statement."""
    sql = "a.blocked = 0 AND COALESCE(u.blocked, 0) = 0 AND (a.tier = 'demo' OR :free = 1 OR a.paid_until >= :today OR COALESCE(u.trial_end, a.trial_end) >= :today)"
    if s.get("block_unlinked"): sql += " AND a.user_id IS NOT NULL"
    return sql, {"free": 0 if s.get("paid_mode") else 1, "today": str(today())}

def start_trial(user_id):
    """The free trial of real accounts starts once per USER (not per account), so extra accounts cannot restart it."""
    s = S()
    if user_id and s.get("paid_mode") and int(s.get("trial_days") or 0) > 0:
        db.x("UPDATE users SET trial_end = :d WHERE id = :i AND trial_end IS NULL", d=str(today() + datetime.timedelta(days=int(s["trial_days"]))), i=user_id)

def user_of(a): return db.one("SELECT id, email, blocked, trial_end FROM users WHERE id=:i", i=a["user_id"]) if a.get("user_id") else None
def acc_full(row):
    a = dict(row); a.update(unseal(row["ref"], row["secrets"])); return a

# ======================================================================================================================
# orders in the database
# ======================================================================================================================
def order_row(o):
    return dict(id=o["id"], account_id=int(o.get("account") or 0), platform=o.get("platform", ""), symbol=o.get("symbol", ""), tf=o.get("tf") or "", sig_key=str(o.get("key") or ""),
                created=o.get("time") or now_s(), outcome=o.get("outcome", ""), status=str(o.get("status", "")), ack=1 if o.get("ea_ack") else 0, r=o.get("r"),
                last_t=int(o.get("last_t") or 0), bars=int(o.get("bars") or 0), ext_id=str(o["ext_id"]) if o.get("ext_id") else None, data=json.dumps(o, default=str))

def insert_order(o, t=None):
    (t or db).x("INSERT INTO orders (id, account_id, platform, symbol, tf, sig_key, created, outcome, status, ack, r, last_t, bars, ext_id, data) VALUES "
                "(:id, :account_id, :platform, :symbol, :tf, :sig_key, :created, :outcome, :status, :ack, :r, :last_t, :bars, :ext_id, :data)", **order_row(o))

def row_to_order(r):
    o = json.loads(r["data"])
    o.update(id=r["id"], account=r["account_id"], outcome=r["outcome"], status=r["status"], last_t=r["last_t"], bars=r["bars"], ea_ack=bool(r["ack"]), time=r["created"])
    if r["r"] is not None: o["r"] = float(r["r"])
    if r["ext_id"]: o["ext_id"] = r["ext_id"]
    return o

def save_order_state(o):
    db.x("UPDATE orders SET outcome=:outcome, status=:status, ack=:ack, r=:r, last_t=:last_t, bars=:bars, ext_id=:ext_id WHERE id=:id",
         **{k: v for k, v in order_row(o).items() if k in ("id", "outcome", "status", "ack", "r", "last_t", "bars", "ext_id")})

def record_track(o):
    """Public track record: ONE row per signal (symbol + setup), money removed. Account orders never appear publicly."""
    if o.get("outcome") not in ("WIN", "LOSS") or o.get("r") is None: return
    tkey = f"{o['symbol']}|{o.get('key')}"
    d = dict(o); d.update(key=tkey, risk_amount=0, account="-")
    db.x("INSERT INTO track (tkey, created, tf, paper, data) VALUES (:k, :c, :tf, :p, :d) ON CONFLICT (tkey) DO NOTHING", k=tkey, c=o.get("time") or now_s(),
         tf=o.get("tf") or "30m", p=1 if str(o.get("status", "")).startswith("PAPER") else 0, d=json.dumps(d, default=str))

_PC = {}
def track_rows(tf=None, limit=5000):
    k = (tf, limit); hit = _PC.get(k)
    if hit and time.time() - hit[0] < 60: return hit[1]
    rows = (db.q("SELECT data FROM track WHERE tf=:tf ORDER BY created DESC LIMIT :n", tf=tf, n=limit) if tf else db.q("SELECT data FROM track ORDER BY created DESC LIMIT :n", n=limit))
    out = [json.loads(r["data"]) for r in reversed(rows)]
    _PC[k] = (time.time(), out); return out

def seen_new(k):
    """Atomic 'have we handled this setup before?': True only for the very first caller, across all processes."""
    return bool(db.q("INSERT INTO seen (k, t) VALUES (:k, :t) ON CONFLICT (k) DO NOTHING RETURNING k", k=k, t=now_s()))

# ---------------- setup detection, cached: many users on the same broker send identical candles ----------------
_SETUP, _SETUP_LOCK = collections.OrderedDict(), threading.Lock()
SETUP_KEYS = ("swing_n", "min_bos", "rr", "ob_entry", "max_mitigation_pct", "min_leg_atr", "timeframe")
def cached_setup(rows, s, tf):
    h = hashlib.sha1((tf + json.dumps([s[k] for k in SETUP_KEYS]) + ";".join(f"{x['t']},{x['o']},{x['h']},{x['l']},{x['c']}" for x in rows)).encode()).hexdigest()
    with _SETUP_LOCK:
        if h in _SETUP: _SETUP.move_to_end(h); v = _SETUP[h]; return dict(v) if v else None
    sig = find_setup(rows, s, tf)
    with _SETUP_LOCK:
        _SETUP[h] = sig
        while len(_SETUP) > 3000: _SETUP.popitem(last=False)
    return dict(sig) if sig else None

def place_order(acc, sig, sym, s):
    bal = float(acc.get("balance") or 0)
    if s["mode"] == "live" and acc["platform"] != "EA":
        try: bal = balance(acc)                                                # use the real balance for sizing
        except Exception: pass
    risk_amt = bal * s["risk_pct"] / 100
    dist = abs(sig["entry"] - sig["sl"])
    o = {"id": uuid.uuid4().hex[:12], "time": now_s(), "account": acc["id"], "platform": acc["platform"],
         "symbol": sym, "size": round(risk_amt / dist, 6) if dist else 0, "risk_amount": round(risk_amt, 2),
         "outcome": "PENDING", "bars": 0, **sig}
    if s["mode"] == "paper": o["status"] = "PAPER"
    elif acc["platform"] == "EA": o["status"], o["ea_ack"] = "LIVE: queued for EA", False
    elif acc["platform"] in LIVE:
        try: o["ext_id"] = LIVE[acc["platform"]](acc, o); o["status"] = "LIVE: orders sent"
        except Exception as e: o["status"], o["outcome"] = f"LIVE FAILED: {e}", "REJECTED"
    else: o["status"], o["outcome"] = f"REJECTED: no live adapter for {acc['platform']}", "REJECTED"
    return o

# ======================================================================================================================
# worker: market scan (one leader) + job queue (any number of workers)
# ======================================================================================================================
def data_account():
    if os.environ.get("META_TOKEN") and os.environ.get("META_ACCOUNT"):
        return {"login": os.environ["META_TOKEN"], "extra": os.environ["META_ACCOUNT"], "server": os.environ.get("META_REGION", "new-york")}
    r = db.one("SELECT * FROM accounts WHERE platform IN ('MT4','MT5') AND secrets != '{}' ORDER BY id LIMIT 1")
    return acc_full(r) if r else None

def record_data_error(sym, tf, e):
    msg = f"DATA ERROR {tf}: {e}"[:300]
    last = db.one("SELECT status FROM orders WHERE platform='SYSTEM' AND symbol=:s AND status LIKE :p ORDER BY created DESC LIMIT 1", s=sym, p=f"DATA ERROR {tf}:%")
    if not (last and last["status"] == msg):                                   # same error again: do not spam the history
        insert_order({"id": uuid.uuid4().hex[:12], "time": now_s(), "account": 0, "platform": "SYSTEM", "symbol": sym, "status": msg, "outcome": "ERROR"})

def finish_signal(o):
    """A signal changed state: copy it to every account order of that signal with ONE statement, add to the public record."""
    k = dict(k=o.get("key"), s=o["symbol"], tf=o.get("tf") or "")
    where = "sig_key=:k AND symbol=:s AND tf=:tf AND platform NOT IN ('EA','SIGNAL','SYSTEM') AND outcome IN ('PENDING','OPEN')"
    if o["outcome"] == "EXPIRED":
        db.x(f"INSERT INTO jobs (kind, account_id, payload, status, tries, created) SELECT 'cancel', account_id, id, 'new', 0, :now FROM orders WHERE {where} AND status LIKE 'LIVE: orders sent%'", now=now_s(), **k)
    db.x(f"UPDATE orders SET outcome=:o, r=:r, last_t=:lt, bars=:b WHERE {where}", o=o["outcome"], r=o.get("r"), lt=int(o["last_t"]), b=int(o.get("bars", 0)), **k)
    record_track(o)

_SCAN_BUSY = threading.Lock()
def run_scan():
    if not _SCAN_BUSY.acquire(blocking=False): return 0
    try:
        s = load_settings(fresh=True); cache = {}; made = 0
        def candles(sym, tf):
            if (sym, tf) not in cache:
                if is_fx(sym):
                    ma = data_account()
                    if not ma: raise RuntimeError(f"{sym}: add an MT4/MT5 account (via MetaApi) or set META_TOKEN/META_ACCOUNT to get forex/gold candles")
                    cache[(sym, tf)] = fetch_candles_meta(ma, sym, tf)
                else: cache[(sym, tf)] = fetch_candles(sym, tf)
            return cache[(sym, tf)]
        for r in db.q("SELECT * FROM orders WHERE platform='SIGNAL' AND outcome IN ('PENDING','OPEN')"):         # 1) resolve open signals
            o = row_to_order(r)
            try:
                if resolve(o, candles(o["symbol"], o.get("tf") or s["timeframe"])): save_order_state(o); finish_signal(o)
            except Exception: pass
        for sym, tf in [(x, t) for x in s["symbols"] for t in s["timeframes"]]:                                  # 2) new setups
            try: sig = find_setup(candles(sym, tf), s, tf)
            except Exception as e: record_data_error(sym, tf, e); continue
            if not sig or not seen_new(f"{sym}-{sig['key']}"): continue
            if any(str(sig[f]) in s["filters"].get(f, []) for f in s["filters"] if f in sig): continue            # learned filters
            skey = uuid.uuid4().hex[:16]
            db.x("INSERT INTO sigs (skey, data, created) VALUES (:k, :d, :c)", k=skey, d=json.dumps({"sym": sym, "sig": sig}), c=now_s())
            insert_order({"id": uuid.uuid4().hex[:12], "time": now_s(), "account": 0, "platform": "SIGNAL", "symbol": sym, "size": 0, "risk_amount": 0, "outcome": "PENDING", "bars": 0,
                          "status": "PAPER" if s["mode"] == "paper" else "LIVE: signal", **sig})
            el, ep = elig_sql(s)
            plat = "a.platform IN ('MT4','MT5')" if is_fx(sym) else "a.platform NOT IN ('MT4','MT5','EA')"
            db.x(f"INSERT INTO jobs (kind, account_id, payload, status, tries, created) SELECT 'place', a.id, :skey, 'new', 0, :now FROM accounts a "
                 f"LEFT JOIN users u ON u.id = a.user_id WHERE {plat} AND a.tf = :tf AND {el}", skey=skey, now=now_s(), tf=tf, **ep)       # fan-out: one statement for all accounts
            made += 1
        return made
    finally: _SCAN_BUSY.release()

def job_place(j, s):
    r = db.one("SELECT * FROM accounts WHERE id=:i", i=j["account_id"])
    if not r: return
    if not sub_state(r, s, user_of(r))[0]: return                                    # blocked / unpaid since the job was queued
    sg = db.one("SELECT data FROM sigs WHERE skey=:k", k=j["payload"])
    if not sg: return
    d = json.loads(sg["data"]); sym, sig = d["sym"], d["sig"]
    if db.one("SELECT 1 AS x FROM orders WHERE account_id=:a AND sig_key=:k AND symbol=:s", a=r["id"], k=sig["key"], s=sym): return          # retry-safe: never place twice
    if db.one("SELECT 1 AS x FROM orders WHERE account_id=:a AND symbol=:s AND tf=:t AND outcome IN ('PENDING','OPEN')", a=r["id"], s=sym, t=sig["tf"]): return
    insert_order(place_order(acc_full(r), sig, sym, s))

def job_cancel(j):
    r, o = db.one("SELECT * FROM accounts WHERE id=:i", i=j["account_id"]), db.one("SELECT * FROM orders WHERE id=:i", i=j["payload"])
    if r and o: cancel_orders(acc_full(r), row_to_order(o))

def run_job(j):
    try:
        if j["kind"] == "place": job_place(j, S())
        elif j["kind"] == "cancel": job_cancel(j)
        db.x("DELETE FROM jobs WHERE id=:i", i=j["id"])
    except Exception as e:
        print("job failed", j["id"], str(e)[:200])
        try: db.x("UPDATE jobs SET status = CASE WHEN tries >= 2 THEN 'failed' ELSE 'new' END, tries = tries + 1 WHERE id=:i", i=j["id"])
        except Exception: pass

def claim_jobs(wid, n=40):
    with db.tx() as t:
        return t.q(f"UPDATE jobs SET status='run', worker=:w, started=:now WHERE id IN (SELECT id FROM jobs WHERE status='new' ORDER BY id LIMIT {int(n)}{db.skip}) "
                   "RETURNING id, kind, account_id, payload, tries", w=wid, now=now_s())

def take_lease(name, owner, ttl):
    """Leader election that also works through connection poolers (a table row, not a session lock)."""
    now = time.time()
    db.x("INSERT INTO leases (name, owner, expires) VALUES (:n, :o, 0) ON CONFLICT (name) DO NOTHING", n=name, o=owner)
    return db.x("UPDATE leases SET owner=:o, expires=:e WHERE name=:n AND (expires < :now OR owner=:o)", n=name, o=owner, e=now + ttl, now=now) == 1

def maintenance():
    c = lambda days: str(datetime.datetime.utcnow() - datetime.timedelta(days=days))
    db.x("UPDATE jobs SET status='new' WHERE status='run' AND started < :t", t=c(0.01))        # a worker died mid-job: give it back
    db.x("DELETE FROM seen WHERE t < :t", t=c(30)); db.x("DELETE FROM sigs WHERE created < :t", t=c(7)); db.x("DELETE FROM jobs WHERE status='failed' AND created < :t", t=c(3))

def worker_main():
    init_app()
    wid, pool, last = uuid.uuid4().hex[:8], ThreadPoolExecutor(int(os.environ.get("JOB_THREADS", "16"))), 0.0
    print("worker", wid, "started")
    while True:
        try:
            if take_lease("scan", wid, 120) and time.time() - last >= 300:
                last = time.time(); run_scan(); maintenance()
        except Exception as e: print("scan error", e)
        try: rows = claim_jobs(wid)
        except Exception as e: print("queue error", e); rows = []
        if rows: list(pool.map(run_job, rows))
        else: time.sleep(2)
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
.links a#inst{background:linear-gradient(135deg,var(--brand),var(--brand2));color:#fff}.links a#inst:hover{filter:brightness(1.1);color:#fff}
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
    name = S()["site_name"]
    return jsonify({"name": name, "short_name": name[:12], "start_url": "/", "scope": "/", "display": "standalone", "background_color": "#0b1220", "theme_color": "#2563eb",
                    "icons": [{"src": f"/icon-{z}.png", "sizes": f"{z}x{z}", "type": "image/png", "purpose": "any maskable"} for z in (192, 512)]}), 200, {"Content-Type": "application/manifest+json"}

@app.route("/sw.js")
def service_worker():
    js = "self.addEventListener('install',function(){self.skipWaiting()});self.addEventListener('activate',function(e){e.waitUntil(self.clients.claim())});" \
         "self.addEventListener('fetch',function(e){e.respondWith(fetch(e.request).catch(function(){return new Response('You are offline. Reconnect to open the app.',{headers:{'Content-Type':'text/plain'}})}))});"
    return js, 200, {"Content-Type": "application/javascript", "Service-Worker-Allowed": "/", "Cache-Control": "no-cache"}

PWA_JS = ("<script>if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js');var dp=null,b=document.getElementById('inst');"
          "function inApp(){return (window.matchMedia&&matchMedia('(display-mode: standalone)').matches)||navigator.standalone===true}"
          "if(b&&inApp())b.style.display='none';"
          "addEventListener('beforeinstallprompt',function(e){e.preventDefault();dp=e});"
          "addEventListener('appinstalled',function(){if(b)b.style.display='none'});"
          "if(b)b.onclick=function(x){x.preventDefault();if(dp){dp.prompt();dp=null}else{alert('To install: open your browser menu (the three dots) and tap Install app or Add to Home screen. On iPhone use Safari: tap Share, then Add to Home Screen.')}};</script>")

LOGO_SVG = "<svg width='18' height='18' viewBox='0 0 18 18'><rect x='2' y='8' width='3' height='8' rx='1' fill='#fff'/><rect x='7.5' y='3' width='3' height='13' rx='1' fill='#fff'/><rect x='13' y='6' width='3' height='10' rx='1' fill='#fff' opacity='.85'/></svg>"


def layout(title, body):
    name = escape(S()["site_name"]); u = cur_user()
    nav = [("/", "HOME"), ("/setup", "SETUP"), ("/performance", "PERFORMANCE"), ("/orders", "SIGNALS"), ("/status", "MY ACCOUNT")]
    nav += [("/logout", "LOG OUT")] if u else [("/login", "LOG IN"), ("/signup", "SIGN UP")]
    nav += [("/admin", "ADMIN")]
    links = "".join(f"<a href='{h}'>{t}</a>" for h, t in nav)
    return (f"<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>{escape(title)} · {name}</title>"
            f"<link rel='preconnect' href='https://fonts.googleapis.com'><link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&display=swap' rel='stylesheet'>"
            f"<link rel='manifest' href='/manifest.webmanifest'><meta name='theme-color' content='#2563eb'><link rel='apple-touch-icon' href='/icon-192.png'><style>{CSS}</style></head><body><div class='nav'><a class='logo' href='/'><span class='mark'>{LOGO_SVG}</span>{name}</a><div class='links'>{links}<a href='#' id='inst'>⬇ INSTALL APP</a></div></div>{body}"
            f"<div class='foot'>© {datetime.datetime.utcnow().year} {name} · Trading involves risk. Past results do not guarantee future results. Not financial advice.</div>{PWA_JS}</body></html>")

@app.after_request
def secure_headers(r):
    r.headers.update({"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer",
        "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
        "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'; object-src 'none'"})
    if os.environ.get("RENDER"): r.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    if request.path.startswith(("/admin", "/status", "/account", "/ea/download", "/save_account", "/login", "/signup")): r.headers["Cache-Control"] = "no-store"
    return r

@app.before_request
def csrf_guard():
    """Browsers always send Origin on form posts: refuse posts that come from another website."""
    if request.method == "POST" and request.path not in ("/ea/sync",):
        o = request.headers.get("Origin")
        if o and urllib.parse.urlparse(o).netloc != request.host: abort(403)

# ---------------- users ----------------
def pw_hash(pw, salt): return hashlib.scrypt(pw.encode(), salt=bytes.fromhex(salt), n=2 ** 14, r=8, p=1, maxmem=64 * 1024 * 1024, dklen=32).hex()

def cur_user():
    if not hasattr(g, "_u"):
        uid = session.get("uid")
        u = db.one("SELECT * FROM users WHERE id=:i", i=uid) if uid else None
        if u and u["blocked"]: session.pop("uid", None); u = None
        g._u = u
    return g._u

def login_user(u): session.clear(); session["uid"] = u["id"]; session.permanent = True; g._u = u

EMAIL_RE = re.compile(r"[^@\s]{1,64}@[^@\s]{1,120}\.[^@\s]{2,}")

# ---------------- admin security (password + optional 2-step code) ----------------
def _pw_hash(pw, salt, kdf="scrypt"):
    if kdf == "pbkdf2": return hashlib.pbkdf2_hmac("sha256", pw.encode(), bytes.fromhex(salt), 200000).hex()
    return hashlib.scrypt(pw.encode(), salt=bytes.fromhex(salt), n=2 ** 15, r=8, p=1, maxmem=96 * 1024 * 1024, dklen=32).hex()
def set_admin_password(pw):
    salt = os.urandom(16).hex(); kv_set("admin_hash", json.dumps({"salt": salt, "hash": _pw_hash(pw, salt), "kdf": "scrypt"}))
def check_admin(pw):
    """The password saved in the admin panel wins; otherwise the ADMIN_PASSWORD environment variable. There is no built-in default password any more."""
    try: h = json.loads(kv_get("admin_hash") or "null")
    except Exception: h = None
    if h: return hmac.compare_digest(_pw_hash(pw, h["salt"], h.get("kdf", "pbkdf2")).encode(), h["hash"].encode())
    return bool(ADMIN_PASSWORD) and hmac.compare_digest(pw.encode(), ADMIN_PASSWORD.encode())

TOTP_PENDING = {"secret": None}
def totp_code(secret, t=None):
    key = base64.b32decode(secret.upper() + "=" * (-len(secret) % 8))
    h = hmac.new(key, struct.pack(">Q", int((time.time() if t is None else t) // 30)), hashlib.sha1).digest()
    o = h[-1] & 15
    return str((struct.unpack(">I", h[o:o + 4])[0] & 0x7FFFFFFF) % 10 ** 6).zfill(6)
def totp_ok(secret, code):
    code = re.sub(r"\D", "", code or "")
    return len(code) == 6 and any(hmac.compare_digest(totp_code(secret, time.time() + w * 30), code) for w in (-1, 0, 1))
def _totp():
    try: t = json.loads(kv_get("admin_totp") or "null") or {}
    except Exception: t = {}
    return t
def _totp_secret():
    t = _totp()
    return _dec2(t["secret"][5:], "admin:totp", kdf_salt()) if t.get("enabled") and t.get("secret") else ""

def admin_only(f):
    def w(*a, **k):
        if session.get("admin") is not True: return redirect("/admin")
        return f(*a, **k)
    w.__name__ = f.__name__; return w

def _warnings(s):
    w = []
    try: has_hash = bool(kv_get("admin_hash"))
    except Exception: has_hash = True
    if not has_hash: w.append("The admin password still comes from the ADMIN_PASSWORD environment variable. Change it under Security below.")
    if not CRYPTO: w.append("Encryption library missing: add 'cryptography' to requirements.txt and redeploy. New exchange keys are refused until then.")
    if not DATABASE_URL: w.append("No DATABASE_URL: the app is using a local SQLite file. That is fine for testing, but it cannot serve many users or more than one server. Set DATABASE_URL to a PostgreSQL database for real traffic.")
    if DATABASE_URL and not (os.environ.get("DATA_KEY") or os.environ.get("SECRET_KEY")): w.append("With a database you MUST set DATA_KEY on Render, otherwise new exchange accounts are refused.")
    if not os.environ.get("DATA_KEY"): w.append("Set a DATA_KEY environment variable (any long random text) so stored keys are encrypted with a key that is not in the database.")
    if not os.environ.get("SECRET_KEY"): w.append("Set SECRET_KEY (long random text) so login cookies stay valid on every server you run.")
    lease = db.one("SELECT expires FROM leases WHERE name='scan'")
    if not (lease and lease["expires"] > time.time()) and os.environ.get("AUTOSCAN") != "1": w.append("No scanner is running. Start a worker (python app.py worker) or the robot will not look for new setups or place orders for exchange/MetaApi accounts.")
    return "".join(f"<div class='card' style='background:#fef2f2;border-color:#fecaca;font-weight:600;color:#991b1b'>⚠️ {escape(x)}</div>" for x in w)

# ======================================================================================================================
# one-time import of the old single JSON blob (bot_state table / bot_data.json) into the new tables
# ======================================================================================================================
def legacy_blob():
    if DATABASE_URL:
        try:
            r = db.q("SELECT data FROM bot_state WHERE id = 1"); return r[0]["data"] if r else None
        except Exception: return None
    p = os.path.join(DATA_DIR, "bot_data.json")
    if os.path.exists(p):
        with open(p) as f: return f.read()

def migrate_legacy():
    if kv_get("legacy_migrated"): return
    if not db.q("INSERT INTO kv (k, v) VALUES ('legacy_migrating', :v) ON CONFLICT (k) DO NOTHING RETURNING k", v=now_s()): return    # another process is doing it
    txt = legacy_blob()
    try: d = json.loads(txt) if txt else None
    except Exception: d = None
    if not d: kv_set("legacy_migrated", "nothing to import"); return
    salt = d.get("kdf_salt") or ""
    if salt: db.x("INSERT INTO kv (k, v) VALUES ('kdf_salt', :v) ON CONFLICT (k) DO NOTHING", v=salt)
    if d.get("admin_hash"): kv_set("admin_hash", json.dumps(d["admin_hash"]))
    if d.get("admin_totp"): kv_set("admin_totp", json.dumps(d["admin_totp"]))
    if d.get("lessons"): kv_set("lessons", d["lessons"])
    st = {k: v for k, v in (d.get("settings") or {}).items() if k in DEFAULTS}
    kv_set("settings", json.dumps(st)); s = load_settings(fresh=True)
    idmap, n_acc = {}, 0
    for a in d.get("accounts", []):
        plat, plain, lost = a.get("platform", ""), {}, False
        for k in SENSITIVE:
            v = a.get(k) or ""
            if v.startswith("enc2:"): t = _dec2(v[5:], f"{a['id']}:{k}", salt)
            elif v.startswith("enc:"): t = _dec_fernet(v[4:])
            else: t = v
            if v and not t: lost = True
            if t: plain[k] = t
        if plat == "EA": plain = {}
        ref = secrets.token_hex(8)
        sealed = seal(ref, plain) if plain and crypto_ready() else "{}"
        if plain and not crypto_ready(): lost = True
        env = "testnet" if plat in TESTNET_OK and str(a.get("server", "")).lower() == "testnet" else ""
        try: bal = float(a.get("balance") or 0)
        except ValueError: bal = 0.0
        te = parse_date(a.get("created"))
        row = dict(ref=ref, legacy_id=str(a["id"]), platform=plat, tf=a.get("tf") or s["timeframe"], env=env, server=("" if plat in TESTNET_OK else str(a.get("server", ""))[:40]),
                   secrets=sealed, key_hash=a.get("key_hash") or (_h(a["login"]) if plat == "EA" and a.get("login") else None), balance=bal, created=(a.get("created") or str(today()))[:10],
                   paid_until=a.get("paid_until") or None, trial_end=str(te + datetime.timedelta(days=int(s["trial_days"]))) if te else None,
                   blocked=1 if (a.get("blocked") or (lost and plat != "EA")) else 0, label=("keys must be re-entered" if lost and plat != "EA" else ""))
        row.update(tier_cols({**row, "attest": "{}", "tier_override": "", "ever_real": 0}, s))
        r = db.q("INSERT INTO accounts (ref, legacy_id, platform, tf, env, server, secrets, key_hash, balance, created, paid_until, trial_end, blocked, label, tier, tier_src, tier_note) VALUES "
                 "(:ref, :legacy_id, :platform, :tf, :env, :server, :secrets, :key_hash, :balance, :created, :paid_until, :trial_end, :blocked, :label, :tier, :tier_src, :tier_note) RETURNING id", **row)
        idmap[str(a["id"])] = r[0]["id"]; n_acc += 1
    cloned = set()
    for o in d.get("orders", []):
        try:
            acc = idmap.get(str(o.get("account")), 0); o = dict(o); o["account"] = acc
            o["platform"] = o.get("platform") or ("SYSTEM" if o.get("outcome") == "ERROR" else "SIGNAL"); o["time"] = o.get("time") or now_s(); o.setdefault("tf", o.get("tf") or "30m")
            if len(str(o.get("id", ""))) < 4: o["id"] = uuid.uuid4().hex[:12]
            insert_order(o)
            if o.get("outcome") in ("WIN", "LOSS"): record_track(o)
            ck = (o.get("key"), o.get("symbol"), o.get("tf"))
            if o.get("outcome") in ("PENDING", "OPEN") and o["platform"] not in ("EA", "SIGNAL", "SYSTEM") and ck not in cloned:      # keep open trades resolvable under the new signal model
                cloned.add(ck); c = dict(o); c.update(id=uuid.uuid4().hex[:12], account=0, platform="SIGNAL", size=0, risk_amount=0); insert_order(c)
        except Exception as e: print("order import skipped:", str(e)[:120])
    for k in d.get("seen", []): db.x("INSERT INTO seen (k, t) VALUES (:k, :t) ON CONFLICT (k) DO NOTHING", k=str(k), t=now_s())
    for j in d.get("journal", []): db.x("INSERT INTO journal (id, created, data) VALUES (:i, :c, :d) ON CONFLICT (id) DO NOTHING", i=j.get("id") or uuid.uuid4().hex[:8], c=j.get("time") or now_s(), d=json.dumps(j))
    kv_set("legacy_migrated", f"{now_s()} · {n_acc} accounts, {len(d.get('orders', []))} orders"); print("legacy data imported:", n_acc, "accounts")

_INIT = {"done": False}
def init_app():
    if _INIT["done"]: return
    for attempt in (1, 2, 3):
        try: init_db(); break
        except Exception as e:
            if attempt == 3: raise
            time.sleep(1 + random.random())                           # two processes creating the tables at the same instant
    migrate_legacy()
    sk = os.environ.get("SECRET_KEY") or kv_get("session_secret")
    if not sk:
        db.x("INSERT INTO kv (k, v) VALUES ('session_secret', :v) ON CONFLICT (k) DO NOTHING", v=secrets.token_hex(32)); sk = kv_get("session_secret")
    app.secret_key = sk
    if not kv_get("admin_hash") and not ADMIN_PASSWORD:
        pw = secrets.token_urlsafe(12); set_admin_password(pw)
        print("=" * 70, "\nNo ADMIN_PASSWORD was set. A one-time admin password was created: ", pw, "\nLog in to /admin with it and change it under Security.\n" + "=" * 70)
    _INIT["done"] = True
init_app()
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
   <div class='kpi'><b data-to='@@TRADES@@'>0</b><span>Closed trades · @@TFK@@</span></div>
   <div class='kpi'><b data-to='@@WIN@@' data-dec='1' data-suf='%'>0</b><span>Win rate · @@TFK@@</span></div>
   <div class='kpi'><b data-to='@@TOTALR@@' data-dec='2' data-pre='@@PRE@@' data-suf='R'>0</b><span>Total result · @@TFK@@</span></div>
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
 <div class='step' style='animation-delay:.05s'><div class='n'>01</div><h3>Break of Structure</h3><p>Reads the @@TF@@ and confirms the trend from consecutive major breaks of structure.</p></div>
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


# ======================================================================================================================
# public pages
# ======================================================================================================================
@app.route("/")
def home():
    s = S(); p = perf(track_rows(s["timeframe"]), "all", s["timeframe"])
    rep = {"@@TFK@@": str(escape(s["timeframe"])), "@@NAME@@": str(escape(s["site_name"])), "@@MODE@@": str(escape(s["mode"])).upper(), "@@RR@@": str(s["rr"]), "@@RISK@@": str(s["risk_pct"]),
           "@@TF@@": str(escape(("chart you choose (" + " or ".join({"30m": "30-minute", "1h": "1-hour"}.get(t, t) for t in s["timeframes"]) + ")") if len(s["timeframes"]) > 1 else ({"30m": "30-minute", "1h": "1-hour"}.get(s["timeframes"][0], s["timeframes"][0]) + " chart"))),
           "@@TRADES@@": str(p["trades"]), "@@WIN@@": str(p["win_rate"]), "@@TOTALR@@": str(p["total_r"]),
           "@@PRE@@": "+" if p["total_r"] >= 0 else "", "@@NPLAT@@": str(len(s["platforms"])), "@@CHART@@": _hero_chart(s["rr"]),
           "@@META@@": str(escape(f"{' / '.join(s['timeframes'])} · {', '.join(s['symbols'])}")), "@@PLATS@@": "".join(f"<span>{escape(x)}</span>" for x in s["platforms"]) * 3,
           "@@STRAT@@": str(escape(s["strategy_text"])), "@@TAGLINE@@": str(escape(s["tagline"])), "@@LEAD@@": str(escape(s["lead"]))}
    html = HOME_TPL
    for k, v in rep.items(): html = html.replace(k, v)
    return layout("Home", html)

@app.route("/healthz")
def healthz():
    try: db.one("SELECT 1 AS ok"); return "ok", 200
    except Exception: return "database down", 503

@app.route("/api/status")
def status():
    s = S(); c = _PC.get("count")
    if not c or time.time() - c[0] > 60:
        c = (time.time(), db.one("SELECT COUNT(*) AS n FROM accounts")["n"]); _PC["count"] = c
    return jsonify({"status": "live", "mode": s["mode"], "accounts": c[1], "storage": "postgresql" if DATABASE_URL else "sqlite"})

# ======================================================================================================================
# sign-up / log-in
# ======================================================================================================================
def _form(title, fields, button, extra="", err="", action=""):
    inputs = "".join(f"<div style='margin-bottom:10px'><label>{l}</label><input class='input' name='{n}' type='{t}' autocomplete='{ac}' required></div>" for n, l, t, ac in fields)
    e = f"<p style='color:#dc2626;font-weight:600;margin-bottom:8px'>{escape(err)}</p>" if err else ""
    return layout(title, f"<div class='card' style='max-width:440px'><h2>{title}</h2>{e}<form method='post' action='{action}'>{inputs}<button class='btn'>{button}</button></form>{extra}</div>")

def _safe_next(n): return n if n and n.startswith("/") and not n.startswith("//") else "/status"

@app.route("/signup", methods=["GET", "POST"])
def signup():
    s = S(); nxt = _safe_next(request.values.get("next", ""))
    fields = [("email", "Email address" + (" (Gmail only)" if s["gmail_only"] else ""), "email", "email"), ("password", "Password (8+ characters)", "password", "new-password"), ("password2", "Repeat password", "password", "new-password")]
    alt = f"<p class='muted' style='margin-top:12px'>Already have an account? <a href='/login?next={escape(nxt)}'>Log in</a></p>"
    if request.method == "GET": return _form("Create your account", fields, "Sign up", alt, action=f"/signup?next={escape(nxt)}")
    f = request.form; em = f.get("email", "").strip().lower()[:150]; pw = f.get("password", "")
    def bad(m): return _form("Create your account", fields, "Sign up", alt, m, action=f"/signup?next={escape(nxt)}"), 400
    if throttled("su", 10, 3600): return bad("Too many sign-ups from your network. Try again later.")
    if not EMAIL_RE.fullmatch(em): return bad("Please enter a valid email address.")
    if s["gmail_only"] and not em.endswith("@gmail.com"): return bad("Please use a Gmail address (ending in @gmail.com).")
    if len(pw) < 8 or len(pw) > 200 or pw != f.get("password2"): return bad("Passwords must match and be at least 8 characters.")
    if int(s["max_users"] or 0) and db.one("SELECT COUNT(*) AS n FROM users")["n"] >= int(s["max_users"]): return bad("Sign-ups are closed for now. Please try again later.")
    salt = os.urandom(16).hex()
    try:
        r = db.q("INSERT INTO users (email, pw_salt, pw_hash, created, ip) VALUES (:e, :s, :h, :c, :ip) RETURNING id, blocked", e=em, s=salt, h=pw_hash(pw, salt), c=now_s(), ip=_ip())
    except Exception: return bad("That email is already registered. Log in instead.")
    throttled("su", 10, 3600, add=True)
    login_user({"id": r[0]["id"], "blocked": 0}); return redirect(nxt if nxt != "/status" else "/setup")

@app.route("/login", methods=["GET", "POST"])
def login():
    nxt = _safe_next(request.values.get("next", ""))
    fields = [("email", "Email address", "email", "email"), ("password", "Password", "password", "current-password")]
    alt = f"<p class='muted' style='margin-top:12px'>New here? <a href='/signup?next={escape(nxt)}'>Create an account</a></p>"
    if request.method == "GET": return _form("Log in", fields, "Log in", alt, action=f"/login?next={escape(nxt)}")
    em, pw = request.form.get("email", "").strip().lower()[:150], request.form.get("password", "")[:200]
    if throttled("li", 10, 600) or throttled("lie", 8, 600, ident=em): return _form("Log in", fields, "Log in", alt, "Too many attempts. Try again in 10 minutes.", f"/login?next={escape(nxt)}"), 429
    u = db.one("SELECT * FROM users WHERE email=:e", e=em)
    if not u or not hmac.compare_digest(pw_hash(pw, u["pw_salt"]).encode(), u["pw_hash"].encode()) or u["blocked"]:
        throttled("li", 10, 600, add=True); throttled("lie", 8, 600, add=True, ident=em)
        return _form("Log in", fields, "Log in", alt, "Wrong email or password.", f"/login?next={escape(nxt)}"), 403
    db.x("UPDATE users SET last_login=:t WHERE id=:i", t=now_s(), i=u["id"]); login_user(u); return redirect(nxt)

@app.route("/logout")
def logout(): session.pop("uid", None); return redirect("/")

# ======================================================================================================================
# adding an account. The server verifies and classifies it: demo/real is NOT a form field the user can type.
# ======================================================================================================================
def _need_login(path):
    return redirect(f"/login?next={path}") if S()["require_signup"] and not cur_user() else None

@app.route("/setup")
def setup():
    s = S(); u = cur_user(); r = _need_login("/setup")
    if r: return r
    plats = "".join(f"<option>{escape(p)}</option>" for p in s["platforms"])
    tfn = {"30m": "30 minutes", "1h": "1 hour"}
    tfopts = "<option value='' disabled selected>Choose one...</option>" + "".join(f"<option value='{escape(t)}'>{escape(tfn.get(t, t))}</option>" for t in s["timeframes"])
    who = (f"<p class='muted'>Signed in as <b>{escape(u['email'])}</b>. <a href='/status'>See my accounts</a></p>" if u else
           ("<p class='muted'><a href='/signup?next=/setup'>Sign up</a> or <a href='/login?next=/setup'>log in</a> to link accounts to your profile (recommended).</p>"))
    money = ("<p class='muted' style='margin:6px 0'><b>Pricing:</b> demo accounts are free forever. Real-money accounts "
             + (f"need a monthly plan after a {int(s['trial_days'])}-day free trial. {escape(s['price_text'])}" if s["paid_mode"] else "are free for now; a monthly plan may be introduced later (demo stays free).") + "</p>")
    return layout("Setup", f"""<div class='card'><h2>Connect an account</h2>{who}{money}
<form action='/save_account' method='post' class='grid'>
<div><label>Platform</label><select class='input' name='platform'>{plats}</select></div>
<div><label>Which chart should the robot trade for you? (choose one)</label><select class='input' name='tf' required>{tfopts}</select></div>
<div><label>Exchange accounts only: is this a DEMO (testnet) or REAL account?</label><select class='input' name='env'><option value='real'>Real account</option><option value='demo'>Demo / testnet account</option></select></div>
<div><label>API key / login (platform EA: leave blank, a key is created for you)</label><input class='input' name='login' autocomplete='off'></div>
<div><label>API secret (MT4/MT5 via MetaApi: type any text)</label><input class='input' name='password' type='password' autocomplete='off'></div>
<div><label>Extra: passphrase (OKX, Bitget) · MetaApi account ID (MT4/MT5)</label><input class='input' name='extra' autocomplete='off'></div>
<div><label>MetaApi region (MT4/MT5), e.g. new-york or london</label><input class='input' name='server'></div>
<div><label>Practice balance used for paper-mode sizing (optional)</label><input class='input' name='balance' type='number' step='any'></div>
<div><button class='btn'>Save</button></div></form>
<div class='muted' style='margin-top:12px;line-height:1.7'><b>How demo and real are decided</b><br>
The app never takes your word for it. <b>Exchanges:</b> a demo account only ever connects to the exchange's own testnet, and real keys do not work there. <b>MT4/MT5 via MetaApi:</b> the broker server name is read through MetaApi.
<b>EA:</b> the EA reports the account type and server from MetaTrader, and the app checks it against the owner's demo-server list. Anything that cannot be proven demo is treated as real.<br><br>
<b>BINANCE / BYBIT:</b> API key + secret. <b>OKX / BITGET:</b> key + secret + passphrase (Extra). <b>MT5 / MT4:</b> metaapi.cloud API token in "API key", account ID in Extra, region in the last box.
<b>EA</b> (free, runs inside MT5 on your computer): choose Platform EA and press Save; the next page gives you the EA code with your key inside.<br>Use trade-only keys with withdrawals disabled.</div></div>""")

def _make_acc(f, user, s):
    """-> (account dict ready to insert, error text). Verifies credentials against the real exchange/broker where possible."""
    plat = f.get("platform", "")
    if plat not in s["platforms"]: return None, "Invalid platform."
    tf = f.get("tf", "")
    if tf not in s["timeframes"]: return None, "Please choose which chart the robot should trade for you (30 minutes or 1 hour)."
    try: bal = float(f.get("balance") or 0)
    except ValueError: bal = 0.0
    a = {"platform": plat, "tf": tf, "env": "", "server": "", "balance": max(0.0, min(bal, 1e12)), "attest": {}, "tier_override": "", "ever_real": 0, "key": None}
    sec = {k: f.get(k, "").strip()[:300] for k in SENSITIVE}
    if plat == "EA":
        a["key"] = secrets.token_urlsafe(24); a["key_hash"] = _h(a["key"]); a["secrets"] = {}
    else:
        if not crypto_ready(): return None, "Secure storage is not ready on this server yet. Please try again later."
        if not sec["login"]: return None, "API key / login is required."
        a["secrets"] = sec
        if plat in TESTNET_OK:
            demo = f.get("env") == "demo"
            a["env"] = "testnet" if demo else ""
            test = {**a, **sec}
            try: a["balance"] = float(balance(test))                      # proves the keys work on THAT environment (testnet keys do not work on the real exchange and vice versa)
            except Exception: return None, ("These keys were not accepted by the exchange's demo (testnet) servers. Demo needs testnet keys." if demo else
                                            "These keys were not accepted by the exchange. If they are testnet keys, choose 'Demo / testnet account'.")
        elif plat == "BITGET":
            if f.get("env") == "demo": return None, "Bitget demo is not supported here. Use the MT5 EA with a demo account instead."
            try: a["balance"] = float(balance({**a, **sec}))
            except Exception: return None, "These keys were not accepted by the exchange."
        elif plat in FX_PLATS:
            if not sec["extra"]: return None, "The MetaApi account ID is required."
            a["server"] = re.sub(r"[^a-z0-9-]", "", f.get("server", "").strip().lower())[:30] or "new-york"
            try:
                info = meta({**a, **sec}, "GET", "/account-information")
                a["attest"] = {"server": str(info.get("server") or "")[:60], "login": str(info.get("login") or "")[:20], "broker": str(info.get("broker") or "")[:60]}
                a["balance"] = float(info.get("balance") or 0)
            except Exception: return None, "MetaApi could not connect to that account. Check the token, the account ID and the region."
            if a["attest"]["server"] and a["attest"]["login"]: a["fp"] = hashlib.sha256(f"{a['attest']['broker']}|{a['attest']['server']}|{a['attest']['login']}".lower().encode()).hexdiges
