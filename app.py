"""
Nnanna Michael - trading robot, scalable edition.

What changed compared with the single-file/JSON version:
  * Storage: real database tables (users, accounts, orders, jobs...) instead of ONE JSON blob that every request re-read and re-wrote.
    PostgreSQL in production (DATABASE_URL), SQLite automatically when DATABASE_URL is empty (local testing / tiny installs).
  * Web processes are stateless (no in-memory state that matters) so you can run many of them behind a load balancer.
  * Heavy work (market scan, placing orders for thousands of accounts) runs in worker processes fed by a database job queue.
  * Sign-up / login for users (admin switch "Require sign-up").
  * One system: PAPER accounts (practice money on the live market) are free; every connected account (exchange, MT4/MT5, EA) is paid once the owner switches charging on.

Run:   web    ->  gunicorn app:app -k gthread -w 4 --threads 8 -b 0.0.0.0:$PORT
       worker ->  python app.py worker          (run 1 or more; they share the job queue safely)
Env:   DATABASE_URL, SECRET_KEY, DATA_KEY, ADMIN_PASSWORD, WEBHOOK_SECRET (optional), REDIS_URL (optional, shared rate limits),
       AUTOSCAN=1 (tiny installs only: runs the worker inside the web process), TRUSTED_PROXIES (default 1), DB_POOL (default 8 per process),
       META_TOKEN / META_ACCOUNT / META_REGION (optional dedicated MetaApi feed for forex/gold candles), ANTHROPIC_API_KEY (optional).
"""
import math, os, re, json, hmac, hashlib, time, datetime, threading, uuid, base64, secrets, struct, queue, random, ssl, contextlib, copy, collections
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
    "symbols": ["BTCUSDT", "ETHUSDT", "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY", "XAUUSD"], "timeframe": "30m", "timeframes": ["30m", "1h"],
    "swing_n": 4, "htf_swing_n": 3, "min_bos": 1, "rr": 2.0, "ob_entry": "top",
    "risk_pct": 1.0, "max_mitigation_pct": 50.0, "min_leg_atr": 1.2,
    "max_pairs": 5,                 # most pairs one account may ask the robot to trade (advice shown to people: no more than 2)
    "sl_buffer_pips": 2.5,          # stop loss = entry order block high/low plus this many pips (2-3)
    "max_sl_pips": 40.0,            # skip the trade when the stop loss would be bigger than this (0 = no limit). Gold / crypto: a "pip" is the same % of price as a forex pip
    "min_sl_pips": 3.0,             # skip the trade when the stop loss would be smaller than this (0 = no limit)
    "min_rr": 1.0,                  # the reward is never smaller than this (1:x): when the present low / high gives less, the take profit is extended to this
    "bos_supersede_pct": 50,        # a NEWER break of structure replaces the waiting order when its leg is at least this % as wide as the older one (smaller ones are just the move continuing)
    "min_leg_bars": 8,              # a MAJOR break of structure must also be WIDE: at least this many candles from the start of its leg to the breaking candle
    "bos_max_age_bars": 120,        # ... and recent: the breaking candle must be no older than this many candles (0 = no limit)
    "reentry_min_rr": 1.5,          # a re-entry keeps the original target, so it only needs this reward (1:x)
    "bot_max_rr": 3.0,              # ... and never bigger than this: when the present low / high gives more, the take profit is cut to this
    "bos_body_pct": 50.0,           # the candle that breaks the structure must have a body of at least this % of its range; a weaker (wick-like) break needs one more candle closing beyond, and then pays exactly the minimum reward
    "crypto_pattern": True,         # crypto/futures only: with no liquidity, enter a tap of an order block only if the last 2-3 breaks in the trend all did exactly that
    "crypto_pattern_legs": 3, "crypto_pattern_min": 2,
    "liq_inside_ob_pct": 50.0,      # an ORDER BLOCK or WICK (liquidity or entry) only needs this % inside the break of structure (it often straddles the broken level)
    "liq_inside_pct": 70.0,         # the liquidity and the entry block must be at least this % inside the break of structure
    "liq_overlap_pct": 50.0,        # the liquidity may touch the entry block; only when it has covered this % of the block (or more) is the block skipped
    "reentry": True,                # after a stop loss with the protected level still unbroken, look for ONE re-entry (FVG / new order block / breaker / 5m MSS)
    "reentry_bars": 48,             # how many candles after the stop loss the robot keeps looking for that re-entry
    "snapshot_days": 2.0,            # how many days the robot's trade chart pictures are kept in Admin (0 = do not make them)
    "max_trades_per_day": 3,        # per account, per UTC day (0 = no limit)
    "paper_currency": "USD",         # label shown next to paper balances
    "paper_min_balance": 100.0,      # smallest starting balance a person may choose; a paper account below this is "finished" and may be reset
    "paper_default_balance": 10000.0,   # balance suggested for new paper accounts and given back by a reset
    "max_rr": 4.0,                   # on A+ setups a person may ask for more than 1:2 reward, never more than this (and never beyond the Daily level that would form a new BOS)
    "trend_only": True,              # ON = trade only in the direction of the Daily present leg; the other way only on the 2nd break of structure (30m / 1h)
    "htf_filter": False,             # ON = only A+ setups (same direction as the Daily and Weekly leg, Monthly not against); OFF = every setup is taken, A+ ones are just labelled
    "max_risk_pct": 2.0,            # highest risk per trade any user may choose for their own account (never above 2)
    # --- access
    "require_signup": False,        # ON = nobody can add an account without signing up / logging in first
    "gmail_only": False, "max_users": 0, "max_accounts_per_user": 5, "block_unlinked": False,
    # --- monetisation (paper trading = free forever; connected accounts = paid monthly once paid_mode is ON)
    "paid_mode": False, "trial_days": 0, "plan_days": 30, "price_text": "", "price_usd": 20.0, "price_ngn": 0.0, "payment_link": "", "pay_note": "",
    "pay_ng": "", "pay_usdt": "", "pay_usdt_net": "TRC20 (Tron)",   # bank / OPay details for Nigerians, USDT wallet + network for crypto payers
    # --- scale
    "ea_poll_seconds": 30, "max_inflight": 200,
    "mode": "live",                                               # no longer used: paper accounts are always simulated, connected accounts are always live
    "filters": {"liquidity": [], "session": [], "side": [], "bos_run": [], "tf": []},
    "strategy_text": "Trade only in the direction of the Daily present leg (bullish = buys only, bearish = sells only); the other way only on the second break of structure on the 30m / 1h chart. "
                     "Only a major break of structure counts (a big, clear leg). Liquidity = the last order block (or, if there is none, the last pullback wick) right before the breakout; it must be at least 70% inside the break of structure. "
                     "Entry = the nearest valid order block further back than the liquidity: above it for sells, below it for buys, also at least 70% inside the break. The liquidity may touch the block a little; a block is skipped only when the liquidity or price has covered 50% or more of it, and then the next block is used. "
                     "SL = just beyond the entry order block (2-3 pips); the protected high/low only says the idea is dead if price closes beyond it. Skip if the stop is over 40 pips. TP = the present low for sells / present high for buys (the level that would form a new break of structure), with the reward kept between 1:1 and 1:3 (extended to 1:1 if smaller, cut to 1:3 if bigger). A break only counts when a candle CLOSES beyond the level; if that candle is mostly a wick, one more candle must close beyond too, and then the trade pays only 1:1. The major break must be wide as well as tall (the widest valid break of the current move). When the 30m and 1h both have a setup the 1h is traded, entering and placing the stop from the 30m order block. "
                     "If the stop is hit but the protected level is NOT broken, the robot looks for ONE re-entry in the same zone (new FVG, new order block, breaker block, or a 5 minute market structure shift) with a new stop at that new block. If the protected level breaks, the idea is dead.",
}
BRAND_KEYS = ("site_name", "tagline", "lead")
ACCESS_KEYS = ("require_signup", "gmail_only", "max_users", "max_accounts_per_user", "block_unlinked", "paid_mode", "trial_days", "plan_days", "price_text", "price_usd", "price_ngn", "payment_link", "pay_note", "pay_ng", "pay_usdt", "pay_usdt_net")
SCALE_KEYS = ("ea_poll_seconds", "max_inflight")
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
            ctx = None
            if not local:
                ctx = ssl.create_default_context()
                if os.environ.get("DB_SSL_VERIFY", "1") == "0":      # Supabase: still encrypted, but the server certificate is not checked against public authorities
                    ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
            return pg8000.native.Connection(user=urllib.parse.unquote(u.username or ""), password=urllib.parse.unquote(u.password or ""), host=u.hostname,
                                            port=u.port or 5432, database=(u.path or "/").lstrip("/"), ssl_context=ctx, timeout=20)
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

    def _retry(self, fn):
        """PostgreSQL closes idle connections (Render does this). A pooled connection may be dead: it is discarded by conn(), so just try again."""
        attempts = 3 if self.pg else 1
        for i in range(attempts):
            try: return fn()
            except Exception:
                if i == attempts - 1: raise

    def q(self, sql, **p):
        def go():
            with self.conn() as c: return self._exec(c, sql, p)[0]
        return self._retry(go)
    def one(self, sql, **p):
        r = self.q(sql, **p); return r[0] if r else None
    def x(self, sql, **p):
        def go():
            with self.conn() as c: return self._exec(c, sql, p)[1]
        return self._retry(go)

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
        "CREATE TABLE IF NOT EXISTS payments (id TEXT PRIMARY KEY, account_id BIGINT NOT NULL, user_id BIGINT, method TEXT NOT NULL, ref TEXT NOT NULL, created TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending')",
        "CREATE INDEX IF NOT EXISTS ix_pay_status ON payments(status, created)",
        "CREATE TABLE IF NOT EXISTS snaps (skey TEXT PRIMARY KEY, symbol TEXT NOT NULL, tf TEXT NOT NULL DEFAULT '', sig_key TEXT NOT NULL DEFAULT '', created TEXT NOT NULL, updated TEXT NOT NULL, outcome TEXT NOT NULL DEFAULT '', side TEXT NOT NULL DEFAULT '', svg TEXT NOT NULL)",
        "CREATE INDEX IF NOT EXISTS ix_snaps_upd ON snaps(updated)",
    ]
    for s_ in stmts: db.x(s_)
    try: db.x("ALTER TABLE accounts ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "paused INTEGER NOT NULL DEFAULT 0")     # start/stop switch; old databases get it here
    except Exception: pass                                           # already there
    try: db.x("ALTER TABLE accounts ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "risk_pct DOUBLE PRECISION")                  # each user's own risk per trade (empty = use the owner's default)
    except Exception: pass                                           # already there
    try:
        db.x("UPDATE accounts SET tier='real', tier_src='connected' WHERE platform != 'PAPER' AND tier != 'real'")      # no demo/real split any more: connected = paid
        db.x("UPDATE accounts SET tier='demo' WHERE platform = 'PAPER' AND tier != 'demo'")
    except Exception: pass
    try:                                                           # one time: installs still on the old two-coin list get the wider pair list (afterwards the owner edits it freely in admin)
        if not kv_get("mig_symbols_v2"):
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            if st_.get("symbols") == ["BTCUSDT", "ETHUSDT"]: st_["symbols"] = DEFAULTS["symbols"]; kv_set("settings", json.dumps(st_))
            kv_set("mig_symbols_v2", "1")
    except Exception: pass
    try:                                                           # one time: the strategy settings follow the owner's method (one clear BOS is enough, any bias is traded, A+ only changes the reward)
        if not kv_get("mig_strategy_v3"):
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            if st_.get("min_bos") == 2: st_["min_bos"] = 1
            if st_.get("min_leg_atr") == 3.0: st_["min_leg_atr"] = 2.0
            if st_.get("htf_filter") is True: st_["htf_filter"] = False
            st_.pop("strategy_text", None)
            kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v3", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v4"):                             # one time: show the new strategy text (trend direction rule)
            st_ = json.loads(kv_get("settings", "{}") or "{}"); st_.pop("strategy_text", None); kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v4", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v5"):                             # one time: only MAJOR breaks of structure (bigger swings, bigger legs) + show the new strategy text
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            st_["swing_n"] = 4; st_["min_leg_atr"] = 3.0; st_.pop("strategy_text", None)
            kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v5", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v6"):                             # one time: take profit at the broken level (minimum 1:2), liquidity can be an order block or a wick, leg size 3
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            if st_.get("min_leg_atr") == 4.0: st_["min_leg_atr"] = 3.0
            st_.pop("strategy_text", None); kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v6", "1")
    except Exception: pass
    try:
        if not kv_get("mig_trial_off_v1"):                            # one time: no free trial for connected accounts unless the owner sets days in Admin
            st_ = json.loads(kv_get("settings", "{}") or "{}"); st_["trial_days"] = 0; kv_set("settings", json.dumps(st_)); kv_set("mig_trial_off_v1", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v7"):                             # one time: leg size 2.5, take profit at the present low / high, new strategy text
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            st_["min_leg_atr"] = 2.5; st_.pop("strategy_text", None); kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v7", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v9"):                             # one time: the new rule set (10% mitigation, new strategy text)
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            st_["max_mitigation_pct"] = 10.0; st_.pop("strategy_text", None); kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v9", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v10"):                            # one time: block skipped only at 50% covered, reward 1:2 to 1:3, new strategy text
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            st_.update({"max_mitigation_pct": 50.0, "min_rr": 2.0, "bot_max_rr": 3.0, "bos_body_pct": 50.0, "liq_overlap_pct": 50.0}); st_.pop("ob_gap_pct", None); st_.pop("strategy_text", None)
            kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v10", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v11"):                            # one time: smaller breaks of structure count (NZDUSD H1 example: leg about 1.2x a normal candle, stop 3.5 pips)
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            st_["min_leg_atr"] = 1.2; st_["min_sl_pips"] = 3.0; kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v11", "1")
    except Exception: pass
    try:
        if not kv_get("mig_strategy_v12"):                            # one time: reward 1:1 to 1:3, the major break must also be wide
            st_ = json.loads(kv_get("settings", "{}") or "{}")
            st_.update({"min_rr": 1.0, "bot_max_rr": 3.0, "min_leg_bars": 8, "bos_max_age_bars": 120}); st_.pop("strategy_text", None)
            kv_set("settings", json.dumps(st_)); kv_set("mig_strategy_v12", "1")
    except Exception: pass
    try: db.x("ALTER TABLE accounts ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "resets INTEGER NOT NULL DEFAULT 0")          # how many times a paper account was reset (shown for honesty)
    except Exception: pass
    try: db.x("ALTER TABLE accounts ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "rr_aplus DOUBLE PRECISION")                 # reward the person asks for on A+ setups (empty = the normal 1:2)
    except Exception: pass
    for col_, typ_ in (("name", "TEXT"), ("amount", "TEXT"), ("email", "TEXT"), ("seen", "INTEGER NOT NULL DEFAULT 1")):                  # who paid, how much, and whether the person has been told the result
        try: db.x("ALTER TABLE payments ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + col_ + " " + typ_)
        except Exception: pass
    try: db.x("ALTER TABLE accounts ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "pairs TEXT")                                       # the pairs this account's robot trades (comma separated)
    except Exception: pass
    try: db.x("UPDATE accounts SET pairs=:p WHERE platform='PAPER' AND (pairs IS NULL OR pairs='')", p=load_settings(fresh=True)["symbols"][0])      # old paper accounts: one pair, never all at once
    except Exception: pass
    try: db.x("ALTER TABLE accounts ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "cur TEXT")
    except Exception: pass
    try: db.x("ALTER TABLE accounts ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "fx DOUBLE PRECISION")                          # exchange rate locked at start / reset
    except Exception: pass
    try: db.x("ALTER TABLE orders ADD COLUMN " + ("IF NOT EXISTS " if db.pg else "") + "risk_amt DOUBLE PRECISION NOT NULL DEFAULT 0")   # money risked on this order (practice or real)
    except Exception: pass                                           # already there

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
    """Binance futures first. fapi.binance.com answers HTTP 451 to US servers, so fall back to Binance's public market-data host (spot prices, very close)."""
    last = None
    for url in (f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}",
                f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"):
        try:
            with urllib.request.urlopen(url, timeout=10) as r: rows = json.loads(r.read())[:-1]   # drop forming candle
            return [{"t": x[0], "o": float(x[1]), "h": float(x[2]), "l": float(x[3]), "c": float(x[4])} for x in rows]
        except Exception as e: last = e
    raise last

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

def mitigation(c, k, bull, frm=None):
    """How deep price has traded into order block c[k] (since the candle index frm, default: since it formed): 0.0 = never touched, 1.0 = completely filled."""
    hi, lo = c[k]["h"], c[k]["l"]
    after = c[max(k + 1, frm or 0):]
    if hi <= lo or not after: return 0.0
    depth = (hi - min(x["l"] for x in after)) if bull else (max(x["h"] for x in after) - lo)
    return max(0.0, min(1.0, depth / (hi - lo)))

def pip_size(sym, price):
    """Size of ONE pip. Forex 0.0001 (JPY pairs 0.01). Gold, silver, oil and crypto have no real pips, so a 'pip' there is the same fraction of the price as a forex pip (about 0.009%).
    That keeps the 2-3 pip buffer and the 40 pip limit fair on every market."""
    core = re.sub(r"[a-z._#]+$", "", sym or "")
    if sym and re.fullmatch(r"[A-Z]{6}", core) and core[:3] not in ("XAU", "XAG"): return 0.01 if core.endswith("JPY") else 0.0001
    if not sym: return 0.0001 if price < 20 else 0.01 if price < 400 else price * 0.00009
    return price * 0.00009

def inside_frac(lo, hi, zlo, zhi):
    """How much of the range lo..hi lies inside the zone zlo..zhi (0.0 to 1.0)."""
    if hi <= lo: return 1.0 if zlo <= lo <= zhi else 0.0
    return max(0.0, min(hi, zhi) - max(lo, zlo)) / (hi - lo)

def mit_since_leave(c, k, bull):
    """How deep price came BACK into order block c[k] after it had first moved away from it (the candles that simply move away from the block do not count)."""
    ob = c[k]
    L = next((j for j in range(k + 1, len(c)) if ((c[j]["c"] > ob["h"]) if bull else (c[j]["c"] < ob["l"]))), None)
    return 0.0 if L is None else mitigation(c, k, bull, L + 1)

def bos_leg(c, e, n, floor=0):
    """The break of structure as a BOX. The candle that closed beyond the level is c[e['i']]. Going back, price was CONTAINED (never traded beyond that close) since the candle after the last time it was:
    that stretch is the structure. Its extreme (highest high for buys / lowest low for sells) is the BROKEN LEVEL, the opposite extreme is the PROTECTED level, and the number of candles is how WIDE the break is."""
    bull = e["dir"] == "bull"
    bi_ = e["i"]; cl = c[bi_]["c"]
    a = bi_ - 1
    while a >= 0 and ((c[a]["h"] < cl) if bull else (c[a]["l"] > cl)): a -= 1
    st = min(max(0, a + 1, floor), max(0, bi_ - 1))                                              # a break that continues an earlier one in the same move starts after that earlier break
    span = range(st, bi_)
    brk_i = max(span, key=lambda k: c[k]["h"]) if bull else min(span, key=lambda k: c[k]["l"])      # leftmost extreme
    brk = c[brk_i]["h"] if bull else c[brk_i]["l"]
    lg = range(st, bi_ + 1)
    og = min(lg, key=lambda k: c[k]["l"]) if bull else max(lg, key=lambda k: c[k]["h"])
    pr = c[og]["l"] if bull else c[og]["h"]
    rg = [x["h"] - x["l"] for x in c[max(0, st - 14):st]]
    lh = (max(c[k]["h"] for k in lg) - pr) if bull else (pr - min(c[k]["l"] for k in lg))
    av = (sum(rg) / len(rg)) if len(rg) >= 5 else 0.0
    return {"e": e, "start": st, "origin": og, "prot": pr, "leg_h": lh, "avg": av, "width": bi_ - st, "brk": brk, "kb": brk_i}

def _is_crypto(sym): return bool(sym) and sym.endswith("USDT")

def crypto_pattern(c, s, n, bull, bos_i):
    """CRYPTO / futures only. Used when the normal picture (liquidity + an entry block further back) is NOT there.
    Looks at the last 2-3 earlier breaks of structure in the SAME direction. Each one must show the same habit: no liquidity sweep, price just pulled back and TAPPED an order block inside that break's box,
    then carried on in the trend. Only when every one of them (at least 2) did that may the robot enter the same way. -> (ok, message)"""
    nmax, nmin = int(s.get("crypto_pattern_legs") or 3), int(s.get("crypto_pattern_min") or 2)
    evs = [e for e in bos_events(c, n) if e["dir"] == ("bull" if bull else "bear") and e["i"] < bos_i]
    allev = [e for e in bos_events(c, n) if e["dir"] == ("bull" if bull else "bear")]
    prev = evs[-nmax:]
    if len(prev) < nmin: return False, f"only {len(prev)} earlier break(s) of structure in this direction, need {nmin} to learn this pair's habit"
    done = 0
    for e in prev:
        ix = allev.index(e); nxt = allev[ix + 1]["i"] if ix + 1 < len(allev) else bos_i
        fl = (allev[ix - 1]["i"] + 1) if ix > 0 else 0
        L2 = bos_leg(c, e, n, floor=fl)
        zlo, zhi = (L2["prot"], L2["brk"]) if bull else (L2["brk"], L2["prot"])
        isob = (lambda x: x["c"] < x["o"]) if bull else (lambda x: x["c"] > x["o"])
        obs2 = [k for k in range(L2["origin"], e["i"]) if isob(c[k]) and inside_frac(c[k]["l"], c[k]["h"], zlo, zhi) >= 0.5 - 1e-9]
        win = c[e["i"] + 1:nxt]
        if not obs2 or not win: return False, "an earlier break of structure had no order block to tap or no continuation"
        pb = min(x["l"] for x in win) if bull else max(x["h"] for x in win)                                 # how deep the pullback went
        tapped = [k for k in obs2 if ((pb <= c[k]["h"] and pb >= c[k]["l"] - 0.25 * (c[k]["h"] - c[k]["l"])) if bull else (pb >= c[k]["l"] and pb <= c[k]["h"] + 0.25 * (c[k]["h"] - c[k]["l"])))]
        if not tapped: return False, "an earlier break of structure did not follow the 'tap an order block and continue' habit"
        done += 1
    return True, f"the last {done} breaks of structure in this direction all did the same: no liquidity sweep, just a tap of an order block and then the trend continued"

def find_setup(c, s, tf=None, why=None, sym=None, tp_fixed=None):
    """The strategy: a break of structure (BOS) -> liquidity = the last order block before the breakout -> entry = the order block BELOW that liquidity (bullish; above it when bearish) -> SL at the protected low/high.
    Returns the setup, or None. If a list is passed as `why`, the reason for saying no is appended to it."""
    def no(msg):
        if why is not None: why.append(msg)
        return None
    n, min_bos = s["swing_n"], max(1, int(s["min_bos"]))
    ev = bos_events(c, n)
    if not ev: return no("no break of structure on the chart yet")
    if len(ev) < min_bos: return no("not enough breaks of structure yet")
    last = ev[-1]
    if any(e["dir"] != last["dir"] for e in ev[-min_bos:]): return no("the last breaks of structure point in different directions")      # no clear trend
    bull = last["dir"] == "bull"
    runev = []
    for e in reversed(ev):
        if e["dir"] != last["dir"]: break
        runev.append(e)
    runev.reverse(); run = len(runev)
    # MAJOR break of structure: size is not only the height of the leg but also how WIDE it is (how many candles it took). Among the breaks of the current run (that are recent enough),
    # every one that is tall enough and wide enough is a candidate and the WIDEST one is the major break; the later breaks are just the move continuing.
    leg_of = lambda e: bos_leg(c, e, n, floor=(runev[runev.index(e) - 1]["i"] + 1) if runev.index(e) > 0 else 0)
    max_age, min_w, why_c = int(s.get("bos_max_age_bars") or 0), int(s.get("min_leg_bars") or 0), "the last break of structure is too small to trust"
    cands = []
    for e in runev:
        if max_age and len(c) - 1 - e["i"] > max_age: why_c = "the major break of structure is too old"; continue
        L = leg_of(e)
        if s.get("min_leg_atr", 0) and L["avg"] and L["leg_h"] < s["min_leg_atr"] * L["avg"]: why_c = "the break of structure is too small to trust"; continue      # not tall enough
        if min_w and L["width"] < min_w: why_c = f"the break of structure is too narrow (only {L['width']} candles wide, needs {min_w})"; continue                     # not wide enough
        cands.append(L)
    if not cands: return no(why_c)
    reasons = []
    wmax = max(x["width"] for x in cands); sup = float(s.get("bos_supersede_pct", 50)) / 100
    order = sorted([x for x in cands if x["width"] >= sup * wmax], key=lambda x: x["e"]["i"], reverse=True) + sorted([x for x in cands if x["width"] < sup * wmax], key=lambda x: x["width"], reverse=True)
    for L in order:                                                                                                  # the NEWEST break that is big enough (a new break replaces the old idea); if it gives no trade, the next one is tried
        w_ = []
        r_ = _setup_core(c, s, tf, w_.append, sym, tp_fixed, n, bull, run, L)
        if r_:
            ok_f, why_f = setup_fresh(r_, c, s, strict=True)                                                           # an idea that a newer break already replaced is not offered again
            if ok_f or tp_fixed is not None: return r_
            reasons.append(why_f); continue
        reasons.append(w_[0] if w_ else why_c)
    return no(reasons[0] if reasons else why_c)

def _setup_core(c, s, tf, no_msg, sym, tp_fixed, n, bull, run, L):
    def no(msg):
        no_msg(msg); return None
    last = L["e"]; start, origin, protected, leg_h, avg_rng = L["start"], L["origin"], L["prot"], L["leg_h"], L["avg"]
    bos_i, broken_i, broken_p = last["i"], L["kb"], L["brk"]
    leg_atr = (leg_h / avg_rng) if avg_rng else 0.0

    bc = c[bos_i]; brng = bc["h"] - bc["l"]
    strong = brng > 0 and abs(bc["c"] - bc["o"]) / brng * 100 >= float(s.get("bos_body_pct", 50)) - 1e-9          # a real body close, not just a wick poking through
    if not strong and not any(((x["c"] > broken_p) if bull else (x["c"] < broken_p)) for x in c[bos_i + 1:]):
        return no("the break of structure candle is mostly a wick; waiting for one more candle to close beyond the level")
    is_ob = (lambda x: x["c"] < x["o"]) if bull else (lambda x: x["c"] > x["o"])
    after, price = c[bos_i + 1:], c[-1]["c"]
    if any(((x["c"] < protected) if bull else (x["c"] > protected)) for x in after): return no("the protected " + ("low" if bull else "high") + " was broken, so this idea is dead")      # final invalidation
    # The BOS zone = from the protected level to the level the break went through. Liquidity and the entry block must be (at least 70%) inside it.
    zlo, zhi = (protected, broken_p) if bull else (broken_p, protected)
    inpct = float(s.get("liq_inside_pct", 70)) / 100
    inside = lambda k: inside_frac(c[k]["l"], c[k]["h"], zlo, zhi) >= inpct - 1e-9
    # LIQUIDITY = the LAST order block (opposing candle) in the BOS leg right before the breakout; only if the last pullback WICK is used instead (no usable order block) it must also be inside the zone.
    obs = [k for k in range(origin, bos_i) if is_ob(c[k])]                     # real opposing candles inside the leg only
    tol_ = lambda k: 0.1 * (c[k]["h"] - c[k]["l"])                              # wicks at about the same level count together: a small tolerance on the neighbours
    if bull: pivs = [k for k in range(origin + 1, bos_i) if c[k]["l"] <= c[k - 1]["l"] + tol_(k) and c[k]["l"] <= c[k + 1]["l"] + tol_(k)]       # pullback lows (wicks)
    else:    pivs = [k for k in range(origin + 1, bos_i) if c[k]["h"] >= c[k - 1]["h"] - tol_(k) and c[k]["h"] >= c[k + 1]["h"] - tol_(k)]       # pullback highs (wicks)
    obpct = min(inpct, float(s.get("liq_inside_ob_pct", 50)) / 100)
    inside_ob = lambda k: inside_frac(c[k]["l"], c[k]["h"], zlo, zhi) >= obpct - 1e-9
    ob_in, pv_in = [k for k in obs if inside_ob(k)], [k for k in pivs if inside_ob(k)]      # blocks AND wicks only need about half inside the box; the latest one that is inside counts (the very last wick may sit outside)
    options = ([(ob_in[-1], "order_block")] if ob_in else []) + ([(pv_in[-1], "wick")] if pv_in else [])
    options.sort(key=lambda x: -x[0])                                           # the liquidity is whatever sits CLOSEST to the break (a pullback wick after the last block beats that block; the block then becomes the entry)
    fail_msg = None
    if not options: fail_msg = f"no order block or wick that is at least {round(inpct * 100)}% inside the break of structure, so there is no liquidity to trade from"
    # ENTRY = the nearest valid ORDER BLOCK further back than the liquidity (above it for sells, below it for buys), inside the zone.
    maxm = float(s.get("max_mitigation_pct", 50)) / 100                         # price came back into the block by more than this = block is used up
    ovl_max = float(s.get("liq_overlap_pct", 50)) / 100                         # the liquidity may touch the block; only when it covered this part of it (or more) is the block skipped
    pip = pip_size(sym, c[-1]["c"]); buf = float(s.get("sl_buffer_pips") or 0) * pip
    chosen, why_last = None, "no order block found"
    for lq, kd in (options if not fail_msg else []):
        if (c[lq]["h"] > c[bos_i]["h"]) if bull else (c[lq]["l"] < c[bos_i]["l"]): why_last = "the break did not clear the liquidity"; continue      # the BOS must reach at least the liquidity line
        cands = [k for k in reversed(obs) if k < lq and inside_ob(k) and (c[k]["l"] < c[lq]["l"] if bull else c[k]["h"] > c[lq]["h"])]       # nearest block to the liquidity first
        if not cands: why_last = "no order block " + ("below" if bull else "above") + " the liquidity inside the break of structure to enter from"; continue
        lz = c[lq]                                                              # the liquidity zone
        for k in cands:
            o_ = c[k]; oh = o_["h"] - o_["l"]
            cover = ((o_["h"] - lz["l"]) if bull else (lz["h"] - o_["l"])) / oh if oh > 0 else (1.0 if (lz["l"] <= o_["h"] if bull else lz["h"] >= o_["l"]) else 0.0)     # how much of the block the liquidity zone covers (0 = clear of it)
            if cover >= ovl_max - 1e-9: why_last = f"the liquidity covers {round(min(cover, 1.0) * 100)}% of that order block, so it is skipped"; continue
            m_ = mit_since_leave(c, k, bull)
            if m_ > maxm + 1e-9: why_last = "price already came back into the entry order block"; continue
            chosen = (lq, kd, k, m_); break
        if chosen: break
    if not chosen:
        fail_msg = fail_msg or why_last
        if not (_is_crypto(sym) and s.get("crypto_pattern", True)): return no(fail_msg)
        okp, pmsg = crypto_pattern(c, s, n, bull, bos_i)
        if not okp: return no(fail_msg + "; crypto habit check: " + pmsg)
        pc = [(k, mit_since_leave(c, k, bull)) for k in reversed(obs) if inside_ob(k)]
        pc = [x for x in pc if x[1] <= maxm + 1e-9]
        if not pc: return no(fail_msg + "; no unused order block inside the break of structure to enter from")
        chosen = (None, "pattern", pc[0][0], pc[0][1])
    liq, kind, ent, mit = chosen; ob = c[ent]
    if bull: entry, sl = (ob["h"] if s["ob_entry"] == "top" else (ob["h"] + ob["l"]) / 2), ob["l"] - buf         # STOP LOSS = just beyond the entry order block (the protected level is only the "idea is dead" line)
    else:    entry, sl = (ob["l"] if s["ob_entry"] == "top" else (ob["h"] + ob["l"]) / 2), ob["h"] + buf
    risk = (entry - sl) if bull else (sl - entry)
    if risk <= 0: return no("the entry is not on the right side of the stop loss")
    rp = risk / pip
    if float(s.get("max_sl_pips") or 0) and rp > float(s["max_sl_pips"]): return no(f"the stop loss would be {rp:.0f} pips, more than the {float(s['max_sl_pips']):g} pip limit")
    if float(s.get("min_sl_pips") or 0) and rp < float(s["min_sl_pips"]): return no(f"the stop loss would be only {rp:.1f} pips, too tight to trade")
    # TAKE PROFIT = the next swing point in the trade's direction: the PRESENT low (sells) / PRESENT high (buys), the most extreme candle of the move so far.
    # Price has not broken it yet: when it does, that is the new break of structure in the trend's direction. Only taken when the reward is at least min_rr (default 1:1.5).
    ext = tp_fixed if tp_fixed is not None else (max(c[k]["h"] for k in range(start, len(c))) if bull else min(c[k]["l"] for k in range(start, len(c))))
    room = (ext - entry) if bull else (entry - ext)
    min_rr, max_rr = float(s.get("min_rr") or 1.0), max(float(s.get("min_rr") or 1.0), float(s.get("bot_max_rr") or 3.0))
    if tp_fixed is not None:                                                    # a re-entry keeps the original target; it only needs its own (smaller) minimum reward
        re_min = float(s.get("reentry_min_rr") or 1.5)
        if room <= 0 or room / risk < re_min - 1e-9: return no(f"the target gives only 1:{max(0.0, room / risk):.1f}, less than the minimum 1:{re_min:g}")
        tp, rr = ext, round(room / risk, 2)
    else:
        raw = max(0.0, room / risk)
        rrf = min_rr if not strong else min(max_rr, max(min_rr, raw))                  # weak break: exactly the minimum. Strong break: the present low/high, kept between the minimum and the maximum
        tp = ext if (strong and min_rr <= raw <= max_rr) else (entry + rrf * risk if bull else entry - rrf * risk)
        rr = round(rrf, 2)

    if bull and (any(x["l"] <= entry for x in after) or price <= entry): return no("price already tapped the entry, so that setup is gone")   # already tapped / stale
    if not bull and (any(x["h"] >= entry for x in after) or price >= entry): return no("price already tapped the entry, so that setup is gone")
    h = datetime.datetime.utcnow().hour
    sess = "Asia" if h < 7 else "London" if h < 13 else "NY" if h < 21 else "Late"
    return {"brk": broken_p, "brk_t": c[broken_i]["t"], "found_t": c[-1]["t"], "leg_w": L["width"], "side": "BUY" if bull else "SELL", "entry": entry, "sl": sl, "tp": tp, "rr": rr, "liquidity": kind,
            "session": sess, "bos_run": min(run, 5), "key": (f"{tf}-" if tf and tf != s["timeframe"] else "") + f"{last['dir']}-{c[bos_i]['t']}", "tf": tf or s["timeframe"], "mitigated_pct": round(100 * mit), "last_t": c[-1]["t"],
            "bos_t": c[bos_i]["t"], "ob_hi": ob["h"], "ob_lo": ob["l"], "ob_t": ob["t"], "liq_t": (c[liq]["t"] if liq is not None else None), "leg_t": c[start]["t"], "prot": protected, "ext": ext, "strong": strong, "swing_n": n, "sl_pips": round(rp, 1), "leg_atr": round(leg_atr, 2),
            "score": round(min(leg_atr, 12) + 1.5 * min(run, 5) - mit * 4 + (0.5 if tf == "1h" else 0), 2)}

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

def _meta_price(acc, sym):
    p = meta(acc, "GET", f"/symbols/{sym}/current-price"); return (float(p["bid"]) + float(p["ask"])) / 2

def pip_money_rate(acc, sym, core, entry):
    """Value (in the account's currency) of ONE unit of the pair's QUOTE currency. Lets any pair (EURUSD, EURJPY, GBPJPY, EURGBP, USDJPY, gold...) be sized on any account currency."""
    quote = core[-3:]; suffix = sym[len(core):]
    try: acur = str(meta(acc, "GET", "/account-information").get("currency") or "USD").upper()
    except Exception: acur = "USD"
    if quote == acur: return 1.0
    try: return _meta_price(acc, quote + acur + suffix)                 # e.g. JPYUSD does not exist, but GBPUSD / EURUSD do
    except Exception: pass
    try: return 1.0 / _meta_price(acc, acur + quote + suffix)           # e.g. USDJPY -> 1 / USDJPY
    except Exception: pass
    if quote == "USD" and acur != "USD": raise RuntimeError(f"cannot convert USD to {acur} for lot sizing")
    raise RuntimeError(f"cannot find a price to convert {quote} to {acur} for lot sizing")

def live_meta(acc, o):
    sym = o["symbol"]; core = re.sub(r"[a-z._#]+$", "", sym)                  # EURUSDm -> EURUSD
    try: sp = meta(acc, "GET", f"/symbols/{sym}/specification")
    except Exception: sp = {}
    cs = float(sp.get("contractSize") or (100 if core.startswith("XAU") else 100000))
    step, mn, digits = float(sp.get("volumeStep") or 0.01), float(sp.get("minVolume") or 0.01), int(sp.get("digits") or 5)
    dist = abs(o["entry"] - o["sl"])
    per_lot = dist * cs * pip_money_rate(acc, sym, core, o["entry"])          # money lost per 1 lot if the stop is hit, in the account's own currency
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
# ONE SYSTEM: there is no demo/real split any more.
#   PAPER account      free for everybody (simulated trades with practice money, no connection at all).
#   Any connected account (exchange, MT4/MT5 or EA)   is paid once the owner switches "Charge for connected accounts" on.
# Internally the old column "tier" is kept: 'demo' = paper (free), 'real' = connected (paid).
# ======================================================================================================================
def csv_list(x): return [p.strip().lower() for p in str(x or "").split(",") if p.strip()]

def _att(a):
    v = a.get("attest")
    if isinstance(v, dict): return v
    try: return json.loads(v or "{}")
    except Exception: return {}

def auto_tier(a, s):
    if a["platform"] == "PAPER": return "demo", "paper", "Paper account: simulated trades with practice money (free)"
    return "real", "connected", "Connected account"

def tier_cols(a, s):
    t, src, note = auto_tier(a, s); return {"tier": t, "tier_src": src, "tier_note": note[:200]}

def parse_date(x):
    try: return datetime.date.fromisoformat(str(x)[:10])
    except Exception: return None

def sub_state(a, s, u=None):
    """(allowed to trade?, text). Paper trading is free forever. A connected account needs a plan once the owner switches 'Charge for connected accounts' on."""
    if a.get("blocked") or (u and u.get("blocked")): return False, "Paused by the owner"
    if s.get("block_unlinked") and not a.get("user_id"): return False, "Sign up and link this account to keep using the robot"
    if a.get("tier") == "demo": return True, "Paper trading: free forever"
    if not s.get("paid_mode"): return True, "Active (free for now)"
    t = today(); pu = parse_date(a.get("paid_until"))
    if pu and pu >= t: return True, "Free access" if pu.year >= 2099 else f"Active until {pu}"
    te = parse_date((u or {}).get("trial_end") or a.get("trial_end"))
    if te and te >= t: return True, f"Free trial until {te}"
    return False, "Connected account: monthly plan needed"

def elig_sql(s):
    """Same rules as sub_state, as SQL, so thousands of accounts are selected with ONE statement."""
    sql = "a.blocked = 0 AND a.paused = 0 AND COALESCE(u.blocked, 0) = 0 AND (a.tier = 'demo' OR :free = 1 OR a.paid_until >= :today OR COALESCE(u.trial_end, a.trial_end) >= :today)"
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
                last_t=int(o.get("last_t") or 0), bars=int(o.get("bars") or 0), ext_id=str(o["ext_id"]) if o.get("ext_id") else None, risk_amt=float(o.get("risk_amount") or 0), data=json.dumps(o, default=str))

def insert_order(o, t=None):
    (t or db).x("INSERT INTO orders (id, account_id, platform, symbol, tf, sig_key, created, outcome, status, ack, r, last_t, bars, ext_id, risk_amt, data) VALUES "
                "(:id, :account_id, :platform, :symbol, :tf, :sig_key, :created, :outcome, :status, :ack, :r, :last_t, :bars, :ext_id, :risk_amt, :data)", **order_row(o))

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
SETUP_KEYS = ("swing_n", "min_bos", "rr", "ob_entry", "max_mitigation_pct", "min_leg_atr", "timeframe", "sl_buffer_pips", "max_sl_pips", "min_sl_pips", "min_rr", "liq_inside_pct", "liq_inside_ob_pct", "liq_overlap_pct", "crypto_pattern", "crypto_pattern_legs", "crypto_pattern_min", "bot_max_rr", "bos_body_pct", "reentry_min_rr", "min_leg_bars", "bos_max_age_bars", "bos_supersede_pct")
def cached_setup(rows, s, tf, why=None, sym=None):
    h = hashlib.sha1((str(sym) + tf + json.dumps([s[k] for k in SETUP_KEYS]) + ";".join(f"{x['t']},{x['o']},{x['h']},{x['l']},{x['c']}" for x in rows)).encode()).hexdigest()
    with _SETUP_LOCK:
        if h in _SETUP:
            _SETUP.move_to_end(h); v, w_ = _SETUP[h]
            if why is not None: why.extend(w_)
            return dict(v) if v else None
    w_ = []; sig = find_setup(rows, s, tf, w_, sym)
    if why is not None: why.extend(w_)
    with _SETUP_LOCK:
        _SETUP[h] = (sig, w_)
        while len(_SETUP) > 3000: _SETUP.popitem(last=False)
    return dict(sig) if sig else None

def risk_cap(s):
    """Highest risk per trade anybody may use. The owner can lower it, but never raise it above 2 percent."""
    try: return max(0.1, min(2.0, float(s.get("max_risk_pct", 2.0))))
    except (TypeError, ValueError): return 2.0

def effective_risk(acc, s):
    """Risk per trade (percent) for one account: the user's own choice if set, else the owner's default, never above the cap."""
    cap = risk_cap(s)
    try: r = float(acc.get("risk_pct")) if acc.get("risk_pct") is not None else float(s.get("risk_pct", 1.0))
    except (TypeError, ValueError): r = float(s.get("risk_pct", 1.0))
    return round(max(0.0, min(r, cap)), 2)

def place_order(acc, sig, sym, s):
    bal = float(acc.get("balance") or 0)
    if acc["platform"] not in ("EA", "PAPER"):                                    # a connected account is ALWAYS live: its owner chose real trading by connecting it
        try: bal = balance(acc)                                                # use the real balance for sizing
        except Exception: pass
    if acc["platform"] == "PAPER": bal = paper_balance(acc)                        # practice money grows and shrinks with every closed trade
    risk_amt = bal * effective_risk(acc, s) / 100
    dist = abs(sig["entry"] - sig["sl"])
    o = {"id": uuid.uuid4().hex[:12], "time": now_s(), "account": acc["id"], "platform": acc["platform"],
         "symbol": sym, "size": round(risk_amt / dist, 6) if dist else 0, "risk_amount": round(risk_amt, 2),
         "outcome": "PENDING", "bars": 0, **sig}
    if acc["platform"] == "PAPER" and sig.get("grade") == "A+" and dist:                  # A+ setup: the person's own (bigger) reward, kept short of the level that forms a new BOS
        use = aplus_rr(acc, sig, s)
        if use > float(sig["rr"]) + 1e-9:
            o["tp"] = round(sig["entry"] + use * dist if sig["side"] == "BUY" else sig["entry"] - use * dist, 8); o["rr"] = use; o["key"] = sig["key"] + "|rr"      # own key: this order is resolved on its own, not copied from the signal
    if acc["platform"] == "PAPER": o["status"] = "PAPER"                                      # paper accounts are ALWAYS simulated; every connected account is ALWAYS live (no owner switch)
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

# ======================================================================================================================
# FRESH ZONE CHECK: a setup is only worth an order while its zone is still good.
#   A setup is STALE when: a newer break of structure has formed, OR price already traded into the entry zone (mitigated),
#   OR price already reached the target without tapping the entry, OR the zone is more mitigated than allowed.
#   * Before placing: every account job re-checks the setup (candles are fetched ONCE and shared, so 1000 accounts add no extra load).
#   * After placing: an order that price has not tapped yet is cancelled automatically the moment it becomes stale.
# ======================================================================================================================
SCAN_EVERY = max(30, int(os.environ.get("SCAN_SECONDS", "120")))      # seconds between market scans (also how fast stale orders get cancelled)
_CC, _CC_LOCK, _CC_KEYLOCK, _EVC = {}, threading.Lock(), {}, {}

_YH = {"XAUUSD": "GC=F", "XAGUSD": "SI=F", "USOIL": "CL=F", "UKOIL": "BZ=F"}
def yahoo_symbol(sym):
    core = re.sub(r"[a-z._#]+$", "", sym)
    if core in _YH: return _YH[core]
    if re.fullmatch(r"[A-Z]{6}", core): return core + "=X"
    raise RuntimeError(f"{sym}: no free price source for this symbol (add a MetaApi data account to read it)")

def _yahoo_get(sym, interval, rng):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yahoo_symbol(sym))}?interval={interval}&range={rng}"
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=10) as r: j = json.loads(r.read())
    res = (j.get("chart") or {}).get("result")
    if not res: raise RuntimeError("the free price source returned no data for " + sym)
    return res[0]

def fetch_yahoo_rows(sym, tf):
    """All candles (the forming one included) from the free Yahoo Finance feed: forex, gold, silver, oil. One price per candle (no separate bid/ask)."""
    iv = {"30m": "30m", "1h": "60m", "15m": "15m", "5m": "5m"}.get(tf)
    if not iv: raise RuntimeError("unsupported chart " + tf)
    return _yahoo_candles(_yahoo_get(sym, iv, "1mo"))[-300:]

def _yahoo_candles(res):
    q = res["indicators"]["quote"][0]; out = []
    for i, t in enumerate(res.get("timestamp") or []):
        o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
        if None in (o, h, l, c): continue
        out.append({"t": int(t) * 1000, "o": float(o), "h": float(h), "l": float(l), "c": float(c)})
    return out

def fetch_yahoo_price(sym):
    res = _yahoo_get(sym, "1m", "1d"); p = (res.get("meta") or {}).get("regularMarketPrice")
    if p is None: raise RuntimeError("no price received for " + sym)
    return float(p)

_BNFX = {"EURUSD": "EURUSDT", "GBPUSD": "GBPUSDT", "AUDUSD": "AUDUSDT", "XAUUSD": "PAXGUSDT"}      # Binance trades these against USDT (PAXG = gold), 24/7, free
def _bn_proxy(sym):
    core = re.sub(r"[a-z._#]+$", "", sym)
    if core not in _BNFX: raise RuntimeError("the free price feed is not answering for " + sym)
    return _BNFX[core]

def fx_split(sym, tf):
    """(closed candles, forming candle or None) for a forex/gold symbol: MetaApi if the owner set it up, otherwise the free feed."""
    ma = data_account()
    if ma: return fetch_candles_meta(ma, sym, tf), None
    try: rows = fetch_yahoo_rows(sym, tf)
    except Exception as e:
        try: rows = fetch_klines_live(_bn_proxy(sym), {"30m": "30m", "1h": "1h"}.get(tf, tf))          # backup free source
        except Exception: raise e
    w, nowms = TF_MS(tf), time.time() * 1000
    return [c for c in rows if c["t"] + w <= nowms], next((c for c in rows if c["t"] + w > nowms), None)

# ---------------- the bigger picture: Monthly, Weekly and Daily direction (for A+ setups) ----------------
HTF = (("1M", "M"), ("1w", "W"), ("1d", "D"))
def fetch_htf(sym, tf):
    """Closed Daily / Weekly / Monthly candles (the forming one is always dropped)."""
    if not is_fx(sym): rows = fetch_klines_live(sym, tf, 200)
    else:
        ma = data_account()
        if ma:
            st = urllib.parse.quote(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")); tag = {"1d": "1d", "1w": "1w", "1M": "1mn"}[tf]
            raw = meta(ma, "GET", f"/historical-market-data/symbols/{sym}/timeframes/{tag}/candles?startTime={st}&limit=200", market=True)
            rows = [{"t": int(datetime.datetime.strptime(k["time"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000), "o": k["open"], "h": k["high"], "l": k["low"], "c": k["close"]} for k in raw]
            rows.sort(key=lambda x: x["t"])
        else:
            try: rows = _yahoo_candles(_yahoo_get(sym, {"1d": "1d", "1w": "1wk", "1M": "1mo"}[tf], {"1d": "2y", "1w": "5y", "1M": "10y"}[tf]))
            except Exception as e:
                try: rows = fetch_klines_live(_bn_proxy(sym), tf, 200)
                except Exception: raise e
    return rows[:-1]

def present_leg(c, n):
    """The leg the market is in RIGHT NOW (not the overall trend): it began at the latest confirmed swing point. Latest swing high = price is moving down (bear), latest swing low = moving up (bull).
    tip = the extreme the leg has already reached or the previous swing in its direction: the level that has to break to form a new BOS in the leg's direction."""
    hi = lo = None
    for j in range(n, len(c) - n):
        if _swing(c, j, n, "high"): hi = j
        if _swing(c, j, n, "low"): lo = j
    if hi is None and lo is None: return None
    if lo is None or (hi is not None and hi > lo):
        tip = min(c[k]["l"] for k in range(hi, len(c)))
        if lo is not None: tip = min(tip, c[lo]["l"])
        return {"dir": "bear", "tip": tip}
    tip = max(c[k]["h"] for k in range(lo, len(c)))
    if hi is not None: tip = max(tip, c[hi]["h"])
    return {"dir": "bull", "tip": tip}

def major_leg(c, n=3):
    """The whole vertical leg the bigger chart is in, not just the last little swing. The leg keeps its direction until price BREAKS STRUCTURE the other way
    (a close beyond the last swing point). A bounce at the bottom of a big fall therefore does not turn the bias bullish until it really breaks structure.
    -> {'dir': 'bull'|'bear', 'tip': extreme the leg has reached, 'start': candle index where the whole leg began} or None."""
    ev = bos_events(c, n)
    if not ev: return None
    e = ev[-1]; bull = e["dir"] == "bull"
    prev = next((x["i"] for x in reversed(ev) if x["dir"] != e["dir"]), 0)           # the leg began where the opposite direction last broke structure
    rng = range(prev, e["i"] + 1)
    if bull:
        st = min(rng, key=lambda k: c[k]["l"]); tip = max(c[k]["h"] for k in range(st, len(c)))
    else:
        st = max(rng, key=lambda k: c[k]["h"]); tip = min(c[k]["l"] for k in range(st, len(c)))
    return {"dir": e["dir"], "tip": tip, "start": st}

_BIAS = {}
def htf_bias(sym, s):
    """{'M': 'bull'|'bear'|None, 'W': ..., 'D': ...}: direction of the PRESENT LEG on each bigger chart. '?' = could not be read. 'tipD' = the Daily level that would form a new BOS in the leg's direction."""
    h = _BIAS.get(sym)
    if h and time.time() - h[0] < h[2]: return h[1]
    out, bad = {}, False
    for tf, name in HTF:
        try:
            rows_ = fetch_htf(sym, tf); leg = major_leg(rows_, int(s.get("htf_swing_n", 3))) or present_leg(rows_, int(s["swing_n"])); out[name] = leg["dir"] if leg else None
            if name == "D" and leg: out["tipD"] = leg["tip"]
        except Exception:
            out[name] = None if name == "M" else "?"; bad = bad or name != "M"                   # a young market may have no monthly history: that is fine
    if len(_BIAS) > 200: _BIAS.clear()
    _BIAS[sym] = (time.time(), out, 120 if bad else 3600)                                          # a failed read is retried after 2 minutes, a good one lasts an hour
    return out

def grade_setup(sig, s, sym):
    """-> (take_it, grade, bias). A+ = same direction as the present Daily and Weekly leg, and the present Monthly leg is not against it."""
    b = htf_bias(sym, s); want = "bull" if sig["side"] == "BUY" else "bear"
    aplus = b["D"] == want and b["W"] == want and b["M"] in (want, None)
    ok, _why = trend_check(sig, b, s)
    return ((aplus or not s.get("htf_filter", False)) and ok), ("A+" if aplus else "B"), b

def trend_check(sig, b, s):
    """-> (allowed, reason). The robot trades WITH the trend: the present leg of the Daily chart says buy only (bullish) or sell only (bearish).
    Against the trend it is only allowed on the SECOND break of structure in the new direction (the first break against the trend is skipped, the second is traded) on the 30m / 1h chart."""
    if not s.get("trend_only", True): return True, ""
    d = b.get("D"); want = "bull" if sig["side"] == "BUY" else "bear"
    if d not in ("bull", "bear"): return False, "the Daily trend could not be read, so the robot stays out"
    if d == want: return True, ""
    if int(sig.get("bos_run") or 1) >= 2: return True, ""
    nm = "bullish" if d == "bull" else "bearish"
    return False, f"the Daily trend is {nm}, so the robot only looks for {'buys' if d == 'bull' else 'sells'}; it trades the other way only after a second break of structure in that direction"

def aplus_max_rr(sig, s):
    """Highest reward (1:x) an A+ setup allows: the take profit has to stay SHORT of the Daily level that would form a new BOS in the leg's direction (and below the owner's max_rr)."""
    base = float(sig.get("rr") or s["rr"]); lim = sig.get("tp_limit")
    if sig.get("grade") != "A+" or lim is None: return base
    risk = abs(sig["entry"] - sig["sl"]); room = (sig["entry"] - lim) if sig["side"] == "SELL" else (lim - sig["entry"])
    if risk <= 0 or room <= 0: return base
    return max(base, min(math.floor(0.98 * room / risk * 10) / 10, float(s["max_rr"])))

def aplus_rr(acc, sig, s):
    """The reward this person gets on this setup: what they asked for, cut down to what the setup allows (never below the normal 1:2)."""
    want = acc.get("rr_aplus")
    base = float(sig.get("rr") or s["rr"])
    return round(max(base, min(float(want) if want else base, aplus_max_rr(sig, s))), 2)

def bias_text(b):
    nm = {"bull": "bullish", "bear": "bearish", None: "unclear", "?": "unknown"}
    return " · ".join(f"{lab} {nm.get(b.get(k), 'unclear')}" for k, lab in (("M", "Monthly"), ("W", "Weekly"), ("D", "Daily")))

def fetch_candles_for(sym, tf):
    if is_fx(sym): return fx_split(sym, tf)[0]
    return fetch_candles(sym, tf)

def candles_cached(sym, tf):
    """Closed candles shared by all jobs in this process. Reused for 10-45 s; refetched sooner if a new candle must have closed. Only ONE thread fetches at a time."""
    k = (sym, tf)
    def usable():
        hit = _CC.get(k)
        if not hit: return None
        age, rows = time.time() - hit[0], hit[1]
        if age < 10: return rows
        if age < 45 and not (rows and time.time() * 1000 >= rows[-1]["t"] + 2 * TF_MS(tf)): return rows
        return None
    v = usable()
    if v is not None: return v
    with _CC_LOCK: kl = _CC_KEYLOCK.setdefault(k, threading.Lock())
    with kl:
        v = usable()
        if v is not None: return v
        rows = fetch_candles_for(sym, tf)
        _CC[k] = (time.time(), rows)
        return rows

def bos_time(sig):
    """Open time of the candle that made the BOS this setup is built on."""
    try: return int(sig["bos_t"])
    except Exception: pass
    try: return int(str(sig.get("key", "")).rsplit("-", 1)[1])
    except Exception: return 0

def _event_times(c, n, with_dir=False):
    k = (c[0]["t"], c[0]["o"], c[-1]["t"], c[-1]["c"], len(c), n)
    v = _EVC.get(k)
    if v is None:
        v = [(c[e["i"]]["t"], "BUY" if e["dir"] == "bull" else "SELL") for e in bos_events(c, n)]
        if len(_EVC) > 200: _EVC.clear()
        _EVC[k] = v
    return v if with_dir else [t for t, _ in v]

def setup_fresh(sig, c, s, strict=False):
    """(True, '') while the setup is still good, otherwise (False, reason).
    strict=True (used right before placing): if the setup cannot be verified, it is NOT fresh.
    strict=False (used for orders already placed): if it cannot be verified, the order is left alone."""
    bt = bos_time(sig)
    if not bt or "entry" not in sig: return (not strict), "setup has no BOS time to check"
    if not c: return (not strict), "no candle data to check the setup"
    if bt < c[0]["t"]: return (not strict), "setup is older than the candle history available"
    n_ = int(sig.get("swing_n") or s["swing_n"])
    evs_, prev_i = bos_events(c, n_), None
    for e_ in evs_:                                                                                                    # a NEWER break of structure cancels the waiting order (it never got tapped)
        d_ = "BUY" if e_["dir"] == "bull" else "SELL"; fl_ = (prev_i + 1) if prev_i is not None else 0; prev_i = e_["i"] if d_ == sig["side"] else None
        if c[e_["i"]]["t"] <= bt: continue
        if d_ != sig["side"]: return False, "a break of structure the other way has formed, so this idea is replaced"
        Lq = bos_leg(c, e_, n_, floor=fl_)
        big = (not s.get("min_leg_atr") or not Lq["avg"] or Lq["leg_h"] >= s["min_leg_atr"] * Lq["avg"]) and Lq["width"] >= int(s.get("min_leg_bars") or 0) \
              and Lq["width"] >= float(s.get("bos_supersede_pct", 50)) / 100 * float(sig.get("leg_w") or 0)
        if big: return False, "a newer break of structure has formed, so this idea is replaced"
    bull, entry, tp = sig["side"] == "BUY", sig["entry"], sig["tp"]
    after = [x for x in c if x["t"] > bt]
    pr_ = sig.get("prot")
    if pr_ is not None and any(((x["c"] < pr_) if bull else (x["c"] > pr_)) for x in after): return False, "the protected " + ("low" if bull else "high") + " was broken, so the idea is dead"
    if after:
        lo_, hi_ = min(x["l"] for x in after), max(x["h"] for x in after)
        if (lo_ <= entry) if bull else (hi_ >= entry): return False, "price already traded into the entry zone (mitigated)"
        ft = float(sig.get("found_t") or sig.get("last_t") or 0)                                                      # only candles AFTER the setup was found count: the target is the present low/high, which price had already reached when the setup formed
        post = [x for x in after if x["t"] > ft]
        if post and ((max(x["h"] for x in post) >= tp) if bull else (min(x["l"] for x in post) <= tp)): return False, "price already reached the target without tapping the entry"
        zh, zl = sig.get("ob_hi"), sig.get("ob_lo")
        if zh is not None and zl is not None and zh > zl:
            frac = max(0.0, min(1.0, ((zh - lo_) if bull else (hi_ - zl)) / (zh - zl)))
            if frac * 100 > float(s.get("max_mitigation_pct", 50)) + 1e-9: return False, f"zone is {round(frac * 100)}% mitigated"
    return True, ""

def expire_stale(o, why):
    """Mark an order as cancelled because its setup went stale. Uses the existing EXPIRED flow, so the cancel is sent to the exchange / MetaApi / EA."""
    pre = "PAPER" if str(o.get("status", "")).startswith("PAPER") else "LIVE"
    o["outcome"], o["status"] = "EXPIRED", f"{pre}: cancelled, {why}"[:200]

# ---------------- trade limit per day + start/stop control ----------------
def trades_today(account_id):
    """Trades this account has taken today (UTC): orders waiting for a tap, open, or finished. Cancelled / expired / rejected orders do not count."""
    return db.one("SELECT COUNT(*) AS n FROM orders WHERE account_id=:a AND created >= :d AND outcome IN ('PENDING','OPEN','WIN','LOSS')", a=account_id, d=str(today()))["n"]

def daily_cap_reached(account_id, s):
    cap = int(s.get("max_trades_per_day") or 0)
    return cap > 0 and trades_today(account_id) >= cap

def over_cap_after_insert(o, s):
    """Safety net for two jobs of the same account racing each other: after saving, an order ranked above the daily limit is cancelled."""
    cap = int(s.get("max_trades_per_day") or 0)
    if cap <= 0 or o.get("outcome") != "PENDING": return False
    rank = db.one("SELECT COUNT(*) AS n FROM orders WHERE account_id=:a AND created >= :d AND outcome IN ('PENDING','OPEN','WIN','LOSS') AND (created < :c OR (created = :c AND id <= :i))",
                  a=o["account"], d=str(today()), c=o["time"], i=o["id"])["n"]
    return rank > cap

def cancel_order_now(o, why):
    """Cancel an order that is still waiting for price (exchange/MetaApi: a cancel job is queued; EA: the cancel is sent on its next call)."""
    if o.get("platform") != "EA" and str(o.get("status", "")).startswith("LIVE: orders sent"):
        db.x("INSERT INTO jobs (kind, account_id, payload, status, tries, created) VALUES ('cancel', :a, :p, 'new', 0, :now)", a=int(o["account"]), p=o["id"], now=now_s())
    expire_stale(o, why); save_order_state(o)

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


# ======================================================================================================================
# TRADE CHART PICTURES: every trade the robot takes gets a picture drawn on the server (candles, break of structure, liquidity, entry order block,
# entry / stop loss / take profit lines). Stored as small SVG text, shown in Admin, deleted automatically after `snapshot_days`.
# ======================================================================================================================
def snap_key(symbol, tf, key): return hashlib.sha1(f"{symbol}|{tf}|{str(key).split('|')[0]}".encode()).hexdigest()[:20]

def _fp(v): return f"{v:.{2 if abs(v) >= 100 else 3 if abs(v) >= 10 else 5}f}"

def snap_svg(o, candles):
    cs = list(candles or [])
    if len(cs) < 10 or "entry" not in o: return None
    bull = o["side"] == "BUY"
    t0 = int(o.get("leg_t") or o.get("bos_t") or cs[0]["t"])
    i0 = next((i for i, x in enumerate(cs) if x["t"] >= t0), 0)
    w = cs[max(0, i0 - 30):][-150:]
    if len(w) < 10: return None
    W, H, PR, PT, PB = 720, 440, 92, 46, 48
    pw, ph = W - PR, H - PT - PB
    brk = o.get("brk")
    if brk is None and o.get("leg_t") and o.get("bos_t"):                                       # older orders: the level that was broken = the extreme of the leg just before the breaking candle
        _l = next((i for i, x in enumerate(cs) if x["t"] >= int(o["leg_t"])), None); _b = next((i for i, x in enumerate(cs) if x["t"] >= int(o["bos_t"])), None)
        if _l is not None and _b and _b > _l: brk = max(x["h"] for x in cs[_l:_b]) if bull else min(x["l"] for x in cs[_l:_b])
    lv = [o["entry"], o["sl"], o["tp"]] + ([o["prot"]] if o.get("prot") is not None else []) + ([brk] if brk is not None else [])
    lo, hi = min([x["l"] for x in w] + lv), max([x["h"] for x in w] + lv)
    pad = (hi - lo) * 0.07 or abs(hi) * 0.001; lo -= pad; hi += pad
    slot = pw / len(w)
    X = lambda i: (i + 0.5) * slot
    Y = lambda v: PT + (hi - v) / (hi - lo) * ph
    idx = lambda t_: next((i for i, x in enumerate(w) if x["t"] == t_), None)
    e = [f"<rect width='{W}' height='{H}' fill='#131722'/>"]
    for g in range(6):
        v = lo + (hi - lo) * g / 5; y = Y(v)
        e.append(f"<text x='{pw + 6}' y='{y + 4:.1f}' font-size='10' fill='#787b86'>{_fp(v)}</text>")
    for g in range(5):
        i = int((len(w) - 1) * g / 4); x = X(i)
        e.append(f"<text x='{x:.1f}' y='{H - 28}' font-size='10' fill='#787b86' text-anchor='middle'>{datetime.datetime.utcfromtimestamp(w[i]['t'] / 1000).strftime('%d %b %H:%M')}</text>")
    bi, li, oi, ci = idx(o.get("bos_t")), idx(o.get("leg_t")), idx(o.get("ob_t")), idx(o.get("liq_t"))
    def tag(x, y, txt, col, anchor="start", fs=10):                                             # a label on a dark pill so it can always be read
        wd = len(txt) * fs * 0.62 + 10; x0 = x if anchor == "start" else (x - wd if anchor == "end" else x - wd / 2)
        return f"<rect x='{x0:.1f}' y='{y - fs - 1:.1f}' width='{wd:.1f}' height='{fs + 6}' rx='4' fill='#0b0e14' fill-opacity='.88' stroke='{col}' stroke-width='1'/><text x='{x0 + 5:.1f}' y='{y + 1:.1f}' font-size='{fs}' font-weight='800' fill='{col}'>{txt}</text>"
    labels = []
    if o.get("prot") is not None and brk is not None:                                           # THE MAJOR BREAK OF STRUCTURE BOX: from the start of its leg to now, from the protected level to the level that was broken
        x0b = X(li) - slot / 2 if li is not None else 0
        y1, y2 = Y(max(o["prot"], brk)), Y(min(o["prot"], brk))
        e.append(f"<rect x='{x0b:.1f}' y='{y1:.1f}' width='{pw - x0b:.1f}' height='{y2 - y1:.1f}' fill='rgba(220,38,38,.07)' stroke='#dc2626' stroke-width='2'/>")
        yb = Y(brk)
        e.append(f"<line x1='{x0b:.1f}' x2='{pw}' y1='{yb:.1f}' y2='{yb:.1f}' stroke='#f59e0b' stroke-width='1.4' stroke-dasharray='3 4'/>")
        labels.append(tag(x0b + 6, y1 + 16, "BOS", "#ef4444", fs=10))
    sx = X(bi if bi is not None else max(0, len(w) - 8))
    ye, ys, yt = Y(o["entry"]), Y(o["sl"]), Y(o["tp"])
    e.append(f"<rect x='{sx:.1f}' y='{min(ye, yt):.1f}' width='{pw - sx:.1f}' height='{abs(yt - ye):.1f}' fill='rgba(38,166,154,.13)'/><rect x='{sx:.1f}' y='{min(ye, ys):.1f}' width='{pw - sx:.1f}' height='{abs(ys - ye):.1f}' fill='rgba(239,83,80,.13)'/>")
    if oi is not None and o.get("ob_hi") is not None:                                          # the entry order block
        y1, y2 = Y(o["ob_hi"]), Y(o["ob_lo"])
        e.append(f"<rect x='{X(oi) - slot / 2:.1f}' y='{y1:.1f}' width='{pw - X(oi) + slot / 2:.1f}' height='{max(2.0, y2 - y1):.1f}' fill='rgba(168,85,247,.25)' stroke='#a855f7'/><text x='{X(oi):.1f}' y='{(y1 - 5) if not bull else (y2 + 13):.1f}' font-size='10' font-weight='700' fill='#c084fc'>OB</text>")
    bw = max(1.0, min(slot * 0.7, 9.0))
    for i, c in enumerate(w):
        col = "#26a69a" if c["c"] >= c["o"] else "#ef5350"; x = X(i)
        e.append(f"<line x1='{x:.1f}' x2='{x:.1f}' y1='{Y(c['h']):.1f}' y2='{Y(c['l']):.1f}' stroke='{col}'/><rect x='{x - bw / 2:.1f}' y='{min(Y(c['o']), Y(c['c'])):.1f}' width='{bw:.1f}' height='{max(1.0, abs(Y(c['o']) - Y(c['c']))):.1f}' fill='{col}'/>")
    for lab, v, col, y in (("TP", o["tp"], "#26a69a", yt), ("ENTRY", o["entry"], "#2962ff", ye), ("SL", o["sl"], "#ef5350", ys)):
        e.append(f"<line x1='0' x2='{pw}' y1='{y:.1f}' y2='{y:.1f}' stroke='{col}' stroke-width='1.6'/><rect x='{pw}' y='{y - 9:.1f}' width='{PR}' height='18' fill='{col}'/><text x='{pw + 5}' y='{y + 4:.1f}' font-size='10' font-weight='700' fill='#fff'>{lab} {_fp(v)}</text>")
    pp = pip_size(o.get("symbol"), o["entry"]) or 1e-4
    labels.append(tag(6, yt - 6, f"TP {abs(o['tp'] - o['entry']) / pp:.0f}p · 1:{o.get('rr', '')}", "#26a69a", fs=10))
    labels.append(tag(6, ye - 6, "ENTRY", "#6c9bff", fs=10))
    labels.append(tag(6, ys + 16, f"SL {abs(o['sl'] - o['entry']) / pp:.0f}p", "#ef5350", fs=10))
    if o.get("prot") is not None:                                                              # the protected high/low: if price CLOSES beyond it the idea is dead (it is not the stop loss)
        yp = Y(o["prot"])
        e.append(f"<line x1='0' x2='{pw}' y1='{yp:.1f}' y2='{yp:.1f}' stroke='#a1a1aa' stroke-width='1.2' stroke-dasharray='6 5'/>")
        labels.append(tag(pw - 4, (yp + 15) if bull else (yp - 5), "PROT", "#a1a1aa", "end", 9))
    if ci is not None:                                                                         # the LIQUIDITY candle: boxed and labelled
        cx = X(ci); yt_, yb_ = Y(w[ci]["h"]), Y(w[ci]["l"]); bwx = max(slot, 9.0)
        e.append(f"<rect x='{cx - bwx / 2 - 2:.1f}' y='{yt_ - 3:.1f}' width='{bwx + 4:.1f}' height='{yb_ - yt_ + 6:.1f}' fill='rgba(234,179,8,.18)' stroke='#eab308' stroke-width='2.2'/>")
        ly = (yb_ + 22) if not bull else (yt_ - 14)
        e.append(f"<line x1='{cx:.1f}' x2='{cx:.1f}' y1='{(yb_ + 3) if not bull else (yt_ - 3):.1f}' y2='{(ly - 11) if not bull else (ly + 5):.1f}' stroke='#eab308' stroke-width='1.4'/>")
        labels.append(tag(min(max(cx, 70), pw - 70), ly, "LIQ", "#eab308", "middle", 10))
    if bi is not None:                                                                         # the candle that broke structure
        cx = X(bi); yy = Y(w[bi]["l"] if not bull else w[bi]["h"]); sgn = 1 if not bull else -1
        e.append(f"<line x1='{cx:.1f}' x2='{cx:.1f}' y1='{PT}' y2='{PT + ph}' stroke='#f59e0b' stroke-width='1' stroke-dasharray='2 4' opacity='.7'/>")
    if brk is not None and bi is not None:                                                     # THE BOS LINE: the broken level from the candle that made it to the candle that closed beyond it
        bk = idx(o.get("brk_t")); xs0 = X(bk if bk is not None else (li if li is not None else 0)); yb = Y(brk)
        e.append(f"<line x1='{xs0:.1f}' x2='{X(bi):.1f}' y1='{yb:.1f}' y2='{yb:.1f}' stroke='#fbbf24' stroke-width='3'/><circle cx='{xs0:.1f}' cy='{yb:.1f}' r='4' fill='#fbbf24'/><circle cx='{X(bi):.1f}' cy='{yb:.1f}' r='5.5' fill='#fbbf24' stroke='#fff' stroke-width='1.5'/>")
    if ci is not None:                                                                         # a DOT on the exact liquidity point (the high of the opposing candle for sells, the low for buys)
        lx, ly_ = X(ci), Y(w[ci]["l"] if bull else w[ci]["h"])
        e.append(f"<circle cx='{lx:.1f}' cy='{ly_:.1f}' r='6' fill='#eab308' stroke='#fff' stroke-width='2'/>")
    ft_ = int(o.get("found_t") or o.get("bos_t") or 0); pend_dist = None
    if o.get("outcome") in ("OPEN", "WIN", "LOSS"):                                            # the trade WAS taken: a dot where price touched the entry
        ti = next((i for i, x in enumerate(w) if x["t"] > ft_ and ((x["l"] <= o["entry"]) if bull else (x["h"] >= o["entry"]))), None)
        if ti is not None:
            e.append(f"<circle cx='{X(ti):.1f}' cy='{ye:.1f}' r='6.5' fill='#2962ff' stroke='#fff' stroke-width='2'/>")
            labels.append(tag(min(max(X(ti), 70), pw - 70), ye + (-12 if not bull else 20), "TAKEN", "#6c9bff", "middle", 10))
    elif o.get("outcome") == "PENDING":                                                        # not taken yet: how far price still has to travel to reach the entry
        lc = w[-1]["c"]; dist = abs(lc - o["entry"]) / pp; yl = Y(lc)
        e.append(f"<line x1='{pw - 14:.1f}' x2='{pw - 14:.1f}' y1='{yl:.1f}' y2='{ye:.1f}' stroke='#9aa4b2' stroke-width='1.5' stroke-dasharray='4 3'/><circle cx='{pw - 14:.1f}' cy='{yl:.1f}' r='3.5' fill='#fff'/>")
        pend_dist = dist
    e.extend(labels)
    oc = o.get("outcome")
    if oc in ("WIN", "LOSS"):
        ei = idx(o.get("last_t"))
        if ei is not None:
            e.append(f"<circle cx='{X(ei):.1f}' cy='{(yt if oc == 'WIN' else ys):.1f}' r='5' fill='{'#26a69a' if oc == 'WIN' else '#ef5350'}' stroke='#fff'/>")
    st_txt = str(o.get("status", ""))
    status = {"PENDING": ("PENDING: trade NOT taken yet" + (f" · price is {pend_dist:.1f} pips from the entry" if pend_dist is not None else " (price has not reached the entry)"), "#9aa4b2", "PENDING · NOT TAKEN YET"), "OPEN": ("TRADE TAKEN: price touched the entry, the trade is running", "#2962ff", "TRADE TAKEN · RUNNING"),
              "WIN": (f"TRADE TAKEN · WIN +{o.get('r')}R: take profit hit", "#26a69a", f"WIN +{o.get('r')}R"), "LOSS": ("TRADE TAKEN · LOSS -1R: stop loss hit", "#ef5350", "LOSS -1R")}.get(oc, ("EXPIRED / CANCELLED, trade was NOT taken: " + (st_txt.split(",", 1)[-1].strip() if "," in st_txt else st_txt)[:80], "#f59e0b", "EXPIRED · NOT TAKEN"))
    e.append(f"<rect x='{W - 214}' y='6' width='208' height='24' rx='12' fill='{status[1]}'/><text x='{W - 110}' y='22' font-size='12' font-weight='800' fill='#0b0e14' text-anchor='middle'>{escape(status[2])}</text>")
    head = f"{o.get('symbol', '')} · {o.get('tf', '')} · {o['side']}" + (f" · {o['grade']} setup" if o.get("grade") else "") + (" · 1H IDEA, 30M ENTRY" if o.get("htf") else "") + (" · RE-ENTRY" if o.get("re_of") else "")
    e.append(f"<text x='8' y='18' font-size='14' font-weight='800' fill='#fff'>{escape(head)}</text><text x='8' y='34' font-size='10' fill='#787b86'>Robot trade picture · placed {escape(str(o.get('time', ''))[:16])} UTC · liquidity: {escape(str(o.get('liquidity', '')).replace('_', ' '))}</text>")
    e.append(f"<text x='8' y='{H - 8}' font-size='12' font-weight='800' fill='{status[1]}'>{escape(status[0][:92])}</text><text x='{W - 8}' y='{H - 8}' font-size='10' fill='#787b86' text-anchor='end'>risk to reward 1:{o.get('rr', '')}</text>")
    return f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {W} {H}' width='100%' style='max-width:100%;height:auto;display:block;border-radius:12px;background:#131722'>{''.join(e)}</svg>"

def snap_save(o, candles):
    """Draw (or redraw) the picture of one trade. Never allowed to break trading: any problem is just printed."""
    try:
        if float(S().get("snapshot_days") or 0) <= 0: return
        svg = snap_svg(o, candles)
        if not svg: return
        now = now_s()
        db.x("INSERT INTO snaps (skey, symbol, tf, sig_key, created, updated, outcome, side, svg) VALUES (:k, :s, :tf, :sk, :n, :n, :oc, :sd, :svg) "
             "ON CONFLICT (skey) DO UPDATE SET updated = EXCLUDED.updated, outcome = EXCLUDED.outcome, svg = EXCLUDED.svg",
             k=snap_key(o["symbol"], o.get("tf") or "", o.get("key")), s=o["symbol"], tf=o.get("tf") or "", sk=str(o.get("key") or "").split("|")[0], n=now, oc=str(o.get("outcome", "")), sd=o["side"], svg=svg)
    except Exception as ex: print("trade picture failed:", str(ex)[:150])

def snap_have():
    try: return {r["skey"] for r in db.q("SELECT skey FROM snaps")}
    except Exception: return set()

def snap_link(o, have):
    if not o.get("key") or snap_key(o.get("symbol", ""), o.get("tf") or "", o["key"]) not in have: return ""
    return f" · <a href='/snap/{urllib.parse.quote(o['symbol'], safe='')}/{urllib.parse.quote(o.get('tf') or '-', safe='')}/{urllib.parse.quote(str(o['key']).split('|')[0], safe='-')}'>chart</a>"

@app.route("/snap/<symbol>/<tf>/<path:key>")
def snap_view(symbol, tf, key):
    r = db.one("SELECT svg FROM snaps WHERE skey=:k", k=snap_key(symbol, "" if tf == "-" else tf, key))
    if not r: return layout("Trade chart", "<div class='card' style='max-width:560px'><h2>Chart not available</h2><p class='muted'>Trade pictures are kept for a short time only, then removed to save space.</p></div>"), 404
    return layout("Trade chart", f"<div class='card' style='max-width:780px'><h2>Robot trade chart</h2>{r['svg']}<p class='muted' style='margin-top:10px'>Blue = entry, red = stop loss, green = take profit, purple = entry order block, yellow circle = liquidity, dark red box = the break of structure.</p></div>")

TCHART_JS = r"""(function(){
if(window.TChart)return;
var GREEN='#26a69a',RED='#ef5350',BLUE='#2962ff',AMB='#fbbf24',YEL='#eab308',PUR='#a855f7',BG='#000000',GRID='#303030',TXT='#c8c8c8',DARKRED='#b91c1c';
function clamp(v,a,b){return Math.max(a,Math.min(b,v))}
function dec(p){var a=Math.abs(p);return a>=100?2:a>=10?3:a>=0.01?5:6}
function fmt(p){if(p===null||p===undefined||isNaN(p))return '-';var d=dec(p);return Number(p).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d})}
function idxAt(A,t){if(t===null||t===undefined)return -1;var lo=0,hi=A.length-1,r=-1;while(lo<=hi){var m=(lo+hi)>>1;if(A[m][0]<=t){r=m;lo=m+1}else hi=m-1}return r}
function hm(t){var d=new Date(t);return d.toLocaleDateString(undefined,{day:'2-digit',month:'short'})+' '+d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}
function tfName(t){return {'30m':'M30','1h':'H1','15m':'M15','5m':'M5','4h':'H4','1d':'D1'}[t]||t}
window.TChart=function(box,opt){
 var bar=document.createElement('div');bar.className='tcbar';
 bar.innerHTML='<button type="button" data-a="in" title="Zoom in">+</button><button type="button" data-a="out" title="Zoom out">&minus;</button><button type="button" data-a="left" title="Move back">&#9664;</button><button type="button" data-a="right" title="Move forward">&#9654;</button><button type="button" data-a="reset" title="Back to the trade">Reset</button><span class="tcbadge" style="margin-left:auto;border-radius:999px;padding:4px 12px;font-size:12px;font-weight:800;color:#0b0e14;display:none"></span><span class="tcinfo" style="flex-basis:100%;margin:2px 0 0">Loading chart...</span>';
 var cnv=document.createElement('canvas');cnv.style.display='block';cnv.style.width='100%';cnv.style.touchAction='none';cnv.style.cursor='crosshair';cnv.style.borderRadius='10px';
 var leg=document.createElement('div');leg.className='tclegend';leg.innerHTML='<span><i style="background:#b91c1c"></i>Major break of structure (box)</span><span><i style="background:#fbbf24"></i>BOS line (broken level)</span><span><i style="background:#eab308"></i>Liquidity (boxed candle + dot)</span><span><i style="background:#a855f7"></i>Entry order block</span><span><i style="background:#2962ff"></i>Entry</span><span><i style="background:#ef5350"></i>Stop loss</span><span><i style="background:#26a69a"></i>Take profit</span><span><i style="background:#a1a1aa"></i>Protected level</span>';
 var why=document.createElement('div');why.className='tcwhy';
 box.appendChild(bar);box.appendChild(cnv);box.appendChild(leg);box.appendChild(why);var ctx=cnv.getContext('2d');
 var S={C:[],F:null,D:null,vis:80,off:0,hov:null,dpr:1,raf:0,err:'',busy:false,shown:false,seen:false,cur:null};
 function all(){return S.F?S.C.concat([S.F]):S.C}
 function fit(){var w=box.clientWidth||600,h=Math.round(w<620?clamp(w*1.05,360,520):clamp(w*0.6,320,480));S.dpr=window.devicePixelRatio||1;cnv.style.height=h+'px';cnv.width=Math.round(w*S.dpr);cnv.height=Math.round(h*S.dpr);kick()}
 function kick(){if(!S.raf)S.raf=requestAnimationFrame(function(){S.raf=0;draw()})}
 function defView(){var A=all(),o=S.D&&S.D.o,n=A.length;if(!n)return;var li=o?idxAt(A,o.leg_t):-1;var span=li>=0?(n-1-li+16):80;S.vis=clamp(span,40,200);S.off=0}
 function levelsOf(o){var L=[o.entry,o.sl,o.tp];if(o.prot!==null&&o.prot!==undefined)L.push(o.prot);if(o.brk!==null&&o.brk!==undefined)L.push(o.brk);return L}
 function draw(){
  var w=cnv.clientWidth,h=cnv.clientHeight,padR=82,padT=14,padB=26,pw=w-padR,ph=h-padT-padB,A=all(),n=A.length,j,p,o=S.D&&S.D.o,D=S.D||{};
  ctx.setTransform(S.dpr,0,0,S.dpr,0,0);ctx.fillStyle=BG;ctx.fillRect(0,0,w,h);
  if(!n){ctx.fillStyle=TXT;ctx.font='14px system-ui,sans-serif';ctx.fillText(S.err||'Loading chart...',16,30);return}
  var R=8,slot=pw/(S.vis+R),last=n-1-S.off;
  function X(k){return pw-(R+(last-k)+0.5)*slot}
  var j0=Math.max(0,Math.floor(last-(pw/slot-R))-1),j1=Math.min(n-1,last+R+1),lo=Infinity,hi=-Infinity;
  for(j=j0;j<=j1;j++){var c=A[j];if(c[3]<lo)lo=c[3];if(c[2]>hi)hi=c[2]}
  if(!isFinite(lo))return;
  if(o)levelsOf(o).forEach(function(q){if(q<lo)lo=q;if(q>hi)hi=q});
  var pad=(hi-lo)*0.08||hi*0.0005;lo-=pad;hi+=pad;
  function Y(q){return padT+(hi-q)/(hi-lo)*ph}
  function P(y){return hi-(y-padT)/ph*(hi-lo)}
  ctx.lineWidth=1;ctx.textBaseline='alphabetic';ctx.font='11px system-ui,sans-serif';
  // price scale and time scale: labels and small ticks only (no grid lines)
  var step=(function(r){var e=Math.pow(10,Math.floor(Math.log10(r))),f=r/e;return (f<1.5?1:f<3?2:f<7?5:10)*e})((hi-lo)/7),cnt=0;
  ctx.strokeStyle='#666';
  for(p=Math.ceil(lo/step)*step;p<=hi&&cnt<40;p+=step,cnt++){var gy=Math.round(Y(p))+0.5;ctx.beginPath();ctx.moveTo(pw,gy);ctx.lineTo(pw+4,gy);ctx.stroke();ctx.fillStyle=TXT;ctx.fillText(fmt(p),pw+8,gy+4)}
  var tk=Math.max(1,Math.ceil(110/slot));
  for(j=j0;j<=j1;j++){if(j%tk!==0)continue;var tx=Math.round(X(j))+0.5,d=new Date(A[j][0]);ctx.beginPath();ctx.moveTo(tx,h-padB);ctx.lineTo(tx,h-padB+4);ctx.stroke();
   var day=(d.getHours()===0&&d.getMinutes()===0),lab=day?d.toLocaleDateString(undefined,{day:'2-digit',month:'short'}):d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
   ctx.fillStyle=day?'#ffffff':TXT;ctx.textAlign='center';ctx.fillText(lab,clamp(tx,24,pw-24),h-8);ctx.textAlign='left'}
  ctx.setLineDash([]);ctx.strokeStyle='#555';ctx.beginPath();ctx.moveTo(pw+0.5,0);ctx.lineTo(pw+0.5,h);ctx.moveTo(0,h-padB+0.5);ctx.lineTo(w,h-padB+0.5);ctx.stroke();
  ctx.save();ctx.beginPath();ctx.rect(0,0,pw,h-padB);ctx.clip();
  var pills=[],compact=w<560;
  function pill(x,y,txt,col,anchor,fs,prio,short){var tt=(short!==undefined)?short:txt;if(tt==='')return;pills.push({x:x,y:y,t:tt,col:col,an:anchor||'start',fs:compact?9:Math.min(fs||10,10),pr:prio||5})}
  function drawPills(){
   var boxes=[[0,0,320,padT+32]];
   function free(r){for(var i=0;i<boxes.length;i++){var b=boxes[i];if(r[0]<b[0]+b[2]+2&&r[0]+r[2]+2>b[0]&&r[1]<b[1]+b[3]+2&&r[1]+r[3]+2>b[1])return false}return true}
   pills.sort(function(a,b){return a.pr-b.pr});
   pills.forEach(function(a){ctx.font='bold '+a.fs+'px system-ui,sans-serif';var tw=ctx.measureText(a.t).width+10,hh=a.fs+7,d=a.fs+9,ds=[0,-1,1,-2,2,-3,3,-4,4],ans=[a.an,a.an==='end'?'start':'end'],placed=null,i,k;
    for(k=0;k<ans.length&&!placed;k++)for(i=0;i<ds.length&&!placed;i++){
     var x0=ans[k]==='end'?(k===0?a.x-tw:pw-tw-4):ans[k]==='middle'?a.x-tw/2:(k===0?a.x:4);x0=clamp(x0,3,pw-tw-3);var yy=a.y+ds[i]*d,r=[x0,yy-a.fs-1,tw,hh];
     if(r[1]<padT+2||r[1]+hh>h-padB-2)continue;if(free(r))placed=[r,yy]}
    if(!placed)return;var r=placed[0],yy=placed[1];boxes.push(r);
    ctx.fillStyle='rgba(0,0,0,.9)';ctx.fillRect(r[0],r[1],r[2],r[3]);ctx.strokeStyle=a.col;ctx.lineWidth=1;ctx.strokeRect(r[0]+0.5,r[1]+0.5,r[2]-1,r[3]-1);ctx.fillStyle=a.col;ctx.fillText(a.t,r[0]+5,yy+2)})}
  var pip=D.pip||1e-4,badge=null;
  if(o){
   var bull=o.side==='BUY',oc=o.outcome;
   var li=idxAt(A,o.leg_t),bi=idxAt(A,o.bos_t),kb=idxAt(A,o.brk_t),oi=idxAt(A,o.ob_t),ci=idxAt(A,o.liq_t),fi=idxAt(A,o.found_t);
   var brk=o.brk;
   if((brk===null||brk===undefined)&&li>=0&&bi>li){var q;brk=bull?-Infinity:Infinity;for(q=li;q<bi;q++){brk=bull?Math.max(brk,A[q][2]):Math.min(brk,A[q][3])}}
   var xl=li>=0?X(li)-slot/2:0;
   // profit / loss zones
   var xs=fi>=0?X(fi):(bi>=0?X(bi):0);
   ctx.fillStyle='rgba(38,166,154,.13)';ctx.fillRect(xs,Math.min(Y(o.entry),Y(o.tp)),pw-xs,Math.abs(Y(o.tp)-Y(o.entry)));
   ctx.fillStyle='rgba(239,83,80,.13)';ctx.fillRect(xs,Math.min(Y(o.entry),Y(o.sl)),pw-xs,Math.abs(Y(o.sl)-Y(o.entry)));
   // the MAJOR BREAK OF STRUCTURE box
   if(o.prot!==null&&o.prot!==undefined&&brk!==null&&brk!==undefined&&isFinite(brk)){
    var yt=Y(Math.max(o.prot,brk)),yb=Y(Math.min(o.prot,brk));
    ctx.fillStyle='rgba(185,28,28,.07)';ctx.fillRect(xl,yt,pw-xl+2,yb-yt);ctx.strokeStyle=DARKRED;ctx.lineWidth=2;ctx.strokeRect(xl,yt,pw-xl+2,yb-yt);ctx.lineWidth=1;
    pill(xl+6,yt+16,'BOS','#ef4444','start',11,5,'BOS');
   }
   // entry order block
   if(oi>=0&&o.ob_hi!==null&&o.ob_hi!==undefined){var xo=X(oi)-slot/2,y1=Y(o.ob_hi),y2=Y(o.ob_lo);ctx.fillStyle='rgba(168,85,247,.24)';ctx.fillRect(xo,y1,pw-xo,Math.max(2,y2-y1));ctx.strokeStyle=PUR;ctx.strokeRect(xo+0.5,y1+0.5,pw-xo,Math.max(2,y2-y1));pill(xo+4,bull?y2+16:y1-5,'OB',PUR,'start',10,4,'OB')}
   // protected level
   if(o.prot!==null&&o.prot!==undefined){var yp=Y(o.prot);ctx.strokeStyle='#a1a1aa';ctx.setLineDash([6,5]);ctx.beginPath();ctx.moveTo(0,yp);ctx.lineTo(pw,yp);ctx.stroke();ctx.setLineDash([]);pill(pw-4,bull?yp+16:yp-5,'PROTECTED '+(bull?'LOW ':'HIGH ')+fmt(o.prot),'#a1a1aa','end',9,6,'PROT')}
  }
  // candles
  var bw=Math.max(1,Math.min(slot*0.7,18));
  for(j=j0;j<=j1;j++){var k=A[j],x=Math.round(X(j))+0.5,col=k[4]>=k[1]?GREEN:RED,yo=Y(k[1]),yc=Y(k[4]);
   ctx.strokeStyle=col;ctx.fillStyle=col;ctx.beginPath();ctx.moveTo(x,Y(k[2]));ctx.lineTo(x,Y(k[3]));ctx.stroke();
   ctx.fillRect(Math.round(x-bw/2),Math.round(Math.min(yo,yc)),Math.max(1,Math.round(bw)),Math.max(1,Math.round(Math.abs(yc-yo))))}
  if(o){
   var bull2=o.side==='BUY';
   // BOS line + dots
   if(brk!==null&&brk!==undefined&&isFinite(brk)&&bi>=0){var xb0=kb>=0?X(kb):(li>=0?X(li):0),yb2=Y(brk);ctx.strokeStyle=AMB;ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(xb0,yb2);ctx.lineTo(X(bi),yb2);ctx.stroke();ctx.lineWidth=1;
    ctx.fillStyle=AMB;ctx.beginPath();ctx.arc(xb0,yb2,4,0,6.3);ctx.fill();ctx.beginPath();ctx.arc(X(bi),yb2,6,0,6.3);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=1.5;ctx.stroke();ctx.lineWidth=1;
    pill(xb0+6,bull2?yb2+16:yb2-6,'',AMB,'start',10,7,'');
    var ybc=bull2?Y(A[bi][2])-10:Y(A[bi][3])+22;pill(X(bi),ybc,'',AMB,'middle',10,3,'')}
   // liquidity: boxed candle + dot on the exact point
   if(ci>=0){var cx=X(ci),ytc=Y(A[ci][2]),ybc2=Y(A[ci][3]),bwx=Math.max(slot,9);ctx.fillStyle='rgba(234,179,8,.18)';ctx.fillRect(cx-bwx/2-2,ytc-3,bwx+4,ybc2-ytc+6);ctx.strokeStyle=YEL;ctx.lineWidth=2;ctx.strokeRect(cx-bwx/2-2,ytc-3,bwx+4,ybc2-ytc+6);ctx.lineWidth=1;
    var ly=bull2?A[ci][3]:A[ci][2];ctx.fillStyle=YEL;ctx.beginPath();ctx.arc(cx,Y(ly),6,0,6.3);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();ctx.lineWidth=1;
    pill(cx,bull2?ytc-14:ybc2+24,'LIQ',YEL,'middle',10,2,'LIQ')}
   // taken dot / result dot
   if(D.taken_t){var ti=idxAt(A,D.taken_t);if(ti>=0){ctx.fillStyle=BLUE;ctx.beginPath();ctx.arc(X(ti),Y(o.entry),7,0,6.3);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();ctx.lineWidth=1;pill(X(ti),Y(o.entry)+(bull2?22:-12),'TAKEN','#6c9bff','middle',10,3,'TAKEN')}}
   if((o.outcome==='WIN'||o.outcome==='LOSS')&&o.last_t){var ei=idxAt(A,o.last_t);if(ei>=0){var ye=o.outcome==='WIN'?o.tp:o.sl,cc=o.outcome==='WIN'?GREEN:RED;ctx.fillStyle=cc;ctx.beginPath();ctx.arc(X(ei),Y(ye),6,0,6.3);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();ctx.lineWidth=1}}
   // entry / sl / tp lines
   [[o.tp,GREEN,'TAKE PROFIT · '+(Math.abs(o.tp-o.entry)/pip).toFixed(1)+' pips · 1:'+o.rr,-6,'TP '+(Math.abs(o.tp-o.entry)/pip).toFixed(0)+'p · 1:'+o.rr],[o.entry,'#6c9bff','ENTRY ('+o.side.toLowerCase()+' limit)',-6,'ENTRY'],[o.sl,RED,'STOP LOSS · '+(Math.abs(o.sl-o.entry)/pip).toFixed(1)+' pips',16,'SL '+(Math.abs(o.sl-o.entry)/pip).toFixed(0)+'p']].forEach(function(l){var y=Math.round(Y(l[0]))+0.5;ctx.strokeStyle=l[1]===('#6c9bff')?BLUE:l[1];ctx.lineWidth=1.6;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(pw,y);ctx.stroke();ctx.lineWidth=1;pill(6,y+l[3],l[2],l[1],'start',10,1,l[4])});
   var pc=S.cur!==null?S.cur:A[n-1][4];
   var oc2=o.outcome;
   if(oc2==='PENDING'){badge=['PENDING · NOT TAKEN YET','#9aa4b2'];var dist=Math.abs(pc-o.entry)/pip;S.info='PENDING: trade not taken yet · price is '+dist.toFixed(1)+' pips from the entry'}
   else if(oc2==='OPEN'){badge=['TRADE TAKEN · RUNNING',BLUE];S.info='TRADE TAKEN: price touched the entry, the trade is running'}
   else if(oc2==='WIN'){badge=['WIN +'+o.r+'R',GREEN];S.info='TRADE TAKEN · WIN +'+o.r+'R: take profit hit'}
   else if(oc2==='LOSS'){badge=['LOSS -1R',RED];S.info='TRADE TAKEN · LOSS -1R: stop loss hit'}
   else{badge=['EXPIRED · NOT TAKEN','#f59e0b'];var stt=String(o.status||'');S.info='EXPIRED / CANCELLED, trade was NOT taken: '+(stt.indexOf(',')>=0?stt.split(',').slice(1).join(',').trim():stt)}
  }
  // current price
  var lastC=A[n-1],pcur=S.cur!==null?S.cur:lastC[4],up=lastC[4]>=lastC[1],yp2=Math.round(Y(pcur))+0.5;
  ctx.strokeStyle=up?GREEN:RED;ctx.setLineDash([2,3]);ctx.beginPath();ctx.moveTo(0,yp2);ctx.lineTo(pw,yp2);ctx.stroke();ctx.setLineDash([]);
  drawPills();ctx.restore();
  if(o){[[o.tp,GREEN],[o.entry,BLUE],[o.sl,RED]].forEach(function(l){var y=Y(l[0]);ctx.fillStyle=l[1];ctx.fillRect(pw,y-8,padR,16);ctx.fillStyle='#fff';ctx.font='bold 10px system-ui,sans-serif';ctx.fillText(fmt(l[0]),pw+6,y+4)})}
  ctx.fillStyle=up?GREEN:RED;ctx.fillRect(pw,yp2-9,padR,18);ctx.fillStyle='#fff';ctx.font='bold 11px system-ui,sans-serif';ctx.fillText(fmt(pcur),pw+6,yp2+4);
  if(S.F&&D.tfms){var left=Math.max(0,Math.floor((S.F[0]+D.tfms-Date.now())/1000)),mm=Math.floor(left/60),ss=left%60;ctx.fillStyle=up?GREEN:RED;ctx.fillRect(pw,yp2+9,padR,15);ctx.fillStyle='#fff';ctx.font='10px system-ui,sans-serif';ctx.fillText((mm<10?'0':'')+mm+':'+(ss<10?'0':'')+ss,pw+6,yp2+20)}
  // crosshair
  var hj=null,hv=S.hov;
  if(hv&&hv[0]<pw&&hv[1]>padT&&hv[1]<h-padB){hj=clamp(Math.round(last-((pw-hv[0])/slot-R-0.5)),0,n-1);var hx=Math.round(X(hj))+0.5;ctx.strokeStyle='#8a8a8a';ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(hx,padT);ctx.lineTo(hx,h-padB);ctx.moveTo(0,hv[1]);ctx.lineTo(pw,hv[1]);ctx.stroke();ctx.setLineDash([]);
   ctx.fillStyle='#363a45';ctx.fillRect(pw,hv[1]-9,padR,18);ctx.fillStyle='#fff';ctx.font='11px system-ui,sans-serif';ctx.fillText(fmt(P(hv[1])),pw+6,hv[1]+4);
   var tl=hm(A[hj][0]),tw2=ctx.measureText(tl).width+14;ctx.fillStyle='#363a45';ctx.fillRect(clamp(hx-tw2/2,0,pw-tw2),h-padB+1,tw2,18);ctx.fillStyle='#fff';ctx.fillText(tl,clamp(hx-tw2/2,0,pw-tw2)+7,h-padB+14)}
  var ro=A[hj===null?n-1:hj];ctx.font='11px system-ui,sans-serif';ctx.fillStyle='#e5e5e5';ctx.fillText((o?o.symbol:'')+' · '+tfName(o?o.tf:'')+(o&&o.grade?' · '+o.grade+' setup':'')+(o&&o.htf?' · 1H IDEA, 30M ENTRY':'')+(o&&o.re_of?' · RE-ENTRY':''),8,padT+10);
  ctx.fillStyle=ro[4]>=ro[1]?GREEN:RED;ctx.fillText('O '+fmt(ro[1])+'  H '+fmt(ro[2])+'  L '+fmt(ro[3])+'  C '+fmt(ro[4]),8,padT+24);
  var bd=bar.querySelector('.tcbadge');if(bd&&badge){bd.style.display='inline-block';bd.style.background=badge[1];bd.textContent=badge[0]}
  var inf=bar.querySelector('.tcinfo');if(inf&&S.info)inf.textContent=S.info;
 }
 function load(){
  if(S.busy||document.hidden)return;S.busy=true;
  fetch(opt.dataUrl,{credentials:'same-origin'}).then(function(r){return r.json().then(function(j){return [r.ok,j]})}).then(function(a){
   S.busy=false;
   if(!a[0]){S.err=a[1].error||'The chart is not available.';var inf=bar.querySelector('.tcinfo');inf.textContent=S.err;kick();return}
   var d=a[1],first=!S.D;S.D=d;S.C=d.candles;S.F=d.forming||null;S.cur=(d.bid!==null&&d.bid!==undefined)?d.bid:null;S.err='';
   if(first)defView();
   var wl=d.why||[];why.innerHTML=wl.length?('<b>Why the robot took this trade</b><ul>'+wl.map(function(x){return '<li>'+String(x).replace(/&/g,'&amp;').replace(/</g,'&lt;')+'</li>'}).join('')+'</ul>'):'';kick();
  }).catch(function(){S.busy=false;S.err=S.C.length?'':'Connection problem. Retrying...';kick()})
 }
 var ptrs={},drag=null,pinch=null;
 function pc(){var c=0,k;for(k in ptrs)c++;return c}
 function pd(){var k,v=[];for(k in ptrs)v.push(ptrs[k]);return Math.hypot(v[0][0]-v[1][0],v[0][1]-v[1][1])||1}
 function sw(){return (cnv.clientWidth-82)/(S.vis+8)}
 cnv.addEventListener('pointerdown',function(e){try{cnv.setPointerCapture(e.pointerId)}catch(x){}ptrs[e.pointerId]=[e.clientX,e.clientY];if(pc()===1)drag={x:e.clientX,off:S.off};else if(pc()===2){pinch={d:pd(),vis:S.vis};drag=null}});
 cnv.addEventListener('pointermove',function(e){var r=cnv.getBoundingClientRect();
  if(ptrs[e.pointerId]){ptrs[e.pointerId]=[e.clientX,e.clientY];
   if(pinch&&pc()===2)S.vis=clamp(Math.round(pinch.vis*pinch.d/pd()),15,300);
   else if(drag)S.off=clamp(Math.round(drag.off+(e.clientX-drag.x)/sw()),-30,Math.max(0,all().length-10))}
  if(e.pointerType==='mouse')S.hov=[e.clientX-r.left,e.clientY-r.top];kick()});
 function up(e){delete ptrs[e.pointerId];if(pc()<2)pinch=null;if(pc()===0)drag=null;else if(pc()===1){var k;for(k in ptrs)drag={x:ptrs[k][0],off:S.off}}if(e.pointerType!=='mouse')S.hov=null;kick()}
 cnv.addEventListener('pointerup',up);cnv.addEventListener('pointercancel',up);cnv.addEventListener('pointerleave',function(){S.hov=null;kick()});
 cnv.addEventListener('dblclick',function(){defView();kick()});
 cnv.addEventListener('wheel',function(e){e.preventDefault();S.vis=clamp(Math.round(S.vis*(e.deltaY>0?1.12:0.89)),15,300);kick()},{passive:false});
 bar.addEventListener('click',function(e){var a=e.target.getAttribute&&e.target.getAttribute('data-a');if(!a)return;
  if(a==='in')S.vis=clamp(Math.round(S.vis*0.8),15,300);else if(a==='out')S.vis=clamp(Math.round(S.vis*1.25),15,300);
  else if(a==='left')S.off=clamp(S.off+Math.round(S.vis/4),-30,Math.max(0,all().length-10));else if(a==='right')S.off=clamp(S.off-Math.round(S.vis/4),-30,Math.max(0,all().length-10));else defView();kick()});
 window.addEventListener('resize',fit);
 function visible(){var r=box.getBoundingClientRect();return r.bottom>0&&r.top<(window.innerHeight||800)}
 function tick(){if(!document.hidden&&visible()){if(!S.seen){S.seen=true;load()}else load()}}
 fit();setTimeout(tick,50);setInterval(tick,opt.pollMs||8000);setInterval(function(){if(!document.hidden&&S.F&&visible())kick()},1000);
 window.addEventListener('scroll',function(){if(!S.seen&&visible()){S.seen=true;load()}},{passive:true});
};
})();
"""

TCHART_CSS = """<style>
.tcbox{background:#000;border:1px solid #2b2b2b;border-radius:12px;padding:8px;margin-top:8px}
.tcbar{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:6px}
.tcbar button{background:#1f2937;color:#e5e7eb;border:1px solid #374151;border-radius:8px;padding:5px 11px;font-weight:700;font-size:13px;cursor:pointer}
.tcbar button:hover{background:#374151}
.tcinfo{font-size:12px;color:#cbd5e1;font-weight:600;margin-left:6px}
.tcitem{border-top:1px solid #e2e8f0;padding:14px 0}
.tchead{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;align-items:center}
.tcchips{display:flex;gap:6px;flex-wrap:wrap;margin:10px 0}
.tcchips button{border:1px solid #cbd5e1;background:#fff;border-radius:999px;padding:5px 12px;font-weight:700;font-size:12px;cursor:pointer}
.tcchips button.on{background:#0f172a;color:#fff;border-color:#0f172a}
.tclegend{display:flex;flex-wrap:wrap;gap:4px 12px;margin-top:8px;font-size:11px;color:#cbd5e1}.tclegend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
.tcwhy{margin-top:8px;font-size:12px;color:#e5e7eb;border-top:1px solid #2b2b2b;padding-top:8px}.tcwhy ul{margin:6px 0 0 16px;padding:0}.tcwhy li{margin:3px 0}
.tcpill{display:inline-block;border-radius:999px;padding:2px 10px;font-size:11px;font-weight:800;color:#0b0e14}
</style>"""

def _tc_pill(outcome):
    m = {"PENDING": ("#9aa4b2", "PENDING · NOT TAKEN"), "OPEN": ("#6c9bff", "TAKEN · RUNNING"), "WIN": ("#26a69a", "TAKEN · WIN"), "LOSS": ("#ef5350", "TAKEN · LOSS")}.get(outcome, ("#f59e0b", "EXPIRED · NOT TAKEN"))
    return f"<span class='tcpill' style='background:{m[0]}'>{m[1]}</span>"

def _snap_card(s):
    days = float(s.get("snapshot_days") or 0)
    if days <= 0: return "<div class='card' id='charts'><h2>Robot trade charts</h2><p class='muted'>Switched off. Set <b>snapshot_days</b> in the settings below to a number above 0 (for example 2) and the robot will keep a live chart of every trade it takes.</p></div>"
    rows = db.q("SELECT skey, symbol, tf, created, updated, outcome, side FROM snaps ORDER BY updated DESC LIMIT 30")
    total = db.one("SELECT COUNT(*) AS n FROM snaps")["n"]
    def grp(o): return "pending" if o == "PENDING" else "taken" if o in ("OPEN", "WIN", "LOSS") else "expired"
    items = "".join(f"<div class='tcitem' data-g='{grp(r['outcome'])}'><div class='tchead'><b>{escape(r['symbol'])} · {escape(r['tf'])} · {escape(r['side'])}</b>{_tc_pill(r['outcome'])}<span class='muted'>placed {escape(str(r['created'])[:16])} UTC</span>"
                    f"<form action='/admin/snap' method='post' style='display:inline' onsubmit='return confirm(&quot;Delete this chart?&quot;)'><input type='hidden' name='op' value='delete'><input type='hidden' name='id' value='{escape(r['skey'])}'><button class='btn' style='padding:4px 10px;font-size:11px;background:#dc2626'>Delete</button></form></div>"
                    f"<div class='tcbox' data-k='{escape(r['skey'])}'></div></div>" for r in rows)
    chips = "<div class='tcchips' id='tcchips'><button type='button' class='on' data-g=''>All</button><button type='button' data-g='pending'>Pending</button><button type='button' data-g='taken'>Taken</button><button type='button' data-g='expired'>Expired</button></div>"
    return (TCHART_CSS + f"<div class='card' id='charts'><h2>Robot trade charts ({total})</h2><p class='muted' style='margin-bottom:4px'>A LIVE chart of every trade the robot takes, pending, taken or expired, drawn like MT5: drag to move it, scroll or pinch to zoom, double-click to go back to the trade. "
            f"It shows the major break of structure (red box), the BOS line, the liquidity (boxed candle and a dot), the entry order block, and the entry, stop loss and take profit lines. "
            f"Charts are removed automatically {days:g} day(s) after the trade last changed (<b>snapshot_days</b> in the settings below, 0 = do not keep charts).</p>"
            + chips
            + ("<form action='/admin/snap' method='post' onsubmit='return confirm(&quot;Delete all trade charts?&quot;)'><input type='hidden' name='op' value='all'><button class='btn' style='background:#dc2626'>Delete all charts</button></form>" if total else "")
            + (items or "<p class='muted' style='margin-top:10px'>No trades yet. A chart appears here as soon as the robot takes a trade.</p>") + (f"<p class='muted'>Showing the latest 30 of {total}.</p>" if total > 30 else "")
            + "</div><script>" + TCHART_JS + "(function(){var boxes=document.querySelectorAll('.tcbox');for(var i=0;i<boxes.length;i++){new TChart(boxes[i],{dataUrl:'/admin/chartdata/'+boxes[i].getAttribute('data-k'),pollMs:8000})}"
            "var ch=document.getElementById('tcchips');if(ch)ch.addEventListener('click',function(e){var g=e.target.getAttribute('data-g');if(g===null)return;var bs=ch.querySelectorAll('button');for(var i=0;i<bs.length;i++)bs[i].className=bs[i].getAttribute('data-g')===g?'on':'';"
            "var it=document.querySelectorAll('.tcitem');for(var j=0;j<it.length;j++)it[j].style.display=(!g||it[j].getAttribute('data-g')===g)?'':'none';window.dispatchEvent(new Event('resize'))})})();</script>")



# ======================================================================================================================
# RE-ENTRY AFTER A SWEEP (max ONE per original setup)
#   The stop loss was hit (the entry block was swept) but the PROTECTED high/low was NOT broken: the idea is still alive.
#   The robot then waits INSIDE the same zone for: a new Fair Value Gap, a new order block, a breaker block, or a 5 minute market structure shift in our direction.
#   The new stop loss goes just beyond THAT new block / gap. If the protected level is ever closed beyond, the idea is dead: no more re-entry on that side.
# ======================================================================================================================
def find_reentry(o, c, get5, s):
    """-> ('dead' | 'wait' | 'found', signal). `o` = the stopped-out signal, `c` = candles of its chart, get5() = 5 minute candles (only fetched when needed)."""
    bull, sym, prot = o["side"] == "BUY", o["symbol"], o.get("prot")
    if prot is None or o.get("ob_hi") is None or not c: return "dead", None
    bt = int(o.get("bos_t") or 0)
    if any(((x["c"] < prot) if bull else (x["c"] > prot)) for x in c if x["t"] >= bt): return "dead", None      # final invalidation
    si = next((i for i, x in enumerate(c) if x["t"] == int(o.get("last_t") or 0)), None)                          # the candle that hit the stop loss
    n = len(c)
    if si is None or n - 1 - si > int(s.get("reentry_bars") or 48): return "dead", None
    zlo, zhi = (prot, o["ob_hi"]) if bull else (o["ob_lo"], prot)                                                  # the zone: the entry block up to the protected level
    pip = pip_size(sym, o["entry"]); buf = float(s.get("sl_buffer_pips") or 0) * pip
    pct, tf = float(s.get("liq_inside_pct", 70)) / 100, o.get("tf") or s["timeframe"]
    near = lambda lo, hi: ((hi if bull else lo) if s["ob_entry"] == "top" else (hi + lo) / 2)
    cands = []
    def accept(rows, kind, lo, hi, fi, entry, sl, tf_):
        if inside_frac(lo, hi, zlo, zhi) < pct: return                                                            # must be inside the same zone
        risk = (entry - sl) if bull else (sl - entry)
        if risk <= 0: return
        rp = risk / pip
        if (float(s.get("max_sl_pips") or 0) and rp > float(s["max_sl_pips"])) or (float(s.get("min_sl_pips") or 0) and rp < float(s["min_sl_pips"])): return
        room = (o["tp"] - entry) if bull else (entry - o["tp"])
        if room <= 0 or room / risk < float(s.get("reentry_min_rr") or 1.5) - 1e-9: return
        aft = rows[fi + 1:]
        if ((any(x["l"] <= entry for x in aft) or rows[-1]["c"] <= entry) if bull else (any(x["h"] >= entry for x in aft) or rows[-1]["c"] >= entry)): return      # already tapped
        cands.append(dict(kind=kind, lo=lo, hi=hi, t=rows[fi]["t"], entry=entry, sl=sl, rr=round(room / risk, 2), tf=tf_, last_t=rows[-1]["t"], sl_pips=round(rp, 1)))
    for j in range(si + 2, n):                                                                                     # a) a new Fair Value Gap
        if bull and c[j - 2]["h"] < c[j]["l"]: lo, hi = c[j - 2]["h"], c[j]["l"]; accept(c, "fvg", lo, hi, j, near(lo, hi), lo - buf, tf)
        if not bull and c[j - 2]["l"] > c[j]["h"]: lo, hi = c[j]["h"], c[j - 2]["l"]; accept(c, "fvg", lo, hi, j, near(lo, hi), hi + buf, tf)
    for k in range(si, n - 1):                                                                                     # b) a new order block: an opposing candle that the next candle closes beyond
        x, nx = c[k], c[k + 1]
        if bull and x["c"] < x["o"] and nx["c"] > x["h"]: accept(c, "order_block", x["l"], x["h"], k + 1, near(x["l"], x["h"]), x["l"] - buf, tf)
        if not bull and x["c"] > x["o"] and nx["c"] < x["l"]: accept(c, "order_block", x["l"], x["h"], k + 1, near(x["l"], x["h"]), x["h"] + buf, tf)
    m = next((i for i in range(si, n) if ((c[i]["c"] > o["ob_hi"]) if bull else (c[i]["c"] < o["ob_lo"]))), None)   # c) a breaker block: the swept block, once price closes back through it
    if m is not None:
        ext = min(x["l"] for x in c[si:m + 1]) if bull else max(x["h"] for x in c[si:m + 1])
        accept(c, "breaker", o["ob_lo"], o["ob_hi"], m, near(o["ob_lo"], o["ob_hi"]), (min(o["ob_lo"], ext) - buf) if bull else (max(o["ob_hi"], ext) + buf), tf)
    for j in range(si - 1, 1, -1):                                                                                 # f) an EXISTING Fair Value Gap inside the zone that price has not filled yet
        if c[j]["t"] < int(o.get("leg_t") or 0): break
        if bull and c[j - 2]["h"] < c[j]["l"]: lo, hi = c[j - 2]["h"], c[j]["l"]
        elif not bull and c[j - 2]["l"] > c[j]["h"]: lo, hi = c[j]["h"], c[j - 2]["l"]
        else: continue
        if (hi <= o["ob_lo"] and lo >= prot) if bull else (lo >= o["ob_hi"] and hi <= prot):
            accept(c, "old_fvg", lo, hi, si, near(lo, hi), (lo - buf) if bull else (hi + buf), tf); break
    lt_, bt_ = int(o.get("leg_t") or 0), int(o.get("bos_t") or 0)                                                  # e) an EXISTING untapped order block further back, between the stopped block and the protected level
    for k in range(len(c) - 1, -1, -1):
        x = c[k]
        if x["t"] < lt_ or x["t"] >= bt_ or k >= si: continue
        if bull and x["c"] < x["o"] and x["l"] < o["ob_lo"] and x["h"] <= o["ob_hi"] and x["l"] >= prot - 1e-12: accept(c, "old_order_block", x["l"], x["h"], si, near(x["l"], x["h"]), x["l"] - buf, tf); break
        if not bull and x["c"] > x["o"] and x["h"] > o["ob_hi"] and x["l"] >= o["ob_lo"] and x["h"] <= prot + 1e-12: accept(c, "old_order_block", x["l"], x["h"], si, near(x["l"], x["h"]), x["h"] + buf, tf); break
    try: c5 = get5()                                                                                               # d) a 5 minute market structure shift back in our direction
    except Exception: c5 = None
    if c5 and len(c5) >= 40:
        g = find_setup(c5, dict(s, swing_n=2, min_leg_atr=0, min_bos=1), "5m", None, sym, o["tp"])
        if g and g["side"] == o["side"] and g["bos_t"] > c[si]["t"] and inside_frac(g["ob_lo"], g["ob_hi"], zlo, zhi) >= pct:
            cands.append(dict(kind="mss_5m", lo=g["ob_lo"], hi=g["ob_hi"], t=g["bos_t"], entry=g["entry"], sl=g["sl"], rr=g["rr"], tf="5m", last_t=g["last_t"], sl_pips=g.get("sl_pips")))
    if not cands: return "wait", None
    d = max(cands, key=lambda x: x["t"])                                                                           # the newest valid one
    h = datetime.datetime.utcnow().hour
    sig = {k: o[k] for k in ("grade", "bias", "tp_limit") if k in o}
    sig.update(side=o["side"], entry=d["entry"], sl=d["sl"], tp=o["tp"], rr=d["rr"], liquidity="reentry_" + d["kind"], session="Asia" if h < 7 else "London" if h < 13 else "NY" if h < 21 else "Late",
               bos_run=1, key=f"{o['key']}-re", tf=d["tf"], mitigated_pct=0, last_t=d["last_t"], bos_t=d["t"], ob_hi=d["hi"], ob_lo=d["lo"], ob_t=d["t"], prot=prot,
               swing_n=(2 if d["tf"] == "5m" else int(o.get("swing_n") or s["swing_n"])), sl_pips=d.get("sl_pips"), re_of=o["key"], score=0)
    return "found", sig

def try_reentry(o, s, candles):
    """One stopped-out signal: make its single re-entry signal if one is ready. Returns 1 if a signal was made."""
    sym, tf, k = o["symbol"], o.get("tf") or s["timeframe"], o.get("key")
    mk = f"red-{sym}-{k}"
    if db.one("SELECT 1 AS x FROM seen WHERE k=:k", k=mk): return 0                                                  # already re-entered, or decided it is dead
    if db.one("SELECT 1 AS x FROM orders WHERE platform='SIGNAL' AND symbol=:s AND outcome IN ('PENDING','OPEN')", s=sym): return 0     # one live signal per market
    c = candles(sym, tf)
    st, sig = find_reentry(o, c, lambda: candles(sym, "5m"), s)
    if st == "dead": seen_new(mk); return 0
    if st != "found": return 0
    rows = c if sig["tf"] == tf else candles(sym, sig["tf"])
    if not setup_fresh(sig, rows, s, strict=True)[0]: return 0
    if not seen_new(mk): return 0
    skey = uuid.uuid4().hex[:16]
    db.x("INSERT INTO sigs (skey, data, created) VALUES (:k, :d, :c)", k=skey, d=json.dumps({"sym": sym, "sig": sig}), c=now_s())
    so = {"id": uuid.uuid4().hex[:12], "time": now_s(), "account": 0, "platform": "SIGNAL", "symbol": sym, "size": 0, "risk_amount": 0, "outcome": "PENDING", "bars": 0, "status": "LIVE: signal", **sig}
    insert_order(so)
    el, ep = elig_sql(s)
    db.x(f"INSERT INTO jobs (kind, account_id, payload, status, tries, created) SELECT 'place', a.id, :skey, 'new', 0, :now FROM accounts a LEFT JOIN users u ON u.id = a.user_id "
         f"WHERE a.platform != 'EA' AND {el} AND a.id IN (SELECT o2.account_id FROM orders o2 WHERE o2.symbol = :sym AND o2.sig_key IN (:k1, :k2) AND o2.platform NOT IN ('SIGNAL', 'SYSTEM', 'EA'))",
         skey=skey, now=now_s(), sym=sym, k1=str(k), k2=str(k) + "|rr", **ep)                                          # only the accounts that took the original trade get the second chance
    try: snap_save(so, rows)
    except Exception: pass
    return 1

_SCAN_BUSY = threading.Lock()
def run_scan():
    if not _SCAN_BUSY.acquire(blocking=False): return 0
    try:
        s = load_settings(fresh=True); cache = {}; made = 0
        def candles(sym, tf):
            if (sym, tf) not in cache:
                cache[(sym, tf)] = fetch_candles_for(sym, tf)
                _CC[(sym, tf)] = (time.time(), cache[(sym, tf)])      # shared with the order placers: no second download
            return cache[(sym, tf)]
        for r in db.q("SELECT * FROM orders WHERE platform='SIGNAL' AND outcome IN ('PENDING','OPEN')"):         # 1) resolve open signals
            o = row_to_order(r)
            try:
                cs_ = candles(o["symbol"], o.get("tf") or s["timeframe"]); changed = resolve(o, cs_)
                if o["outcome"] == "PENDING":                       # price has not tapped the zone yet
                    ok_, why_ = setup_fresh(o, cs_, s)
                    if not ok_: expire_stale(o, why_); changed = True
                if changed: save_order_state(o); finish_signal(o); snap_save(o, cs_)
            except Exception: pass
        for r in db.q("SELECT * FROM orders WHERE platform='PAPER' AND sig_key LIKE '%|rr' AND outcome IN ('PENDING','OPEN')"):    # 1b) paper orders with their own bigger reward are resolved one by one
            o = row_to_order(r)
            try:
                cs_ = candles(o["symbol"], o.get("tf") or s["timeframe"]); changed = resolve(o, cs_)
                if o["outcome"] == "PENDING":
                    ok_, why_ = setup_fresh(o, cs_, s)
                    if not ok_: expire_stale(o, why_); changed = True
                if changed: save_order_state(o)
            except Exception: pass
        if s.get("reentry", True):                                                                                # 1c) second chance after a sweep (protected level still unbroken)
            for r in db.q("SELECT * FROM orders WHERE platform='SIGNAL' AND outcome='LOSS' AND created > :c ORDER BY created DESC LIMIT 40", c=str(datetime.datetime.utcnow() - datetime.timedelta(days=3))):
                o = row_to_order(r)
                if o.get("re_of") or "prot" not in o: continue                                                       # a re-entry never gets another re-entry
                try: made += try_reentry(o, s, candles)
                except Exception as e: record_data_error(o["symbol"], "re-entry", e)
        for sym in s["symbols"]:                                                                                  # 2) new setups: read every chart, keep the better one
            found = []
            for tf in s["timeframes"]:
                try: sg_ = find_setup(candles(sym, tf), s, tf, None, sym)
                except Exception as e: record_data_error(sym, tf, e); continue
                if not sg_: continue
                try: ok_, gr_, b_ = grade_setup(sg_, s, sym)
                except Exception as e: record_data_error(sym, "bias", e); continue
                if ok_:                                                                      # setups against the bigger picture are skipped
                    sg_["grade"], sg_["bias"] = gr_, {k: v for k, v in b_.items() if k != "tipD"}
                    if gr_ == "A+" and b_.get("tipD") is not None: sg_["tp_limit"] = b_["tipD"]
                    found.append(sg_)
            if not found: continue
            sig = max(found, key=lambda x: TF_MS(x["tf"]))                                                         # both charts have a setup: the higher timeframe (1 hour) is the trade; 30 minutes only when the 1 hour has none
            lo_ = [x for x in found if x["tf"] != sig["tf"] and TF_MS(x["tf"]) < TF_MS(sig["tf"]) and x["side"] == sig["side"]]
            if lo_: sig = merge_htf(sig, min(lo_, key=lambda x: TF_MS(x["tf"])), s)                                # ... but the ENTRY and the STOP LOSS come from the 30 minute chart
            if db.one("SELECT 1 AS x FROM orders WHERE platform='SIGNAL' AND symbol=:s AND outcome IN ('PENDING','OPEN')", s=sym): continue    # one live signal per market
            if not seen_new(f"{sym}-{sig['key']}"): continue
            if any(str(sig[f]) in s["filters"].get(f, []) for f in s["filters"] if f in sig): continue            # learned filters
            skey = uuid.uuid4().hex[:16]
            db.x("INSERT INTO sigs (skey, data, created) VALUES (:k, :d, :c)", k=skey, d=json.dumps({"sym": sym, "sig": sig}), c=now_s())
            so = {"id": uuid.uuid4().hex[:12], "time": now_s(), "account": 0, "platform": "SIGNAL", "symbol": sym, "size": 0, "risk_amount": 0, "outcome": "PENDING", "bars": 0,
                  "status": "LIVE: signal", **sig}
            insert_order(so)
            try: snap_save(so, candles(sym, sig["tf"]))                                                            # the picture of this trade
            except Exception: pass
            el, ep = elig_sql(s)
            plat = "a.platform IN ('MT4','MT5','PAPER')" if is_fx(sym) else "a.platform NOT IN ('MT4','MT5','EA')"
            db.x(f"INSERT INTO jobs (kind, account_id, payload, status, tries, created) SELECT 'place', a.id, :skey, 'new', 0, :now FROM accounts a "
                 f"LEFT JOIN users u ON u.id = a.user_id WHERE {plat} AND {el} AND ((COALESCE(a.pairs, '') = '' AND a.platform != 'PAPER') OR (',' || a.pairs || ',') LIKE :pat)", skey=skey, now=now_s(), pat="%," + sym + ",%", **ep)       # fan-out: one statement for all accounts
            made += 1
        try: kv_set("last_scan", now_s())                          # heartbeat: shown on the paper trading page and in admin
        except Exception: pass
        return made
    finally: _SCAN_BUSY.release()

def merge_htf(h, l, s):
    """Both the 1 hour and the 30 minute chart have a setup in the same direction: the trade is the 1 hour idea (its break of structure, protected level and present low/high target),
    entered from the 30 minute order block with the stop loss just beyond that block. The result lives on the 30 minute candles (that is where the entry is watched)."""
    bull = h["side"] == "BUY"
    risk = abs(l["entry"] - l["sl"])
    if risk <= 0: return h
    ext = h.get("ext", h["tp"]); room = (ext - l["entry"]) if bull else (l["entry"] - ext)
    min_rr, max_rr = float(s.get("min_rr") or 1.0), max(float(s.get("min_rr") or 1.0), float(s.get("bot_max_rr") or 3.0))
    strong = bool(h.get("strong", True)) and bool(l.get("strong", True))
    raw = max(0.0, room / risk)
    rrf = min_rr if not strong else min(max_rr, max(min_rr, raw))
    tp = ext if (strong and min_rr <= raw <= max_rr) else (l["entry"] + rrf * risk if bull else l["entry"] - rrf * risk)
    return dict(l, tp=tp, rr=round(rrf, 2), ext=ext, strong=strong, htf="1h", grade=h.get("grade"), bias=h.get("bias"), score=h.get("score", l.get("score")),
                **({"tp_limit": h["tp_limit"]} if h.get("tp_limit") is not None else {}))

def job_place(j, s):
    r = db.one("SELECT * FROM accounts WHERE id=:i", i=j["account_id"])
    if not r: return
    if r.get("paused") or not sub_state(r, s, user_of(r))[0]: return                                    # blocked / unpaid since the job was queued
    sg = db.one("SELECT data FROM sigs WHERE skey=:k", k=j["payload"])
    if not sg: return
    d = json.loads(sg["data"]); sym, sig = d["sym"], d["sig"]
    if db.one("SELECT 1 AS x FROM orders WHERE account_id=:a AND sig_key IN (:k, :k2) AND symbol=:s", a=r["id"], k=sig["key"], k2=sig["key"] + "|rr", s=sym): return          # retry-safe: never place twice
    if db.one("SELECT 1 AS x FROM orders WHERE account_id=:a AND symbol=:s AND outcome IN ('PENDING','OPEN')", a=r["id"], s=sym): return
    if daily_cap_reached(r["id"], s): return                      # max trades per day reached
    if sym not in acc_pairs(r, s): return                          # this person did not ask the robot to trade this pair
    if r["platform"] == "PAPER" and paper_balance(r) < float(s["paper_min_balance"]): return      # practice money has run out: the person must reset first
    cs_ = candles_cached(sym, sig["tf"])                  # one shared fetch for all accounts
    if not setup_fresh(sig, cs_, s, strict=True)[0]: return        # zone mitigated / new BOS formed: forget this order
    o_ = place_order(acc_full(r), sig, sym, s); insert_order(o_)
    if over_cap_after_insert(o_, s): cancel_order_now(o_, "daily trade limit reached")

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
    try: db.x("DELETE FROM snaps WHERE updated < :t", t=c(max(0.0, float(load_settings(fresh=True).get("snapshot_days") or 0))))        # 0 days = nothing is kept
    except Exception as ex: print("picture clean-up failed:", str(ex)[:100])
    db.x("DELETE FROM seen WHERE t < :t", t=c(30)); db.x("DELETE FROM sigs WHERE created < :t", t=c(7)); db.x("DELETE FROM jobs WHERE status='failed' AND created < :t", t=c(3))

def worker_main():
    init_app()
    wid, pool, last = uuid.uuid4().hex[:8], ThreadPoolExecutor(int(os.environ.get("JOB_THREADS", "16"))), 0.0
    print("worker", wid, "started")
    while True:
        try:
            if take_lease("scan", wid, 120) and time.time() - last >= SCAN_EVERY:
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
.links a{white-space:nowrap}
.links a.hot{background:linear-gradient(135deg,var(--brand),var(--brand2));color:#fff;box-shadow:0 4px 12px rgba(37,99,235,.3)}
.links a.hot:hover{color:#fff;filter:brightness(1.08)}
@media(max-width:760px){.nav{padding:10px 12px}.links{display:flex;width:100%;gap:6px;overflow-x:auto;padding-bottom:4px;-webkit-overflow-scrolling:touch;scrollbar-width:none}.links::-webkit-scrollbar{display:none}.links a{margin-left:0;flex:none;background:#f1f5f9}}
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
.foot{text-align:center;color:var(--mut);font-size:12px;padding:30px 20px 90px}
.instbar{position:fixed;left:12px;right:12px;bottom:12px;z-index:100;display:none;align-items:center;gap:10px;background:#0f172a;color:#fff;border-radius:16px;padding:12px 14px;box-shadow:0 12px 40px rgba(0,0,0,.35);max-width:560px;margin:0 auto}
.instbar b{flex:1;font-size:14px}
.instbar .btn{padding:9px 14px;box-shadow:none;white-space:nowrap}
.instbar .x{background:none;border:0;color:#94a3b8;font-size:22px;cursor:pointer;padding:0 4px;line-height:1}
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
    return jsonify({"id": "/", "name": name, "short_name": name[:12], "start_url": "/", "scope": "/", "display": "standalone", "display_override": ["standalone"], "orientation": "portrait",
                    "prefer_related_applications": False, "background_color": "#0b1220", "theme_color": "#2563eb",
                    "icons": [{"src": f"/icon-{z}.png", "sizes": f"{z}x{z}", "type": "image/png", "purpose": p} for z in (192, 512) for p in ("any", "maskable")]}), 200, {"Content-Type": "application/manifest+json"}

@app.route("/sw.js")
def service_worker():
    js = "self.addEventListener('install',function(){self.skipWaiting()});self.addEventListener('activate',function(e){e.waitUntil(self.clients.claim())});" \
         "self.addEventListener('fetch',function(e){e.respondWith(fetch(e.request).catch(function(){return new Response('You are offline. Reconnect to open the app.',{headers:{'Content-Type':'text/plain'}})}))});"
    return js, 200, {"Content-Type": "application/javascript", "Service-Worker-Allowed": "/", "Cache-Control": "no-cache"}

# Install prompt: ONE banner at the bottom of every page (the only install button). It hides itself once the app is installed / opened as an app.
PWA_JS = ("<div class='instbar' id='instbar'><b>📲 Install the app on your phone</b><button class='btn instbtn' type='button'>INSTALL</button><button class='x' id='instx' type='button' aria-label='Close'>×</button></div>"
          "<script>if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js');var dp=null,bar=document.getElementById('instbar');"
          "function inApp(){return (window.matchMedia&&matchMedia('(display-mode: standalone)').matches)||navigator.standalone===true}"
          "function hideAll(){document.querySelectorAll('.instbtn').forEach(function(e){e.style.display='none'});if(bar)bar.style.display='none'}"
          "var closed=false;try{closed=sessionStorage.getItem('instx')==='1'}catch(e){}"
          "if(inApp())hideAll();else if(bar&&!closed)bar.style.display='flex';"
          "addEventListener('beforeinstallprompt',function(e){e.preventDefault();dp=e});"
          "addEventListener('appinstalled',hideAll);"
          "document.getElementById('instx').onclick=function(){bar.style.display='none';try{sessionStorage.setItem('instx','1')}catch(e){}};"
          "document.querySelectorAll('.instbtn').forEach(function(b){b.onclick=function(x){x.preventDefault();"
          "if(dp){dp.prompt();dp=null}else{alert('To install as a real app: open your browser menu (the three dots) and tap Install app. Do NOT choose Add to Home screen or Create shortcut, because a shortcut only opens the browser. If you already added a shortcut, delete it first. On iPhone use Safari: tap Share, then Add to Home Screen.')}}});</script>")

LOGO_SVG = "<svg width='18' height='18' viewBox='0 0 18 18'><rect x='2' y='8' width='3' height='8' rx='1' fill='#fff'/><rect x='7.5' y='3' width='3' height='13' rx='1' fill='#fff'/><rect x='13' y='6' width='3' height='10' rx='1' fill='#fff' opacity='.85'/></svg>"


# ---------------- how to connect (shown on /setup) ----------------
CONNECT_GUIDE = """
<style>
.cg details{border:1px solid var(--line);border-radius:14px;padding:12px 14px;margin:10px 0;background:#fff}
.cg summary{cursor:pointer;font-weight:700;line-height:1.4}
.cg ol{margin:8px 0 0 20px;line-height:1.8;font-size:14px;color:#334155}
.cg p{font-size:14px;color:#334155;margin:8px 0}
.cg table{width:100%;border-collapse:collapse;font-size:13px;margin:10px 0}
.cg td,.cg th{padding:8px;border-top:1px solid #f1f5f9;text-align:left;vertical-align:top}
.cg .fill{background:#f8fafc;border:1px solid var(--line);border-radius:10px;padding:10px 12px;font-size:13px;line-height:1.7;margin:8px 0;color:#334155}
.cg .tag{display:inline-block;border-radius:999px;padding:2px 10px;font-size:11px;font-weight:800;color:#fff;margin-left:6px;white-space:nowrap}
.cg .free{background:#16a34a}.cg .paid{background:#b45309}
.plathint{grid-column:1/-1;background:#eff6ff;border:1px solid #bfdbfe;color:#1e3a8a;border-radius:12px;padding:10px 12px;font-size:13px;line-height:1.6}
</style>
<div class='card cg' style='max-width:1100px'>
<h2>Connect your real account</h2>
<p class='muted'>Pick ONE way to connect. Tap an option below to see the exact steps and what to type in each box. Not ready to connect anything? <a href='/paper'>Try paper trading</a> first: it is free.</p>
<div style='overflow-x:auto'><table>
<tr><th>Option</th><th>What it costs to connect</th><th>Computer must stay on?</th></tr>
<tr><td><b>1. Your own computer (EA)</b></td><td>Free <span class='tag free'>FREE</span></td><td>Yes (or a rented VPS)</td></tr>
<tr><td><b>2. Cloud link (MetaApi)</b></td><td>MetaApi charges its own fee <span class='tag paid'>PAID TO METAAPI</span></td><td>No, works 24/7</td></tr>
<tr><td><b>3. Crypto exchange key</b></td><td>Free to connect (exchange trading fees apply) <span class='tag free'>FREE</span></td><td>No, works 24/7</td></tr>
</table></div>
<p class='muted'><b>Our plan:</b> <a href='/paper'>paper trading</a> is free forever. @@COST@@</p>

<details><summary>Option 1 · Use your own computer (EA) <span class='tag free'>FREE</span></summary>
<p>A small free program (the EA) runs inside MetaTrader 5 on your own computer and places the robot's trades on your broker account. Your computer and MetaTrader 5 must stay switched on and online while you want the robot to trade. Many people rent a cheap VPS for this.</p>
<div class='fill'><b>What to fill in the form below</b><br>Platform: <b>EA</b> · Chart: choose 30 minutes or 1 hour <br><b>Leave EMPTY:</b> API key, API secret, Extra, MetaApi region, Balance (your EA key is created for you)</div>
<ol>
<li>Choose Platform <b>EA</b> in the form below, pick your chart, and press <b>Save</b>. The next page shows your EA key and the EA code. Keep the key private.</li>
<li>Install <b>MetaTrader 5</b> and log in to your broker account. Use the same broker account you want the robot to trade.</li>
<li>In MT5 press <b>F4</b> (MetaEditor), then File, New, Expert Advisor (template). Name it ISIG_EA, select all, paste the code, press <b>F7</b>. It should say 0 errors.</li>
<li>In MT5 go to Tools, Options, Expert Advisors. Tick <b>Allow WebRequest</b> and add this website's address (it is shown on the key page).</li>
<li>Open a chart on the <b>same timeframe</b> you chose, drag ISIG_EA onto it, tick Allow Algo Trading, press OK, and switch on the <b>Algo Trading</b> button at the top.</li>
<li>Open <b>My account</b>. It should say <b>Connected</b>. Keep MT5 open.</li>
</ol></details>

<details><summary>Option 2 · Cloud link with MetaApi (works even when your computer is off) <span class='tag paid'>PAID TO METAAPI</span></summary>
<p>MetaApi (metaapi.cloud) is a separate cloud service that logs in to your MetaTrader 4 or 5 broker account for the robot, so it can trade 24/7 without your computer. It works with most MT4/MT5 brokers. MetaApi charges its own fee to you, so check the current plans on their website. On top of that, our plan above applies to real accounts.</p>
<div class='fill'><b>What to fill in the form below</b><br>Platform: <b>MT5</b> or <b>MT4</b> (the same type as your broker account) · Chart: choose 30 minutes or 1 hour<br>
API key / login: your <b>MetaApi API token</b><br>API secret: type any text (for example <b>x</b>)<br>Extra: your <b>MetaApi Account ID</b><br>MetaApi region: for example <b>new-york</b> or <b>london</b> (the region of your MetaApi account)<br>
<b>Leave EMPTY:</b> Balance.</div>
<ol>
<li>Create an account on <b>metaapi.cloud</b> and sign in.</li>
<li>Add your broker account inside MetaApi (your MT4/MT5 login number, password and broker server name). Wait until it shows as connected.</li>
<li>Copy your <b>MetaApi API token</b> from your MetaApi dashboard (look for the API access / token page).</li>
<li>Copy the <b>Account ID</b> of the broker account you added, and note its <b>region</b>.</li>
<li>Fill the form below as listed above and press <b>Save</b>, then press <b>Test connection</b> on your account page. It shows your balance when everything is right.</li>
</ol></details>

<details><summary>Option 3 · Crypto exchange (Binance, Bybit, OKX, Bitget) <span class='tag free'>FREE</span></summary>
<p>You create an API key on the exchange and give it to the robot. Orders go straight to your exchange account and work 24/7. Connecting is free (the exchange's normal trading fees apply).</p>
<div class='fill'><b>What to fill in the form below</b><br>Platform: your exchange · Chart: choose 30 minutes or 1 hour<br>
API key and API secret: from the exchange<br>Extra: the <b>passphrase</b> for OKX and Bitget. <b>Leave empty</b> for Binance and Bybit<br><b>Leave EMPTY:</b> MetaApi region, Balance</div>
<ol>
<li>On the exchange (or its testnet for practice) create a new <b>API key</b>.</li>
<li>Allow <b>reading</b> and <b>trading</b> only. Keep <b>withdrawals switched OFF</b>.</li>
<li>Paste the key (and secret, and passphrase if asked) in the form below and press <b>Save</b>.</li>
<li>Open your account page and press <b>Test connection</b>.</li>
</ol></details>
<p class='muted' style='margin-top:10px'><b>Not sure which to choose?</b> Want to practise for free: use <a href='/paper'>paper trading</a>. Want to trade your own broker account from your computer: Option 1. Want it running 24/7 without your computer: Option 2 (MT4/MT5) or Option 3 (crypto).</p>
</div>
<script>
document.addEventListener('DOMContentLoaded',function(){
var sel=document.querySelector('select[name=platform]');if(!sel)return;
var box=document.createElement('div');box.className='plathint';sel.parentNode.insertAdjacentElement('afterend',box);
var T={EA:'<b>EA (your computer):</b> leave API key, API secret, Extra and MetaApi region <b>empty</b>. Just choose your chart and press Save. Your key is shown on the next page.',
MT5:'<b>MetaApi (MT5):</b> API key = your MetaApi token. API secret = type any text (for example x). Extra = your MetaApi Account ID. Region = for example new-york. Leave Balance empty.',
MT4:'<b>MetaApi (MT4):</b> API key = your MetaApi token. API secret = type any text (for example x). Extra = your MetaApi Account ID. Region = for example new-york. Leave Balance empty.',
BINANCE:'<b>Binance:</b> fill API key and API secret. Leave Extra and MetaApi region <b>empty</b>. ',
BYBIT:'<b>Bybit:</b> fill API key and API secret. Leave Extra and MetaApi region <b>empty</b>. ',
OKX:'<b>OKX:</b> fill API key, API secret and put the passphrase in Extra. Leave MetaApi region <b>empty</b>.',
BITGET:'<b>Bitget:</b> fill API key, API secret and put the passphrase in Extra. Leave MetaApi region <b>empty</b>.'};
function upd(){box.innerHTML=T[sel.value]||'';box.style.display=T[sel.value]?'block':'none'}
sel.addEventListener('change',upd);upd()});
</script>
"""

def _connect_guide(s):
    if s.get("paid_mode"):
        cost = ("Connected accounts need the monthly plan" + (f" after a {int(s['trial_days'])}-day free trial" if int(s.get('trial_days') or 0) > 0 else "") + ". " + str(escape(price_line(s))))
    else:
        cost = "Connected accounts are free for now. A monthly plan may be introduced later (paper trading always stays free)."
    return CONNECT_GUIDE.replace("@@COST@@", str(cost))

# ---------------- welcome prompt (shown ONCE, right after sign-up, only when "Require sign-up" is ON) ----------------
WELCOME_TPL = """
<style>
#wlc{position:fixed;inset:0;z-index:300;display:flex;align-items:center;justify-content:center;padding:14px;background:rgba(2,6,23,.72);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);animation:wlfade .35s ease both}
@keyframes wlfade{from{opacity:0}to{opacity:1}}
.wl{background:#fff;border-radius:24px;max-width:520px;width:100%;max-height:92vh;overflow:auto;box-shadow:0 40px 120px rgba(0,0,0,.55);animation:wlpop .5s cubic-bezier(.2,.9,.3,1.2) both}
@keyframes wlpop{from{opacity:0;transform:translateY(24px) scale(.96)}to{opacity:1;transform:none}}
.wl-top{position:relative;overflow:hidden;background:radial-gradient(500px 220px at 80% -20%,#4338ca 0,transparent 60%),linear-gradient(135deg,#0b1220,#13214a);padding:22px 20px 14px;text-align:center;color:#fff}
.wl-top h2{font-size:22px;font-weight:800;margin:8px 0 2px;letter-spacing:-.01em}.wl-top p{color:#cbd5e1;font-size:13px;margin:0}
.wl-shield{width:96px;height:112px;margin:0 auto;display:block;animation:wlbob 4s ease-in-out infinite;filter:drop-shadow(0 12px 28px rgba(59,130,246,.55))}
@keyframes wlbob{50%{transform:translateY(-6px)}}
.wl-ring{transform-box:fill-box;transform-origin:center;animation:wlring 2.4s ease-out infinite}
@keyframes wlring{from{transform:scale(.8);opacity:.7}to{transform:scale(1.5);opacity:0}}
.wl-tick{stroke-dasharray:80;stroke-dashoffset:80;animation:wltick 1s .5s ease forwards}
@keyframes wltick{to{stroke-dashoffset:0}}
.wl-c{position:absolute;bottom:0;width:10px;border-radius:2px;opacity:.85;animation:wlrise 3.2s ease-in-out infinite}
@keyframes wlrise{50%{transform:translateY(-14px)}}
.wl-body{padding:16px 20px 20px}
.wl-step{display:flex;gap:12px;align-items:flex-start;margin:0 0 12px}
.wl-n{flex:none;width:30px;height:30px;border-radius:10px;background:linear-gradient(135deg,var(--brand),var(--brand2));color:#fff;display:grid;place-items:center;font-weight:800;font-size:14px}
.wl-step b{display:block;font-size:14px;color:#0f172a}.wl-step span{font-size:13px;color:#475569;line-height:1.5}
.wl-risk{background:#fffbeb;border:1px solid #fde68a;border-radius:14px;padding:11px 13px;font-size:12px;line-height:1.55;color:#78350f;margin:14px 0 12px}
.wl-risk b{color:#92400e}
.wl-ck{display:flex;gap:10px;align-items:flex-start;font-size:13px;font-weight:600;color:#0f172a;cursor:pointer;margin-bottom:12px}
.wl-ck input{margin-top:3px;width:17px;height:17px;flex:none}
.wl .btn{width:100%;text-align:center;font-size:15px}
.wl .wl-off{opacity:.45;cursor:not-allowed;box-shadow:none;transform:none}
</style>
<div id='wlc' role='dialog' aria-modal='true' aria-labelledby='wlt'><div class='wl'>
<div class='wl-top'>
<svg class='wl-shield' viewBox='0 0 120 140' aria-hidden='true'><defs><linearGradient id='wlg' x1='0' y1='0' x2='1' y2='1'><stop offset='0' stop-color='#3b82f6'/><stop offset='1' stop-color='#8b5cf6'/></linearGradient></defs>
<path class='wl-ring' d='M60 8 L108 26 V68 C108 100 86 124 60 134 C34 124 12 100 12 68 V26 Z' fill='none' stroke='#60a5fa' stroke-width='2'/>
<path d='M60 8 L108 26 V68 C108 100 86 124 60 134 C34 124 12 100 12 68 V26 Z' fill='url(#wlg)'/>
<path d='M60 20 L98 34 V68 C98 94 80 114 60 122 C40 114 22 94 22 68 V34 Z' fill='rgba(255,255,255,.12)'/>
<path class='wl-tick' d='M40 70 L56 86 L84 52' fill='none' stroke='#fff' stroke-width='9' stroke-linecap='round' stroke-linejoin='round'/></svg>
<h2 id='wlt'>Welcome to @@NAME@@</h2><p>Your account is ready. Here is the safest way to begin.</p>
</div>
<div class='wl-body'>
<div class='wl-step'><div class='wl-n'>1</div><div><b>Start with paper trading</b><span>Press START on the Paper Trading page and watch the robot trade a practice account on the real market. It is free and nothing real is at risk.</span></div></div>
<div class='wl-step'><div class='wl-n'>2</div><div><b>Build trust from the results</b><span>Your own paper performance is saved on your page. Take as long as you need.</span></div></div>
<div class='wl-step'><div class='wl-n'>3</div><div><b>Connect your own account when you are ready</b><span>Connecting an account is a paid step, and it is always your choice and your timing.</span></div></div>
<div class='wl-risk'><b>Important:</b> trading involves risk. You can lose some or all of the money you trade with, so only use money you can afford to lose. Past results do not guarantee future results, and nothing here is financial advice. Use exchange keys with trading permission only and withdrawals switched off.</div>
<label class='wl-ck'><input type='checkbox' id='wlck'><span>I have read this and I understand the risks of trading.</span></label>
<button type='button' class='btn wl-off' id='wlgo'>Start paper trading</button><button type='button' class='btn wl-off' id='wlgo2' style='margin-top:8px;background:#0f172a'>Connect my real account</button>
</div></div></div>
<script>
(function(){var o=document.getElementById('wlc'),c=document.getElementById('wlck'),b=document.getElementById('wlgo');
document.body.style.overflow='hidden';
var b2=document.getElementById('wlgo2');c.onchange=function(){b.className=c.checked?'btn':'btn wl-off';b2.className=c.checked?'btn':'btn wl-off';if(c.checked)b2.style.background='#0f172a'};b2.onclick=function(){if(!c.checked)return;o.style.display='none';document.body.style.overflow='';location.href='/setup'};
b.onclick=function(){if(!c.checked)return;o.style.display='none';document.body.style.overflow='';
location.href='/paper'};})();
</script>
"""

def _welcome_html(name): return WELCOME_TPL.replace("@@NAME@@", str(name))


def _pay_notice(where, val):
    """A payment result the person has not seen yet: shown ONCE as a banner, then marked as seen."""
    try:
        n = db.one(f"SELECT p.id, p.status, p.account_id FROM payments p WHERE {where} AND p.seen=0 AND p.status IN ('confirmed','rejected') ORDER BY p.created LIMIT 1", v=val)
        if not n: return ""
        db.x("UPDATE payments SET seen=1 WHERE id=:i", i=n["id"])
        if n["status"] == "confirmed":
            a = db.one("SELECT paid_until FROM accounts WHERE id=:i", i=n["account_id"]); until = escape(str(a["paid_until"])[:10]) if a and a["paid_until"] else ""
            return f"<div class='card' style='border:2px solid #16a34a;background:#f0fdf4;max-width:720px'><b style='color:#15803d'>✅ Payment confirmed.</b> Account #{n['account_id']} is now eligible to trade" + (f" until <b>{until}</b>." if until else ".") + "</div>"
        return f"<div class='card' style='border:2px solid #dc2626;background:#fef2f2;max-width:720px'><b style='color:#b91c1c'>Payment not confirmed.</b> We could not find your payment for account #{n['account_id']}. Please check the details and send them again, or contact support.</div>"
    except Exception: return ""

def layout(title, body):
    name = escape(S()["site_name"]); u = cur_user(); welcome = _welcome_html(name) if session.pop("welcome", None) else ""
    if u: body = _pay_notice("p.user_id=:v", u["id"]) + body
    nav = [("/", "HOME"), ("/setup", "CONNECT REAL ACCOUNT"), ("/paper", "PAPER TRADING"), ("/performance", "PERFORMANCE"), ("/orders", "SIGNALS"), ("/status", "MY ACCOUNT")]
    nav += [("/logout", "LOG OUT")] if u else [("/signup", "SIGN UP / LOG IN")]
    nav += [("/admin", "ADMIN")]
    links = "".join(f"<a href='{h}'{' class=\"hot\"' if h in ('/paper', '/setup') else ''}>{t}</a>" for h, t in nav)
    return (f"<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>{escape(title)} · {name}</title>"
            f"<link rel='preconnect' href='https://fonts.googleapis.com'><link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&display=swap' rel='stylesheet'>"
            f"<link rel='manifest' href='/manifest.webmanifest'><meta name='theme-color' content='#2563eb'><meta name='mobile-web-app-capable' content='yes'><meta name='apple-mobile-web-app-capable' content='yes'><meta name='apple-mobile-web-app-title' content='{name}'><link rel='apple-touch-icon' href='/icon-192.png'><style>{CSS}</style></head><body><div class='nav'><a class='logo' href='/'><span class='mark'>{LOGO_SVG}</span>{name}</a><div class='links'>{links}</div></div>{body}"
            f"<div class='foot'>© {datetime.datetime.utcnow().year} {name} · Trading involves risk. Past results do not guarantee future results. Not financial advice.</div>{welcome}{PWA_JS}</body></html>")

@app.after_request
def secure_headers(r):
    if request.path in ("/ping", "/healthz"): return r      # keep-alive pings stay tiny: no extra headers
    r.headers.update({"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer",
        "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
        "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'; object-src 'none'"})
    if os.environ.get("RENDER"): r.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    if request.path.startswith(("/admin", "/status", "/account", "/ea/download", "/save_account", "/login", "/signup")): r.headers["Cache-Control"] = "no-store"
    return r

@app.before_request
def csrf_guard():
    """Browsers always send Origin on form posts: refuse posts that come from another website. The site's own address may arrive as Host or X-Forwarded-Host."""
    if request.method == "POST" and request.path not in ("/ea/sync",):
        o = request.headers.get("Origin")
        if o:
            h = urllib.parse.urlparse(o).netloc.lower()
            ok = {request.host.lower(), (request.headers.get("Host") or "").lower(), (request.headers.get("X-Forwarded-Host") or "").split(",")[0].strip().lower()}
            if h not in ok:
                print("CSRF blocked: Origin", o, "| host", request.host, "| X-Forwarded-Host", request.headers.get("X-Forwarded-Host"))
                abort(403)

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

ADMIN_IDLE = int(os.environ.get("ADMIN_IDLE_SECONDS", "120"))      # admin is logged out after this many seconds without activity (default 2 minutes)
# Runs on the admin page only: the moment the page is hidden (you switch app, lock the phone, change tab) the browser tells the server to log the admin out.
ADMIN_JS = "<script>document.addEventListener('visibilitychange',function(){if(document.visibilityState==='hidden'){navigator.sendBeacon('/admin/logout')}});</script>"
def admin_ok():
    """True only while the admin is logged in AND active within the last ADMIN_IDLE seconds. Each admin page view renews the timer."""
    if session.get("admin") is not True: return False
    if time.time() - float(session.get("admin_t", 0)) > ADMIN_IDLE:
        session.pop("admin", None); session.pop("admin_t", None); return False
    session["admin_t"] = time.time(); return True

def admin_only(f):
    def w(*a, **k):
        if not admin_ok(): return redirect("/admin")
        return f(*a, **k)
    w.__name__ = f.__name__; return w

@app.route("/admin/snap", methods=["POST"])
@admin_only
def admin_snap():
    op = request.form.get("op", "")
    if op == "all": db.x("DELETE FROM snaps"); _flash("All trade pictures deleted.")
    elif op == "delete": db.x("DELETE FROM snaps WHERE skey=:k", k=request.form.get("id", "")); _flash("Picture deleted.")
    return redirect("/admin#charts")

@app.route("/admin/chartdata/<skey>")
@admin_only
def admin_chartdata(skey):
    """Everything one live trade chart needs: the candles (and the forming one), the live price, and the robot's trade with all its marks."""
    hdr = {"Cache-Control": "no-store"}
    r = db.one("SELECT * FROM snaps WHERE skey=:k", k=skey)
    if not r: return jsonify(error="This chart was deleted."), 404, hdr
    row = db.one("SELECT * FROM orders WHERE sig_key=:k AND symbol=:s ORDER BY CASE WHEN platform='SIGNAL' THEN 0 ELSE 1 END, created DESC LIMIT 1", k=r["sig_key"], s=r["symbol"])
    if not row: return jsonify(error="The trade behind this chart is no longer stored."), 404, hdr
    o, s = row_to_order(row), S(); sym = o["symbol"]; tf = o.get("tf") or s["timeframe"]
    try: closed = candles_cached(sym, tf)
    except Exception as e: return jsonify(error="Prices are unavailable right now: " + str(e)[:100]), 503, hdr
    forming, bid = None, None
    try: forming = live_market(sym, tf).get("forming")
    except Exception: pass
    try: bid = quote_cached(sym)[0]
    except Exception: pass
    closed = closed[-260:]
    ft = int(o.get("found_t") or o.get("bos_t") or 0); taken = None
    if o.get("outcome") in ("OPEN", "WIN", "LOSS") and "entry" in o:
        bull = o["side"] == "BUY"
        for x in closed:
            if x["t"] > ft and ((x["l"] <= o["entry"]) if bull else (x["h"] >= o["entry"])): taken = x["t"]; break
    keys = ("symbol", "tf", "side", "entry", "sl", "tp", "rr", "outcome", "status", "prot", "brk", "brk_t", "bos_t", "leg_t", "ob_t", "ob_hi", "ob_lo", "liq_t", "liquidity", "found_t", "last_t", "r", "sl_pips", "htf", "re_of", "grade", "time")
    ov = {k: o.get(k) for k in keys}; ov["tf"] = tf
    pp_ = pip_size(sym, o.get("entry") or 1); bull_ = o.get("side") == "BUY"; why = []
    if o.get("brk") is not None and o.get("prot") is not None:
        why.append(f"Major break of structure: a {'bullish' if bull_ else 'bearish'} candle CLOSED {'above' if bull_ else 'below'} {_fp(o['brk'])}" + (f", and the break is {o['leg_w']} candles wide" if o.get("leg_w") else "") + (", with a full body" if o.get("strong") else (" (the first candle was mostly a wick, a later candle also closed beyond, so the reward is only 1:%s)" % o.get("rr") if o.get("strong") is False else "")) + ". The protected " + ("low" if bull_ else "high") + f" is {_fp(o['prot'])} (the idea is dead if price closes beyond it).")
    else: why.append("This trade was taken by an older version of the robot that did not save every detail of the setup.")
    if o.get("liquidity") == "pattern": why.append("No liquidity here. This is a crypto pair, and the last 2-3 breaks of structure in this trend all did the same thing: price tapped an order block and carried on without sweeping liquidity. The robot followed that habit and entered the order block.")
    elif o.get("liq_t"): why.append("Liquidity: the " + ("last order block (opposing candle)" if o.get("liquidity") == "order_block" else "last pullback wick") + " right before the break, inside the break of structure. It is boxed on the chart.")
    else: why.append("No liquidity was saved with this trade: it was taken before the robot started recording it (older version).")
    if o.get("ob_hi") is not None: why.append(f"Entry: the next order block {'below' if bull_ else 'above'} the liquidity, inside the box. Entry {_fp(o['entry'])}, stop loss {_fp(o['sl'])} just beyond the block ({abs(o['sl'] - o['entry']) / pp_:.1f} pips).")
    why.append(f"Target: the present {'high' if bull_ else 'low'} kept between 1:1 and 1:3. Reward 1:{o.get('rr')} ({abs(o['tp'] - o['entry']) / pp_:.1f} pips).")
    if o.get("grade") or o.get("bias"): why.append(f"Bigger picture: {bias_text(o['bias']) if isinstance(o.get('bias'), dict) else (o.get('bias') or '')}" + (f" · grade {o['grade']}" if o.get("grade") else "") + (f" · {o['session']} session" if o.get("session") else ""))
    if o.get("htf"): why.append("The 1 hour chart had the idea; the entry and stop were taken from the 30 minute order block.")
    if o.get("re_of"): why.append("This is the ONE re-entry after the first stop loss: the protected level was never broken.")
    row_ = lambda x: [x["t"], x["o"], x["h"], x["l"], x["c"]]
    return jsonify(candles=[row_(x) for x in closed], forming=row_(forming) if forming else None, bid=bid, o=ov, taken_t=taken, pip=pip_size(sym, o.get("entry") or 1), tfms=TF_MS(tf), why=why, ts=int(time.time())), 200, hdr

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
        env = ""
        try: bal = float(a.get("balance") or 0)
        except ValueError: bal = 0.0
        te = parse_date(a.get("created"))
        row = dict(ref=ref, legacy_id=str(a["id"]), platform=plat, tf=a.get("tf") or s["timeframe"], env=env, server=("" if plat in ("BINANCE", "BYBIT", "OKX") else str(a.get("server", ""))[:40]),
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
    if ADMIN_PASSWORD:                      # a NEW value in Render's ADMIN_PASSWORD replaces the saved admin password (once per change)
        fp = hashlib.sha256((kdf_salt() + ADMIN_PASSWORD).encode()).hexdigest()
        if kv_get("admin_env_fp") != fp:
            set_admin_password(ADMIN_PASSWORD); kv_set("admin_env_fp", fp)
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
    SL_Y, EN_Y = 234, 200
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
  <span class='badge'><span class='dot'></span>TRADES YOUR REAL ACCOUNT · OR PRACTICE FOR FREE</span>
  <h1><span class='grad'>@@NAME@@</span><br>@@TAGLINE@@</h1>
  <p class='lead'>@@LEAD@@</p>
  <div class='cta'><a class='btn' href='@@CONNECT@@'>CONNECT YOUR REAL ACCOUNT →</a><a class='btn ghost' href='/paper'>TRY PAPER TRADING FREE</a><a class='btn ghost' href='/performance'>VIEW PERFORMANCE</a></div>
  @@RESULTS@@
  <div class='kpis' style='grid-template-columns:repeat(2,1fr);margin-top:12px'><div class='kpi'><b data-to='@@NPLAT@@'>0</b><span>Platforms supported</span></div><div class='kpi'><b>@@MODE@@</b><span>Mode</span></div></div>
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
 <div class='step' style='animation-delay:.35s'><div class='n'>04</div><h3>Risk managed</h3><p>Stop loss just beyond the entry order block, take profit at the next swing point (at least 1:1.5), sized to risk only @@RISK@@% per trade.</p></div>
</div></div>
<div class='sec' style='margin-top:30px'><h2>Two ways to use the robot</h2><p class='sub'>Trade your own money with the robot, or learn with practice money first.</p>
<div class='steps'>
 <div class='step' style='border:2px solid #2563eb'><div class='n'>💼</div><h3>Connect your real account</h3><p>Link your Binance, Bybit, OKX or Bitget account, your MT4 / MT5 broker account, or MT5 on your own computer (EA). The robot then places the trades for you, with the entry, stop loss and take profit set by its rules.</p><p style='margin-top:12px'><a class='btn' href='@@CONNECT@@'>CONNECT YOUR REAL ACCOUNT →</a></p></div>
 <div class='step'><div class='n'>🧪</div><h3>Practice with paper trading</h3><p>Free. The robot trades the real market with practice money so you can watch every trade and judge the results before you risk anything.</p><p style='margin-top:12px'><a class='btn' style='background:#0f172a' href='/paper'>START PAPER TRADING</a></p></div>
</div></div>
<div class='card'><h3>Strategy rules</h3><pre>@@STRAT@@</pre></div>
<div class='band'><h2>Ready to let the robot trade your real account?</h2><p>Connect your exchange or MT4 / MT5 account and the robot follows its rules for you, 24/7. Not sure yet? Paper trading is free.</p><a class='btn' href='@@CONNECT@@'>CONNECT YOUR REAL ACCOUNT →</a> <a class='btn' style='background:transparent;color:#fff;border:1px solid rgba(255,255,255,.6)' href='/paper'>TRY PAPER TRADING</a></div>
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
    rows_ = track_rows(None, 20000) if True else []
    def _cell(lab, q, bold=False):
        w = "font-weight:800;" if bold else ""
        return (f"<tr style='{w}border-top:1px solid rgba(255,255,255,.14)'><td style='padding:8px 6px;text-align:left'>{lab}</td><td>{q['trades']}</td><td>{q['win_rate']}%</td>"
                f"<td style='color:{'#22c55e' if q['total_r'] >= 0 else '#f87171'}'>{'+' if q['total_r'] >= 0 else ''}{q['total_r']}R</td><td>{'+' if q['avg_r'] >= 0 else ''}{q['avg_r']}R</td></tr>")
    try: pa, pb, pc = perf(rows_, "all", "30m"), perf(rows_, "all", "1h"), perf(rows_, "all", None)
    except Exception: pa = pb = pc = perf([], "all", None)
    results = ("<div class='kpi' style='margin-top:32px;padding:10px 12px'><div style='font-size:11px;color:#94a3b8;margin-bottom:6px'>RESULTS OF THE ROBOT, BY CHART</div>"
               "<table style='width:100%;border-collapse:collapse;font-size:13px;text-align:right'><tr style='color:#94a3b8;font-size:11px'><th style='text-align:left;padding:4px 6px'>Chart</th><th>Trades</th><th>Win rate</th><th>Total</th><th>Avg / trade</th></tr>"
               + _cell("30 minutes", pa) + _cell("1 hour", pb) + _cell("Both charts (average)", pc, True) + "</table></div>")
    rep = {"@@TFK@@": str(escape(s["timeframe"])), "@@NAME@@": str(escape(s["site_name"])), "@@MODE@@": str(escape(s["mode"])).upper(), "@@RR@@": str(s["rr"]), "@@RISK@@": str(s["risk_pct"]),
           "@@TF@@": str(escape(("chart you choose (" + " or ".join({"30m": "30-minute", "1h": "1-hour"}.get(t, t) for t in s["timeframes"]) + ")") if len(s["timeframes"]) > 1 else ({"30m": "30-minute", "1h": "1-hour"}.get(s["timeframes"][0], s["timeframes"][0]) + " chart"))),
           "@@RESULTS@@": results, "@@NPLAT@@": str(len(s["platforms"])), "@@CHART@@": _hero_chart(s["rr"]),
           "@@META@@": str(escape(f"{' / '.join(s['timeframes'])} · {', '.join(s['symbols'])}")), "@@PLATS@@": "".join(f"<span>{escape(x)}</span>" for x in s["platforms"]) * 3,
           "@@CONNECT@@": "/setup" if cur_user() else "/signup?next=/setup", "@@STRAT@@": str(escape(s["strategy_text"])), "@@TAGLINE@@": str(escape(s["tagline"])), "@@LEAD@@": str(escape(s["lead"]))}
    html = HOME_TPL
    for k, v in rep.items(): html = html.replace(k, v)
    return layout("Home", html)

@app.route("/healthz")
def healthz():
    try: db.one("SELECT 1 AS ok"); return "ok", 200
    except Exception: return "database down", 503


@app.route("/ping", methods=["GET", "HEAD"])
def ping():
    """Keep-alive endpoint: empty reply (no body, no extra headers), still wakes the web process."""
    return Response(status=204)

@app.route("/health")
def health():
    """Readiness endpoint: confirms the app can reach its configured database."""
    try:
        db.one("SELECT 1 AS ok")
        return jsonify(ok=True), 200, {"Cache-Control": "no-store"}
    except Exception:
        # Do not expose database exception details on a public endpoint.
        app.logger.exception("Health check database query failed")
        return jsonify(ok=False, error="database unavailable"), 503, {"Cache-Control": "no-store"}

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
    """ONE page for everybody: a new email creates a free account, a known email with the right password logs in."""
    s = S(); nxt = _safe_next(request.values.get("next", ""))
    fields = [("email", "Email address" + (" (Gmail only for new accounts)" if s["gmail_only"] else ""), "email", "email"), ("password", "Password (8+ characters)", "password", "current-password")]
    alt = "<p class='muted' style='margin-top:12px'>New here? Type your email and a password to create your free account. Already registered? Type the same email and password to log in.</p>"
    act = f"/signup?next={escape(nxt)}"
    if request.method == "GET": return _form("Sign up or log in", fields, "Continue", alt, action=act)
    f = request.form; em = f.get("email", "").strip().lower()[:150]; pw = f.get("password", "")[:200]
    def bad(m, code=400): return _form("Sign up or log in", fields, "Continue", alt, m, action=act), code
    if not EMAIL_RE.fullmatch(em): return bad("Please enter a valid email address.")
    u = db.one("SELECT * FROM users WHERE email=:e", e=em)
    if u:                                                                   # known email: log in
        if throttled("li", 10, 600) or throttled("lie", 8, 600, ident=em): return bad("Too many attempts. Try again in 10 minutes.", 429)
        if not hmac.compare_digest(pw_hash(pw, u["pw_salt"]).encode(), u["pw_hash"].encode()) or u["blocked"]:
            throttled("li", 10, 600, add=True); throttled("lie", 8, 600, add=True, ident=em)
            return bad("That email is already registered and the password does not match. Try again, or use a different email to create a new account.", 403)
        db.x("UPDATE users SET last_login=:t WHERE id=:i", t=now_s(), i=u["id"]); login_user(u); return redirect(nxt if nxt != "/status" else "/paper")
    if throttled("su", 10, 3600): return bad("Too many sign-ups from your network. Try again later.")      # new email: create the account
    if s["gmail_only"] and not em.endswith("@gmail.com"): return bad("Please use a Gmail address (ending in @gmail.com).")
    if len(pw) < 8 or len(pw) > 200: return bad("Your password must be at least 8 characters.")
    if f.get("password2") is not None and pw != f.get("password2"): return bad("The two passwords do not match.")
    if int(s["max_users"] or 0) and db.one("SELECT COUNT(*) AS n FROM users")["n"] >= int(s["max_users"]): return bad("Sign-ups are closed for now. Please try again later.")
    salt = os.urandom(16).hex()
    try:
        r = db.q("INSERT INTO users (email, pw_salt, pw_hash, created, ip) VALUES (:e, :s, :h, :c, :ip) RETURNING id, blocked", e=em, s=salt, h=pw_hash(pw, salt), c=now_s(), ip=_ip())
    except Exception: return bad("That email is already registered. Please try again.")
    throttled("su", 10, 3600, add=True)
    login_user({"id": r[0]["id"], "blocked": 0})
    if s["require_signup"]: session["welcome"] = 1                     # welcome prompt: shown ONCE, only when sign-up is required
    return redirect(nxt if nxt != "/status" else "/paper")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST": return signup()                                  # old forms keep working
    return redirect("/signup?next=" + urllib.parse.quote(_safe_next(request.values.get("next", ""))))

@app.route("/logout")
def logout(): session.pop("uid", None); return redirect("/")

# ======================================================================================================================
# adding an account. The server verifies the keys before anything is saved.
# ======================================================================================================================
def _need_login(path):
    return redirect(f"/login?next={path}") if S()["require_signup"] and not cur_user() else None

@app.route("/setup")
def setup():
    s = S(); u = cur_user(); r = _need_login("/setup")
    if r: return r
    plats = "".join(f"<option>{escape(p)}</option>" for p in s["platforms"])
    tfn = {"30m": "30 minutes", "1h": "1 hour"}
    tfopts = "<option value='auto' selected>Automatic: the robot reads 30 min and 1 hour and takes the better setup</option>" + "".join(f"<option value='{escape(t)}'>{escape(tfn.get(t, t))} (EA only)</option>" for t in s["timeframes"])
    who = (f"<p class='muted'>Signed in as <b>{escape(u['email'])}</b>. <a href='/status'>See my accounts</a></p>" if u else
           ("<p class='muted'><a href='/signup?next=/setup'>Sign up</a> or <a href='/login?next=/setup'>log in</a> to link accounts to your profile (recommended).</p>"))
    money = ("<p class='muted' style='margin:6px 0'><b>Pricing:</b> <a href='/paper'>paper trading</a> is free forever. Connecting your own account "
             + (("needs a monthly plan" + (f" after a {int(s['trial_days'])}-day free trial" if int(s.get('trial_days') or 0) > 0 else "") + ". " + str(escape(price_line(s)))) if s["paid_mode"] else "is free for now; a monthly plan may be introduced later (paper trading stays free).") + "</p>")
    return layout("Setup", f"""{_connect_guide(s)}<div class='card'><h2>Connect your real account</h2>{who}{money}
<form action='/save_account' method='post' class='grid'>
<div><label>Platform</label><select class='input' name='platform'>{plats}</select></div>
<div><label>Chart (only the EA option needs a fixed chart; every other platform uses both charts automatically)</label><select class='input' name='tf' required>{tfopts}</select></div>
<div><label>API key / login (platform EA: leave blank, a key is created for you)</label><input class='input' name='login' autocomplete='off'></div>
<div><label>API secret (MT4/MT5 via MetaApi: type any text)</label><input class='input' name='password' type='password' autocomplete='off'></div>
<div><label>Extra: passphrase (OKX, Bitget) · MetaApi account ID (MT4/MT5)</label><input class='input' name='extra' autocomplete='off'></div>
<div><label>MetaApi region (MT4/MT5), e.g. new-york or london</label><input class='input' name='server'></div>
<div><button class='btn'>Save</button></div></form>
<div class='muted' style='margin-top:12px;line-height:1.7'><b>Not sure yet?</b> <a href='/paper'>Try paper trading first</a>: it is free, needs no connection, and shows exactly how the robot trades.<br><br>
<b>BINANCE / BYBIT:</b> API key + secret. <b>OKX / BITGET:</b> key + secret + passphrase (Extra). <b>MT5 / MT4:</b> metaapi.cloud API token in "API key", account ID in Extra, region in the last box.
<b>EA</b> (free, runs inside MT5 on your computer): choose Platform EA and press Save; the next page gives you the EA code with your key inside.<br>Use trade-only keys with withdrawals disabled.</div></div>""")

def _make_acc(f, user, s):
    """-> (account dict ready to insert, error text). Verifies credentials against the real exchange/broker where possible."""
    plat = f.get("platform", "")
    if plat not in s["platforms"]: return None, "Invalid platform."
    tf = f.get("tf", "auto")
    if plat != "EA": tf = "auto"                                       # the robot reads both charts and takes the better setup
    elif tf not in s["timeframes"]: return None, "The EA reads one MT5 chart. Please choose 30 minutes or 1 hour for it."
    try: bal = float(f.get("balance") or 0)
    except ValueError: bal = 0.0
    a = {"platform": plat, "tf": tf, "env": "", "server": "", "balance": max(0.0, min(bal, 1e12)), "attest": {}, "key": None}
    sec = {k: f.get(k, "").strip()[:300] for k in SENSITIVE}
    if plat == "EA":
        a["key"] = secrets.token_urlsafe(24); a["key_hash"] = _h(a["key"]); a["secrets"] = {}
    else:
        if not crypto_ready(): return None, "Secure storage is not ready on this server yet. Please try again later."
        if not sec["login"]: return None, "API key / login is required."
        a["secrets"] = sec
        if plat in ("BINANCE", "BYBIT", "OKX", "BITGET"):
            try: a["balance"] = float(balance({**a, **sec}))              # proves the keys work before anything is saved
            except Exception as e: return None, connect_error(plat, e)
        elif plat in FX_PLATS:
            if not sec["extra"]: return None, "The MetaApi account ID is required."
            a["server"] = re.sub(r"[^a-z0-9-]", "", f.get("server", "").strip().lower())[:30] or "new-york"
            try:
                info = meta({**a, **sec}, "GET", "/account-information")
                a["attest"] = {"server": str(info.get("server") or "")[:60], "login": str(info.get("login") or "")[:20], "broker": str(info.get("broker") or "")[:60]}
                a["balance"] = float(info.get("balance") or 0)
            except Exception as e: return None, connect_error(plat, e)
            if a["attest"]["server"] and a["attest"]["login"]: a["fp"] = hashlib.sha256(f"{a['attest']['broker']}|{a['attest']['server']}|{a['attest']['login']}".lower().encode()).hexdigest()[:40]
    return a, ""

def connect_error(plat, e):
    """A plain-words reason a connection failed, with the exchange's own message when it helps."""
    m = str(e); low = m.lower()
    why = m[:160].replace("\n", " ")
    if "451" in low or "restricted location" in low or "eligibility" in low or "403" in low and plat in ("BYBIT", "BINANCE"):
        return f"{plat} refuses connections from the country where this server runs. The owner needs to host the app in another region (for example Frankfurt or Singapore on Render). Message: {why}"
    if "ip" in low and ("whitelist" in low or "restrict" in low or "not in" in low):
        return f"{plat} says this server's address is not on the key's allowed list. Remove the IP restriction on the key, or add this server's address. Message: {why}"
    if plat in FX_PLATS:
        return f"MetaApi could not connect to that account. Check the token, the account ID and the region, and make sure the account is deployed in MetaApi. Message: {why}"
    if "permission" in low or "futures" in low or "-2015" in low:
        return f"{plat} accepted the key but it cannot use futures trading. Enable Futures + trading permission on the key (withdrawals stay off). Message: {why}"
    return f"{plat} did not accept these keys. Check the key, the secret and (OKX, Bitget) the passphrase. Message: {why}"

def default_pair(platform, s):
    """The pair a new connected account starts with: a forex/gold pair for MT4/MT5, a crypto pair for exchanges (the person changes it on the account page)."""
    fx = platform in FX_PLATS
    return next((x for x in s["symbols"] if is_fx(x) == fx), s["symbols"][0])

def _insert_account(a, user, s):
    ref = secrets.token_hex(8); a.update(ref=ref, user_id=user["id"] if user else None, created=str(today()), trial_end=None)
    a.update(tier_cols(a, s))
    # Accounts made WITHOUT a sign-up carry their own free trial (signed-up users get theirs through start_trial, once per user).
    trial_end = None
    if not user and a["tier"] == "real" and s.get("paid_mode") and int(s.get("trial_days") or 0) > 0:
        trial_end = str(today() + datetime.timedelta(days=int(s["trial_days"])))
    row = dict(ref=ref, user_id=a["user_id"], platform=a["platform"], tf=a["tf"], env=a["env"], server=a["server"], secrets=seal(ref, a["secrets"]) if a["secrets"] else "{}",
               key_hash=a.get("key_hash"), balance=a["balance"], created=a["created"], trial_end=trial_end, tier=a["tier"], tier_src=a["tier_src"], tier_note=a["tier_note"], attest=json.dumps(a["attest"]), fp=a.get("fp"), paused=1, pairs=(None if a["platform"] == "EA" else default_pair(a["platform"], load_settings())))
    r = db.q("INSERT INTO accounts (ref, user_id, platform, tf, env, server, secrets, key_hash, balance, created, trial_end, tier, tier_src, tier_note, attest, fp, paused, pairs) VALUES "
             "(:ref, :user_id, :platform, :tf, :env, :server, :secrets, :key_hash, :balance, :created, :trial_end, :tier, :tier_src, :tier_note, :attest, :fp, :paused, :pairs) RETURNING id", **row)
    if a["tier"] == "real" and user: start_trial(user["id"])
    return r[0]["id"]

@app.route("/save_account", methods=["POST"])
def save_account():
    s, user = S(), cur_user()
    if s["require_signup"] and not user: return redirect("/login?next=/setup")
    if throttled("mk", 20, 3600): return layout("Slow down", "<div class='card' style='max-width:520px'>Too many accounts created from your network. Try again later.</div>"), 429
    if user and db.one("SELECT COUNT(*) AS n FROM accounts WHERE user_id=:u AND platform != 'PAPER'", u=user["id"])["n"] >= int(s["max_accounts_per_user"] or 0) > 0:
        return layout("Limit", f"<div class='card' style='max-width:520px'>You can connect up to {int(s['max_accounts_per_user'])} accounts. Delete one on <a href='/status'>My account</a> first.</div>"), 403
    a, err = _make_acc(request.form, user, s)
    if err: return layout("Could not save", f"<div class='card' style='max-width:520px'><h2>Not saved</h2><p style='color:#dc2626;font-weight:600;margin:8px 0'>{escape(err)}</p><a class='btn' href='/setup'>Back</a></div>"), 400
    try: aid = _insert_account(a, user, s)
    except Exception as e:
        return layout("Could not save", "<div class='card' style='max-width:520px'><h2>Not saved</h2><p class='muted'>This broker login is already linked to another account.</p><a class='btn' href='/setup'>Back</a></div>"), 409
    throttled("mk", 20, 3600, add=True)
    if a["platform"] == "EA": return _ea_created(aid, a["key"], new=True)
    if user: return redirect(f"/account/{aid}")
    return layout("Saved", f"<div class='card' style='max-width:640px'><h2>Account saved</h2><p class='muted'>Write down your account number and the API key you entered: without a login you need them to open your account page.</p><pre>Account #{aid}</pre>"
                           f"<form method='post' action='/status' style='margin-top:12px'><input type='hidden' name='id' value='{aid}'><input type='hidden' name='key' value='{escape(a['secrets']['login'])}'><button class='btn'>Open my account page</button></form></div>")

EX5_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ISIG_EA.ex5")
def ex5_ready(): return os.path.exists(EX5_FILE)
COMPILED_ONLY = os.environ.get("EA_COMPILED_ONLY") == "1" and os.path.exists(EX5_FILE)      # hand out only the compiled EA, never the source

def _ea_created(aid, key, new=False):
    src = None if COMPILED_ONLY else _ea_source(key)
    code = (f"<textarea id='eacode' readonly rows='6' style='width:100%;margin-top:10px;font-family:monospace;font-size:11px;border:1px solid #e2e8f0;border-radius:10px;padding:8px'>{escape(src)}</textarea>"
            "<button class='btn' type='button' id='cpbtn' style='margin-top:8px'>Copy EA code</button> <span id='cpmsg' class='muted'></span>"
            "<script>document.getElementById('cpbtn').onclick=function(){var t=document.getElementById('eacode'),m=document.getElementById('cpmsg');function ok(){m.textContent='Copied. Paste it in MetaEditor.'}"
            "if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(t.value).then(ok,function(){t.select();document.execCommand('copy');ok()})}else{t.select();document.execCommand('copy');ok()}}</script>") if src else ""
    hid = f"<input type='hidden' name='id' value='{aid}'><input type='hidden' name='key' value='{escape(key)}'>"
    srcpart = (f"<form method='post' action='/ea/download' style='margin-top:12px'>{hid}<button class='btn'>Download ISIG_EA.mq5</button></form><h3 style='margin-top:16px'>Or copy the code</h3>{code}") if src else ""
    if not src and not COMPILED_ONLY: srcpart = "<p class='muted'>The EA file is not on the server yet (ISIG_EA.mq5 next to app.py).</p>"
    ex5 = (f"<form method='post' action='/ea/download' style='margin-top:12px'>{hid}<input type='hidden' name='fmt' value='ex5'><button class='btn' style='background:#0f172a'>Download compiled EA (ISIG_EA.ex5)</button></form>"
           "<p class='muted'>Put it in MT5 > File > Open Data Folder > MQL5 > Experts, restart MT5, drag it on a chart and paste your key and the app address into the EA inputs (F7).</p>") if ex5_ready() else ""
    note = "Your key is already inside the code below." if src else "Paste this key into the EA inputs."
    return layout("EA key", f"<div class='card' style='max-width:760px'><h2>{'Account saved' if new else 'New EA key'}</h2><p class='muted'>Your EA key. It is shown only now (the server keeps just a fingerprint). {note} Keep it private.</p>"
                            f"<pre style='font-size:14px;word-break:break-all'>Account #{aid}\nKey: {escape(key)}\nApp address: {escape(request.host_url.rstrip('/'))}</pre>{srcpart}{ex5}"
                            f"<p style='margin-top:14px'><a class='btn' style='background:#0f172a' href='/account/{aid}'>Open my account page</a></p></div>")

def _ea_source(key):
    try:
        with open(EA_FILE, encoding="utf-8") as fh: src = fh.read()
    except OSError: return None
    server = request.host_url.rstrip("/")
    src = re.sub(r'(input\s+string\s+InpServer\s*=\s*)"[^"]*"', lambda m: m.group(1) + '"' + server + '"', src)
    return re.sub(r'(input\s+string\s+InpKey\s*=\s*)"[^"]*"', lambda m: m.group(1) + '"' + key + '"', src)

# ======================================================================================================================
# my accounts
# ======================================================================================================================
def _owner_or_key(aid, key=""):
    """Account the caller may act on: its owner (logged in), or someone holding the account's key (accounts created without sign-up)."""
    try: aid = int(aid)
    except (TypeError, ValueError): return None
    a = db.one("SELECT * FROM accounts WHERE id=:i", i=aid)
    if not a: return None
    u = cur_user()
    if u and a["user_id"] == u["id"]: return a
    return a if key and _key_ok(a, key) else None

def _key_ok(a, key):
    if a["platform"] == "EA": return bool(a["key_hash"]) and hmac.compare_digest(a["key_hash"], _h(key))
    lg = unseal(a["ref"], a["secrets"]).get("login", "")
    return bool(lg) and hmac.compare_digest(lg.encode(), key.encode())

def tier_badge(a):
    paper = a["tier"] == "demo"; col = "#16a34a" if paper else "#2563eb"
    return f"<span style='background:{col};color:#fff;border-radius:999px;padding:2px 10px;font-size:11px;font-weight:800'>{'PAPER' if paper else 'CONNECTED'}</span> <span class='muted'>{escape(a['tier_note']) if a['tier_note'] else ''}</span>"

def _online(a):
    try: return bool(a["last_seen"]) and (datetime.datetime.utcnow() - datetime.datetime.strptime(a["last_seen"][:19], "%Y-%m-%d %H:%M:%S")).total_seconds() < max(180, 4 * int(S()["ea_poll_seconds"]))
    except ValueError: return False

@app.route("/status", methods=["GET", "POST"])
def my_status():
    s, u = S(), cur_user()
    if request.method == "POST":                                       # account page without sign-up: account number + key
        if throttled("st", 10): return layout("My account", "<div class='card' style='max-width:520px'>Too many attempts. Try again in 10 minutes.</div>"), 429
        key = request.form.get("key", "").strip(); ident = request.form.get("id", "").strip()
        cands = db.q("SELECT * FROM accounts WHERE legacy_id=:l", l=ident) + (db.q("SELECT * FROM accounts WHERE id=:i", i=int(ident)) if ident.isdigit() else [])
        a = next((c for c in cands if key and _key_ok(c, key)), None)
        if not a: throttled("st", 10, add=True); return layout("My account", _status_form("Account number or key not recognised.")), 403
        if u and not a["user_id"]:                                     # logged in + valid key = claim this account for the profile
            db.x("UPDATE accounts SET user_id=:u WHERE id=:a AND user_id IS NULL", u=u["id"], a=a["id"]); return redirect(f"/account/{a['id']}")
        return render_account(a, key=key)
    if u:
        rows = db.q("SELECT * FROM accounts WHERE user_id=:u ORDER BY id", u=u["id"])
        cards = "".join(f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px;margin:8px 0'><b>Account #{a['id']} · {escape(a['platform'])} · {escape(a['tf'])} chart</b><br>{tier_badge(a)}<br>"
                        f"<span style='color:{'#16a34a' if sub_state(a, s, u)[0] else '#dc2626'};font-weight:600'>{escape(sub_state(a, s, u)[1])}</span> · <a href='/account/{a['id']}'>Open</a></div>" for a in rows) or "<p class='muted'>No accounts yet. <a href='/setup'>Connect one</a>.</p>"
        return layout("My account", f"<div class='card' style='max-width:760px'><h2>My accounts</h2><p class='muted'>Signed in as {escape(u['email'])}</p>{cards}<p><a class='btn' href='/setup'>+ Connect another account</a></p></div>"
                                    f"<div class='card' style='max-width:760px'><h3>Have an older account without sign-up?</h3><p class='muted'>Enter its number and key to link it to your profile.</p>{_status_form('', bare=True)}</div>")
    if s["require_signup"]: return redirect("/login?next=/status")
    return layout("My account", _status_form(""))

def _status_form(err, bare=False):
    e = f"<p style='color:#dc2626;font-weight:600;margin-bottom:8px'>{escape(err)}</p>" if err else ""
    f = (f"{e}<form method='post' action='/status'><label>Account #</label><input class='input' name='id' required><br><br><label>Key (EA key, or the exchange API key)</label>"
         "<input class='input' name='key' type='password' required><br><br><button class='btn'>Open my account</button></form>")
    return f if bare else f"<div class='card' style='max-width:520px'><h2>My account</h2><p class='muted' style='margin-bottom:10px'>Log in to see your accounts, or enter an account number and key.</p>{f}<p class='muted' style='margin-top:12px'><a href='/login?next=/status'>Log in</a> · <a href='/signup'>Sign up</a></p></div>"

@app.route("/account/<aid>")
def account_page(aid):
    a = _owner_or_key(aid)
    if not a: return redirect("/login?next=" + request.path)
    return render_account(a)

def profile_tables(mine, fx=1.0, cur=""):
    """The person's own record, from their own orders only: one line per pair and one per day (newest first)."""
    done = [o for o in mine if o.get("outcome") in ("WIN", "LOSS") and "r" in o]
    if not done: return "<p class='muted'>Your record by pair and by day appears here after your first closed trade.</p>"
    def agg(keyf):
        g = {}
        for o in done:
            x = g.setdefault(keyf(o), {"n": 0, "w": 0, "r": 0.0, "m": 0.0}); x["n"] += 1; x["w"] += 1 if o["r"] > 0 else 0; x["r"] += o["r"]; x["m"] += float(o.get("risk_amount") or 0) * o["r"] * fx
        return g
    def table(g, head, order):
        rows = "".join(f"<tr><td>{escape(str(k))}</td><td>{v['n']}</td><td>{round(100 * v['w'] / v['n'])}%</td><td style='color:{'#16a34a' if v['r'] >= 0 else '#dc2626'};font-weight:700'>{v['r']:+.2f}R</td>"
                       f"<td>{v['m']:+,.2f} {escape(cur)}</td></tr>" for k, v in order(g))
        return f"<table style='width:100%;border-collapse:collapse;font-size:13px;margin:6px 0'><tr style='text-align:left'><th>{head}</th><th>Trades</th><th>Win rate</th><th>Result</th><th>Money</th></tr>{rows}</table>"
    by_pair = agg(lambda o: o.get("symbol", "?")); by_day = agg(lambda o: str(o.get("time", ""))[:10])
    return ("<h3 style='margin-top:14px'>My record by pair</h3>" + table(by_pair, "Pair", lambda g: sorted(g.items(), key=lambda kv: -kv[1]["r"]))
            + "<h3 style='margin-top:14px'>My record by day</h3>" + table(by_day, "Day", lambda g: sorted(g.items(), reverse=True)[:30]))

def render_account(a, key="", note=""):
    s, u = S(), cur_user(); ee = a["platform"] == "EA"; pp = a["platform"] == "PAPER"; owner = user_of(a)
    pn_ = _pay_notice("p.account_id=:v", a["id"]) if not a.get("user_id") else ""         # accounts without a sign-up get the "payment confirmed" banner here
    hid = f"<input type='hidden' name='key' value='{escape(key)}'>" if key else ""
    ok, txt = sub_state(a, s, owner)
    if ee:
        ls = a["last_seen"] or ""; on = _online(a)
        badge = (f"<b style='color:#16a34a'>● Connected</b> <span class='muted'>(EA last heard {escape(ls)} UTC)</span>" if on else
                 f"<b style='color:#dc2626'>● Not connected</b> <span class='muted'>({'last heard ' + escape(ls) + ' UTC' if ls else 'the EA has not contacted the app yet'})</span>")
    else: badge = f"<b style='color:#64748b'>● {escape(note) if note else ('Paper account: trades are simulated with practice money.' if pp else 'Saved. Press Test connection to check your keys.')}</b>"
    notebar = f"<p style='margin:6px 0;font-weight:600;color:#0f172a'>{escape(note)}</p>" if note else ""
    mode = "LIVE: the robot sends real orders to your account." if not pp else "PAPER account: trades are always simulated with practice money."
    eff, cap = effective_risk(a, s), risk_cap(s)
    mine = [row_to_order(r) for r in db.q("SELECT * FROM orders WHERE account_id=:a ORDER BY created DESC LIMIT 200", a=a["id"])]
    p = perf(mine); tr = f"{p['total_r']:+}R"
    box = lambda l, v: f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px'><div class='muted'>{l}</div><div style='font-size:22px;font-weight:800'>{v}</div></div>"
    have = snap_have()
    rows = "".join(f"<div style='padding:8px;border-bottom:1px solid #f1f5f9;font-size:12px'>{escape(o.get('time', '')[:16])} · {escape(o.get('symbol', ''))} {escape(o.get('side', ''))} · E {o['entry']:.6g} SL {o['sl']:.6g} TP {o['tp']:.6g} · <b>{escape(o.get('outcome', ''))}</b> {escape(str(o.get('status', '')))}{snap_link(o, have)}</div>"
                   for o in mine[:10] if "entry" in o)
    lv_ = next((o for o in mine if o.get("outcome") in ("PENDING", "OPEN") and "entry" in o), None)
    live_b = (f"<div style='border:2px solid #16a34a;background:#f0fdf4;border-radius:14px;padding:12px;margin:10px 0'><b style='color:#15803d'>🤖 The robot is trading for you now</b>"
              f"<p style='margin:6px 0;font-size:13px'>{escape(lv_['side'])} {escape(lv_['symbol'])} · entry {lv_['entry']:.6g} · stop loss {lv_['sl']:.6g} · take profit {lv_['tp']:.6g} · "
              f"{'the trade is open' if lv_.get('outcome') == 'OPEN' else 'waiting for price to reach the entry'}{snap_link(lv_, have)}</p></div>") if lv_ else "" or "<p class='muted'>No trades yet. The robot only trades when its pattern appears.</p>"
    pay = f"<p style='margin:8px 0'>{tier_badge(a)}</p><p style='margin:8px 0'><b style='color:{'#16a34a' if ok else '#dc2626'}'>Plan: {escape(txt)}</b></p>"
    if a["tier"] == "real" and s["paid_mode"] and not (ok and txt.startswith("Free access")):
        hid_ = f"<input type='hidden' name='key' value='{escape(key)}'>" if key else ""
        ways = ""
        if _ps_key() and (float(s["price_usd"] or 0) or float(s["price_ngn"] or 0)):
            em_ = "" if (u and u.get("email")) else "<input class='input' name='email' type='email' placeholder='Your email (for the receipt)' required style='margin:6px 0'>"
            bt_ = "".join(f"<button class='btn' name='cur' value='{c_}' style='margin:4px 6px 4px 0'>Pay {lab_} by card</button>" for c_, lab_, on_ in (("USD", "$%g" % float(s["price_usd"] or 0), float(s["price_usd"] or 0)), ("NGN", "₦%s" % format(float(s["price_ngn"] or 0), ",.0f"), float(s["price_ngn"] or 0))) if on_)
            ways += f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px;margin:8px 0'><b>Pay by card (switched on automatically)</b><form method='post' action='/account/{a['id']}/card'>{hid_}{em_}{bt_}</form><p class='muted'>You are sent to Paystack's secure page. Your plan switches on by itself when the payment goes through.</p></div>"
        if s["payment_link"]: ways += f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px;margin:8px 0'><b>Pay by card</b><p style='margin:6px 0'><a class='btn' href='{escape(s['payment_link'])}' target='_blank' rel='noopener'>Pay now</a></p></div>"
        if s["pay_ng"]: ways += f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px;margin:8px 0'><b>Bank / OPay transfer (Nigeria)</b><p style='margin:6px 0;white-space:pre-wrap;word-break:break-word'>{escape(s['pay_ng'])}</p></div>"
        if s["pay_usdt"]: ways += (f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px;margin:8px 0'><b>USDT (crypto)</b><p style='margin:6px 0'>Network: <b>{escape(s['pay_usdt_net'])}</b></p>"
                                   f"<p style='margin:6px 0;word-break:break-all;font-family:monospace;background:#f1f5f9;border-radius:8px;padding:8px' id='usdtaddr'>{escape(s['pay_usdt'])}</p>"
                                   "<button type='button' class='btn' style='padding:6px 12px;font-size:12px' onclick=\"navigator.clipboard&&navigator.clipboard.writeText(document.getElementById('usdtaddr').textContent);this.textContent='Copied'\">Copy address</button>"
                                   "<p class='muted' style='margin-top:6px'>Send only USDT on this network. Money sent on a different network can be lost for good.</p></div>")
        pend = db.q("SELECT method, ref, created FROM payments WHERE account_id=:a AND status='pending' ORDER BY created DESC LIMIT 3", a=a["id"])
        pend_html = "".join(f"<p class='muted'>Waiting for confirmation: {escape(p_['method'])} · {escape(p_['ref'][:40])} · sent {escape(p_['created'][:16])}</p>" for p_ in pend)
        paidform = (f"<form method='post' action='/account/{a['id']}/paid' style='margin-top:10px'>{hid_}<p style='margin:6px 0'>Paid by transfer or USDT? Write <b>NM-{a['id']}</b> as the reference/narration if you can, then tell us:</p>"
                    "<label>How did you pay?</label><select class='input' name='method'><option>Bank / OPay transfer</option><option>USDT</option><option>Card (payment link)</option></select>"
                    "<label style='display:block;margin-top:8px'>Name on the payment (the name of the sender or account holder)</label><input class='input' name='name' maxlength='80' required>"
                    "<label style='display:block;margin-top:8px'>Amount you sent (with currency, e.g. 20 USDT or 30000 NGN)</label><input class='input' name='amount' maxlength='40' required>"
                    "<label style='display:block;margin-top:8px'>Reference (USDT transaction ID, or the transfer reference/session ID if you have it)</label>"
                    "<input class='input' name='ref' maxlength='150' required><button class='btn' style='margin-top:8px'>I HAVE PAID</button></form>")
        pay += ((f"<p class='muted'><b>{escape(price_line(s))}</b></p>" if price_line(s) else "") + ways
                + (f"<p class='muted'>{escape(s['pay_note'])}</p>" if s["pay_note"] else "") + f"<p class='muted'>Quote your account number: <b>#{a['id']}</b></p>" + pend_html + paidform
                + "<p class='muted'>Your plan is switched on after the payment is confirmed. Sending a screenshot is not needed.</p>")
    tfopts = "".join(f"<option value='{escape(t)}'{' selected' if t == a['tf'] else ''}>{escape({'30m': '30 minutes', '1h': '1 hour'}.get(t, t))}</option>" for t in s["timeframes"])
    act = f"/account/{a['id']}"
    running = not a.get("paused")
    col, bg, btncol = ("#16a34a", "#f0fdf4", "#dc2626") if running else ("#dc2626", "#fef2f2", "#16a34a")
    cap_ = int(s.get("max_trades_per_day") or 0)
    msg_ = (("The robot is watching the market for you and places a trade only when its setup appears" + (f" (maximum {cap_} per day)." if cap_ else ".")) if running
            else "The robot will NOT place any new trade on this account. Press START TRADING when you are ready.")
    trade = (f"<div style='border:2px solid {col};background:{bg};border-radius:14px;padding:14px;margin:10px 0'><b style='font-size:16px;color:{col}'>{'● TRADING IS ON' if running else '● TRADING IS OFF'}</b>"
             f"<p class='muted' style='margin:6px 0 10px'>{msg_}</p><form method='post' action='{act}/{'stop' if running else 'start'}'>{hid}<button class='btn' style='background:{btncol}'>{'STOP TRADING' if running else 'START TRADING'}</button></form>"
             + ("<p class='muted' style='margin-top:8px'>Stopping cancels orders that are not filled yet. Trades that are already open keep their stop loss and take profit.</p>" if running else "") + "</div>")
    tfform = (f"<form method='post' action='{act}/tf' style='margin:10px 0'>{hid}<label>Chart the EA reads (30 minutes or 1 hour)</label><div style='display:flex;gap:8px;margin-top:4px'>"
              f"<select class='input' name='tf' style='max-width:200px'>{tfopts}</select><button class='btn'>Save</button></div></form>")
    pairsform = (f"<form method='post' action='{act}/pairs' style='margin:10px 0'>{hid}{pairs_form(a, s, act)}<button class='btn' style='margin-top:6px'>Save pairs</button></form>") if not ee else ""
    riskform = (f"<form method='post' action='{act}/risk' style='margin:10px 0'>{hid}<label>Risk per trade, in percent (maximum {cap}%)</label><div style='display:flex;gap:8px;margin-top:4px'>"
                f"<input class='input' name='risk' type='number' step='0.1' min='0.1' max='{cap}' value='{eff}' style='max-width:200px'><button class='btn'>Save</button></div></form>")
    sizeform = (f"<form method='post' action='{act}/size' style='margin:10px 0'>{hid}<label>Account size for paper trading (practice money)</label><div style='display:flex;gap:8px;margin-top:4px'>"
                f"<input class='input' name='balance' type='number' step='any' min='10' value='{a['balance']:.2f}' style='max-width:200px'><button class='btn'>Save</button></div></form>") if pp else ""
    btns = f"<form method='post' action='{act}' style='display:inline'>{hid}<button class='btn'>Refresh</button></form> "
    btns += (f"<form method='post' action='{act}/rekey' style='display:inline'>{hid}<button class='btn' style='background:#0f172a'>New EA key + code</button></form> " if ee else
             ("" if pp else f"<form method='post' action='{act}/test' style='display:inline'>{hid}<button class='btn' style='background:#0f172a'>Test connection</button></form> "))
    btns += f"<form method='post' action='{act}/delete' style='display:inline' onsubmit='return confirm(\"Delete this account?\")'>{hid}<button class='btn' style='background:#dc2626'>Delete</button></form>"
    guide = ("<details style='margin-top:14px'><summary style='cursor:pointer;font-weight:700'>How to connect the EA (about 5 minutes)</summary><ol class='muted' style='line-height:1.9;margin:8px 0 0 18px'>"
             "<li>Install MetaTrader 5 and log in to your broker account (the account you want the robot to trade).</li><li>Use <b>New EA key + code</b> above to get the code. In MT5 press F4 (MetaEditor), File, New, Expert Advisor (template), name it ISIG_EA, select all, paste, press F7: 0 errors.</li>"
             f"<li>In MT5: Tools, Options, Expert Advisors. Tick Allow WebRequest and add <b>{escape(request.host_url.rstrip('/'))}</b></li>"
             "<li>Open a chart of your pair on the timeframe you chose, drag ISIG_EA onto it, tick Allow Algo Trading, press OK, and turn on the Algo Trading button.</li></ol></details>") if ee else ""
    body = (f"<div class='card' style='max-width:760px'><h2>Account #{a['id']} · {escape(a['platform'])}</h2><p style='margin:8px 0'>{badge}</p>{pn_}{notebar}{live_b}"
            f"<p class='muted'>Balance: {a['balance']:,.2f} · risk per trade {eff}% · target 1:{s['rr']} · {('chart ' + escape(a['tf'])) if a['platform'] == 'EA' else 'reads 30m + 1h, takes the better setup'}{(' · max ' + str(s['max_trades_per_day']) + ' trades per day') if s.get('max_trades_per_day') else ''}</p><p class='muted' style='margin:6px 0 12px'>Mode: {escape(mode)}</p>{trade}{pay}{tfform if a['platform'] == 'EA' else ''}{pairsform}{riskform}{sizeform}{btns}{guide}</div>"
            f"<div class='card' style='max-width:760px'><h3>My results</h3><div class='grid'>{box('Closed trades', p['trades'])}{box('Win rate', str(p['win_rate']) + '%')}{box('Total result', tr)}</div>"
            f"{profile_tables(mine, *((lambda c_: (c_[1], c_[0]))(acc_cur(a))) if pp else (1.0, ''))}<h3 style='margin-top:14px'>My latest trades</h3>{rows}</div>")
    return layout("My account", body)

@app.route("/account/<aid>/<op>", methods=["POST"])
def account_op(aid, op):
    key = request.form.get("key", "").strip()
    if throttled("st", 30): return "Too many attempts. Try again later.", 429
    a = _owner_or_key(aid, key)
    if not a: throttled("st", 30, add=True); abort(403)
    s = S()
    if op == "pairs" and a["platform"] != "EA":
        pr, perr = clean_pairs(request.form.getlist("pair"), s)
        if perr: return render_account(a, key, perr)
        db.x("UPDATE accounts SET pairs=:p WHERE id=:i", p=pr, i=a["id"]); a = db.one("SELECT * FROM accounts WHERE id=:i", i=a["id"])
        return render_account(a, key, "Saved. The robot now trades: " + pr.replace(",", ", ") + ".")
    if op == "paid":
        f = request.form; ref = f.get("ref", "").strip()[:150]; name = f.get("name", "").strip()[:80]; amt = f.get("amount", "").strip()[:40]
        method = f.get("method", "") if f.get("method", "") in ("Card (payment link)", "Bank / OPay transfer", "USDT") else "Bank / OPay transfer"
        u = cur_user()
        if not (ref and name and amt): return render_account(a, key, "Please fill in the name, the amount and the reference of your payment.")
        if throttled("pay", 10, 3600, add=True) or db.one("SELECT COUNT(*) AS n FROM payments WHERE account_id=:a AND status='pending'", a=a["id"])["n"] >= 5:
            return render_account(a, key, "You already have payments waiting for confirmation. Please wait for them to be checked.")
        db.x("INSERT INTO payments (id, account_id, user_id, method, ref, created, status, name, amount, email, seen) VALUES (:i, :a, :u, :m, :r, :c, 'pending', :n, :am, :e, 1)", i=uuid.uuid4().hex[:10], a=a["id"], u=a["user_id"], m=method, r=ref, c=now_s(), n=name, am=amt, e=(u["email"] if u else ""))
        return render_account(a, key, "Thank you. Your payment will be checked and your plan switched on once it is confirmed. You will see a message here when that happens.")
    if op == "tf" and request.form.get("tf") in s["timeframes"]:
        db.x("UPDATE accounts SET tf=:t WHERE id=:i", t=request.form["tf"], i=a["id"]); a = db.one("SELECT * FROM accounts WHERE id=:i", i=a["id"]); return render_account(a, key, "Saved. The robot now trades only the " + a["tf"] + " chart for you.")
    if op == "test" and a["platform"] not in ("EA", "PAPER"):
        try:
            full = acc_full(a)
            if a["platform"] in FX_PLATS:
                info = meta(full, "GET", "/account-information"); b = float(info.get("balance") or 0)
                att = {"server": str(info.get("server") or "")[:60], "login": str(info.get("login") or "")[:20], "broker": str(info.get("broker") or "")[:60]}
                a2 = {**a, "attest": att}; db.x("UPDATE accounts SET attest=:t, balance=:b, tier=:tier, tier_src=:src, tier_note=:note WHERE id=:i", t=json.dumps(att), b=b, i=a["id"], **{"tier": tier_cols(a2, s)["tier"], "src": tier_cols(a2, s)["tier_src"], "note": tier_cols(a2, s)["tier_note"]})
            else: b = float(balance(full)); db.x("UPDATE accounts SET balance=:b WHERE id=:i", b=b, i=a["id"])
            note = f"Connected. Your balance: {b:,.2f}"
        except Exception as e: note = "Could not connect: " + str(e)[:200]
        return render_account(db.one("SELECT * FROM accounts WHERE id=:i", i=a["id"]), key, note)
    if op in ("start", "stop"):
        db.x("UPDATE accounts SET paused=:p WHERE id=:i", p=0 if op == "start" else 1, i=a["id"])
        if op == "stop":
            for r_ in db.q("SELECT * FROM orders WHERE account_id=:a AND outcome='PENDING'", a=a["id"]): cancel_order_now(row_to_order(r_), "trading stopped by you")
        return render_account(db.one("SELECT * FROM accounts WHERE id=:i", i=a["id"]), key, "Trading started." if op == "start" else "Trading stopped.")
    if op == "risk":
        cap = risk_cap(s)
        try: rp = float(request.form.get("risk", ""))
        except ValueError: return render_account(a, key, "Please type a number for the risk, for example 1 or 1.5.")
        if not (rp == rp): return render_account(a, key, "Please type a number for the risk, for example 1 or 1.5.")      # NaN
        rp = round(max(0.1, min(rp, cap)), 2)
        db.x("UPDATE accounts SET risk_pct=:r WHERE id=:i", r=rp, i=a["id"])
        return render_account(db.one("SELECT * FROM accounts WHERE id=:i", i=a["id"]), key, f"Saved. Risk per trade is now {rp}% (the highest allowed is {cap}%).")
    if op == "size" and a["platform"] == "PAPER":
        try: bal = float(request.form.get("balance", ""))
        except ValueError: return render_account(a, key, "Please type the account size as a number.")
        if not (10 <= bal <= 1e9): return render_account(a, key, "The account size must be between 10 and 1,000,000,000.")
        db.x("UPDATE accounts SET balance=:b WHERE id=:i", b=round(bal, 2), i=a["id"])
        return render_account(db.one("SELECT * FROM accounts WHERE id=:i", i=a["id"]), key, f"Saved. Paper account size is now {bal:,.2f}. It applies to new trades.")
    if op == "rekey" and a["platform"] == "EA":
        k = secrets.token_urlsafe(24); db.x("UPDATE accounts SET key_hash=:h WHERE id=:i", h=_h(k), i=a["id"]); return _ea_created(a["id"], k)
    if op == "delete":
        db.x("DELETE FROM orders WHERE account_id=:i", i=a["id"]); db.x("DELETE FROM accounts WHERE id=:i", i=a["id"]); return redirect("/status")
    return render_account(a, key)

@app.route("/ea/download", methods=["POST"])
def ea_download():
    if throttled("st", 30): return "Too many attempts. Try again later.", 429
    key = request.form.get("key", "").strip(); a = _owner_or_key(request.form.get("id", ""), key)
    if not a or a["platform"] != "EA" or not _key_ok(a, key): throttled("st", 30, add=True); return "Account number or key not recognised.", 403
    if request.form.get("fmt") == "ex5":
        if not ex5_ready(): return "The compiled EA is not on the server.", 404
        return Response(open(EX5_FILE, "rb").read(), mimetype="application/octet-stream", headers={"Content-Disposition": "attachment; filename=ISIG_EA.ex5"})
    if COMPILED_ONLY: return "Only the compiled EA is available.", 403
    src = _ea_source(key)
    if src is None: return "The EA file (ISIG_EA.mq5) is not on the server yet. Ask the owner to upload it next to app.py.", 404
    return Response(src, mimetype="text/plain", headers={"Content-Disposition": "attachment; filename=ISIG_EA.mq5"})
def _curve_svg(c):
    if len(c) < 2: return "<p class='muted'>Chart appears after 2+ closed trades.</p>"
    w, h, lo, hi = 640, 180, min(c + [0]), max(c + [0]); span = (hi - lo) or 1
    pts = " ".join(f"{i * w / (len(c) - 1):.1f},{h - 10 - (v - lo) / span * (h - 20):.1f}" for i, v in enumerate(c))
    zero = h - 10 - (0 - lo) / span * (h - 20)
    col = "#16a34a" if c[-1] >= 0 else "#dc2626"
    return f"<svg viewBox='0 0 {w} {h}' style='width:100%;height:auto'><line x1='0' x2='{w}' y1='{zero:.1f}' y2='{zero:.1f}' stroke='#cbd5e1' stroke-dasharray='4'/><polyline fill='none' stroke='{col}' stroke-width='2.5' points='{pts}'/></svg>"


# ======================================================================================================================
# EA bridge: MT5 on the user's own computer talks to the app. Cheap by design: one indexed lookup per poll,
# candles only when a new bar closed, setup detection cached, and the app tells every EA how long to wait (NEXT|seconds).
# ======================================================================================================================
EA_KEYS = ("KEY", "SYMBOL", "TF", "BAL", "TRADEMODE", "ACCSERVER", "ACCCOMPANY", "ACCLOGIN", "ACCCCY", "EAVER", "PLACED", "FAILED")
_INFL, _INFL_LOCK = {"n": 0}, threading.Lock()
UPD_OK = ("balance", "last_seen", "attest", "fp", "fp_changes", "flags", "ever_real", "blocked", "tier", "tier_src", "tier_note")

@app.route("/ea/sync", methods=["POST"])
def ea_sync():
    """Plain-text protocol. The EA sends KEY= SYMBOL= TF= BAL= TRADEMODE= ACCSERVER= ACCCOMPANY= ACCLOGIN= [PLACED=id] [FAILED=id~reason], then '---' and candle lines t,o,h,l,c.
    The app answers: 'OK <mode>', NEXT|seconds, TIER|demo/real|text, optional MSG|text, then ORDER|id|side|entry|sl|tp|risk and CANCEL|id lines."""
    s = S(); raw = request.get_data(as_text=True)
    if len(raw) > 400000: return "ERR too large", 413
    head, rows, placed, failed = {}, [], [], []
    for ln in raw.splitlines():
        ln = ln.strip()
        if not ln or ln == "---": continue
        k, sep, v = ln.partition("=")
        if sep and k in EA_KEYS:
            if k == "PLACED": placed.append(v[:40])
            elif k == "FAILED": failed.append(v[:300])
            else: head[k] = v[:120]
        elif ln.count(",") == 4:
            try: t, o_, h, l, c = ln.split(","); rows.append({"t": int(t) * 1000, "o": float(o_), "h": float(h), "l": float(l), "c": float(c)})
            except ValueError: return "ERR bad candle data", 400
    sym = re.sub(r"[^A-Za-z0-9._#]", "", head.get("SYMBOL", ""))[:20]
    if not sym or len(rows) > 1000: return "ERR bad request", 400
    if throttled("ea", 30): return "ERR too many wrong keys. Wait a few minutes.", 429
    if throttled("eak", 12, 60, add=True, ident=_h(head.get("KEY", ""))[:16]): return "ERR slow down", 429, {"Retry-After": "30"}
    with _INFL_LOCK:
        if _INFL["n"] >= int(s["max_inflight"]): return "ERR busy, retry shortly", 503, {"Retry-After": "30"}
        _INFL["n"] += 1
    try: return _ea_sync(s, head, rows, placed, failed, sym)
    finally:
        with _INFL_LOCK: _INFL["n"] -= 1

def _ea_sync(s, head, rows, placed, failed, sym):
    kh = _h(head.get("KEY", "")); a = db.one("SELECT * FROM accounts WHERE key_hash=:h", h=kh)
    if not a:
        throttled("ea", 30, add=True); return "ERR unknown EA key. Check the key in the EA inputs.", 401
    tf = head.get("TF", ""); want = a["tf"] or s["timeframe"]
    if rows and tf != want: return f"ERR you chose the {want} chart for the robot, but this MT5 chart is {tf}. Switch this chart to {want}, or change your timeframe on the My Account page.", 400
    if rows and want not in s["timeframes"]: return f"ERR the {want} chart is not available right now.", 400
    if rows and len(rows) < 30: return "ERR not enough candles on the chart yet", 400
    now = now_s(); upd = {}
    # ---- what MetaTrader says about the account (self-reported: used only through the strict checks in eval_ea) ----
    att = _att(a); old_att = dict(att)
    mode = head.get("TRADEMODE", "").upper()
    if mode in ("DEMO", "CONTEST", "REAL"): att["mode"] = mode
    for k, hk, n in (("server", "ACCSERVER", 60), ("company", "ACCCOMPANY", 60), ("ccy", "ACCCCY", 8)):
        if head.get(hk): att[k] = re.sub(r"[^\w .:#()\-]", "", head[hk])[:n]
    if head.get("ACCLOGIN"): att["login"] = re.sub(r"\D", "", head["ACCLOGIN"])[:20]
    if att != old_att: upd["attest"] = json.dumps(att)
    if att.get("mode") == "REAL" and not a["ever_real"]: upd["ever_real"] = 1; a["ever_real"] = 1       # once real, never demo again on this key
    if att.get("login") and att.get("server"):
        fp = hashlib.sha256(f"{att.get('company', '')}|{att['server']}|{att['login']}".lower().encode()).hexdigest()[:40]
        if fp != a["fp"]:
            if db.one("SELECT id FROM accounts WHERE fp=:f AND id != :i", f=fp, i=a["id"]): return "ERR this MetaTrader login is already linked to another account.", 409
            upd["fp"] = fp
            if a["fp"]:                                                                                    # the key moved to a different login/server
                upd["fp_changes"] = a["fp_changes"] + 1; fl = set(filter(None, a["flags"].split(",")))
                fl.add("login_changed"); upd["flags"] = ",".join(sorted(fl))
                if upd["fp_changes"] >= 6: upd["blocked"] = 1
    try: bal = float(head.get("BAL", 0))
    except ValueError: bal = 0
    if bal > 0 and abs(bal - float(a["balance"] or 0)) > 0.004: upd["balance"] = round(bal, 2)
    a.update({k: v for k, v in upd.items() if k in a}); a["attest"] = json.dumps(att)
    tc = tier_cols(a, s)
    if any(a[k] != v for k, v in tc.items()): upd.update(tc); a.update(tc)
    try: stale = not a["last_seen"] or (datetime.datetime.utcnow() - datetime.datetime.strptime(a["last_seen"][:19], "%Y-%m-%d %H:%M:%S")).total_seconds() > 40
    except ValueError: stale = True
    if stale: upd["last_seen"] = now[:19]; a["last_seen"] = now[:19]
    if upd:
        db.x("UPDATE accounts SET " + ", ".join(f"{k}=:{k}" for k in upd if k in UPD_OK) + " WHERE id=:id", id=a["id"], **{k: v for k, v in upd.items() if k in UPD_OK})
        if "tier" in upd and upd["tier"] == "real": start_trial(a["user_id"])
    u = user_of(a); okk, why = sub_state(a, s, u); paused = bool(a.get("paused"))
    for pid in placed: db.x("UPDATE orders SET ack=1 WHERE id=:i AND account_id=:a", i=pid, a=a["id"])
    for f in failed:
        oid, _, err = f.partition("~")
        db.x("UPDATE orders SET status=:st, outcome='REJECTED' WHERE id=:i AND account_id=:a AND outcome='PENDING' AND ack=0", st=f"LIVE FAILED: {err[:150]}", i=oid, a=a["id"])
    if rows:
        for r in db.q("SELECT * FROM orders WHERE account_id=:a AND symbol=:s AND tf=:t AND outcome IN ('PENDING','OPEN')", a=a["id"], s=sym, t=tf):
            o = row_to_order(r)
            if resolve(o, rows):
                save_order_state(o); record_track(o) if o["outcome"] in ("WIN", "LOSS") else None
            if o["outcome"] == "PENDING" and "entry" in o:        # not tapped yet: cancel it if the setup is no longer fresh
                ok_, why_ = setup_fresh(o, rows, s)
                if not ok_: expire_stale(o, why_); save_order_state(o)
        sig = cached_setup(rows, s, tf, None, sym)
        if sig and okk and not paused and seen_new(f"ea:{a['id']}:{sym}-{sig['key']}"):                                  # a setup missed while the plan was inactive is not marked as seen
            skip = any(str(sig[f]) in s["filters"].get(f, []) for f in s["filters"] if f in sig)
            busy = db.one("SELECT 1 AS x FROM orders WHERE account_id=:a AND symbol=:s AND tf=:t AND outcome IN ('PENDING','OPEN')", a=a["id"], s=sym, t=tf)
            if not skip and not busy and not daily_cap_reached(a["id"], s):
                o_ = place_order(a, sig, sym, s); insert_order(o_); snap_save(o_, rows)
                if over_cap_after_insert(o_, s): cancel_order_now(o_, "daily trade limit reached")
    poll = max(10, int(s["ea_poll_seconds"])); poll += int(kh[:4], 16) % max(5, poll // 4)                 # spread the EAs out so they do not all call at once
    out = [f"OK live", f"NEXT|{poll}", f"TIER|{a['tier']}|{a['tier_note']}"]
    if not okk: out.append("MSG|" + why + ". Open the My Account page in the app.")
    if paused and okk: out.append("MSG|Trading is stopped. Press START TRADING on your account page.")
    if okk and not paused:
        for r in db.q("SELECT * FROM orders WHERE account_id=:a AND symbol=:s AND tf=:t AND ack=0 AND outcome='PENDING' AND status LIKE 'LIVE: queued%'", a=a["id"], s=sym, t=tf):
            o = row_to_order(r); out.append(f"ORDER|{o['id']}|{o['side']}|{o['entry']!r}|{o['sl']!r}|{o['tp']!r}|{o['risk_amount']}")
    for r in db.q("SELECT id FROM orders WHERE account_id=:a AND symbol=:s AND tf=:t AND outcome='EXPIRED' AND status LIKE 'LIVE%' AND created > :c", a=a["id"], s=sym, t=tf, c=str(datetime.datetime.utcnow() - datetime.timedelta(days=3))): out.append(f"CANCEL|{r['id']}")
    return "\n".join(out), 200, {"Content-Type": "text/plain"}

@app.route("/webhook", methods=["POST", "GET"])
def webhook():
    if not WEBHOOK_SECRET or not hmac.compare_digest(request.args.get("key", ""), WEBHOOK_SECRET): return jsonify({"error": "unauthorized"}), 401
    return jsonify({"status": "scanned", "new": run_scan()})

# ======================================================================================================================
# public track record (built from the `track` table: one row per signal, no account data)
# ======================================================================================================================
def closed_trades(orders, view="all", tf=None):
    paper = lambda o: str(o.get("status", "")).startswith("PAPER")
    rows = [o for o in orders if o.get("outcome") in ("WIN", "LOSS") and (view == "all" or (view == "paper") == paper(o)) and (tf is None or (o.get("tf") or "30m") == tf)]
    return sorted(rows, key=lambda o: o.get("time", ""), reverse=True)

def trade_table(trs, tf=""):
    if not trs: return "<div class='card'><h2>Trade log</h2><p class='muted'>No closed trades yet.</p></div>"
    h = "<tr>" + "".join(f"<th style='text-align:left;padding:6px'>{x}</th>" for x in ("Time", "Symbol", "TF", "Side", "Entry", "SL", "TP", "Result", "Type")) + "</tr>"
    rows = ""
    for o in trs[:300]:
        col = "#16a34a" if o["r"] > 0 else "#dc2626"
        typ = "PAPER" if str(o.get("status", "")).startswith("PAPER") else "LIVE"
        cells = [escape(o["time"][:16]), escape(o["symbol"]), escape(o.get("tf") or "30m"), o["side"], f"{o['entry']:.6g}", f"{o['sl']:.6g}", f"{o['tp']:.6g}", f"<b style='color:{col}'>{o['outcome']} {o['r']:+}R</b>", typ]
        rows += "<tr style='border-top:1px solid #f1f5f9'>" + "".join(f"<td style='padding:6px'>{c}</td>" for c in cells) + "</tr>"
    return f"<div class='card'><h2>Trade log ({len(trs)})</h2><p class='muted'><a href='/trades.csv?tf={escape(tf)}'>Download CSV</a> · latest 300 shown</p><div style='overflow-x:auto;font-size:12px'><table style='width:100%;border-collapse:collapse'>{h}{rows}</table></div></div>"

@app.route("/trades.csv")
def trades_csv():
    import csv, io
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["time", "symbol", "tf", "side", "entry", "sl", "tp", "outcome", "r", "type"])
    tf_ = request.args.get("tf")
    for o in closed_trades(track_rows(tf_ if tf_ in ("30m", "1h") else None, 20000), "all", tf_ if tf_ in ("30m", "1h") else None):
        w.writerow([o["time"], o["symbol"], o.get("tf") or "30m", o["side"], o["entry"], o["sl"], o["tp"], o["outcome"], o["r"], "PAPER" if str(o.get("status", "")).startswith("PAPER") else "LIVE"])
    return buf.getvalue(), 200, {"Content-Type": "text/csv", "Content-Disposition": "attachment; filename=trades.csv"}

@app.route("/performance")
def performance():
    s = S(); view = request.args.get("view", "all"); view = view if view in ("all", "paper", "live") else "all"
    tfs_ok = s["timeframes"]; tf = request.args.get("tf", s["timeframe"]); tf = tf if tf in tfs_ok else tfs_ok[0]
    rows = track_rows(tf); p = perf(rows, view, tf)
    tftabs = "".join(f"<a class='btn' style='padding:6px 14px;margin-right:6px;{'' if t == tf else 'background:#e2e8f0;color:#0f172a'}' href='/performance?view={view}&tf={t}'>{t.upper()} chart</a>" for t in tfs_ok)
    good = lambda v: "#16a34a" if v >= 0 else "#dc2626"
    box = lambda label, val, col="#0f172a": f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:14px'><div class='muted'>{label}</div><div style='font-size:24px;font-weight:800;color:{col}'>{val}</div></div>"
    tabs = "".join(f"<a class='btn' style='padding:6px 14px;margin-right:6px;{'' if v == view else 'background:#e2e8f0;color:#0f172a'}' href='/performance?view={v}&tf={tf}'>{v.upper()}</a>" for v in ("all", "paper", "live"))
    pf = p["profit_factor"] if p["profit_factor"] is not None else "-"
    body = f"""<div class='card'><h2>Bot performance · {escape(tf)} chart</h2><p class='muted'>Each timeframe has its own record. Nothing is combined. One row per signal; personal account results are private.</p><div style='margin:10px 0'>{tftabs}</div><div style='margin:10px 0'>{tabs}</div>
<div class='grid'>{box('Closed trades', p['trades'])}{box('Win rate', str(p['win_rate']) + '%', good(p['win_rate'] - 33.4))}{box('Total result', f"{p['total_r']:+}R", good(p['total_r']))}
{box('Avg win / avg loss', f"{p['avg_win_r']:+}R / {p['avg_loss_r']:+}R")}{box('Profit factor', pf)}{box('Max drawdown', f"-{p['max_drawdown_r']}R", '#dc2626')}{box('Avg per trade', f"{p['avg_r']:+}R", good(p['avg_r']))}</div>
<h3 style='margin-top:16px'>Equity curve (cumulative R)</h3>{_curve_svg(p['curve'])}
<p class='muted' style='margin-top:12px'>1R = the amount risked on one trade. Results are measured from market price data after each signal, not from broker statements. PAPER trades are simulated. Past results do not guarantee future results; this is not financial advice.</p></div>"""
    tfstat = stats(track_rows(None))[0].get("tf", {})
    if tfstat: body += "<div class='card'><h2>By timeframe</h2>" + "".join(f"<p style='margin:6px 0'><b>{escape(k)}</b>: {m['n']} trades · win {m['win']}% · average {m['exp']:+}R</p>" for k, m in sorted(tfstat.items())) + "<p class='muted'>Each signal counted once.</p></div>"
    body += trade_table(closed_trades(rows, view, tf), tf)
    return layout("Performance", body)

@app.route("/api/performance")
def api_performance():
    v, tf = request.args.get("view", "all"), request.args.get("tf")
    tf = tf if tf in ("30m", "1h") else None
    return jsonify(perf(track_rows(tf), v if v in ("all", "paper", "live") else "all", tf))

# ======================================================================================================================
# PAPER TRADING: real market prices, a smooth live chart, a practice account with an MT5-style terminal (balance, equity, orders, history,
# robot journal). Free for everybody. The market scan is shared by all accounts; START switches on the person's own paper account.
# ======================================================================================================================
_MK, _QT, _SGC = {}, {}, {}                       # shared caches: candles per market, bid/ask per symbol, recent signals
_SH_LOCK, _SH_KEYLOCK = threading.Lock(), {}
MK_TTL, QT_TTL = 15, 2                            # seconds a candle download / a price quote is shared by everybody watching

def _shared(cache, key, ttl, fn):
    """Run fn() at most once per ttl seconds for everybody asking for the same key (a failure is remembered for ttl too, so a broken source is not hammered)."""
    def hit():
        h = cache.get(key)
        if h and time.time() - h[0] < ttl:
            if isinstance(h[1], Exception): raise h[1]
            return True, h[1]
        return False, None
    ok, v = hit()
    if ok: return v
    with _SH_LOCK: kl = _SH_KEYLOCK.setdefault((id(cache), key), threading.Lock())
    with kl:
        ok, v = hit()
        if ok: return v
        try: v = fn()
        except Exception as e:
            if len(cache) > 300: cache.clear()
            cache[key] = (time.time(), e); raise
        if len(cache) > 300: cache.clear()
        cache[key] = (time.time(), v)
        return v

def _binance_get(fut_path, spot_path, params):
    """Binance futures first. US servers get HTTP 451 there, so fall back to Binance's public market-data host (spot prices, very close)."""
    last = None
    for base, path in (("https://fapi.binance.com", fut_path), ("https://data-api.binance.vision", spot_path)):
        try:
            with urllib.request.urlopen(f"{base}{path}?{params}", timeout=8) as r: return json.loads(r.read())
        except Exception as e: last = e
    raise last

def fetch_klines_live(symbol, interval, limit=300):
    rows = _binance_get("/fapi/v1/klines", "/api/v3/klines", f"symbol={symbol}&interval={interval}&limit={limit}")
    return [{"t": x[0], "o": float(x[1]), "h": float(x[2]), "l": float(x[3]), "c": float(x[4])} for x in rows]

def fetch_quote(symbol):
    """(bid, ask) right now."""
    if is_fx(symbol):
        ma = data_account()
        if ma: px = meta(ma, "GET", f"/symbols/{symbol}/current-price"); return float(px["bid"]), float(px["ask"])
        try: p = fetch_yahoo_price(symbol); return p, p                 # the free feed has one price, so bid and ask are the same
        except Exception as e:
            try: return _binance_quote(_bn_proxy(symbol))              # backup free source (has a real bid and ask)
            except Exception: raise e
    q = _binance_get("/fapi/v1/ticker/bookTicker", "/api/v3/ticker/bookTicker", f"symbol={symbol}")
    return float(q["bidPrice"]), float(q["askPrice"])

def _binance_quote(bs):
    q = _binance_get("/fapi/v1/ticker/bookTicker", "/api/v3/ticker/bookTicker", f"symbol={bs}"); return float(q["bidPrice"]), float(q["askPrice"])

def quote_cached(symbol): return _shared(_QT, symbol, 6 if (is_fx(symbol) and not data_account()) else QT_TTL, lambda: fetch_quote(symbol))

def live_market(symbol, tf):
    """One shared view of a market: closed candles, the forming candle, and the setup the robot currently sees."""
    def build():
        if is_fx(symbol): closed, forming = fx_split(symbol, tf)
        else:
            rows = fetch_klines_live(symbol, tf)
            nowms, w = time.time() * 1000, TF_MS(tf)
            closed = [c for c in rows if c["t"] + w <= nowms]; forming = next((c for c in rows if c["t"] + w > nowms), None)
        if not closed: raise RuntimeError("no price data received")
        w_ = []; sig = cached_setup(closed, S(), tf, w_, symbol) if len(closed) >= 30 else None
        if len(closed) < 30: w_.append("not enough candles loaded yet")
        return {"closed": closed[-120:], "forming": forming, "setup": sig, "why": (w_[0] if w_ else ""), "cv": int(time.time() * 1000)}
    return _shared(_MK, (symbol, tf), MK_TTL, build)

# ---------------- the person's own currency: money is kept in US dollars inside, shown in the currency they chose at the exchange rate of the day they started ----------------
CURRENCIES = ["USD", "EUR", "GBP", "NGN", "GHS", "KES", "ZAR", "INR", "CAD", "AUD", "NZD", "JPY", "CHF", "AED", "BRL", "MXN", "PHP", "IDR", "PKR", "TRY"]
_RATES = {"t": 0.0, "v": {}}
def usd_rates():
    """{code: units of that currency per 1 US dollar}, from a free public feed, refreshed every 6 hours (an old copy is used if the feed is down)."""
    if _RATES["v"] and time.time() - _RATES["t"] < 21600: return _RATES["v"]
    try:
        with urllib.request.urlopen("https://open.er-api.com/v6/latest/USD", timeout=8) as r: j = json.loads(r.read())
        v = {k: float(x) for k, x in (j.get("rates") or {}).items() if k in CURRENCIES and float(x) > 0}
        if v.get("USD"): _RATES.update(t=time.time(), v=v)
    except Exception: pass
    if not _RATES["v"]:
        v = {"USD": 1.0}; t_end = time.time() + 12
        for c in CURRENCIES:
            if c == "USD" or time.time() > t_end: continue
            try: v[c] = fetch_yahoo_price("USD" + c)
            except Exception: pass
        if len(v) > 1: _RATES.update(t=time.time(), v=v)
        else: _RATES["t"] = time.time() - 21000                       # nothing worked: try again in about 10 minutes
    return _RATES["v"] or {"USD": 1.0}

def fx_rate(cur):
    r = usd_rates().get(cur)
    if not r: raise RuntimeError("The exchange rate for " + cur + " is not available right now. Please try again in a few minutes, or choose USD.")
    return float(r)

def acc_cur(acc):
    """(currency code, rate locked when the account was started or reset)"""
    if acc and acc.get("cur") and acc.get("fx"): return acc["cur"], float(acc["fx"])
    return "USD", 1.0

def size_info(symbol, entry, sl, risk_amt):
    """Practice position size for a risk amount: units (crypto) or lots (forex/gold, USD account). An estimate for display only."""
    dist = abs(entry - sl)
    if dist <= 0: return None
    out = {"risk_amount": round(risk_amt, 2), "units": round(risk_amt / dist, 6), "lots": None}
    if is_fx(symbol):
        core = re.sub(r"[a-z._#]+$", "", symbol); cs = 100 if core.startswith("XAU") else 100000
        per_lot = dist * cs if core.endswith("USD") else (dist * cs / entry if core.startswith("USD") else None)
        out["lots"] = round(risk_amt / per_lot, 2) if per_lot else None
    return out

def paper_account(u): return db.one("SELECT * FROM accounts WHERE user_id=:u AND platform='PAPER' ORDER BY id LIMIT 1", u=u["id"]) if u else None

def paper_realized(account_id):
    """Money made or lost so far by the closed trades of an account (each trade is its result in R times the amount risked on it)."""
    r = db.one("SELECT COALESCE(SUM(r * risk_amt), 0) AS p FROM orders WHERE account_id=:a AND outcome IN ('WIN','LOSS')", a=account_id)
    return float(r["p"] or 0)

def paper_balance(acc):
    """Current practice balance = the account size the person chose + everything won or lost so far (so position sizes grow and shrink like a real account)."""
    return max(0.0, float(acc.get("balance") or 0) + paper_realized(acc["id"]))

def scan_age():
    """Seconds since the last market scan finished (None = never)."""
    ls = kv_get("last_scan")
    try: return max(0, int((datetime.datetime.utcnow() - datetime.datetime.strptime(ls[:19], "%Y-%m-%d %H:%M:%S")).total_seconds()))
    except Exception: return None

def _ms(x):
    try: return int(datetime.datetime.strptime(str(x)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    except Exception: return 0

def _recent_signals():
    def go():
        out = []
        for r in db.q("SELECT * FROM orders WHERE platform='SIGNAL' ORDER BY created DESC LIMIT 15"):
            o = row_to_order(r)
            if "entry" in o: out.append({"t": _ms(o.get("time")), "symbol": o["symbol"], "side": o["side"], "tf": o.get("tf") or "", "entry": o["entry"], "sl": o["sl"], "tp": o["tp"], "outcome": o["outcome"], "r": o.get("r")})
        return out
    return _shared(_SGC, "s", 5, go)

def _otype(o, pending=None):
    p = (o.get("outcome") == "PENDING") if pending is None else pending
    return o["side"].lower() + (" limit" if p else "")

@app.route("/api/live")
def api_live():
    """Public market data for the chart: shared candles (only sent when they changed), live bid/ask, the setup the robot sees, and the viewer's own orders on this market."""
    s = S(); hdr = {"Cache-Control": "no-store"}
    if throttled("live", 900, 60, add=True): return jsonify(error="Too many requests. Please slow down."), 429, hdr
    sym, tf = request.args.get("symbol", s["symbols"][0]), request.args.get("tf", s["timeframe"])
    if sym not in s["symbols"] or tf not in s["timeframes"]: return jsonify(error="That market is not available."), 400, hdr
    try: m = live_market(sym, tf)
    except Exception as e: return jsonify(error="Prices are unavailable right now: " + str(e)[:120]), 503, hdr
    try: bid, ask = quote_cached(sym)
    except Exception:
        last = m["forming"] or m["closed"][-1]; bid = ask = last["c"]
    cap = risk_cap(s)
    try: bal = max(1.0, min(float(request.args.get("balance", 10000)), 1e12))
    except ValueError: bal = 10000.0
    u = cur_user(); acc = paper_account(u)
    if acc: cc, fx = acc_cur(acc)
    else:
        cc = request.args.get("cur", "USD")
        try: fx = fx_rate(cc) if cc in CURRENCIES else 1.0
        except Exception: cc, fx = "USD", 1.0
    try: rp = max(0.1, min(float(request.args.get("risk", s["risk_pct"])), cap))
    except ValueError: rp = effective_risk({}, s)
    mine = []
    if acc:
        for r in db.q("SELECT * FROM orders WHERE account_id=:a AND symbol=:s ORDER BY created DESC LIMIT 12", a=acc["id"], s=sym):
            o = row_to_order(r)
            if "entry" in o and o.get("outcome") in ("PENDING", "OPEN", "WIN", "LOSS"):
                mine.append({"side": o["side"], "entry": o["entry"], "sl": o["sl"], "tp": o["tp"], "outcome": o["outcome"], "t": _ms(o.get("time")), "end": int(o.get("last_t") or 0), "r": o.get("r"),
                             "risk": round(float(o.get("risk_amount") or 0) * fx, 2), "reward": round(float(o.get("risk_amount") or 0) * fx * abs(o["tp"] - o["entry"]) / max(1e-12, abs(o["entry"] - o["sl"])), 2)})
    age = scan_age(); online = age is not None and age < max(300, 4 * SCAN_EVERY); sig = m["setup"]; pick = None
    blocked = None
    try:
        alts = [(live_market(sym, t2) if t2 != tf else m)["setup"] for t2 in s["timeframes"]]
        good = []
        for x in [x for x in alts if x]:
            ok_, gr_, b_ = grade_setup(x, s, sym); x = dict(x, grade=gr_, bias=bias_text(b_), blocked_why=trend_check(x, b_, s)[1])
            if ok_: good.append(x)
            elif not blocked: blocked = x
        if good: pick = max(good, key=lambda x: TF_MS(x["tf"]))                      # the 1 hour chart wins when both have a good setup
    except Exception: pick = None
    sig = pick if (pick and pick["tf"] == tf) else None                              # the chart only draws the setup the robot would really take
    aplus = None
    if pick and pick.get("grade") == "A+":
        mr = aplus_max_rr(pick, s); use = aplus_rr(acc or {}, pick, s)
        aplus = {"max_rr": mr, "use": use, "limited": bool(pick.get("tp_limit") is not None and mr < float(s["max_rr"]) - 1e-9), "limit": pick.get("tp_limit")}
        if sig and use > float(sig["rr"]) + 1e-9:
            sig = dict(sig, rr=use, tp=(sig["entry"] + use * abs(sig["entry"] - sig["sl"])) if sig["side"] == "BUY" else (sig["entry"] - use * abs(sig["entry"] - sig["sl"])))
    live_o = [o for o in mine if o["outcome"] in ("PENDING", "OPEN")]
    if not online: st = ("offline", "The market scanner is not running right now, so no new setups can be found until it starts.")
    elif acc and acc["paused"]: st = ("stopped", "Your paper trading is stopped. Press START to let the robot trade your practice account.")
    elif live_o: st = ("trade", "Your paper trade is " + ("OPEN" if any(o["outcome"] == "OPEN" for o in live_o) else "waiting for price to reach the entry") + " on this market.")
    elif pick: st = ("setup", "The robot sees a valid " + pick["grade"] + " " + pick["side"] + " setup on the " + pick["tf"] + " chart (the 1 hour chart wins when both have one) and is waiting for price to reach the entry." + ("" if pick["tf"] == tf else " Switch the chart above to " + pick["tf"] + " to see it."))
    elif blocked: st = ("scanning", "There is a " + blocked["side"] + " setup on the " + blocked["tf"] + " chart, but the robot skips it: " + (blocked.get("blocked_why") or ("it goes against the bigger picture (" + blocked["bias"] + "). It only takes A+ setups.")))
    else:
        whys = []
        try: whys = [(t2, (live_market(sym, t2) if t2 != tf else m).get("why")) for t2 in s["timeframes"]]
        except Exception: pass
        why_txt = " ".join(f"{t2}: {w}." for t2, w in whys if w)
        st = ("scanning", "Scanning the " + " and ".join(s["timeframes"]) + " charts. No valid setup right now" + (" (" + why_txt + ")" if why_txt else "") + "; the robot trades the moment one forms.")
    cl = lambda c: [c["t"], c["o"], c["h"], c["l"], c["c"]]
    try: cv = int(request.args.get("cv", 0))
    except ValueError: cv = 0
    return jsonify({"symbol": sym, "tf": tf, "tfms": TF_MS(tf), "cv": m["cv"], "candles": [cl(c) for c in m["closed"]] if cv != m["cv"] else None,
                    "forming": cl(m["forming"]) if m["forming"] else None, "bid": bid, "ask": ask,
                    "setup": ({k: sig.get(k) for k in ("side", "entry", "sl", "tp", "rr", "liquidity", "session", "ob_hi", "ob_lo", "grade", "bias", "tf", "prot", "brk", "brk_t", "bos_t", "leg_t", "ob_t", "liq_t")} if sig else None),
                    "size": (dict(size_info(sym, sig["entry"], sig["sl"], bal * rp / 100 / fx), risk_amount=round(bal * rp / 100, 2)) if (sig and size_info(sym, sig["entry"], sig["sl"], 1.0)) else None), "currency": cc, "orders": mine,
                    "status": {"code": st[0], "text": st[1]}, "scan_age": age, "scan_every": SCAN_EVERY, "pick_tf": pick["tf"] if pick else None, "aplus": aplus}), 200, hdr

@app.route("/api/paper")
def api_paper():
    """The viewer's own paper terminal: balance, equity, open and pending orders, history and the robot journal. Anyone not signed in only gets the public signal feed."""
    hdr = {"Cache-Control": "no-store"}
    if throttled("papi", 600, 60, add=True): return jsonify(error="Too many requests. Please slow down."), 429, hdr
    s, u = S(), cur_user(); acc = paper_account(u); sigs = _recent_signals()
    if not acc: return jsonify({"account": None, "signals": sigs, "logged": bool(u)}), 200, hdr
    rows = [row_to_order(r) for r in db.q("SELECT * FROM orders WHERE account_id=:a ORDER BY created DESC LIMIT 200", a=acc["id"])]
    st = db.one("SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN r > 0 THEN 1 ELSE 0 END), 0) AS w, COALESCE(SUM(r), 0) AS tr, COALESCE(SUM(r * risk_amt), 0) AS pnl "
                "FROM orders WHERE account_id=:a AND outcome IN ('WIN','LOSS')", a=acc["id"])
    start, realized = float(acc["balance"] or 0), float(st["pnl"] or 0)
    cc, fx = acc_cur(acc); M = lambda v: round(float(v) * fx, 2)                       # USD inside -> the person's currency
    positions, history, journal, floating = [], [], [], 0.0
    for o in rows:
        if "entry" not in o: continue
        oc, sym, buy = o.get("outcome"), o["symbol"], o["side"] == "BUY"
        risk_amt = float(o.get("risk_amount") or 0); si = size_info(sym, o["entry"], o["sl"], risk_amt) or {}
        size = si.get("lots") if (is_fx(sym) and si.get("lots")) else o.get("size"); unit = "lots" if (is_fx(sym) and si.get("lots")) else "units"
        t0, tend = _ms(o.get("time")), int(o.get("last_t") or 0)
        base = {"id": o["id"], "t": t0, "symbol": sym, "side": o["side"], "tf": o.get("tf") or "", "entry": o["entry"], "sl": o["sl"], "tp": o["tp"], "size": size, "unit": unit, "risk_amount": M(risk_amt), "outcome": oc}
        journal.append({"t": t0, "kind": "order", "text": "Robot placed " + _otype(o, True) + " " + sym + " at " + format(o["entry"], ".6g") + " (stop loss " + format(o["sl"], ".6g") + ", take profit " + format(o["tp"], ".6g") + ", risk " + format(M(risk_amt), ",.2f") + " " + cc + ")"})
        if oc in ("PENDING", "OPEN"):
            try: bid_, ask_ = quote_cached(sym)
            except Exception: bid_ = ask_ = None
            price, profit, touched = None, None, False
            if bid_ is not None:
                price = bid_ if buy else ask_
                if oc == "OPEN":
                    dist = abs(o["entry"] - o["sl"]); rn = (((bid_ - o["entry"]) if buy else (o["entry"] - ask_)) / dist) if dist else 0.0
                    profit = M(rn * risk_amt); floating += profit
                else: touched = (ask_ <= o["entry"]) if buy else (bid_ >= o["entry"])
            positions.append(dict(base, type=_otype(o), price=price, profit=profit, touched=touched))
        elif len(history) < 40:
            rr_ = o.get("r"); profit = M(float(rr_) * risk_amt) if rr_ is not None else None
            history.append(dict(base, end=tend, type=_otype(o, False), r=rr_, profit=profit, note=str(o.get("status", ""))[:80]))
            if oc in ("WIN", "LOSS"): journal.append({"t": tend or t0, "kind": "win" if oc == "WIN" else "loss", "text": ("Take profit hit" if oc == "WIN" else "Stop loss hit") + ": " + sym + " closed " + format(float(rr_ or 0), "+g") + "R (" + format(profit or 0, "+,.2f") + " " + cc + ")"})
            elif oc == "EXPIRED": journal.append({"t": tend or t0, "kind": "cancel", "text": "Order cancelled on " + sym + ": " + str(o.get("status", ""))[:90]})
    for g_ in sigs[:12]: journal.append({"t": g_["t"], "kind": "signal", "text": "Market scan found a " + g_["side"] + " setup on " + g_["symbol"] + " " + g_["tf"] + " (entry " + format(g_["entry"], ".6g") + ")"})
    journal.sort(key=lambda e: e["t"], reverse=True)
    n = int(st["n"] or 0); w = int(st["w"] or 0); balance_ = max(0.0, start + realized)
    return jsonify({"account": {"start": M(start), "realized": M(realized), "balance": M(balance_), "floating": round(floating, 2), "equity": round(M(balance_) + floating, 2), "risk_pct": effective_risk(acc, s),
                                "tf": "auto", "currency": cc, "min_balance": M(s["paper_min_balance"]), "default_balance": M(s["paper_default_balance"]), "resets": int(acc.get("resets") or 0),
                                "depleted": balance_ < float(s["paper_min_balance"]), "running": not acc["paused"], "max_trades": int(s.get("max_trades_per_day") or 0), "trades_today": trades_today(acc["id"])},
                    "stats": {"n": n, "wins": w, "win_rate": round(100 * w / n, 1) if n else 0, "total_r": round(float(st["tr"] or 0), 2)},
                    "positions": positions, "history": history[:30], "journal": journal[:40], "signals": sigs, "logged": True}), 200, hdr

def _paper_err(msg):
    return layout("Paper trading", "<div class='card' style='max-width:520px'><h2>Not saved</h2><p style='color:#dc2626;font-weight:600;margin:8px 0'>" + str(escape(msg)) + "</p><a class='btn' href='/paper'>Back</a></div>"), 400

@app.route("/paper/start", methods=["POST"])
def paper_start():
    s, u = S(), cur_user()
    if not u: return redirect("/signup?next=/paper")
    if throttled("pp", 30, 3600, add=True): return layout("Slow down", "<div class='card' style='max-width:520px'>Too many requests from your network. Try again later.</div>"), 429
    f = request.form; tf = "auto"                                   # the robot reads the 30 minute AND the 1 hour chart and takes the better setup
    acc = paper_account(u); mn, cur_ = float(s["paper_min_balance"]), s["paper_currency"]; bal = 0.0
    cc, fx = "USD", 1.0
    if not acc:                                                     # the account size is chosen once; after that only a reset can change it
        cc = f.get("cur", "USD")
        if cc not in CURRENCIES: return _paper_err("Please choose a currency from the list.")
        try: fx = fx_rate(cc)
        except Exception as e: return _paper_err(str(e))
        try: amount = float(f.get("balance", ""))
        except ValueError: return _paper_err("Please type your account size as a number.")
        bal = amount / fx                                           # inside, money is kept in US dollars
        if amount != amount or bal < mn - 0.005 or bal > 1e9: return _paper_err(f"The account size must be at least {mn * fx:,.0f} {cc} (and at most {1e9 * fx:,.0f}).")
    try: rp = float(f.get("risk", ""))
    except ValueError: return _paper_err("Please type the risk as a number, for example 1.")
    if rp != rp: return _paper_err("Please type the risk as a number, for example 1.")
    try: rr_ = float(f.get("rr", s["rr"]))
    except ValueError: return _paper_err("Please type the reward as a number, for example 3.")
    if rr_ != rr_: return _paper_err("Please type the reward as a number, for example 3.")
    rr_ = round(max(float(s["rr"]), min(rr_, float(s["max_rr"]))), 2)
    pairs_, perr = clean_pairs(f.getlist("pair"), s)
    if perr: return _paper_err(perr)
    rp = round(max(0.1, min(rp, risk_cap(s))), 2); bal = round(bal, 2)
    if acc:
        if paper_balance(acc) < mn: return _paper_err("Your practice money has run out. Press RESET to start again with a new balance.")
        db.x("UPDATE accounts SET risk_pct=:r, rr_aplus=:rr, tf=:t, pairs=:p, paused=0 WHERE id=:i", r=rp, rr=rr_, t=tf, p=pairs_, i=acc["id"])
    else:
        tc = tier_cols({"platform": "PAPER", "tier_override": ""}, s)
        db.q("INSERT INTO accounts (ref, user_id, platform, tf, env, server, secrets, key_hash, balance, created, trial_end, tier, tier_src, tier_note, attest, fp, paused, risk_pct, rr_aplus, cur, fx, pairs) VALUES "
             "(:ref, :u, 'PAPER', :tf, '', '', '{}', NULL, :b, :c, NULL, :tier, :src, :note, '{}', NULL, 0, :r, :rr, :cc, :fx, :pairs) RETURNING id",
             ref=secrets.token_hex(8), u=u["id"], tf=tf, b=bal, c=str(today()), tier=tc["tier"], src=tc["tier_src"], note=tc["tier_note"], r=rp, rr=rr_, cc=cc, fx=fx, pairs=pairs_)
    return redirect("/paper?started=1")

@app.route("/paper/reset", methods=["POST"])
def paper_reset():
    """Practice money has run out: give the person the owner's default balance again. History stays (nothing is erased) and the number of resets is shown."""
    s, u = S(), cur_user()
    if not u: return redirect("/signup?next=/paper")
    acc = paper_account(u)
    if not acc: return redirect("/paper")
    if throttled("pr", 10, 3600, add=True): return _paper_err("Too many resets. Try again later.")
    if paper_balance(acc) >= float(s["paper_min_balance"]): return _paper_err("You can only reset when your practice money has run out.")
    if db.one("SELECT 1 AS x FROM orders WHERE account_id=:a AND outcome='OPEN'", a=acc["id"]): return _paper_err("You still have an open trade. Wait for it to close, then reset.")
    for r_ in db.q("SELECT * FROM orders WHERE account_id=:a AND outcome='PENDING'", a=acc["id"]): cancel_order_now(row_to_order(r_), "account reset by you")
    cc, fx = acc_cur(acc); want = request.form.get("cur", "")
    if want in CURRENCIES and want != cc:
        try: fx = fx_rate(want); cc = want
        except Exception as e: return _paper_err(str(e))
    db.x("UPDATE accounts SET balance=:b, resets=resets + 1, cur=:c, fx=:x WHERE id=:i", b=round(float(s["paper_default_balance"]) - paper_realized(acc["id"]), 2), c=cc, x=fx, i=acc["id"])      # balance = chosen base + results, so base = default - results so far
    return redirect("/paper?reset=1")

@app.route("/paper/stop", methods=["POST"])
def paper_stop():
    u = cur_user()
    if not u: return redirect("/login?next=/paper")
    acc = paper_account(u)
    if acc:
        db.x("UPDATE accounts SET paused=1 WHERE id=:i", i=acc["id"])
        for r_ in db.q("SELECT * FROM orders WHERE account_id=:a AND outcome='PENDING'", a=acc["id"]): cancel_order_now(row_to_order(r_), "trading stopped by you")
    return redirect("/paper?stopped=1")

PAPER_TPL = """
<style>
.pw{max-width:1180px;margin:16px auto;padding:0 12px}
.pgrid{display:grid;grid-template-columns:minmax(0,1.75fr) minmax(0,1fr);gap:16px;align-items:start}
@media(max-width:900px){.pgrid{grid-template-columns:minmax(0,1fr)}}
.pgrid .card,.pw .card{margin:0 0 16px;max-width:none}
.term{background:#131722;border:1px solid #2a2e39;border-radius:16px;overflow:hidden;color:#d1d4dc;margin-bottom:16px}
.tbar{display:flex;gap:10px;flex-wrap:wrap;align-items:center;padding:10px 12px;border-bottom:1px solid #2a2e39}
.tbar select{background:#1e222d;color:#d1d4dc;border:1px solid #2a2e39;border-radius:8px;padding:7px 10px;font-weight:700;font-size:14px}
.pill{background:#1e222d;color:#9598a1;border:1px solid #2a2e39;border-radius:8px;padding:7px 11px;font-weight:700;font-size:12px;cursor:pointer}
.pill.on{background:#2962ff;color:#fff;border-color:#2962ff}
.px{margin-left:auto;text-align:right;line-height:1.15}
.px b{font-size:22px;font-variant-numeric:tabular-nums}.px span{display:block;font-size:11px;color:#787b86}
#pc{width:100%;display:block;touch-action:pan-y;cursor:crosshair}
.tsum{display:flex;gap:0;flex-wrap:wrap;border-top:1px solid #2a2e39;background:#1a1e2a}
.tsum div{padding:8px 14px;border-right:1px solid #2a2e39;min-width:110px}
.tsum span{display:block;font-size:10px;color:#787b86;text-transform:uppercase;letter-spacing:.05em}.tsum b{font-size:15px;font-variant-numeric:tabular-nums}
.tabs{display:flex;gap:4px;padding:8px 10px 0;border-top:1px solid #2a2e39}
.tab{background:none;border:0;color:#787b86;font-weight:700;font-size:12px;padding:8px 12px;border-radius:8px 8px 0 0;cursor:pointer}
.tab.on{background:#1e222d;color:#fff}
.tw{overflow-x:auto;background:#1e222d;min-height:96px;max-height:300px;overflow-y:auto}
.tt{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums;white-space:nowrap}
.tt th{position:sticky;top:0;background:#1e222d;color:#787b86;text-align:left;font-weight:600;padding:7px 10px;border-bottom:1px solid #2a2e39}
.tt td{padding:7px 10px;border-bottom:1px solid #262a36}
.tt tr.fl td{animation:flash 2.4s ease-out}
@keyframes flash{from{background:rgba(41,98,255,.35)}to{background:transparent}}
.up{color:#26a69a}.dn{color:#ef5350}.mut{color:#787b86}
.empty{padding:22px 14px;color:#787b86;font-size:13px}
.jr{padding:7px 14px;border-bottom:1px solid #262a36;font-size:12px}.jr i{font-style:normal;color:#787b86;margin-right:8px}
.pbot{border-radius:12px;padding:10px 12px;font-weight:600;font-size:14px;background:#f1f5f9;color:#0f172a}
.pbot.scanning{background:#eff6ff;color:#1e3a8a}.pbot.setup{background:#faf5ff;color:#6b21a8}.pbot.trade{background:#ecfdf5;color:#065f46}
.pbot.offline{background:#fef2f2;color:#991b1b}.pbot.stopped{background:#fffbeb;color:#92400e}
.pside p{margin:8px 0;font-size:13px;line-height:1.6}
#toasts{position:fixed;right:12px;top:72px;z-index:70;display:flex;flex-direction:column;gap:8px;max-width:min(360px,calc(100vw - 24px))}
.toast{background:#131722;color:#fff;border-left:4px solid #2962ff;border-radius:10px;padding:10px 12px;font-size:13px;box-shadow:0 10px 30px rgba(0,0,0,.4);animation:tin .35s ease both}
.toast.now{border-color:#26a69a;background:#0b3d36;font-weight:700;font-size:14px}.toast.win{border-color:#26a69a}.toast.loss{border-color:#ef5350}
@keyframes tin{from{opacity:0;transform:translateX(20px)}to{opacity:1;transform:none}}
</style>
<div class='pw'><div class='card'><h2>Paper trading: watch the robot trade the real market</h2>
<p class='muted'>The prices are real market prices, but the money is practice money. Nothing is connected to any exchange or broker and nothing real is at risk. Choose your account size and risk, press START, and the robot trades your practice account whenever its setup appears. Every order, balance change and result is saved on your own page, so you can judge the robot over time before you connect an account of your own.</p>@@BANNER@@</div>
<div class='pgrid'>
 <div>
  <div class='term'>
   <div class='tbar'><select id='psym'>@@SYMOPTS@@</select>@@TFPILLS@@<div class='px'><b id='pbig'>-</b><span id='psub'>Bid - / Ask -</span></div></div>
   <canvas id='pc' height='380'></canvas>
   <div class='tsum' id='tsum'></div>
   <div class='tabs'><button class='tab on' data-tab='trade'>Trade</button><button class='tab' data-tab='history'>History</button><button class='tab' data-tab='journal'>Robot journal</button></div>
   <div class='tw' id='tw'><div class='empty'>Loading...</div></div>
  </div>
  <p class='muted' id='pstat' style='margin:-8px 4px 16px'>Loading prices...</p>
 </div>
 <div class='pside'>
  <div class='card'><h3>What the robot is doing</h3><div class='pbot' id='pbot'>Loading...</div><p id='pset'></p><p class='muted' id='psize'></p><p class='muted' id='pscan'></p></div>
  <div class='card'><h3>Your paper account</h3>@@STATE@@
   <form method='post' action='/paper/start'>
    @@BALFIELD@@
    <div style='margin-top:10px'>@@PAIRS@@</div>
    <label style='display:block;margin-top:10px'>Risk per trade, in percent (maximum @@CAP@@%)</label><input class='input' id='prisk' name='risk' type='number' step='0.1' min='0.1' max='@@CAP@@' value='@@RISK@@' required>
    <label style='display:block;margin-top:10px'>Reward on A+ setups: 1 : (from @@RRMIN@@ to @@RRMAX@@)</label><input class='input' name='rr' type='number' step='0.1' min='@@RRMIN@@' max='@@RRMAX@@' value='@@RRV@@' required>
    <p class='muted'>A+ means the setup goes the same way as the present Daily and Weekly leg. On those you can ask for more than 1:@@RRMIN@@. The take profit is never allowed to reach the Daily level that would form a new BOS, so a setup may give less than you ask for. Every setup puts its take profit at the present low (sells) or present high (buys) of the move, and the robot only takes the setup when that gives at least 1:@@MINRR@@.</p>
    <p class='muted'>The robot reads the 30 minute and the 1 hour chart on every pair and takes the clearer setup. It takes at most @@MAXT@@ trades a day on your account and uses the same rules for everybody.</p>
    <button class='btn'>@@BTN@@</button></form>@@STOP@@
   <div id='rbox' style='display:none;margin-top:12px'><p style='color:#dc2626;font-weight:700;margin-bottom:8px'>Your practice money has run out.</p>
    <form method='post' action='/paper/reset'><label>Currency</label><select class='input' name='cur'>@@RCUR@@</select><button class='btn' style='background:#7c3aed;margin-top:8px'>RESET WITH @@DEF@@ @@CUR@@ (or the same value in the currency you pick)</button></form></div></div>
 </div>
</div>
@@RESULTS@@
<div class='card'><p class='muted'>Paper results are simulated from market prices. Orders and results are confirmed when each candle closes (every 30 minutes on the 30 minute chart). They assume a trade fills as soon as price touches the entry, and they ignore fees and slippage, so real trading will usually be a little worse. Past results do not guarantee future results. Not financial advice.</p></div></div>
<div id='toasts'></div>
<script>
(function(){
var CFG=@@CFG@@;function CUR(){return CFG.cur||''}
var cnv=document.getElementById('pc'),ctx=cnv.getContext('2d'),dpr=1;
var C=[],F=null,SF=null,bid=null,ask=null,setup=null,orders=[],cvn=0,cur=null,tgt=null,vis=80,off=0,hov=null,raf=0,lastTs=0,errmsg='',busy=false,busy2=false,T=null,SIG=[],tab='trade',states={},told={},first=true;
var GREEN='#26a69a',RED='#ef5350',BLUE='#2962ff',GRID='#6a6a6a',TXT='#c8c8c8',BG='#000000';
function $(i){return document.getElementById(i)}
function esc(t){return String(t).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
function dec(p){var a=Math.abs(p);return a>=1000?2:a>=10?3:a>=0.01?5:6}
function fmt(p){if(p===null||p===undefined||isNaN(p))return '-';var d=dec(p);return Number(p).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d})}
function money(v){return Number(v||0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function sgn(v){var n=Number(v||0);return (n>0?'+':n<0?'-':'')+Math.abs(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function cls(v){return v>0?'up':v<0?'dn':'mut'}
function hm(t){var d=new Date(t);return d.toLocaleDateString(undefined,{day:'2-digit',month:'short'})+' '+d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}
function sz(v){return v===null||v===undefined?'-':String(+Number(v).toPrecision(3))}
function all(){return F?C.concat([F]):C}
function clamp(v,a,b){return Math.max(a,Math.min(b,v))}
function tfName(t){return {'30m':'M30','1h':'H1','15m':'M15','5m':'M5','4h':'H4','1d':'D1'}[t]||t}
function fit(){var w=cnv.parentNode.clientWidth,h=Math.round(clamp(w*0.62,300,480));dpr=window.devicePixelRatio||1;cnv.style.width=w+'px';cnv.style.height=h+'px';cnv.width=Math.round(w*dpr);cnv.height=Math.round(h*dpr);kick()}
function levels(){
 var L=[],dup=false;
 orders.forEach(function(o){
  if(o.outcome!=='PENDING'&&o.outcome!=='OPEN')return;
  if(setup&&setup.side===o.side&&Math.abs(setup.entry-o.entry)<=Math.abs(o.entry)*1e-9)dup=true;
  var cu=CUR(),mny=function(v){return v?(' '+Number(v).toLocaleString(undefined,{maximumFractionDigits:2})+' '+cu):''};
  L.push({p:o.tp,col:GREEN,txt:'TP  +'+mny(o.reward).trim(),dash:[],always:true});L.push({p:o.entry,col:BLUE,txt:o.side+(o.outcome==='PENDING'?' LIMIT':' OPEN'),dash:[],always:true});L.push({p:o.sl,col:RED,txt:'SL  -'+mny(o.risk).trim(),dash:[],always:true});
 });
 if(setup&&!dup){L.push({p:setup.tp,col:GREEN,txt:'TP',dash:[6,4]});L.push({p:setup.entry,col:BLUE,txt:'SETUP '+setup.side,dash:[6,4]});L.push({p:setup.sl,col:RED,txt:'SL',dash:[6,4]})}
 return L;
}
function idxAt(A,t){var lo=0,hi=A.length-1,r=-1;while(lo<=hi){var m=(lo+hi)>>1;if(A[m][0]<=t){r=m;lo=m+1}else hi=m-1}return r}
function draw(){
 var w=cnv.clientWidth,h=cnv.clientHeight,padR=80,padT=12,padB=26,pw=w-padR,ph=h-padT-padB,A=all(),n=A.length,j,p;
 ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);ctx.fillStyle=BG;ctx.fillRect(0,0,w,h);
 if(!n){ctx.fillStyle=TXT;ctx.font='14px system-ui,sans-serif';ctx.fillText(errmsg||'Loading prices...',16,30);return}
 var R=7,slot=pw/(vis+R),last=n-1-off;
 function X(k){return pw-(R+(last-k)+0.5)*slot}
 var j0=Math.max(0,Math.floor(last-(pw/slot-R))-1),j1=Math.min(n-1,last+R+1),lo=Infinity,hi=-Infinity;
 for(j=j0;j<=j1;j++){var c=A[j];if(c[3]<lo)lo=c[3];if(c[2]>hi)hi=c[2]}
 if(!isFinite(lo))return;
 var span=(hi-lo)||hi*0.001,L=levels();
 L.forEach(function(l){if(l.always||(l.p>lo-span*1.2&&l.p<hi+span*1.2)){if(l.p<lo)lo=l.p;if(l.p>hi)hi=l.p}});
 var pad=(hi-lo)*0.08;lo-=pad;hi+=pad;
 function Y(q){return padT+(hi-q)/(hi-lo)*ph}
 function P(y){return hi-(y-padT)/ph*(hi-lo)}
 ctx.lineWidth=1;ctx.font='11px system-ui,sans-serif';ctx.textBaseline='alphabetic';
 var step=(function(r){var e=Math.pow(10,Math.floor(Math.log10(r))),f=r/e;return (f<1.5?1:f<3?2:f<7?5:10)*e})((hi-lo)/9),cnt=0;
 for(p=Math.ceil(lo/step)*step;p<=hi&&cnt<40;p+=step,cnt++){var gy=Math.round(Y(p))+0.5;ctx.strokeStyle='#666';ctx.beginPath();ctx.moveTo(pw,gy);ctx.lineTo(pw+4,gy);ctx.stroke();ctx.fillStyle=TXT;ctx.fillText(fmt(p),pw+8,gy+4)}
 var tk=Math.max(1,Math.ceil(100/slot));
 for(j=j0;j<=j1;j++){if(j%tk!==0)continue;var tx=Math.round(X(j))+0.5,d=new Date(A[j][0]);ctx.strokeStyle='#666';ctx.beginPath();ctx.moveTo(tx,h-padB);ctx.lineTo(tx,h-padB+4);ctx.stroke();
  var day=(d.getHours()===0&&d.getMinutes()===0),lab=CFG.tfms>=86400000?d.toLocaleDateString(undefined,{day:'2-digit',month:'short'}):(day?d.toLocaleDateString(undefined,{day:'2-digit',month:'short'}):d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}));
  ctx.fillStyle=day?'#d1d4dc':TXT;ctx.textAlign='center';ctx.fillText(lab,clamp(tx,24,pw-24),h-8);ctx.textAlign='left'}
 ctx.strokeStyle='#555';ctx.beginPath();ctx.moveTo(pw+0.5,0);ctx.lineTo(pw+0.5,h);ctx.moveTo(0,h-padB+0.5);ctx.lineTo(w,h-padB+0.5);ctx.stroke();
 ctx.save();ctx.beginPath();ctx.rect(0,0,pw,h-padB);ctx.clip();
 if(setup&&setup.prot!=null&&setup.brk!=null){var sl_=idxAt(A,setup.leg_t),sx_=sl_>=0?X(sl_)-slot/2:0,sy1=Y(Math.max(setup.prot,setup.brk)),sy2=Y(Math.min(setup.prot,setup.brk));ctx.fillStyle='rgba(185,28,28,.07)';ctx.fillRect(sx_,sy1,pw-sx_+2,sy2-sy1);ctx.strokeStyle='#b91c1c';ctx.lineWidth=2;ctx.strokeRect(sx_,sy1,pw-sx_+2,sy2-sy1);ctx.lineWidth=1;ctx.font='bold 10px system-ui,sans-serif';ctx.fillStyle='#ef4444';ctx.fillText('BOS',sx_+6,sy1+14)}
 orders.forEach(function(o){
  var ix=idxAt(A,o.t),x0=ix<0?0:clamp(X(ix)-slot/2,0,pw);
  if(o.outcome==='PENDING'||o.outcome==='OPEN'){
   ctx.fillStyle='rgba(38,166,154,.14)';ctx.fillRect(x0,Math.min(Y(o.entry),Y(o.tp)),pw-x0,Math.abs(Y(o.tp)-Y(o.entry)));
   ctx.fillStyle='rgba(239,83,80,.14)';ctx.fillRect(x0,Math.min(Y(o.entry),Y(o.sl)),pw-x0,Math.abs(Y(o.sl)-Y(o.entry)));
  }
 });
 if(setup&&setup.ob_hi&&setup.ob_lo){var oi_=idxAt(A,setup.ob_t),ox_=oi_>=0?X(oi_)-slot/2:0;ctx.fillStyle='rgba(168,85,247,.24)';ctx.fillRect(ox_,Y(setup.ob_hi),pw-ox_,Math.max(2,Y(setup.ob_lo)-Y(setup.ob_hi)));ctx.strokeStyle='#a855f7';ctx.strokeRect(ox_+0.5,Y(setup.ob_hi)+0.5,pw-ox_,Math.max(2,Y(setup.ob_lo)-Y(setup.ob_hi)));ctx.font='bold 10px system-ui,sans-serif';ctx.fillStyle='#c084fc';ctx.fillText('OB',Math.min(ox_+4,pw-ctx.measureText('OB').width-6),setup.side==='BUY'?Y(setup.ob_lo)+13:Y(setup.ob_hi)-5)}
 var bw=Math.max(1,Math.min(slot*0.7,18));
 for(j=j0;j<=j1;j++){var k=A[j],x=Math.round(X(j))+0.5,col=k[4]>=k[1]?GREEN:RED,yo=Y(k[1]),yc=Y(k[4]);
  ctx.strokeStyle=col;ctx.fillStyle=col;ctx.beginPath();ctx.moveTo(x,Y(k[2]));ctx.lineTo(x,Y(k[3]));ctx.stroke();
  ctx.fillRect(Math.round(x-bw/2),Math.round(Math.min(yo,yc)),Math.max(1,Math.round(bw)),Math.max(1,Math.round(Math.abs(yc-yo))))}
 if(setup&&setup.brk!=null){var bk_=idxAt(A,setup.brk_t),bb_=idxAt(A,setup.bos_t);if(bb_>=0){var bx0=bk_>=0?X(bk_):0,by_=Y(setup.brk);ctx.strokeStyle='#fbbf24';ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(bx0,by_);ctx.lineTo(X(bb_),by_);ctx.stroke();ctx.lineWidth=1;ctx.fillStyle='#fbbf24';ctx.beginPath();ctx.arc(X(bb_),by_,5.5,0,6.3);ctx.fill();ctx.font='bold 10px system-ui,sans-serif';ctx.fillText('BOS',bx0+6,by_+(setup.side==='BUY'?14:-6))}}
 if(setup&&setup.liq_t){var lq_=idxAt(A,setup.liq_t);if(lq_>=0){var lk_=A[lq_],lcx=X(lq_),lw_=Math.max(slot,9);ctx.fillStyle='rgba(234,179,8,.18)';ctx.fillRect(lcx-lw_/2-2,Y(lk_[2])-3,lw_+4,Y(lk_[3])-Y(lk_[2])+6);ctx.strokeStyle='#eab308';ctx.lineWidth=2;ctx.strokeRect(lcx-lw_/2-2,Y(lk_[2])-3,lw_+4,Y(lk_[3])-Y(lk_[2])+6);ctx.lineWidth=1;var lly=setup.side==='BUY'?lk_[3]:lk_[2];ctx.fillStyle='#eab308';ctx.beginPath();ctx.arc(lcx,Y(lly),6,0,6.3);ctx.fill();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.stroke();ctx.lineWidth=1;ctx.font='bold 10px system-ui,sans-serif';ctx.fillStyle='#eab308';ctx.fillText('LIQ',Math.min(lcx+10,pw-ctx.measureText('LIQ').width-6),setup.side==='BUY'?Y(lk_[2])-8:Y(lk_[3])+18)}}
 orders.forEach(function(o){
  if(o.outcome!=='WIN'&&o.outcome!=='LOSS')return;
  var a=idxAt(A,o.t),b=idxAt(A,o.end);if(a<0||b<0)return;
  var xa=X(a),xb=X(b),ye=o.outcome==='WIN'?o.tp:o.sl,cc=o.outcome==='WIN'?GREEN:RED;
  ctx.strokeStyle=cc;ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(xa,Y(o.entry));ctx.lineTo(xb,Y(ye));ctx.stroke();ctx.setLineDash([]);
  ctx.fillStyle=cc;ctx.beginPath();ctx.arc(xb,Y(ye),4,0,6.3);ctx.fill();
  ctx.fillStyle='#fff';ctx.font='bold 10px system-ui,sans-serif';ctx.fillText((o.r>0?'+':'')+(o.r||0)+'R',xb+7,Y(ye)-6);
 });
 orders.forEach(function(o){
  if(o.outcome==='WIN'||o.outcome==='LOSS')return;
  var ix=idxAt(A,o.t);if(ix<0)return;var xx=X(ix),yy=Y(o.entry);
  ctx.fillStyle=o.side==='BUY'?GREEN:RED;ctx.beginPath();if(o.side==='BUY'){ctx.moveTo(xx,yy+2);ctx.lineTo(xx-6,yy+12);ctx.lineTo(xx+6,yy+12)}else{ctx.moveTo(xx,yy-2);ctx.lineTo(xx-6,yy-12);ctx.lineTo(xx+6,yy-12)}ctx.closePath();ctx.fill();
 });
 var lastC=A[n-1];
 L.forEach(function(l){var y=Math.round(Y(l.p))+0.5;ctx.strokeStyle=l.col;ctx.lineWidth=1.2;ctx.setLineDash(l.dash);ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(pw,y);ctx.stroke();ctx.setLineDash([]);
  ctx.font='bold 10px system-ui,sans-serif';var tw=ctx.measureText(l.txt).width;ctx.fillStyle=l.col;ctx.fillRect(6,y-15,tw+10,14);ctx.fillStyle='#fff';ctx.fillText(l.txt,11,y-5)});
 if(ask!==null){var ya=Math.round(Y(ask))+0.5;ctx.strokeStyle='rgba(245,158,11,.7)';ctx.lineWidth=1;ctx.setLineDash([2,3]);ctx.beginPath();ctx.moveTo(0,ya);ctx.lineTo(pw,ya);ctx.stroke();ctx.setLineDash([])}
 var pcur=cur!==null?cur:lastC[4],up=lastC[4]>=lastC[1],yp=Math.round(Y(pcur))+0.5;
 ctx.strokeStyle=up?GREEN:RED;ctx.lineWidth=1;ctx.setLineDash([2,3]);ctx.beginPath();ctx.moveTo(0,yp);ctx.lineTo(pw,yp);ctx.stroke();ctx.setLineDash([]);
 ctx.restore();
 L.forEach(function(l){var y=Y(l.p);ctx.fillStyle=l.col;ctx.fillRect(pw,y-8,padR,16);ctx.fillStyle='#fff';ctx.font='bold 10px system-ui,sans-serif';ctx.fillText(fmt(l.p),pw+6,y+4)});
 ctx.fillStyle=up?GREEN:RED;ctx.fillRect(pw,yp-9,padR,18);ctx.fillStyle='#fff';ctx.font='bold 11px system-ui,sans-serif';ctx.fillText(fmt(pcur),pw+6,yp+4);
 if(F&&CFG.tfms){var left=Math.max(0,Math.floor((F[0]+CFG.tfms-Date.now())/1000)),mm=Math.floor(left/60),ss=left%60;ctx.fillStyle=up?GREEN:RED;ctx.fillRect(pw,yp+9,padR,15);ctx.fillStyle='#fff';ctx.font='10px system-ui,sans-serif';ctx.fillText((mm<10?'0':'')+mm+':'+(ss<10?'0':'')+ss,pw+6,yp+20)}
 var hj=null;
 if(hov&&hov[0]<pw&&hov[1]>padT&&hov[1]<h-padB){
  hj=clamp(Math.round(last-((pw-hov[0])/slot-R-0.5)),0,n-1);var hx=Math.round(X(hj))+0.5;
  ctx.strokeStyle='#787b86';ctx.lineWidth=1;ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(hx,padT);ctx.lineTo(hx,h-padB);ctx.moveTo(0,hov[1]);ctx.lineTo(pw,hov[1]);ctx.stroke();ctx.setLineDash([]);
  ctx.fillStyle='#363a45';ctx.fillRect(pw,hov[1]-9,padR,18);ctx.fillStyle='#fff';ctx.font='11px system-ui,sans-serif';ctx.fillText(fmt(P(hov[1])),pw+6,hov[1]+4);
  var dd=new Date(A[hj][0]),tl=hm(A[hj][0]),tw2=ctx.measureText(tl).width+14;ctx.fillStyle='#363a45';ctx.fillRect(clamp(hx-tw2/2,0,pw-tw2),h-padB+1,tw2,18);ctx.fillStyle='#fff';ctx.fillText(tl,clamp(hx-tw2/2,0,pw-tw2)+7,h-padB+14);
 }
 var ro=A[hj===null?n-1:hj],chg=ro[1]?(ro[4]-ro[1])/ro[1]*100:0;
 ctx.font='11px system-ui,sans-serif';ctx.fillStyle='#d1d4dc';ctx.fillText(CFG.symbol+' · '+tfName(CFG.tf),8,padT+8);
 var ot='O '+fmt(ro[1])+'   H '+fmt(ro[2])+'   L '+fmt(ro[3])+'   C '+fmt(ro[4])+'   '+(chg>=0?'+':'')+chg.toFixed(2)+'%';ctx.fillStyle=ro[4]>=ro[1]?GREEN:RED;ctx.fillText(ot,8,padT+22);
}
function syncF(){if(F&&cur!==null){F[4]=cur;if(cur>F[2])F[2]=cur;if(cur<F[3])F[3]=cur}}
function step(ts){
 raf=0;var dt=lastTs?Math.min(100,ts-lastTs):16;lastTs=ts;var more=false;
 if(cur!==null&&tgt!==null){var diff=tgt-cur;if(Math.abs(diff)>Math.abs(tgt)*1e-8){cur+=diff*(1-Math.exp(-dt/380));more=true}else cur=tgt}
 syncF();draw();if(more)kick();else lastTs=0;
}
function kick(){if(!raf)raf=requestAnimationFrame(step)}
function applyLive(d){
 CFG.tfms=d.tfms;errmsg='';
 if(d.candles){C=d.candles;cvn=d.cv}
 if(d.forming){F=d.forming.slice();SF=null}
 else if(C.length){var lt=C[C.length-1][0]+d.tfms;if(Date.now()>=lt){if(!SF||SF[0]!==lt)SF=[lt,d.bid,d.bid,d.bid,d.bid];F=SF}else F=null}
 bid=d.bid;ask=d.ask;setup=d.setup;orders=d.orders||[];tgt=d.bid;if(cur===null)cur=tgt;
 if(F&&!d.forming){F[2]=Math.max(F[2],d.bid);F[3]=Math.min(F[3],d.bid)}
 var A=all(),prev=C.length>1?C[C.length-1][4]:null;
 $('pbig').textContent=fmt(d.bid);$('pbig').className=(prev!==null&&d.bid<prev)?'dn':'up';
 $('psub').textContent='Bid '+fmt(d.bid)+' / Ask '+fmt(d.ask)+' · Spread '+fmt(d.ask-d.bid);
 var st=d.status||{};$('pbot').className='pbot '+(st.code||'');$('pbot').textContent=st.text||'';
 var s=d.setup,h;
 if(s){h='<b>'+esc(s.grade||'')+' '+esc(s.side)+' setup</b> on '+esc(s.tf||'')+' ('+esc(s.liquidity||'')+', '+esc(s.session||'')+' session)<br>Entry '+fmt(s.entry)+' · Stop loss '+fmt(s.sl)+' · Take profit '+fmt(s.tp)+' (1:'+esc(s.rr)+')<br><span class="muted">Bigger picture (present leg): '+esc(s.bias||'')+'</span>'}else{h='No valid setup on this chart right now.'}
 var ap=d.aplus;if(ap){h+='<br><span class="muted">A+ reward: your setting gives <b>1:'+ap.use+'</b>. This setup allows up to 1:'+ap.max_rr+(ap.limited?' (the take profit has to stay short of the Daily level '+fmt(ap.limit)+', which would form a new BOS)':'')+'.</span>'}
 $('pset').innerHTML=h;
 var z=d.size;$('psize').innerHTML=z?('With your account size and risk: risk amount <b>'+money(z.risk_amount)+' '+esc(CUR())+'</b>, position size <b>'+sz(z.units)+'</b> units'+(z.lots?' (about <b>'+z.lots+'</b> lots)':'')+'.'):'Your position size appears here when the robot has a setup.';
 var a=d.scan_age;$('pscan').textContent=(a===null||a===undefined)?'No market scan has been recorded yet.':'Last market scan: '+a+' seconds ago. The robot scans about every '+d.scan_every+' seconds.';
 $('pstat').textContent='Prices update every 2 seconds · last update '+new Date().toLocaleTimeString()+' · drag the chart to move it, scroll or pinch to zoom';
 renderTerm();kick();
}
function url(){return '/api/live?symbol='+encodeURIComponent(CFG.symbol)+'&tf='+encodeURIComponent(CFG.tf)+'&cv='+cvn+'&balance='+encodeURIComponent($('pbal').value||10000)+'&cur='+encodeURIComponent(CFG.cur||'USD')+'&risk='+encodeURIComponent($('prisk').value||1)}
function poll(){
 if(busy||document.hidden)return;busy=true;var sym=CFG.symbol,tf=CFG.tf;
 fetch(url(),{credentials:'same-origin'}).then(function(r){return r.json().then(function(j){return [r.ok,j]})}).then(function(a){
  busy=false;if(sym!==CFG.symbol||tf!==CFG.tf)return;
  if(a[0])applyLive(a[1]);else{errmsg=a[1].error||'Prices are unavailable right now.';$('pstat').textContent=errmsg;kick()}
 }).catch(function(){busy=false;errmsg=C.length?'':'Connection problem. Retrying...';$('pstat').textContent='Connection problem. Retrying...';kick()})
}
function toast(msg,kind){var t=document.createElement('div');t.className='toast '+(kind||'');t.textContent=msg;$('toasts').appendChild(t);setTimeout(function(){if(t.parentNode)t.parentNode.removeChild(t)},kind==='now'?30000:7000)}
function posProfit(p){
 if(p.outcome!=='OPEN')return null;
 if(p.symbol===CFG.symbol&&bid!==null&&ask!==null){var dist=Math.abs(p.entry-p.sl)||1,r=p.side==='BUY'?(bid-p.entry)/dist:(p.entry-ask)/dist;return r*p.risk_amount}
 return p.profit;
}
function posNow(p){if(p.symbol===CFG.symbol&&bid!==null&&ask!==null)return p.side==='BUY'?bid:ask;return p.price}
function pollTerm(){
 if(busy2||document.hidden)return;busy2=true;
 fetch('/api/paper',{credentials:'same-origin'}).then(function(r){return r.json().then(function(j){return [r.ok,j]})}).then(function(a){
  busy2=false;if(!a[0])return;var d=a[1];T=d;SIG=d.signals||[];
  if(d.account){
   var cur2={};d.positions.forEach(function(p){cur2[p.id]=p.outcome});
   if(!first){
    d.positions.forEach(function(p){if(states[p.id]===undefined)toast('THE ROBOT IS NOW TRADING: '+p.type.toUpperCase()+' '+p.symbol+' at '+fmt(p.entry)+' | SL '+fmt(p.sl)+' | TP '+fmt(p.tp),'now');else if(states[p.id]==='PENDING'&&p.outcome==='OPEN')toast('Order filled: '+p.side+' '+p.symbol+' at '+fmt(p.entry),'')});
    d.history.slice(0,12).forEach(function(h){if(states[h.id]!==undefined&&cur2[h.id]===undefined&&!told[h.id]&&(h.outcome==='WIN'||h.outcome==='LOSS')){told[h.id]=1;toast((h.outcome==='WIN'?'Take profit hit: ':'Stop loss hit: ')+h.symbol+' '+(h.r>0?'+':'')+h.r+'R ('+sgn(h.profit)+')',h.outcome==='WIN'?'win':'loss')}})
   }
   states=cur2;first=false;
  }
  renderTerm();
 }).catch(function(){busy2=false})
}
function renderTerm(){
 var sum=$('tsum'),body=$('tw');
 if(!T||!T.account){
  sum.innerHTML='<div><span>Balance ('+CUR()+')</span><b>-</b></div><div><span>Equity ('+CUR()+')</span><b>-</b></div><div><span>Profit</span><b>-</b></div>';
  if(tab==='journal'){body.innerHTML=SIG.length?SIG.map(function(g){return '<div class="jr"><i>'+hm(g.t)+'</i>Market scan found a '+esc(g.side)+' setup on '+esc(g.symbol)+' '+esc(g.tf)+' (entry '+fmt(g.entry)+')'+(g.outcome==='WIN'||g.outcome==='LOSS'?' · result '+esc(g.outcome):'')+'</div>'}).join(''):'<div class="empty">No signals yet.</div>'}
  else body.innerHTML='<div class="empty">'+(CFG.logged?'Press START PAPER TRADING on the right to open your practice account. Your orders, balance and results will appear here.':'Create a free account or log in, then press START. Your practice orders, balance and results will appear here.')+'</div>';
  return;
 }
 var ac=T.account,fl=0,i;
 T.positions.forEach(function(p){var v=posProfit(p);if(v!==null&&v!==undefined)fl+=v});
 var eq=ac.balance+fl;
 var rb=$('rbox');if(rb)rb.style.display=(ac.depleted&&!T.positions.some(function(p){return p.outcome==='OPEN'}))?'block':'none';
 sum.innerHTML='<div><span>Balance ('+esc(ac.currency||CUR())+')</span><b>'+money(ac.balance)+'</b></div><div><span>Equity ('+esc(ac.currency||CUR())+')</span><b>'+money(eq)+'</b></div><div><span>Floating P/L</span><b class="'+cls(fl)+'">'+sgn(fl)+'</b></div><div><span>Closed P/L</span><b class="'+cls(ac.realized)+'">'+sgn(ac.realized)+'</b></div><div><span>Trades</span><b>'+T.stats.n+' · '+T.stats.win_rate+'% win</b></div><div><span>Today</span><b>'+ac.trades_today+(ac.max_trades?' / '+ac.max_trades:'')+'</b></div>'+(ac.resets?'<div><span>Resets</span><b>'+ac.resets+'</b></div>':'');
 var h='';
 if(tab==='trade'){
  if(!T.positions.length)h='<div class="empty">No open or pending orders. The robot places an order here as soon as it finds a valid setup'+(ac.running?'.':' (paper trading is stopped).')+'</div>';
  else{h='<table class="tt"><tr><th>Symbol</th><th>Ticket</th><th>Time</th><th>Type</th><th>Size</th><th>Price</th><th>S/L</th><th>T/P</th><th>Now</th><th>Profit</th></tr>';
   T.positions.forEach(function(p){var pr=posProfit(p),nw=posNow(p),isNew=!told['n'+p.id];told['n'+p.id]=1;
    h+='<tr class="'+(isNew&&!first?'fl':'')+'"><td><b>'+esc(p.symbol)+'</b></td><td>#'+esc(p.id.slice(0,6))+'</td><td>'+hm(p.t)+'</td><td class="'+(p.side==='BUY'?'up':'dn')+'">'+esc(p.type)+'</td><td>'+sz(p.size)+' '+esc(p.unit)+'</td><td>'+fmt(p.entry)+'</td><td>'+fmt(p.sl)+'</td><td>'+fmt(p.tp)+'</td><td>'+fmt(nw)+'</td><td class="'+(pr===null?'mut':cls(pr))+'">'+(pr===null?(p.touched?'entry touched, confirming':'waiting'):sgn(pr))+'</td></tr>'});
   h+='</table>'}
 }else if(tab==='history'){
  if(!T.history.length)h='<div class="empty">No closed trades yet.</div>';
  else{h='<table class="tt"><tr><th>Symbol</th><th>Ticket</th><th>Opened</th><th>Closed</th><th>Type</th><th>Size</th><th>Price</th><th>S/L</th><th>T/P</th><th>Result</th><th>Profit</th></tr>';
   T.history.forEach(function(x){var res=x.outcome==='WIN'||x.outcome==='LOSS'?((x.r>0?'+':'')+x.r+'R'):'cancelled';
    h+='<tr><td><b>'+esc(x.symbol)+'</b></td><td>#'+esc(x.id.slice(0,6))+'</td><td>'+hm(x.t)+'</td><td>'+(x.end?hm(x.end):'-')+'</td><td class="'+(x.side==='BUY'?'up':'dn')+'">'+esc(x.side.toLowerCase())+'</td><td>'+sz(x.size)+' '+esc(x.unit)+'</td><td>'+fmt(x.entry)+'</td><td>'+fmt(x.sl)+'</td><td>'+fmt(x.tp)+'</td><td class="'+(x.outcome==='WIN'?'up':x.outcome==='LOSS'?'dn':'mut')+'">'+res+'</td><td class="'+cls(x.profit)+'">'+(x.profit===null?'-':sgn(x.profit))+'</td></tr>'});
   h+='</table>'}
 }else{
  h=T.journal.length?T.journal.map(function(e){return '<div class="jr"><i>'+hm(e.t)+'</i><span class="'+(e.kind==='win'?'up':e.kind==='loss'?'dn':'')+'">'+esc(e.text)+'</span></div>'}).join(''):'<div class="empty">The robot journal is empty. Events appear here as the robot works.</div>';
 }
 body.innerHTML=h;
}
var tabs=document.querySelectorAll('.tab');
for(var ti=0;ti<tabs.length;ti++)tabs[ti].onclick=function(){tab=this.getAttribute('data-tab');for(var q=0;q<tabs.length;q++)tabs[q].className='tab'+(tabs[q]===this?' on':'');renderTerm()};
function reset(){C=[];F=null;SF=null;cvn=0;cur=null;tgt=null;setup=null;orders=[];off=0;errmsg='';kick();poll()}
var pc=$('pcur');if(pc)pc.onchange=function(){var c=this.value,r=(CFG.rates||{})[c]||1;CFG.cur=c;$('pcl').textContent=c;var mn=CFG.min_usd*r;$('pmin').textContent=Math.round(mn).toLocaleString();$('pbal').min=mn;$('pbal').value=(CFG.def_usd*r).toFixed(2);poll()};
function setUrl(){try{history.replaceState(null,'','/paper?symbol='+encodeURIComponent(CFG.symbol)+'&tf='+encodeURIComponent(CFG.tf))}catch(e){}}
$('psym').onchange=function(){CFG.symbol=this.value;setUrl();reset()};
var pills=document.querySelectorAll('.pill');
for(var pi=0;pi<pills.length;pi++)pills[pi].onclick=function(){CFG.tf=this.getAttribute('data-tf');for(var q=0;q<pills.length;q++)pills[q].className='pill'+(pills[q]===this?' on':'');setUrl();reset()};
var ptrs={},drag=null,pinch=null;
function pcount(){var c=0,k;for(k in ptrs)c++;return c}
function pdist(){var k,v=[];for(k in ptrs)v.push(ptrs[k]);return Math.hypot(v[0][0]-v[1][0],v[0][1]-v[1][1])||1}
function slotW(){return (cnv.clientWidth-80)/(vis+7)}
cnv.addEventListener('pointerdown',function(e){try{cnv.setPointerCapture(e.pointerId)}catch(x){}ptrs[e.pointerId]=[e.clientX,e.clientY];if(pcount()===1){drag={x:e.clientX,off:off}}else if(pcount()===2){pinch={d:pdist(),vis:vis};drag=null}});
cnv.addEventListener('pointermove',function(e){
 var r=cnv.getBoundingClientRect();
 if(ptrs[e.pointerId]){ptrs[e.pointerId]=[e.clientX,e.clientY];
  if(pinch&&pcount()===2){vis=clamp(Math.round(pinch.vis*pinch.d/pdist()),20,220)}
  else if(drag){off=clamp(Math.round(drag.off+(e.clientX-drag.x)/slotW()),0,Math.max(0,all().length-10))}
 }
 if(e.pointerType==='mouse')hov=[e.clientX-r.left,e.clientY-r.top];
 kick();
});
function up(e){delete ptrs[e.pointerId];if(pcount()<2)pinch=null;if(pcount()===0)drag=null;else if(pcount()===1){var k;for(k in ptrs){drag={x:ptrs[k][0],off:off}}}if(e.pointerType!=='mouse')hov=null;kick()}
cnv.addEventListener('pointerup',up);cnv.addEventListener('pointercancel',up);
cnv.addEventListener('pointerleave',function(){hov=null;kick()});
cnv.addEventListener('dblclick',function(){off=0;vis=80;kick()});
cnv.addEventListener('wheel',function(e){e.preventDefault();vis=clamp(Math.round(vis*(e.deltaY>0?1.12:0.89)),20,220);kick()},{passive:false});
window.addEventListener('resize',fit);
document.addEventListener('visibilitychange',function(){if(!document.hidden){poll();pollTerm()}});
setInterval(poll,2000);setInterval(pollTerm,4000);setInterval(function(){if(!document.hidden&&F)kick()},1000);
fit();poll();pollTerm();
})();
</script>
"""

@app.route("/paper")
def paper_page():
    s, u = S(), cur_user(); acc = paper_account(u); cap = risk_cap(s)
    syms, tfs = s["symbols"], s["timeframes"]
    sym = request.args.get("symbol", syms[0]); sym = sym if sym in syms else syms[0]
    tf = request.args.get("tf") or s["timeframe"]; tf = tf if tf in tfs else tfs[0]
    tfn = {"30m": "30 minutes", "1h": "1 hour"}; tfl = {"30m": "M30", "1h": "H1"}
    opt = lambda items, cur, label=(lambda x: x): "".join("<option value='%s'%s>%s</option>" % (escape(i), " selected" if i == cur else "", escape(label(i))) for i in items)
    pills = "".join("<button type='button' class='pill%s' data-tf='%s'>%s</button>" % (" on" if t == tf else "", escape(t), escape(tfl.get(t, t))) for t in tfs)
    mn_, df_ = float(s["paper_min_balance"]), float(s["paper_default_balance"])        # these two are in US dollars
    rates = {k: v for k, v in usd_rates().items() if k in CURRENCIES}
    cur_, fx_ = acc_cur(acc) if acc else ((s["paper_currency"] if s["paper_currency"] in rates else "USD"), rates.get(s["paper_currency"] if s["paper_currency"] in rates else "USD", 1.0))
    bal = (paper_balance(acc) if acc else df_) * fx_
    curopts = lambda cur: "".join("<option%s>%s</option>" % (" selected" if c == cur else "", c) for c in CURRENCIES if c in rates)
    rp = effective_risk(acc, s) if acc else min(effective_risk({}, s), cap)
    box = "border-radius:12px;padding:10px 12px;font-weight:600;margin-top:10px;"
    banner = ("<p style='" + box + "background:#ecfdf5;border:1px solid #a7f3d0'>Paper trading is ON. The robot will trade your practice account when its setup appears.</p>" if request.args.get("started") else
              "<p style='" + box + "background:#fffbeb;border:1px solid #fde68a'>Paper trading is stopped.</p>" if request.args.get("stopped") else
              "<p style='" + box + "background:#f5f3ff;border:1px solid #ddd6fe'>Your practice account was reset. Press START when you are ready.</p>" if request.args.get("reset") else "")
    if not u: state = "<p class='muted'>To keep your own paper results, <a href='/signup?next=/paper'>sign up or log in</a> (free), then press START.</p>"
    elif acc and not acc["paused"]: state = "<p><b style='color:#16a34a'>● PAPER TRADING IS ON</b> · reads 30m + 1h · <a href='/account/%s'>account page</a></p>" % acc["id"]
    elif acc: state = "<p><b style='color:#dc2626'>● PAPER TRADING IS OFF</b> · <a href='/account/%s'>account page</a></p>" % acc["id"]
    else: state = "<p class='muted'>You have not started yet. Choose your settings and press START.</p>"
    stop = ("<form method='post' action='/paper/stop' style='margin-top:10px'><button class='btn' style='background:#dc2626'>STOP PAPER TRADING</button></form>" if (acc and not acc["paused"]) else "")
    results = ""
    if acc:
        mine = [row_to_order(r) for r in db.q("SELECT * FROM orders WHERE account_id=:a ORDER BY created DESC LIMIT 200", a=acc["id"])]
        p = perf(mine)
        bx = lambda l, v: "<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px'><div class='muted'>%s</div><div style='font-size:22px;font-weight:800'>%s</div></div>" % (l, v)
        tr_, money_ = "%+g" % p["total_r"], "%+.2f %s" % (p["money"] * fx_, cur_)
        results = ("<div class='card'><h3>My paper trading performance</h3><div class='grid'>" + bx("Closed trades", p["trades"]) + bx("Win rate", str(p["win_rate"]) + "%") + bx("Total result", tr_ + "R") + bx("Practice profit / loss", money_)
                   + bx("Profit factor", p["profit_factor"] if p["profit_factor"] is not None else "-") + bx("Max drawdown", "-" + str(p["max_drawdown_r"]) + "R")
                   + "</div><h3 style='margin-top:14px'>Equity curve (cumulative R)</h3>" + _curve_svg(p["curve"]) + "<p class='muted' style='margin-top:8px'>1R is the amount you risk on one trade. Every closed trade is counted once.</p></div>")
    if acc: balfield = ("<input type='hidden' id='pbal' value='%.2f'><p>Account balance now: <b>%s %s</b>%s</p>" % (bal, format(bal, ",.2f"), escape(cur_), (" · resets: %d" % acc["resets"]) if acc.get("resets") else ""))
    else: balfield = ("<label>Currency you want to trade in</label><select class='input' id='pcur' name='cur'>%s</select>"
                      "<label style='display:block;margin-top:10px'>Starting account size, in <span id='pcl'>%s</span> (minimum <span id='pmin'>%s</span>)</label><input class='input' id='pbal' name='balance' type='number' step='any' min='%.2f' value='%.2f' required>"
                      "<p class='muted'>Your balance is shown in this currency, converted at today's exchange rate.</p>" % (curopts(cur_), escape(cur_), format(mn_ * fx_, ",.0f"), mn_ * fx_, bal))
    cfg = json.dumps({"symbol": sym, "tf": tf, "tfms": TF_MS(tf), "logged": bool(u), "cur": cur_, "rates": rates, "min_usd": mn_, "def_usd": df_, "newacc": not acc}).replace("</", "<\\/")
    rep = {"@@BANNER@@": banner, "@@SYMOPTS@@": opt(syms, sym), "@@TFPILLS@@": pills, 
           "@@MINRR@@": ("%g" % float(s.get("min_rr") or 1.0)), "@@RRMIN@@": ("%g" % float(s["rr"])), "@@RRMAX@@": ("%g" % float(s["max_rr"])), "@@RRV@@": ("%g" % float((acc.get("rr_aplus") if acc and acc.get("rr_aplus") else s["rr"]))),
           "@@STATE@@": state, "@@BALFIELD@@": balfield, "@@PAIRS@@": pairs_form(acc or {"pairs": sym, "platform": "PAPER"}, s, "/paper/start"), "@@DEF@@": format(df_ * fx_, ",.0f"), "@@CUR@@": str(escape(cur_)), "@@RCUR@@": curopts(cur_), "@@RISK@@": str(rp), "@@CAP@@": str(cap), "@@MAXT@@": str(int(s.get("max_trades_per_day") or 0) or "any number of"),
           "@@BTN@@": "SAVE AND START" if acc else "START PAPER TRADING", "@@STOP@@": stop, "@@RESULTS@@": results, "@@CFG@@": cfg}
    html = PAPER_TPL
    for k, v in rep.items(): html = html.replace(k, v)
    return layout("Paper trading", html)

@app.route("/orders")
def orders():
    rows = [row_to_order(r) for r in db.q("SELECT * FROM orders WHERE platform='SIGNAL' ORDER BY created DESC LIMIT 40")]
    n = db.one("SELECT COUNT(*) AS n FROM track")["n"]
    have = snap_have()
    def row(r): return f"<div style='padding:8px;border-bottom:1px solid #f1f5f9;font-size:12px'>{escape(r.get('time', '')[:16])} · {escape(r.get('symbol', ''))} {escape(r.get('side', ''))} {escape(r.get('tf', ''))} · E {r['entry']:.6g} SL {r['sl']:.6g} TP {r['tp']:.6g} · <b>{escape(r.get('outcome', ''))}</b>{snap_link(r, have)}</div>"
    return layout("Signals", f"<div class='card'><h2>Latest signals</h2><p class='muted'>{n} resolved signals</p>{''.join(row(r) for r in rows) or '<p class=muted>No signals yet.</p>'}</div>")

# ======================================================================================================================
# admin
# ======================================================================================================================
def _flash(m): session["msg"] = m

@app.route("/admin", methods=["GET", "POST"])
def admin():
    if request.method == "POST":
        if throttled("adm", 5, 600) or throttled("adm*", 20, 600, ident="all"): _flash("Too many wrong attempts. Try again in 10 minutes.")
        else:
            pw = request.form.get("pw", ""); tot = _totp(); on = bool(tot.get("enabled")) and os.environ.get("DISABLE_ADMIN_2FA") != "1"
            sec = _totp_secret() if on else ""
            if check_admin(pw) and (not on or (sec and totp_ok(sec, request.form.get("code", "")))): session.clear(); session["admin"] = True; session["admin_t"] = time.time(); session.permanent = False
            else: throttled("adm", 5, 600, add=True); throttled("adm*", 20, 600, add=True, ident="all"); _flash("Wrong password or code.")
        return redirect("/admin")
    if not admin_ok():
        m = session.pop("msg", None)
        note = f"<p style='color:#dc2626;margin:8px 0;font-weight:600'>{escape(m)}</p>" if m else ""
        return layout("Admin", f"<div class='card' style='max-width:420px'><h2>Admin login</h2><p class='muted'>Only the owner can open this panel.</p>{note}<form method='post'><input class='input' name='pw' type='password' placeholder='Password' autofocus><br><br><input class='input' name='code' inputmode='numeric' autocomplete='one-time-code' placeholder='Authenticator code (only if 2-step login is on)'><br><br><button class='btn'>Unlock</button></form></div>")
    s = load_settings(fresh=True); return _admin_page(s)

def _onoff(s, name): return f"<select class='input' name='{name}'><option value='1'{' selected' if s[name] else ''}>On</option><option value='0'{'' if s[name] else ' selected'}>Off</option></select>"
def _inp(s, name, typ="text", extra=""): return f"<input class='input' name='{name}' type='{typ}' value='{escape(str(s[name]))}' {extra}>"

def _arow(a, s, ret):
    u = {"blocked": a.get("ublocked"), "trial_end": a.get("utrial")} if a["user_id"] else None
    ok, txt = sub_state(a, s, u)
    def btn(op, label, style="", conf=""):
        return (f"<form action='/admin/account' method='post' style='display:inline'{conf}><input type='hidden' name='id' value='{a['id']}'><input type='hidden' name='op' value='{op}'><input type='hidden' name='ret' value='{escape(ret)}'>"
                f"<button class='btn' style='padding:4px 9px;font-size:11px;margin:2px;{style}'>{label}</button></form>")
    flags = f" · <b style='color:#dc2626'>⚑ {escape(a['flags'])}</b>" if a["flags"] else ""
    return (f"<div style='border-top:1px solid #e2e8f0;padding:10px 0;font-size:13px'><b>#{a['id']}</b> · {escape(a['platform'])} · {escape(a['tf'])} · {escape(a.get('email') or 'no sign-up')}{flags}<br>{tier_badge(a)}<br>"
            f"<span style='color:{'#16a34a' if ok else '#dc2626'}'>{escape(txt)}</span> · balance {a['balance']:,.2f} · last seen {escape(a['last_seen'] or '-')}<br>"
            + btn("activate", f"Activate +{int(s['plan_days'])} days") + btn("free", "Free access") + (btn("resume", "Resume") if a["blocked"] else btn("pause", "Pause"))
            + (btn("reset", "Clear flags") if a["flags"] else "") + btn("delete", "Delete", "background:#dc2626", " onsubmit='return confirm(&quot;Delete this account?&quot;)'") + "</div>")

# ---------------- Paystack: card payments confirmed automatically ----------------
def _ps_key(): return os.environ.get("PAYSTACK_SECRET_KEY", "").strip()

def _ps_call(method, path, body=None):
    req = urllib.request.Request("https://api.paystack.co" + path, data=(json.dumps(body).encode() if body is not None else None), method=method,
                                 headers={"Authorization": "Bearer " + _ps_key(), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r: return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try: msg = json.loads(e.read()).get("message", "")
        except Exception: msg = ""
        raise RuntimeError(msg or ("Paystack error " + str(e.code)))

def paystack_settle(reference):
    """Idempotent: asks Paystack (not the customer, not the webhook body) whether this payment really succeeded for the amount we expected, then switches the plan on. -> 'confirmed' | 'pending' | 'failed' | 'unknown'"""
    p = db.one("SELECT * FROM payments WHERE ref=:r AND method='Paystack'", r=reference)
    if not p: return "unknown"
    if p["status"] == "confirmed": return "confirmed"
    try: d = _ps_call("GET", "/transaction/verify/" + urllib.parse.quote(reference))["data"]
    except Exception: return "pending"
    if d.get("status") != "success": return "failed" if d.get("status") in ("failed", "abandoned", "reversed") else "pending"
    parts = (p["amount"] or "").split(" ")
    if len(parts) < 2: return "pending"
    cur_, expect = parts[0], parts[1]
    try: paid, want = int(d.get("amount")), int(round(float(expect) * 100))
    except Exception: return "pending"
    if str(d.get("currency", "")).upper() != cur_.upper() or paid < want: return "failed"                      # wrong currency or too little: never activate
    if db.x("UPDATE payments SET status='confirmed', seen=0 WHERE id=:i AND status IN ('initiated','pending')", i=p["id"]) != 1: return "confirmed"     # someone else confirmed it a moment ago
    a = db.one("SELECT * FROM accounts WHERE id=:i", i=p["account_id"])
    if a:
        base = max(today(), parse_date(a["paid_until"]) or today())
        db.x("UPDATE accounts SET paid_until=:d WHERE id=:i", d=str(base + datetime.timedelta(days=int(S()["plan_days"]))), i=a["id"])
    return "confirmed"

@app.route("/account/<aid>/card", methods=["POST"])
def account_card(aid):
    """Start a card payment: the amount is the owner's fixed price, set by the server (the customer cannot change it)."""
    s = S(); key = request.form.get("key", "").strip(); a = _owner_or_key(aid, key)
    if not a: abort(403)
    if not _ps_key(): return render_account(a, key, "Card payment is not switched on yet.")
    cur_ = request.form.get("cur", "")
    price = float(s["price_usd"] or 0) if cur_ == "USD" else float(s["price_ngn"] or 0) if cur_ == "NGN" else 0
    if price <= 0: return render_account(a, key, "That payment option is not available.")
    if throttled("card", 10, 3600, add=True): return render_account(a, key, "Too many attempts. Please try again later.")
    u = cur_user(); email = (u["email"] if u else request.form.get("email", "").strip().lower()[:150])
    if not EMAIL_RE.fullmatch(email or ""): return render_account(a, key, "Please type your email address for the payment receipt.")
    ref = f"NM{a['id']}-{secrets.token_hex(6)}"
    try:
        r = _ps_call("POST", "/transaction/initialize", {"email": email, "amount": int(round(price * 100)), "currency": cur_, "reference": ref, "metadata": {"account_id": a["id"]},
                                                         "callback_url": request.host_url.rstrip("/") + f"/account/{a['id']}/card-return"})
        url = r["data"]["authorization_url"]
    except Exception as e: return render_account(a, key, "The card payment could not start: " + str(e)[:120])
    db.x("INSERT INTO payments (id, account_id, user_id, method, ref, created, status, name, amount, email, seen) VALUES (:i, :a, :u, 'Paystack', :r, :c, 'initiated', '', :m, :e, 1)",
         i=uuid.uuid4().hex[:10], a=a["id"], u=a["user_id"], r=ref, c=now_s(), m=f"{cur_} {price:g}", e=email)
    return redirect(url, 303)

@app.route("/account/<aid>/card-return")
def account_card_return(aid):
    ref = request.args.get("reference", "") or request.args.get("trxref", "")
    st = paystack_settle(ref) if ref.startswith("NM") else "unknown"
    p = db.one("SELECT account_id FROM payments WHERE ref=:r", r=ref) if ref else None
    msg = {"confirmed": "Payment received. Your plan is now active.", "pending": "We are still waiting for your bank to confirm the payment. This page updates when it does.", "failed": "The payment did not go through. You have not been charged for a plan."}.get(st, "We could not find that payment.")
    return layout("Payment", f"<div class='card' style='max-width:520px'><h2>Card payment</h2><p style='margin:10px 0'>{escape(msg)}</p><a class='btn' href='/status'>Back to my account</a></div>")

@app.route("/webhook/paystack", methods=["POST"])
def paystack_webhook():
    """Paystack tells us a payment succeeded. The message is signed with our secret key (HMAC SHA512): anything without a correct signature is thrown away, and even a correct one is double-checked with Paystack before anything is activated."""
    key = _ps_key(); raw = request.get_data()
    if not key: return "not configured", 404
    sig = request.headers.get("x-paystack-signature", "")
    if not hmac.compare_digest(hmac.new(key.encode(), raw, hashlib.sha512).hexdigest(), sig): return "bad signature", 401
    try: ev = json.loads(raw)
    except Exception: return "bad body", 400
    if ev.get("event") == "charge.success": paystack_settle(str((ev.get("data") or {}).get("reference", "")))
    return "ok", 200

PAIR_ADVICE = "We advise trading no more than 2 pairs at the same time. More pairs means more trades open at once and more risk."

def acc_pairs(acc, s):
    """Pairs this account's robot trades. Paper accounts and newer accounts have an explicit list; older connected accounts with none chosen keep trading everything until their owner picks."""
    chosen = [p for p in (acc.get("pairs") or "").split(",") if p in s["symbols"]]
    return chosen or ([] if acc.get("platform") == "PAPER" else list(s["symbols"]))

def clean_pairs(vals, s):
    """-> (comma separated string, error). Only pairs the owner enabled, no repeats, at least one, not above max_pairs."""
    out = [p for p in dict.fromkeys(v.strip() for v in vals) if p in s["symbols"]]
    if not out: return None, "Please choose at least one pair for the robot to trade."
    mx = int(s.get("max_pairs") or 0)
    if mx and len(out) > mx: return None, f"Please choose at most {mx} pairs. " + PAIR_ADVICE
    return ",".join(out), None

def pairs_form(a, s, action, hid="", button="Save pairs"):
    cur = set(acc_pairs(a, s)) if a else {s["symbols"][0]}
    boxes = "".join(f"<label style='display:inline-flex;align-items:center;gap:6px;margin:4px 10px 4px 0;font-weight:600'><input type='checkbox' class='pairbox' name='pair' value='{escape(p)}'{' checked' if p in cur else ''}>{escape(p)}</label>" for p in s["symbols"])
    return (f"<div class='pairs'><label>Pairs the robot trades for you</label><div>{boxes}</div><p class='muted'>{escape(PAIR_ADVICE)}</p>"
            "<p class='pairwarn' style='display:none;color:#b45309;font-weight:700;margin:4px 0'>You chose more than 2 pairs. This is possible, but we advise against it.</p></div>"
            "<script>(function(){var b=document.querySelectorAll('.pairbox'),w=document.querySelector('.pairwarn');function f(){var n=0;for(var i=0;i<b.length;i++)if(b[i].checked)n++;if(w)w.style.display=n>2?'block':'none'}for(var i=0;i<b.length;i++)b[i].onchange=f;f()})()</script>")

def price_line(s):
    """The fixed monthly price in words (the owner's own wording if set)."""
    if s.get("price_text"): return s["price_text"]
    parts = []
    if float(s.get("price_usd") or 0): parts.append("$%g" % float(s["price_usd"]))
    if float(s.get("price_ngn") or 0): parts.append("₦%s" % format(float(s["price_ngn"]), ",.0f"))
    return (" or ".join(parts) + " per %d days" % int(s["plan_days"])) if parts else ""

def _pay_card(s):
    rows = db.q("SELECT p.*, u.email AS uemail FROM payments p LEFT JOIN users u ON u.id = p.user_id WHERE p.status='pending' ORDER BY p.created LIMIT 50")
    done = db.q("SELECT p.*, u.email AS uemail FROM payments p LEFT JOIN users u ON u.id = p.user_id WHERE p.status='confirmed' ORDER BY p.created DESC LIMIT 10")
    f = lambda pid, op, label, st="": f"<form action='/admin/pay' method='post' style='display:inline'><input type='hidden' name='id' value='{escape(pid)}'><input type='hidden' name='op' value='{op}'><button class='btn' style='padding:4px 10px;font-size:11px;margin:2px;{st}'>{label}</button></form>"
    who = lambda p: f"<b>Account #{p['account_id']}</b> · {escape(p.get('uemail') or p.get('email') or 'no sign-up')}"
    if rows:
        body = "".join(f"<div style='border-top:1px solid #e2e8f0;padding:10px 0;font-size:13px'>{who(p)}<br>Method: <b>{escape(p['method'])}</b> · Name on the payment: <b>{escape(p.get('name') or '-')}</b> · Amount: <b>{escape(p.get('amount') or '-')}</b><br>"
                       f"Reference they gave: <b>{escape(p['ref'])}</b> · they were told to write <b>NM-{p['account_id']}</b> · claimed {escape(p['created'][:16])} UTC<br>"
                       + f(p["id"], "confirm", f"Confirm and activate +{int(s['plan_days'])} days") + f(p["id"], "reject", "Reject", "background:#dc2626") + "</div>" for p in rows)
        top = "<h2>Payments waiting for you (" + str(len(rows)) + ")</h2><p class='muted'>Look for the name, the amount and NM-(account number) in your bank history, Paystack dashboard or wallet. Never trust a screenshot or a link a customer sends.</p>" + body
    else: top = "<h2>Payments waiting for you</h2><p class='muted'>Nothing is waiting. When someone presses \"I have paid\" it appears here with their name, amount and reference. Always check the money in your own bank app or wallet before you confirm.</p>"
    rec = "".join(f"<div style='font-size:12px;padding:4px 0;border-top:1px solid #f1f5f9'>{escape(p['created'][:16])} · {who(p)} · {escape(p['method'])} · {escape(p.get('amount') or '-')} · {escape(p.get('name') or '')}</div>" for p in done)
    return "<div class='card'>" + top + (("<h3 style='margin-top:14px'>Recently confirmed</h3>" + rec) if rec else "") + "</div>"

@app.route("/admin/pay", methods=["POST"])
@admin_only
def admin_pay():
    pid, op, s = request.form.get("id", ""), request.form.get("op", ""), load_settings(fresh=True)
    p = db.one("SELECT * FROM payments WHERE id=:i AND status='pending'", i=pid)
    if p and op in ("confirm", "reject"):
        if op == "confirm":
            a = db.one("SELECT * FROM accounts WHERE id=:i", i=p["account_id"])
            if a:
                base = max(today(), parse_date(a["paid_until"]) or today())
                db.x("UPDATE accounts SET paid_until=:d WHERE id=:i", d=str(base + datetime.timedelta(days=int(s["plan_days"]))), i=a["id"])
        db.x("UPDATE payments SET status=:s, seen=0 WHERE id=:i", s="confirmed" if op == "confirm" else "rejected", i=pid)
        _flash("Payment confirmed: the account was activated." if op == "confirm" else "Payment rejected.")
    return redirect("/admin")

def _admin_page(s):
    q, page = request.args.get("q", "").strip()[:80], max(0, int(request.args.get("page", 0) or 0)) if request.args.get("page", "0").isdigit() else 0
    ret = request.full_path.rstrip("?") if request.full_path else "/admin"
    st, n = stats(track_rows(None))
    m = session.pop("msg", None)
    msg = f"<div class='card' style='background:#ecfdf5;border-color:#a7f3d0;font-weight:600'>{escape(m)}</div>" if m else ""
    fields = ""
    for k in DEFAULTS:
        if k in BRAND_KEYS or k in ACCESS_KEYS or k in SCALE_KEYS or k == "mode": continue
        cur = s[k]; val = json.dumps(cur) if isinstance(cur, (list, dict)) else cur
        fields += f"<div><label>{k}</label><input class='input' name='{k}' value='{escape(str(val))}'></div>"
    # ---- numbers
    c = lambda sql, **p: db.one(sql, **p)["n"]
    cut = str(datetime.datetime.utcnow() - datetime.timedelta(minutes=4))[:19]
    tiers = {r["tier"]: r["n"] for r in db.q("SELECT tier, COUNT(*) AS n FROM accounts GROUP BY tier")}
    lease = db.one("SELECT expires FROM leases WHERE name='scan'"); wk = "running" if (lease and lease["expires"] > time.time()) else ("in-process" if os.environ.get("AUTOSCAN") == "1" else "NOT running")
    kp = lambda l, v: f"<div style='border:1px solid #e2e8f0;border-radius:12px;padding:12px'><div class='muted'>{l}</div><div style='font-size:22px;font-weight:800'>{v}</div></div>"
    n_users = c("SELECT COUNT(*) AS n FROM users"); n_online = c("SELECT COUNT(*) AS n FROM accounts WHERE platform='EA' AND last_seen >= :c", c=cut)
    n_flag = c("SELECT COUNT(*) AS n FROM accounts WHERE flags != ''"); n_jobs = c("SELECT COUNT(*) AS n FROM jobs WHERE status='new'")
    dash = ("<div class='card'><h2>Overview</h2><div class='grid'>" + kp("Users", f"{n_users:,}") + kp("Paper accounts (free)", f"{tiers.get('demo', 0):,}") + kp("Connected accounts (paid)", f"{tiers.get('real', 0):,}")
            + kp("EAs online", f"{n_online:,}") + kp("Flagged accounts", f"{n_flag:,}") + kp("Job queue", f"{n_jobs:,}") + kp("Worker", wk) + "</div></div>")
    brand = f"""<div class='card'><h2>Branding</h2><form action='/admin/save' method='post' class='grid'>
<div><label>App name</label><input class='input' name='site_name' maxlength='60' value='{escape(s['site_name'])}'></div>
<div><label>Headline under the name</label><input class='input' name='tagline' maxlength='80' value='{escape(s['tagline'])}'></div>
<div style='grid-column:1/-1'><label>Homepage description</label><textarea class='input' name='lead' rows='3' maxlength='400'>{escape(s['lead'])}</textarea></div>
<div><button class='btn'>Save branding</button></div></form></div>"""
    access = ("<div class='card'><h2>Sign-up &amp; monetization</h2><p class='muted' style='margin-bottom:10px'>Switch these on when you are ready. They apply to every account straight away. <b>Paper trading is always free.</b> "
              "If you also set a free trial (days above 0), switching on \"Charge for connected accounts\" gives everyone with a connected account a fresh free trial first. With 0 days there is no trial and no trial wording anywhere.</p><form action='/admin/save' method='post' class='grid'>"
              f"<div><label>Require sign-up (no account can be added without logging in)</label>{_onoff(s, 'require_signup')}</div><div><label>Gmail addresses only</label>{_onoff(s, 'gmail_only')}</div>"
              f"<div><label>Block old accounts that were never linked to a sign-up</label>{_onoff(s, 'block_unlinked')}</div><div><label>Max users (0 = no limit)</label>{_inp(s, 'max_users', 'number')}</div>"
              f"<div><label>Max accounts per user (0 = no limit)</label>{_inp(s, 'max_accounts_per_user', 'number')}</div>"
              f"<div><label>Charge for connected accounts</label>{_onoff(s, 'paid_mode')}</div><div><label>Free trial for connected accounts (days)</label>{_inp(s, 'trial_days', 'number')}</div>"
              f"<div><label>Days added when you activate an account</label>{_inp(s, 'plan_days', 'number')}</div><div><label>Fixed monthly price in US dollars (0 = not offered)</label>{_inp(s, 'price_usd', 'number', "step='0.01' min='0'")}</div>"
              f"<div><label>Fixed monthly price in naira (0 = not offered)</label>{_inp(s, 'price_ngn', 'number', "step='1' min='0'")}</div><div><label>Price wording shown to users (optional)</label>{_inp(s, 'price_text')}</div>"
              f"<div><label>Payment link (must start with https://)</label>{_inp(s, 'payment_link')}</div>"
              f"<div style='grid-column:1/-1'><label>Nigerian bank / OPay details (bank, account number, account name). Shown to users.</label><textarea class='input' name='pay_ng' rows='2' maxlength='300'>{escape(s['pay_ng'])}</textarea></div>"
              f"<div><label>USDT wallet address</label>{_inp(s, 'pay_usdt')}</div><div><label>USDT network (e.g. TRC20 (Tron), BEP20)</label>{_inp(s, 'pay_usdt_net')}</div>"
              f"<div style='grid-column:1/-1'><label>Other instructions (shown to users)</label><textarea class='input' name='pay_note' rows='3' maxlength='300'>{escape(s['pay_note'])}</textarea></div>"
              "<div><button class='btn'>Save</button></div></form></div>")
    scale = ("<div class='card'><h2>Load control</h2><form action='/admin/save' method='post' class='grid'>"
             f"<div><label>Seconds between EA calls (the app tells every EA)</label>{_inp(s, 'ea_poll_seconds', 'number')}</div><div><label>Max EA requests handled at once per server</label>{_inp(s, 'max_inflight', 'number')}</div><div><button class='btn'>Save</button></div></form></div>")
    security = """<div class='card'><h2>Security</h2><p class='muted' style='margin-bottom:10px'>Change the admin password (at least 8 characters).</p><form action='/admin/password' method='post' class='grid'>
<div><label>Current password</label><input class='input' name='cur' type='password' required></div><div><label>New password</label><input class='input' name='new' type='password' minlength='8' required></div>
<div><label>Repeat new password</label><input class='input' name='new2' type='password' minlength='8' required></div><div><button class='btn'>Change password</button></div></form>
<form action='/admin/logout' method='post' style='margin-top:12px'><button class='btn' style='background:#0f172a'>Log out</button></form></div>"""
    # ---- users (searchable, paginated: works with millions of rows)
    per = 25
    like = f"%{q.lower()}%"
    ids = f"/admin?q={urllib.parse.quote(q)}"
    urows = db.q("SELECT u.id, u.email, u.created, u.blocked, u.trial_end, (SELECT COUNT(*) FROM accounts a WHERE a.user_id = u.id) AS n FROM users u " + ("WHERE u.email LIKE :q " if q else "") + "ORDER BY u.id DESC LIMIT :l OFFSET :o",
                 **({"q": like} if q else {}), l=per + 1, o=page * per)
    def ubtn(uid, op, label, style="", conf=""):
        return (f"<form action='/admin/user' method='post' style='display:inline'{conf}><input type='hidden' name='id' value='{uid}'><input type='hidden' name='op' value='{op}'><input type='hidden' name='ret' value='{escape(ret)}'>"
                f"<button class='btn' style='padding:4px 9px;font-size:11px;margin:2px;{style}'>{label}</button></form>")
    def urow(u):
        return (f"<div style='border-top:1px solid #e2e8f0;padding:8px 0;font-size:13px'><b>#{u['id']}</b> {escape(u['email'])} · {u['n']} accounts · joined {escape(str(u['created'])[:10])}"
                f"{' · <b style=color:#dc2626>BANNED</b>' if u['blocked'] else ''} · <a href='/admin?user={u['id']}'>accounts</a><br>"
                + (ubtn(u["id"], "unban", "Unban") if u["blocked"] else ubtn(u["id"], "ban", "Ban", "background:#dc2626")) + ubtn(u["id"], "resetpw", "Reset password") + ubtn(u["id"], "free", "Free access (all)") + (ubtn(u["id"], "trial", "Restart trial") if int(s.get("trial_days") or 0) > 0 else "")
                + ubtn(u["id"], "delete", "Delete user", "background:#dc2626", " onsubmit='return confirm(&quot;Delete this user and all their accounts?&quot;)'") + "</div>")
    nav = (f"<p style='margin-top:10px'>" + (f"<a href='{ids}&page={page - 1}'>← newer</a> " if page else "") + (f"<a href='{ids}&page={page + 1}'>older →</a>" if len(urows) > per else "") + "</p>")
    users = (f"<div class='card'><h2>Users</h2><form method='get' action='/admin' style='display:flex;gap:8px;margin:8px 0'><input class='input' name='q' value='{escape(q)}' placeholder='Search by email'><button class='btn'>Search</button></form>"
             + ("".join(urow(u) for u in urows[:per]) or "<p class='muted'>No users yet.</p>") + nav + "</div>")
    # ---- accounts
    uid, acct, tf_ = request.args.get("user", ""), request.args.get("acct", "").strip(), request.args.get("show", "")
    base = "SELECT a.*, u.email AS email, u.blocked AS ublocked, u.trial_end AS utrial FROM accounts a LEFT JOIN users u ON u.id = a.user_id "
    if uid.isdigit(): arows, title = db.q(base + "WHERE a.user_id=:u ORDER BY a.id", u=int(uid)), f"Accounts of user #{uid}"
    elif acct: arows, title = db.q(base + "WHERE a.id=:i OR a.legacy_id=:l", i=int(acct) if acct.isdigit() else -1, l=acct), "Account search"
    elif tf_ == "flagged": arows, title = db.q(base + "WHERE a.flags != '' ORDER BY a.id DESC LIMIT 50"), "Flagged accounts"
    elif tf_ in ("paper", "connected"): arows, title = db.q(base + "WHERE a.tier=:t ORDER BY a.id DESC LIMIT 50", t=("demo" if tf_ == "paper" else "real")), f"Latest {tf_} accounts"
    else: arows, title = db.q(base + "ORDER BY a.id DESC LIMIT 25"), "Latest accounts"
    accts = (f"<div class='card'><h2>{escape(title)}</h2><form method='get' action='/admin' style='display:flex;gap:8px;margin:8px 0;flex-wrap:wrap'><input class='input' name='acct' placeholder='Account # (or old number)' style='max-width:240px'><button class='btn'>Find</button>"
             "<a class='btn' style='background:#e2e8f0;color:#0f172a' href='/admin?show=flagged'>Flagged</a><a class='btn' style='background:#e2e8f0;color:#0f172a' href='/admin?show=paper'>Paper</a><a class='btn' style='background:#e2e8f0;color:#0f172a' href='/admin?show=connected'>Connected</a></form>"
             + ("".join(_arow(a, s, ret) for a in arows) or "<p class='muted'>Nothing here.</p>") + "</div>")
    # ---- 2-step login
    tot = _totp()
    if tot.get("enabled"):
        twofa = ("<div class='card'><h2>2-step login is ON</h2><p class='muted' style='margin-bottom:10px'>To turn it off, enter your password and a current code.</p><form action='/admin/2fa' method='post' class='grid'><input type='hidden' name='op' value='disable'>"
                 "<div><label>Password</label><input class='input' name='pw' type='password' required></div><div><label>Authenticator code</label><input class='input' name='code' inputmode='numeric' required></div>"
                 "<div><button class='btn' style='background:#dc2626'>Turn off</button></div></form></div>")
    elif TOTP_PENDING["secret"]:
        sk = TOTP_PENDING["secret"]
        twofa = (f"<div class='card'><h2>Set up 2-step login</h2><ol class='muted' style='line-height:1.9;margin:0 0 10px 18px'><li>Open Google Authenticator and tap +, then Enter a setup key.</li><li>Account: <b>Admin</b>. Key: <b style='word-break:break-all'>{' '.join(sk[i:i + 4] for i in range(0, len(sk), 4))}</b>. Time based.</li>"
                 "<li>Type the 6-digit code below.</li></ol><form action='/admin/2fa' method='post' class='grid'><input type='hidden' name='op' value='confirm'><div><label>Code</label><input class='input' name='code' inputmode='numeric' required></div><div><button class='btn'>Turn on</button></div></form></div>")
    else:
        twofa = ("<div class='card'><h2>2-step login</h2><p class='muted' style='margin-bottom:10px'>Adds a code from your phone to the admin login.</p><form action='/admin/2fa' method='post'><input type='hidden' name='op' value='start'><button class='btn'>Set up 2-step login</button></form></div>")
    tbl, tips = "", []
    for f, gr in st.items():
        for k, mm in gr.items():
            tbl += f"<div class='muted'>{f}={escape(k)}: {mm['n']} trades · win {mm['win']}% · avg {mm['exp']}R</div>"
            if mm["n"] >= 10 and mm["exp"] < 0: tips.append(f"Consider filtering {f}={k} ({mm['n']} trades, {mm['exp']}R avg)")
    jr = "".join(f"<div style='border-top:1px solid #e2e8f0;padding:10px 0'><b>{escape(j.get('tf', ''))} · {escape(j.get('kind', ''))}</b> <span class='muted'>{escape(j.get('time', '')[:16])}</span><div style='margin:6px 0'>{escape(j.get('lesson') or j.get('analysis', '') or '(old entry without a lesson)')}</div>"
                 f"<form action='/admin/lesson_delete/{escape(j['id'])}' method='post' onsubmit='return confirm(&quot;Delete this lesson?&quot;)'><button class='btn' style='padding:4px 10px;font-size:11px;background:#dc2626'>Delete lesson</button></form></div>"
                 for j in (json.loads(r["data"]) for r in db.q("SELECT data FROM journal ORDER BY created DESC LIMIT 30")))
    errs = "".join(f"<div class='muted'>{escape(r['created'][:16])} · {escape(r['symbol'])} · {escape(r['status'])}</div>" for r in db.q("SELECT created, symbol, status FROM orders WHERE platform='SYSTEM' ORDER BY created DESC LIMIT 5"))
    return layout("Admin", f"""{msg}{_warnings(s)}{dash}{_snap_card(s)}{_pay_card(s)}{brand}{access}{scale}{users}{accts}<div class='card'><h2>1. Settings</h2><form action='/admin/save' method='post' class='grid'>{fields}
<div><button class='btn'>Save settings</button></div></form>
<p class='muted' style='margin-top:8px'>Lists/filters are JSON. Example filter: {{"session": ["Asia"]}}</p>{errs}
<form action='/admin/scan' method='post' style='margin-top:10px'><button class='btn' style='background:#0f172a'>Run scan now</button></form></div>
<div class='card'><h2>2. Learning from results ({n} resolved signals)</h2>{tbl or '<p class=muted>Needs resolved trades.</p>'}<pre>{escape(chr(10).join(tips) or 'No suggestions yet (needs 10+ trades per group).')}</pre>
<form action='/admin/synthesize' method='post' style='margin-top:8px'><button class='btn'>Ask Claude for improvements</button></form><pre>{escape(kv_get('lessons', ''))}</pre></div>
<div class='card'><h2>3. Teach the robot from charts</h2><p class='muted' style='margin-bottom:10px'>Upload a 30 minute or 1 hour chart that shows a BOS or liquidity. Claude reads it once and saves a short written lesson. <b>The image itself is never stored</b>, so it uses no storage and the lesson stays after you close this page. Lessons are used when you press "Ask Claude for improvements" and when new charts are read. They do not change the robot's trading rules by themselves: you decide which settings to change.</p>
<form action='/admin/upload' method='post' enctype='multipart/form-data' class='grid'>
<div><label>Chart image (30m or 1h)</label><input class='input' type='file' name='img' accept='image/*'></div>
<div><label>Chart</label><select class='input' name='tf'><option value='30m'>30 minutes</option><option value='1h'>1 hour</option></select></div>
<div><label>What to learn</label><select class='input' name='kind'><option>BOS and liquidity</option><option>BOS</option><option>Liquidity</option></select></div>
<div style='grid-column:1/-1'><label>Notes (optional if you upload an image; type your own lesson here to save it without an image)</label><textarea class='input' name='note' rows='2' maxlength='600'></textarea></div>
<div><button class='btn'>Save lesson</button></div></form>{jr}</div>{security}{twofa}{ADMIN_JS}""")

@app.route("/admin/save", methods=["POST"])
@admin_only
def admin_save():
    s = load_settings(fresh=True); was_paid = bool(s["paid_mode"])
    for k, v in DEFAULTS.items():
        raw = request.form.get(k)
        if raw is None: continue
        try:
            new = json.loads(raw) if isinstance(v, (list, dict)) else (raw.strip().lower() in ("1", "true", "yes", "on") if isinstance(v, bool) else type(v)(raw))
            if isinstance(v, (list, dict)) and not isinstance(new, type(v)): continue
            if k == "mode" and (new not in ("paper", "live") or (new == "live" and request.form.get("confirm") != "LIVE")): continue
            if k == "ob_entry" and new not in ("top", "mid"): continue
            if k == "timeframe" and new not in ("30m", "1h"): continue
            if k == "timeframes": new = [t for t in new if t in ("30m", "1h")] or ["30m"]
            if k in BRAND_KEYS: new = new.strip()[:400 if k == "lead" else 80] or DEFAULTS[k]
            if k == "max_mitigation_pct": new = min(100.0, max(0.0, new))
            if k == "min_leg_atr": new = max(0.0, new)
            if k == "max_risk_pct": new = min(2.0, max(0.1, new))
            if k == "paper_currency":
                new = re.sub(r"[^A-Za-z0-9$€£]", "", new)[:6].upper()
                if not new: continue
            if k == "max_pairs": new = min(40, max(1, new))
            if k == "max_rr": new = min(10.0, max(2.0, new))
            if k == "paper_min_balance": new = min(1e9, max(1.0, new))
            if k == "paper_default_balance": new = min(1e9, max(1.0, new))
            if k == "risk_pct": new = min(2.0, max(0.1, new))
            if k in ("trial_days", "plan_days"): new = min(3650, max(0, new))
            if k in ("max_users", "max_accounts_per_user"): new = min(10 ** 9, max(0, new))
            if k == "ea_poll_seconds": new = min(600, max(10, new))
            if k == "max_inflight": new = min(5000, max(10, new))
            if k == "max_trades_per_day": new = min(100, max(0, new))
            if k == "snapshot_days": new = min(30.0, max(0.0, new))
            if k in ("sl_buffer_pips", "min_sl_pips"): new = min(100.0, max(0.0, new))
            if k == "max_sl_pips": new = min(1000.0, max(0.0, new))
            if k == "min_rr": new = min(10.0, max(0.5, new))
            if k == "bos_supersede_pct": new = min(100.0, max(0.0, new))
            if k in ("min_leg_bars", "bos_max_age_bars"): new = int(min(1000, max(0, new)))
            if k == "bot_max_rr": new = min(10.0, max(float(s.get("min_rr") or 1.0), new))
            if k in ("liq_inside_pct", "liq_inside_ob_pct", "liq_overlap_pct", "bos_body_pct"): new = min(100.0, max(0.0, new))
            if k == "reentry_bars": new = min(500, max(1, new))
            if k == "symbols":
                new = [(str(x).strip().upper() if str(x).strip().upper().endswith("USDT") else str(x).strip()) for x in new if re.fullmatch(r"[A-Za-z0-9._#]{3,14}", str(x).strip())][:40]
                new = list(dict.fromkeys(new))
                if not new: continue
            if k in ("price_text", "pay_note", "pay_ng"): new = new.strip()[:300]
            if k in ("price_usd", "price_ngn"): new = min(1e7, max(0.0, new))
            if k == "pay_usdt": new = re.sub(r"[^A-Za-z0-9]", "", new)[:120]
            if k == "pay_usdt_net": new = new.strip()[:40]
            if k == "payment_link":
                new = new.strip()[:300]
                if new and not new.startswith("https://"): continue
            s[k] = new
        except Exception: pass
    s["paper_default_balance"] = max(float(s["paper_default_balance"]), float(s["paper_min_balance"]))      # a reset must always give at least the minimum
    save_settings(s)
    if s["paid_mode"] and not was_paid and int(s.get("trial_days") or 0) > 0:         # monetisation just switched on AND a trial is set: everyone gets a fresh trial first
        d = str(today() + datetime.timedelta(days=int(s["trial_days"])))
        db.x("UPDATE users SET trial_end=:d", d=d); db.x("UPDATE accounts SET trial_end=:d WHERE user_id IS NULL", d=d)
        _flash(f"Saved. Charging for connected accounts is ON: every connected account has a free trial until {d}. Paper trading stays free.")
    else: _flash("Saved.")
    return redirect("/admin")

@app.route("/admin/password", methods=["POST"])
@admin_only
def admin_password():
    f = request.form
    if not check_admin(f.get("cur", "")): _flash("Current password is wrong.")
    elif len(f.get("new", "")) < 8 or f.get("new") != f.get("new2"): _flash("New passwords must match and be at least 8 characters.")
    else: set_admin_password(f["new"]); _flash("Password changed.")
    return redirect("/admin")

@app.route("/admin/logout", methods=["POST"])
def admin_logout(): session.clear(); return redirect("/admin")

def _ret(): r = request.form.get("ret", "/admin"); return r if r.startswith("/admin") else "/admin"

@app.route("/admin/account", methods=["POST"])
@admin_only
def admin_account():
    try: aid = int(request.form.get("id", ""))
    except ValueError: return redirect("/admin")
    op, s = request.form.get("op", ""), load_settings(fresh=True)
    a = db.one("SELECT * FROM accounts WHERE id=:i", i=aid)
    if a and op in ("activate", "free", "pause", "resume", "reset", "delete"):
        if op == "activate":
            base = max(today(), parse_date(a["paid_until"]) or today()); db.x("UPDATE accounts SET paid_until=:p WHERE id=:i", p=str(base + datetime.timedelta(days=int(s["plan_days"]))), i=aid)
        elif op == "free": db.x("UPDATE accounts SET paid_until='2099-12-31' WHERE id=:i", i=aid)
        elif op == "pause": db.x("UPDATE accounts SET blocked=1 WHERE id=:i", i=aid)
        elif op == "resume": db.x("UPDATE accounts SET blocked=0 WHERE id=:i", i=aid)
        elif op == "reset": db.x("UPDATE accounts SET flags='', fp_changes=0 WHERE id=:i", i=aid)
        elif op == "delete": db.x("DELETE FROM orders WHERE account_id=:i", i=aid); db.x("DELETE FROM accounts WHERE id=:i", i=aid)
        _flash(f"Done: {op} account #{aid}.")
    return redirect(_ret())

@app.route("/admin/user", methods=["POST"])
@admin_only
def admin_user():
    try: uid = int(request.form.get("id", ""))
    except ValueError: return redirect("/admin")
    op, s = request.form.get("op", ""), load_settings(fresh=True)
    u = db.one("SELECT * FROM users WHERE id=:i", i=uid)
    if u and op in ("ban", "unban", "resetpw", "free", "trial", "delete"):
        if op in ("ban", "unban"): db.x("UPDATE users SET blocked=:b WHERE id=:i", b=1 if op == "ban" else 0, i=uid)
        elif op == "resetpw":
            tmp = secrets.token_urlsafe(9); salt = os.urandom(16).hex(); db.x("UPDATE users SET pw_salt=:s, pw_hash=:h WHERE id=:i", s=salt, h=pw_hash(tmp, salt), i=uid)
            _flash(f"Temporary password for {u['email']} (tell them to log in with it): {tmp}"); return redirect(_ret())
        elif op == "free": db.x("UPDATE accounts SET paid_until='2099-12-31' WHERE user_id=:i", i=uid)
        elif op == "trial": db.x("UPDATE users SET trial_end=:d WHERE id=:i", d=str(today() + datetime.timedelta(days=int(s["trial_days"]))), i=uid)
        elif op == "delete":
            db.x("DELETE FROM orders WHERE account_id IN (SELECT id FROM accounts WHERE user_id=:i)", i=uid); db.x("DELETE FROM accounts WHERE user_id=:i", i=uid); db.x("DELETE FROM users WHERE id=:i", i=uid)
        _flash(f"Done: {op} user #{uid}.")
    return redirect(_ret())

@app.route("/admin/2fa", methods=["POST"])
@admin_only
def admin_2fa():
    f, op = request.form, request.form.get("op")
    if op == "start":
        if crypto_ready(): TOTP_PENDING["secret"] = base64.b32encode(os.urandom(20)).decode().rstrip("=")
        else: _flash("Set DATA_KEY on Render first, then try again.")
    elif op == "confirm":
        sec = TOTP_PENDING["secret"]
        if sec and totp_ok(sec, f.get("code", "")):
            kv_set("admin_totp", json.dumps({"enabled": True, "secret": "enc2:" + _enc2(sec, "admin:totp", kdf_salt())})); TOTP_PENDING["secret"] = None; _flash("2-step login is ON.")
        else: _flash("That code is not right. Check the clock on your phone and try again.")
    elif op == "disable":
        sec = _totp_secret()
        if check_admin(f.get("pw", "")) and sec and totp_ok(sec, f.get("code", "")): kv_set("admin_totp", "null"); _flash("2-step login is OFF.")
        else: _flash("Password or code is wrong.")
    return redirect("/admin")

def _lessons_text(n=10):
    out = []
    for r in db.q("SELECT data FROM journal ORDER BY created DESC LIMIT :n", n=n):
        j = json.loads(r["data"])
        if j.get("lesson"): out.append(f"[{j.get('tf', '')} {j.get('kind', '')}] {j['lesson']}")
    return out

@app.route("/admin/scan", methods=["POST"])
@admin_only
def admin_scan(): run_scan(); return redirect("/orders")

@app.route("/admin/upload", methods=["POST"])
@admin_only
def upload():
    """Chart lesson: the image is read once, turned into a short written lesson, and then thrown away (it is never saved), so it costs no storage."""
    f, fm = request.files.get("img"), request.form
    tf = fm.get("tf", "") if fm.get("tf", "") in ("30m", "1h") else "30m"; kind = fm.get("kind", "") if fm.get("kind", "") in ("BOS", "Liquidity", "BOS and liquidity") else "BOS and liquidity"
    note = fm.get("note", "").strip()[:600]; lesson = ""
    if f and f.filename:
        ext = os.path.splitext(secure_filename(f.filename))[1].lower()
        if ext not in (".png", ".jpg", ".jpeg", ".webp"): return "Unsupported file", 400
        raw = f.read(); mt = {".png": "image/png", ".webp": "image/webp"}.get(ext, "image/jpeg")
        known = "\n".join("- " + x for x in _lessons_text(6))
        sysmsg = ("You teach a trading robot by studying chart screenshots. The robot's rules: " + S()["strategy_text"] + " Write ONE compact lesson (at most 600 characters, plain text, no markdown) that a rule-based robot could use: "
                  "where the BOS (break of structure) and the liquidity (the order block or wick before the BOS) are, what a valid versus invalid example looks like on this chart, and what to watch for. "
                  "Do not describe the picture, and do not repeat these lessons it already knows:\n" + (known or "(none yet)"))
        txt = f"This is a {tf} chart. The admin wants the robot to learn about: {kind}. Admin notes: {note or 'none'}."
        out = claude([{"type": "image", "source": {"type": "base64", "media_type": mt, "data": base64.b64encode(raw).decode()}}, {"type": "text", "text": txt}], sysmsg)
        del raw
        if out.startswith(("Set ANTHROPIC_API_KEY", "Analysis failed")):
            if not note: _flash("The chart could not be read: " + out[:160] + " Type your own lesson in the notes box and upload again."); return redirect("/admin")
            lesson = note
        else: lesson = out.strip()[:700]
    elif note: lesson = note
    else: _flash("Choose a chart image, or type a lesson in the notes box."); return redirect("/admin")
    jid = uuid.uuid4().hex[:8]; j = {"id": jid, "tf": tf, "kind": kind, "note": note, "lesson": lesson, "time": now_s()}
    db.x("INSERT INTO journal (id, created, data) VALUES (:i, :c, :d)", i=jid, c=j["time"], d=json.dumps(j))
    _flash("Lesson saved. The image was not stored."); return redirect("/admin")

@app.route("/admin/lesson_delete/<jid>", methods=["POST"])
@admin_only
def lesson_delete(jid):
    db.x("DELETE FROM journal WHERE id=:i", i=jid); _flash("Lesson deleted."); return redirect("/admin")

@app.route("/admin/synthesize", methods=["POST"])
@admin_only
def synthesize():
    st, n = stats(track_rows(None)); notes = "\n---\n".join(_lessons_text(10))
    out = claude(f"Strategy rules: {S()['strategy_text']}\nBot stats ({n} trades): {json.dumps(st)}\nScreenshot reviews:\n{notes}\n\nFind repeating patterns in the losses and propose at most 3 testable changes (settings or filters). Flag small sample sizes honestly.", "You are a careful quantitative trading reviewer. Do not overfit; recommend paper-testing every change.")
    kv_set("lessons", out); return redirect("/admin")

# ======================================================================================================================
if os.environ.get("AUTOSCAN") == "1": threading.Thread(target=worker_main, daemon=True).start()      # tiny installs only; real traffic: run `python app.py worker` separately

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "worker": worker_main()
    else: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True)
