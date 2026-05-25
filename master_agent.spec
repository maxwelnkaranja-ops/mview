# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = []
binaries = []
hiddenimports = ['engineio.async_drivers.threading', 'pkg_resources.extern', 'socketio', 'socketio.async_drivers.threading', 'engineio', 'mss', 'PIL', 'PIL.Image', 'PIL.ImageDraw', 'PIL.ImageFilter', 'PIL.ImageEnhance', 'PIL.ImageGrab', 'psutil', 'requests', 'pyautogui', 'pyperclip', 'cryptography', 'cryptography.fernet', 'cryptography.hazmat.primitives.hashes', 'cryptography.hazmat.primitives.kdf.pbkdf2', 'watchdog', 'watchdog.observers', 'watchdog.observers.api', 'watchdog.observers.winapi', 'watchdog.observers.read_directory_changes', 'watchdog.events', 'cv2', 'numpy', 'pynput.keyboard', 'pynput.mouse', 'sounddevice', 'scipy', 'scipy.io', 'scipy.io.wavfile', 'scipy.signal', 'av', 'dxcam', 'comtypes', 'comtypes.client', 'comtypes.server', 'wmi', 'win32api', 'win32con', 'win32gui', 'win32process', 'win32security', 'win32evtlog', 'win32evtlogutil', 'win32com', 'win32com.client', 'pywintypes', 'pythoncom', 'winreg', 'ctypes.wintypes', 'logging.handlers', 'concurrent.futures']
tmp_ret = collect_all('cv2')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('numpy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('pynput')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('mss')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('PIL')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('cryptography')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('socketio')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('engineio')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('psutil')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('pyautogui')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('win32com')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('comtypes')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('av')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('dxcam')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('scipy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('sounddevice')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('requests')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('watchdog')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['agent_source.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='master_agent',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['C:\\Users\\maxxi\\Videos\\stitch_m_view_enterprise_suite\\icon.ico'],
)
