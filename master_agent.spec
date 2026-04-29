# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['agent_source.py'],
    pathex=['./clean_env/Lib/site-packages'],
    binaries=[],
    datas=[],
    hiddenimports=['mss', 'cv2', 'engineio.async_drivers.threading', 'pynput.keyboard', 'pynput.mouse'],
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
    icon=['icon.ico'],
)
