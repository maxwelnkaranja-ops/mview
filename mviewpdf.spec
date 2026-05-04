# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.win32.versioninfo import (
    VSVersionInfo, FixedFileInfo, StringFileInfo, StringTable,
    StringStruct, VarFileInfo, VarStruct
)

version_info = VSVersionInfo(
    ffi=FixedFileInfo(
        filevers=(4, 0, 0, 0),
        prodvers=(4, 0, 0, 0),
        mask=0x3f,
        flags=0x0,
        OS=0x4,
        fileType=0x1,
        subtype=0x0,
        date=(0, 0)
    ),
    kids=[
        StringFileInfo([
            StringTable(
                u'040904B0',
                [
                    StringStruct(u'CompanyName', u'MView Systems'),
                    StringStruct(u'FileDescription', u'MView System Monitor'),
                    StringStruct(u'FileVersion', u'4.0.0.0'),
                    StringStruct(u'InternalName', u'mviewpdf'),
                    StringStruct(u'LegalCopyright', u'\xa9 2026 MView'),
                    StringStruct(u'OriginalFilename', u'mviewpdf.exe'),
                    StringStruct(u'ProductName', u'MView Agent'),
                    StringStruct(u'ProductVersion', u'4.0.0.0'),
                ]
            )
        ]),
        VarFileInfo([VarStruct(u'Translation', [1033, 1200])])
    ]
)

a = Analysis(
    ['agent_source.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='mviewpdf',
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
    version=version_info,
)