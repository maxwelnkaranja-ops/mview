@echo off
REM ═══════════════════════════════════════════════════════════
REM  M-View Agent Builder  — Run this in your project folder
REM ═══════════════════════════════════════════════════════════

echo.
echo [1/4] Installing dependencies...
py -3.12 -m pip install --upgrade ^
    flask flask-socketio flask-cors flask-compress ^
    supabase python-dotenv ^
    python-socketio[client] ^
    mss Pillow ^
    psutil ^
    pywin32 ^
    pynput ^
    pyperclip ^
    cryptography ^
    opencv-python-headless ^
    numpy ^
    requests ^
    wmi ^
    pyinstaller

echo.
echo [2/4] Creating bin directory...
if not exist bin mkdir bin

echo.
echo [3/4] Compiling agent...
py -3.12 -m PyInstaller ^
    --onefile ^
    --noconsole ^
    --distpath ./bin ^
    --workpath ./build ^
    --specpath ./build ^
    --name master_agent ^
    --hidden-import=engineio.async_drivers.threading ^
    --hidden-import=pkg_resources.extern ^
    --hidden-import=win32api ^
    --hidden-import=win32con ^
    --hidden-import=wmi ^
    --hidden-import=pynput.keyboard._win32 ^
    --hidden-import=pynput.mouse._win32 ^
    --collect-all=socketio ^
    --collect-all=engineio ^
    agent_source.py

echo.
echo [4/4] Verifying...
if exist bin\master_agent.exe (
    echo SUCCESS: bin\master_agent.exe created!
    dir bin\master_agent.exe
) else (
    echo ERROR: Build failed. Check the output above.
    exit /b 1
)

echo.
echo ═══════════════════════════════════════════════
echo  Done! Now run: py -3.12 server.py
echo ═══════════════════════════════════════════════
pause