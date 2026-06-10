import socketio
import time
import sys

sio = socketio.Client()
frames = 0
start_time = time.time()

@sio.on('frame_bin')
def on_frame(data):
    global frames
    frames += 1
    # Force immediate print to stdout for debug visibility
    elapsed = time.time() - start_time
    fps = frames / elapsed
    sys.stdout.write(f"\rDASHBOARD SIM: Received {frames} frames | E2E FPS: {fps:.1f}")
    sys.stdout.flush()

@sio.on('connect')
def on_connect():
    print("Connected to server")
    sio.emit('watch_device', {'device_id': 'TEST-AGENT', 'fps': 60, 'quality': 95})

try:
    sio.connect('http://localhost:10000')
    time.sleep(15)
    print("\nTest complete.")
except Exception as e:
    print(f"Error: {e}")
finally:
    sio.disconnect()
