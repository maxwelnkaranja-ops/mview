import socketio
import time
import collections

sio = socketio.Client()

# Metrics tracking
frame_times = collections.deque(maxlen=100)
frame_sizes = collections.deque(maxlen=100)

@sio.event
def connect():
    print("Viewer connected to local server")
    device_id = "MV-1986392E2951A9C7D035"
    sio.emit("watch_device", {"device_id": device_id})
    print(f"Sent watch_device for {device_id}")
    
    # Also join dashboards to see global updates
    sio.emit("join_dashboard", {})
    
    # Try to force a frame
    print("Requesting screenshot to force frame...")
    sio.emit("request_screenshot", {"device_id": device_id})

@sio.on("*")
def catch_all(event, data):
    if event == "frame_bin":
        now = time.monotonic()
        frame_times.append(now)
        frame_sizes.append(len(data))
        if len(frame_times) % 10 == 0:
            duration = frame_times[-1] - frame_times[0]
            fps = (len(frame_times) - 1) / duration
            print(f"FPS: {fps:.2f} | Last Frame: {len(data)} bytes")
    else:
        print(f"Event: {event} | Data: {data}")

try:
    sio.connect("http://127.0.0.1:10000", wait_timeout=10)
    time.sleep(40)
finally:
    if len(frame_times) > 1:
        duration = frame_times[-1] - frame_times[0]
        fps = (len(frame_times) - 1) / duration
        print(f"\nFINAL FPS: {fps:.2f}")
    else:
        print("\nNo frames received.")
    sio.disconnect()
