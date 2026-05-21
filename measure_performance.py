import socketio
import time
import collections

sio = socketio.Client()

# Metrics tracking
frame_times = collections.deque(maxlen=100)
frame_sizes = collections.deque(maxlen=100)
latencies = collections.deque(maxlen=100)
start_time = time.monotonic()

@sio.event
def connect():
    print("Viewer connected to local server")
    # Device ID from previous logs
    device_id = "MV-1986392E2951A9C7D035"
    sio.emit("watch_device", {"device_id": device_id})
    print(f"Sent watch_device for {device_id}")

@sio.on("*")
def any_event(event, data):
    if event not in ["frame_bin", "cursor_bin"]:
        print(f"Received event: {event} | Data: {data}")

@sio.on("frame_bin")
def on_frame(data):
    print(f"Received frame: {len(data)} bytes") # Debug
    now = time.monotonic()
    frame_times.append(now)
    frame_sizes.append(len(data))
    
    # Calculate metrics every 20 frames
    if len(frame_times) >= 20 and len(frame_times) % 10 == 0:
        duration = frame_times[-1] - frame_times[0]
        fps = (len(frame_times) - 1) / duration
        kbps = (sum(frame_sizes) * 8 / 1024) / duration
        avg_size = sum(frame_sizes) / len(frame_sizes) / 1024
        print(f"Metrics: {fps:.2f} FPS | {kbps:.2f} Kbps | Avg Frame: {avg_size:.2f} KB")

@sio.on("watch_ok")
def on_watch_ok(data):
    print(f"Watch OK: {data}")
    # Manually request stream start
    device_id = data.get("device_id")
    if device_id:
        stream_payload = {
            "tab":       "monitor",
            "action":    "start",
            "device_id": device_id,
            "fps":       25,
            "quality":   70,
            "scale":     0.8,
            "monitor":   1,
        }
        sio.emit("request_action", stream_payload)
        print(f"Sent manual request_action start for {device_id}")
        
        # Also try start_stream event
        sio.emit("start_stream", {"device_id": device_id})
        print(f"Sent start_stream for {device_id}")

        # Try a screenshot
        sio.emit("request_screenshot", {"device_id": device_id})
        print(f"Sent request_screenshot for {device_id}")

        # Try a ping
        sio.emit("ping_agent", {"device_id": device_id, "t": time.time()})
        print(f"Sent ping_agent for {device_id}")

@sio.on("pong_agent")
def on_pong_agent(data):
    rtt = (time.time() - data.get("t", time.time())) * 1000
    print(f"Pong received! RTT: {rtt:.2f} ms")

@sio.on("pong_check") # Assuming server might send some timing data or we can measure round-trip
def on_pong(data):
    pass

try:
    sio.connect("http://127.0.0.1:10000", wait_timeout=10)
    # Run for 60 seconds to allow watchdog recovery if needed
    time.sleep(60)
finally:
    if len(frame_times) > 1:
        duration = frame_times[-1] - frame_times[0]
        fps = (len(frame_times) - 1) / duration
        total_kb = sum(frame_sizes) / 1024
        kbps = (total_kb * 8) / duration
        print("\n--- FINAL PERFORMANCE REPORT ---")
        print(f"Sample Duration: {duration:.2f}s")
        print(f"Total Frames:    {len(frame_times)}")
        print(f"Average FPS:     {fps:.2f}")
        print(f"Average KB/s:    {total_kb/duration:.2f}")
        print(f"Average Kbps:    {kbps:.2f}")
    else:
        print("\nNo frames received from agent.")
    sio.disconnect()
