import socketio
import time
import collections

# CONFIG
SERVER_URL = "https://screen-connect-rtca.onrender.com"
DEVICE_ID = "MV-A5ABD3848FA805741C1B"

sio = socketio.Client()

# Metrics tracking
frame_times = collections.deque(maxlen=100)
frame_sizes = collections.deque(maxlen=100)
latencies = collections.deque(maxlen=100)

@sio.event
def connect():
    print(f"Connected to {SERVER_URL}")
    sio.emit("watch_device", {"device_id": DEVICE_ID})
    print(f"Sent watch_device for {DEVICE_ID}")

@sio.on("frame_bin")
def on_frame(data):
    now = time.monotonic()
    frame_times.append(now)
    frame_sizes.append(len(data))
    
    if len(frame_times) % 10 == 0:
        duration = frame_times[-1] - frame_times[0]
        if duration > 0:
            fps = (len(frame_times) - 1) / duration
            kbps = (sum(frame_sizes) * 8 / 1024) / duration
            print(f"[{len(frame_times)}] Metrics: {fps:.2f} FPS | {kbps:.2f} Kbps | Size: {len(data)/1024:.2f} KB")

@sio.on("watch_ok")
def on_watch_ok(data):
    print(f"Watch OK: {data}")
    # Kickstart stream
    sio.emit("request_action", {
        "tab": "monitor", "action": "start", "device_id": DEVICE_ID,
        "fps": 20, "quality": 70, "scale": 0.8, "monitor": 1
    })
    
    # Measure Latency
    sio.emit("ping_agent", {"device_id": DEVICE_ID, "t": time.time()})

@sio.on("pong_agent")
def on_pong_agent(data):
    rtt = (time.time() - data.get("t", time.time())) * 1000
    latencies.append(rtt)
    print(f"Latency: {rtt:.2f} ms")

@sio.on("agent_alert")
def on_alert(data):
    print(f"AGENT ALERT: {data}")

try:
    sio.connect(SERVER_URL, wait_timeout=15)
    time.sleep(30) # Run for 30s
finally:
    if len(frame_times) > 1:
        duration = frame_times[-1] - frame_times[0]
        fps = (len(frame_times) - 1) / duration
        total_kb = sum(frame_sizes) / 1024
        kbps = (total_kb * 8) / duration
        print("\n--- PERFORMANCE REPORT ---")
        print(f"Average FPS:     {fps:.2f}")
        print(f"Average Kbps:    {kbps:.2f}")
        if latencies:
            print(f"Average Latency: {sum(latencies)/len(latencies):.2f} ms")
    else:
        print("\nNo frames received. Check if agent is actually streaming.")
    sio.disconnect()
