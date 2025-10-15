import json, random, socket, time
from datetime import datetime, timezone

HOST, PORT = "0.0.0.0", 9999
SLEEP_SECONDS = 0.3

def make_record(i:int):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "trip_id": f"t{i:08d}",
        "driver_id": f"d{random.randint(1, 25):03d}",
        "distance_km": round(random.uniform(0.5, 30.0), 2),
        "fare_amount": round(random.uniform(3.0, 80.0), 2),
        "timestamp": now
    }

def serve():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(1)
        print(f"[generator] Listening on {HOST}:{PORT} … waiting for Spark to connect")
        conn, addr = s.accept()
        with conn:
            print(f"[generator] Client connected from {addr}")
            i = 0
            while True:
                rec = make_record(i)
                line = json.dumps(rec) + "\n"
                conn.sendall(line.encode("utf-8"))
                i += 1
                time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    serve()
