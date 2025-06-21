import pandas as pd
import json
from kafka import KafkaProducer


CSV_FILE     = "l1_day.csv"
TOPIC        = "mock_l1_stream"
BROKER       = "kafka:9092"

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load CSV
df = pd.read_csv(CSV_FILE, parse_dates=["ts_event"])
df = df[(df["ts_event"] >= "2024-08-01T13:36:32Z") & (df["ts_event"] <= "2024-08-01T13:45:14Z")]

prev_ts = None
for _, row in df.sort_values("ts_event").iterrows():
    ts = row["ts_event"]
    snapshot = {
        "timestamp": ts.isoformat(),
        "venues": [{
            "publisher_id": int(row["publisher_id"]),
            "ask_px_00":    float(row["ask_px_00"]),
            "ask_sz_00":    int(row["ask_sz_00"])
        }]
    }
    producer.send(TOPIC, snapshot)
    # print("Sent:", snapshot)

    # # Optional real-time pacing using timestamp delta
    # if prev_ts is not None:
    #     delta = (ts_event - prev_ts).total_seconds()
    #     time.sleep(min(delta, 1))  # cap max sleep to 1 sec
    # else:
    #     time.sleep(0.01)
    # prev_ts = ts_event
producer.flush()
producer.close()
