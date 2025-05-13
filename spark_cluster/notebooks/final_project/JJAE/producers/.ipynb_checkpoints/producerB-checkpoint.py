import json
import logging
import time
from datetime import datetime

import websocket
from kafka import KafkaProducer # Comentar SOLO PARA PRUEBAS DEL PRODUCTOR antes de enviarlo al Kafka, pip install kafka

# Confgs
API_KEY      = "d05r249r01qgqsu91h9gd05r249r01qgqsu91ha0"
KAFKA_SERVER = "eb8b2f78a91f:9092"
TOPIC        = "stream-btc"
SYMBOL       = "BINANCE:BTCUSDT"
SEND_DELAY   = 1.0
LOG_EVERY_N  = 50

logging.basicConfig(
    filename=f"{TOPIC}.log",
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

# COMENTAR producer PARA PRUEBAS antes del Kafka
producer = KafkaProducer(
     bootstrap_servers=KAFKA_SERVER,
     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

count = 0  # records sent

def on_message(ws, message):
    global count
    data = json.loads(message)

    if "data" not in data:
        return

    latest = data["data"][-1]
    quote = {
        "price":   latest["p"],
        "symbol":  latest["s"],
        "volume":  latest["v"],
        "ts":      datetime.utcnow().isoformat(timespec="seconds"),
    }

    print("Quote, for testing purposes:", quote)

    producer.send(TOPIC, quote) # COMENTAR para deshabilitar kafka
    print(quote)
    count += 1

    if count % LOG_EVERY_N == 0:
        logging.info(f"records_processed={count}")

    time.sleep(SEND_DELAY)


def on_error(ws, error):
    logging.error(error)


def on_close(ws, *_):
    logging.warning("WebSocket closed")


def on_open(ws):
    ws.send(json.dumps({"type": "subscribe", "symbol": SYMBOL}))
    logging.info(f"Subscribed to {SYMBOL}")


if __name__ == "__main__":
    ws_url = f"wss://ws.finnhub.io?token={API_KEY}"
    websocket.enableTrace(False)
    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    try:
        ws_app.run_forever()
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
