"""Stream SeismicPortal events directly into PostgreSQL."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

from websocket import WebSocketApp

from writer import PostgresWriter


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger("seismic-streamer")


def parse_time(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


class EarthquakeStreamer:
    def __init__(self):
        self.websocket_url = os.getenv(
            "SEISMIC_WEBSOCKET_URL",
            "wss://www.seismicportal.eu/standing_order/websocket",
        )
        self.db = PostgresWriter(
            host=os.getenv("DB_HOST", "postgres"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "maritime_logistics"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )

    def on_message(self, _websocket, message):
        try:
            data = json.loads(message)
            event = data.get("data", {})
            properties = event.get("properties")
            if not properties:
                LOGGER.warning("Ignoring a message without event properties")
                return

            coordinates = event.get("geometry", {}).get("coordinates", [0, 0, 0])
            fields = {
                "source_id": properties.get("source_id", "N/A"),
                "source_catalog": properties.get("source_catalog", "N/A"),
                "lastupdate": parse_time(properties.get("lastupdate")),
                "time": parse_time(properties.get("time")),
                "flynn_region": properties.get("flynn_region", "N/A"),
                "lat": coordinates[1] if len(coordinates) > 1 else 0,
                "lon": coordinates[0] if coordinates else 0,
                "depth": abs(coordinates[2]) if len(coordinates) > 2 else 0,
                "evtype": properties.get("evtype", "N/A"),
                "auth": properties.get("auth", "N/A"),
                "mag": properties.get("mag", 0),
                "magtype": properties.get("magtype", "N/A"),
                "unid": properties.get("unid", "N/A"),
                "action": data.get("action", "unknown"),
            }

            if fields["action"] == "delete":
                self.db.delete_event(fields["unid"])
            else:
                self.db.upsert_event(fields)
            LOGGER.info(
                "Processed event %s with action %s",
                fields["unid"],
                fields["action"],
            )
        except Exception:
            LOGGER.exception("Failed to process seismic message")

    @staticmethod
    def on_error(_websocket, error):
        LOGGER.error("WebSocket error: %s", error)

    @staticmethod
    def on_open(_websocket):
        LOGGER.info("Connected to SeismicPortal")

    def on_close(self, _websocket, status_code, message):
        LOGGER.warning("WebSocket closed: %s %s", status_code, message)
        self.db.close()

    def run(self):
        websocket = WebSocketApp(
            self.websocket_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        websocket.run_forever(ping_interval=15, ping_timeout=10)


def main():
    retry_seconds = int(os.getenv("RETRY_SECONDS", "10"))
    while True:
        try:
            EarthquakeStreamer().run()
        except KeyboardInterrupt:
            LOGGER.info("Stream stopped")
            return
        except Exception:
            LOGGER.exception("Streamer failed; retrying in %s seconds", retry_seconds)
        time.sleep(retry_seconds)


if __name__ == "__main__":
    main()
