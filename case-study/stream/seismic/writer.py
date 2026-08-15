"""PostgreSQL persistence for seismic events."""

from __future__ import annotations

import psycopg2
from psycopg2.extras import DictCursor


class PostgresWriter:
    def __init__(self, host, port, dbname, user, password):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self.create_table()

    def create_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS earthquakes (
                    unid TEXT PRIMARY KEY,
                    source_id TEXT,
                    source_catalog TEXT,
                    lastupdate TIMESTAMP,
                    time TIMESTAMP,
                    flynn_region TEXT,
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    depth DOUBLE PRECISION,
                    evtype TEXT,
                    auth TEXT,
                    mag DOUBLE PRECISION,
                    magtype TEXT,
                    action TEXT,
                    received_at TIMESTAMP DEFAULT NOW()
                );
                """
            )
            self.conn.commit()

    def upsert_event(self, event):
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO earthquakes (
                    source_id, source_catalog, lastupdate, time, flynn_region,
                    lat, lon, depth, evtype, auth, mag, magtype, unid, action
                ) VALUES (
                    %(source_id)s, %(source_catalog)s, %(lastupdate)s,
                    %(time)s, %(flynn_region)s, %(lat)s, %(lon)s, %(depth)s,
                    %(evtype)s, %(auth)s, %(mag)s, %(magtype)s, %(unid)s,
                    %(action)s
                )
                ON CONFLICT (unid) DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    source_catalog = EXCLUDED.source_catalog,
                    lastupdate = EXCLUDED.lastupdate,
                    time = EXCLUDED.time,
                    flynn_region = EXCLUDED.flynn_region,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    depth = EXCLUDED.depth,
                    evtype = EXCLUDED.evtype,
                    auth = EXCLUDED.auth,
                    mag = EXCLUDED.mag,
                    magtype = EXCLUDED.magtype,
                    action = EXCLUDED.action,
                    received_at = NOW();
                """,
                event,
            )
            self.conn.commit()

    def delete_event(self, unid):
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM earthquakes WHERE unid = %s", (unid,))
            self.conn.commit()

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
