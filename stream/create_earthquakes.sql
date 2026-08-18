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
