CREATE TABLE IF NOT EXISTS streaming_db.kafka.earthquake_archive_watermark (
    pipeline_name STRING,
    last_ingestion_date TIMESTAMP_LTZ,
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO streaming_db.kafka.earthquake_archive_watermark
    (pipeline_name, last_ingestion_date)
SELECT
    'earthquake_daily_archive',
    '1970-01-01 00:00:00'::TIMESTAMP_LTZ
WHERE NOT EXISTS (
    SELECT 1
    FROM streaming_db.kafka.earthquake_archive_watermark
    WHERE pipeline_name = 'earthquake_daily_archive'
);