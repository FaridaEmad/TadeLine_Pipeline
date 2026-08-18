# Bronze Layer

The Bronze Layer is responsible for ingesting raw batch data from the SFTP server into HDFS without applying any transformations.

## Flow

```text
SFTP → Ingestion Script → HDFS /raw_layer
```
The scripts automatically discover all CSV files in the SFTP directories, download them temporarily, and upload them to HDFS using date-based partitions.

## Scripts
 - `config.sh` — Contains SFTP and HDFS configuration.
 - `load_ports_to_hdfs.sh` — Ingests port data.
 - `load_vessels_to_hdfs.sh` — Ingests vessel data.

## How to Run

Start the containers:

```bash
docker compose up -d
```

Enter the itvdelab container:

```bash
docker exec -it itvdelab bash
```

Run ports ingestion:

``` bash
bash /bronze_scripts/load_ports_to_hdfs.sh
```

Run vessels ingestion:

```bash
bash /bronze_scripts/load_vessels_to_hdfs.sh
```

## HDFS Structure

- Files are partitioned by year and month based on the date in the filename.

Example:

```text
/raw_layer/ports/year=2026/month=08/
/raw_layer/vessels/year=2026/month=08/
```

## Verify HDFS
```bash
hdfs dfs -ls -R /raw_layer
```

The scripts process all available CSV files automatically, so no filename needs to be provided manually.