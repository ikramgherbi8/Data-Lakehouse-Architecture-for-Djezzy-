# Djezzy 5G Data Lakehouse — PFE Project

End-to-end Data Lakehouse pipeline built for 5G deployment reporting, developed as a Master's PFE (Mention Excellente, 18.5/20) at Djezzy (Optimum Telecom Algérie).

## Architecture

NiFi → Kafka → Spark/Scala → Apache Hudi (MergeOnRead) → HDFS → Hive → Trino → Apache Superset

Two ingestion paths:

- **Batch**: NiFi pulls source files (EGSN, EGSN_5G, OFR, CELL_SITE) → HDFS → Spark batch job → Hudi table `kpi_daily_subscriber`
- **Streaming**: NiFi → Kafka → Spark Structured Streaming job → Hudi table `party` (subscriber records, upserts)

Trino sits on top for SQL access (Hudi tables exposed as plain Parquet external tables due to a Trino/Hudi compatibility issue), and Superset connects to Trino for dashboards.

## Versions

| Component | Version |
|---|---|
| Hadoop (namenode/datanode) | 2.7.4 |
| Hive | 2.3.2 (metastore on PostgreSQL 2.3.0) |
| Spark | 2.4.5 (Hadoop 2.7) |
| Hudi | 0.10.0 (`hudi-spark-bundle_2.11`) |
| spark-avro | 2.4.4 |
| Trino | 406 |
| NiFi | 1.24.0 |
| Hive JDBC driver (used by NiFi) | 3.1.3 |
| Kafka / Zookeeper | Confluent 7.5.0 |
| Kafdrop | latest |

## Repository structure

```
├── README.md
├── stack/                  # everything needed to run the stack
│   ├── docker-compose.yml          # Hadoop, Hive, Trino, Spark, NiFi
│   ├── docker-compose-kafka.yml    # Zookeeper, Kafka, Kafdrop
│   ├── hadoop-hive.env
│   ├── core-site.xml, hdfs-site.xml
│   ├── trino/                      # Trino catalog and config
│   ├── hudi-jars/                  # Hudi + spark-avro jars for Spark
│   ├── nifi-libs/                  # Hive JDBC driver for NiFi
│   └── data/                       # put source data here (not included)
├── Nifi/                   # exported NiFi flow
├── Dashboards/             # Superset dashboard exports
├── Scala Spark jobs/       # batch and streaming jobs
├── Diagrams/
└── DDL Hive external tables batch.txt
```

## Run the stack

Requires Docker Desktop on Windows. Real data is not included (see `stack/data/README.md` for the expected files).

```bash
cd stack
docker compose up -d
docker compose -p docker-hive -f docker-compose-kafka.yml up -d
```

The compose project is named `docker-hive`, so containers are named like `docker-hive-namenode-1`, which the NiFi flow expects. Wait a minute or two for the namenode, datanode and Hive metastore to become healthy.

## Not included in this repo

- **Source data**: place the files listed in `stack/data/README.md` into `stack/data/`.
- **Spark job jar**: build `batchkpi.jar` from `Scala Spark jobs/` and upload it to HDFS at `hdfs://docker-hive-namenode-1:8020/spark-jobs/batchkpi.jar`.
- **NiFi runtime config** (keystores and generated passwords) is deliberately left out; NiFi generates its own on first start.

## Import the NiFi flow

1. Open NiFi at `https://localhost:8443/nifi`.
2. Drag a Process Group onto the canvas and use the upload option to select the flow JSON from `Nifi/`.
3. Right-click the canvas → **Enable all controller services**.
4. Check that hostnames match (`kafka:29092`, `hive-metastore:9083`, `docker-hive-namenode-1:8020`).
5. Update the Hive user in the DBCP connection pool if yours differs.
6. Right-click the canvas → **Start**.

## Import the Superset dashboards

1. Superset → Dashboards → **Import dashboards**.
2. Upload the zip files from `Dashboards/`.
3. Check the Trino connection: `trino://admin@host.docker.internal:8080/hive/djezzy_silver`.
4. Datasets included: `kpi_daily_subscriber`, `party`, plus 2 SQL-defined datasets (regional 5G volume and top-15 wilayas by subscribers).
