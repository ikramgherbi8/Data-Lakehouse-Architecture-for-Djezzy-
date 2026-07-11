# Djezzy 5G Data Lakehouse — PFE Project

End-to-end Data Lakehouse pipeline built for 5G deployment reporting, developed as a Master's PFE (Mention Excellente, 18.5/20) at Djezzy (Optimum Telecom Algérie).

## Architecture

```
NiFi → Kafka → Spark/Scala → Apache Hudi (MergeOnRead) → HDFS → Hive → Trino → Apache Superset
```

Two ingestion paths:
- **Batch**: NiFi pulls source files (EGSN, EGSN_5G, OFR, CELL_SITE) → HDFS → Spark batch job → Hudi table `kpi_daily_subscriber`
- **Streaming**: NiFi → Kafka → Spark Structured Streaming job → Hudi table `party` (subscriber records, upserts)

Trino sits on top for SQL access (Hudi tables exposed as plain Parquet external tables due to a Trino/Hudi compatibility issue), and Superset connects to Trino for dashboards.

## Tech stack & versions

| Component | Version |
|---|---|
| Apache NiFi | 1.24.0 |
| Apache Kafka | 7.5.0 (Confluent Platform image, Kafka wire-compatible) |
| Zookeeper | 7.5.0 (Confluent Platform image) |
| Kafdrop (Kafka UI) | latest |
| Apache Spark | 2.4.5 (Hadoop 2.7 build) — Scala jobs |
| Apache Hudi | MergeOnRead table type |
| Hadoop / HDFS | 2.7.4 (namenode/datanode), client libs Hadoop 2.7 |
| Apache Hive | 2.3.2 (metastore + server), JDBC driver `hive-jdbc-3.1.3-standalone.jar` used from NiFi |
| PostgreSQL (Hive metastore DB) | 2.3.0 (bde2020 metastore-postgresql image) |
| Trino | 406 |
| Apache Superset | (add your version — not visible in the compose files above) |
| Docker Compose | 2 stacks: (1) Hadoop/Hive/Trino/Spark/NiFi, (2) Kafka/Zookeeper/Kafdrop |

## Repository structure

```
├── README.md
├── docker/
│   ├── hadoop-hive-trino-spark-nifi-compose.yml   # namenode, datanode, hive-server,
│   │                                                # hive-metastore, postgres, trino,
│   │                                                # spark-master/worker1/worker2, nifi
│   ├── kafka-compose.yml                           # zookeeper, kafka, kafdrop
│   ├── hadoop-hive.env
│   ├── core-site.xml
│   └── hdfs-site.xml
├── spark-jobs/
│   ├── BatchKpiJob.scala
│   └── StreamingPartyJob.scala
├── nifi/
│   └── full_pipeline_flow.json      # exported "with external services"
├── superset/
│   ├── dashboard_export_kpi_daily_subscriber.zip
│   └── dashboard_export_streaming_party.zip
├── diagrams/
└── docs/
```

## Restoring this project from scratch

### 1. Prerequisites (not stored in this repo)

These aren't captured by the compose files or NiFi flow export and must be provided separately:
- Hadoop config files `core-site.xml` and `hdfs-site.xml` — mounted into both NiFi (`/opt/nifi/hadoop/`) and Trino (`/etc/trino/`)
- `hadoop-hive.env` — shared env file used by namenode, datanode, hive-server, hive-metastore, spark-master/workers
- `hive-jdbc-3.1.3-standalone.jar`, mounted into the NiFi container at `/opt/nifi/nifi-current/`
- Hudi jars folder (mounted from a local `hudi-jars` directory into `spark-master`, `spark-worker1`, `spark-worker2` at `/spark/jars/hudi-jars`)
- Trino catalog config: `trino/catalog/`, `trino/config.properties`, `trino/node.properties`
- Compiled Spark job jar `batchkpi.jar`, expected on HDFS at `hdfs://<namenode>:8020/spark-jobs/batchkpi.jar`
- Source data files (mounted into NiFi at `/opt/nifi/data/`), expected at:
  - `/opt/nifi/data/egsn_5g/EGSN_5G.txt`
  - `/opt/nifi/data/egsn/egsn_06.csv`
  - `/opt/nifi/data/cell_site/cell_site.csv`
  - `/opt/nifi/data/ofr/OFR_06.csv`
  - `/opt/nifi/data/TOPIC/` (`payment_update.txt`, `customer_subscription.txt`)

> Note: the original compose file uses absolute Windows host paths (`C:/hudi-jars`, `C:/nifi-1.24.0/conf`, `C:/djezzy-data`, etc.). Update these bind-mount paths to match whatever machine you're restoring on.

### 2. Start the stack

```bash
docker compose -f docker/hadoop-hive-trino-spark-nifi-compose.yml up -d
docker compose -f docker/kafka-compose.yml up -d
```

Give the namenode/datanode/hive-metastore a minute or two to become healthy before starting NiFi flows that depend on HDFS — the compose file's `SERVICE_PRECONDITION` env vars handle most of this automatically, but Hive metastore in particular can take a bit to come up (Postgres-backed).

### 3. Import the NiFi flow

1. Open NiFi UI (`https://localhost:8443/nifi` or your mapped port)
2. Drag a Process Group onto the canvas
3. In the naming dialog, use the import/upload option and select `nifi/full_pipeline_flow.json`
4. Right-click the canvas → **Enable all controller services**
5. Verify container hostnames match your environment (`kafka:29092`, `hive-metastore:9083`, `docker-hive-namenode-1:8020`) — update in processor configs if different
6. Right-click canvas → **Start**

### 4. Import Superset dashboards

1. Superset UI → Dashboards → **Import dashboards**
2. Upload `superset/dashboard_export_kpi_daily_subscriber.zip`, then `dashboard_export_streaming_party.zip`
3. When prompted, confirm/re-enter the Trino database connection (password is stripped from export by design)
4. Datasets included: `kpi_daily_subscriber`, `party`, plus 2 virtual (SQL-defined) datasets — regional 5G volume breakdown and top-15-wilayas subscriber count

## Notes
- The Hive DBCP connection pool in the NiFi flow uses a placeholder database user; update to your actual Hive user before running.
## Author

Ikram Gherbi — Data Engineer, Master 2 ISIA, Université Hassiba Benbouali de Chlef
GitHub: [github.com/ikramgherbi8](https://github.com/ikramgherbi8)
