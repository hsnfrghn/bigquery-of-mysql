# Retail Data Mesh on GCP

Terraform-defined GCP platform where two domains (orders, inventory) each own and publish a tested
dbt data product in BigQuery; a Python Beam pipeline ingests order events, and Airflow schedules the
runs and freshness checks.

> Work in progress — built step by step per `HANDOFF.md`. The full README is written in Step 8.

Stack: Python · SQL · Terraform · BigQuery · Dataflow (Apache Beam) · dbt · Apache Airflow · DuckDB (local mode)
