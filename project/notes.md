`conda create -n zoomdeproj python=3.9`
`python -m venv project`
`source project_venv/bin/activate`
`pip install -r requirements.txt`
created a .gitignore
configure Prefect to communicate with the server:
    `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`
    `prefect cloud login`
    `prefect cloud workspace set --workspace "tonyhaowugmailcom/zoomde"`
    set PREFECT_API_URL to NULL to enable ephemeral api
create bq and gc resources using terraform
dbt
    setup profile locally







Contents:
Problem description
2 points: Problem is well described and it's clear what the problem the project solves
-   `create a md file which describes all the steps and process`
Cloud
4 points: The project is developed in the cloud and IaC tools are used
-   `terraform files, setup gcp`
Data ingestion (choose either batch or stream)
Batch / Workflow orchestration
4 points: End-to-end pipeline: multiple steps in the DAG, uploading data to data lake
-   `flows to download payments data, split into multiple parquet files, upload to gcs`
Data warehouse
4 points: Tables are partitioned and clustered in a way that makes sense for the upstream queries (with explanation)
-   `partitioning and clustering on BQ after dbt transformations`
Transformations (dbt, spark, etc)
4 points: Tranformations are defined with dbt, Spark or similar technologies
-   `use dbt core to do this`
Dashboard
4 points: A dashboard with 2 tiles
-   `shiny`
Reproducibility
4 points: Instructions are clear, it's easy to run the code, and the code works
-   `fully repro`
