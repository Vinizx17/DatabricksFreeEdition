# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.49.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


Spark_Job = Job.from_dict(
    {
        "name": "Spark_Job",
        "tasks": [
            {
                "task_key": "Spark_job",
                "spark_python_task": {
                    "python_file": "/Workspace/Spark-jobs/main.py",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "SQL_estado_civil",
                "depends_on": [
                    {
                        "task_key": "Spark_job",
                    },
                ],
                "sql_task": {
                    "query": {
                        "query_id": "0e4e4f78-5bfd-4012-9fb4-26db84e52d08",
                    },
                    "warehouse_id": "3406b93f719bc4ca",
                },
            },
            {
                "task_key": "SQL_hora_extra",
                "depends_on": [
                    {
                        "task_key": "Spark_job",
                    },
                ],
                "sql_task": {
                    "query": {
                        "query_id": "baf24ca0-9684-444c-9883-e133c74311b5",
                    },
                    "warehouse_id": "3406b93f719bc4ca",
                },
            },
            {
                "task_key": "SQL_salario_departamento",
                "depends_on": [
                    {
                        "task_key": "Spark_job",
                    },
                ],
                "sql_task": {
                    "query": {
                        "query_id": "6803f010-92f0-452b-8eb9-f8897f537c3c",
                    },
                    "warehouse_id": "3406b93f719bc4ca",
                },
            },
            {
                "task_key": "SQL_satisfacao",
                "depends_on": [
                    {
                        "task_key": "Spark_job",
                    },
                ],
                "sql_task": {
                    "query": {
                        "query_id": "de68d569-1702-4a6d-b53a-c48f4ad8a8d0",
                    },
                    "warehouse_id": "3406b93f719bc4ca",
                },
            },
            {
                "task_key": "SQL_tempo_empresa",
                "depends_on": [
                    {
                        "task_key": "Spark_job",
                    },
                ],
                "sql_task": {
                    "query": {
                        "query_id": "0c5c304f-5b05-45f0-9260-ba95427447cb",
                    },
                    "warehouse_id": "3406b93f719bc4ca",
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "client": "2",
                },
            },
        ],
        "performance_target": "STANDARD",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Spark_Job, job_id=747249905370303)
# or create a new job using: w.jobs.create(**Spark_Job.as_shallow_dict())
