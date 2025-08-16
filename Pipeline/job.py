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
