import subprocess
import json
import os

try:
    result = subprocess.run(
        [
            "gcloud", "run", "jobs", "executions", "list",
            "--job=bus-density-image",
            "--region=us-central1",
            "--limit=1",
            "--sort-by=~createTime",
            "--format=json"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True
    )
    raw_json = result.stdout
    parsed = json.loads(raw_json)
    job_stauts = json.dumps(parsed, indent=2)
    print(job_stauts)
except subprocess.CalledProcessError as e:
    print("❌ Error running gcloud:", e.stderr)
    
