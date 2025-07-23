import subprocess
import json

def get_execution_status(execution_id="bus-density-image-7dld9", region="us-central1"):
    try:
        result = subprocess.run(
            [
                "gcloud", "run", "jobs", "executions", "describe", execution_id,
                "--region", region,
                "--format=json"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        raw_json = result.stdout
        parsed = json.loads(raw_json)
        return json.dumps(parsed, indent=2)
    except subprocess.CalledProcessError as e:
        print("❌ Error running gcloud:", e.stderr)
        return None

# print(get_execution_status("bus-density-image-7dld9"))

# gcloud run jobs executions describe bus-density-image-7jzkz --region=us-central1  --format=json
# gcloud run jobs executions list   --job=bus-density-image   --region=us-central1   --limit=1   --sort-by=~createTime