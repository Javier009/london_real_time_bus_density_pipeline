
# Use Google’s Python39 Flex-Template launcher base
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

WORKDIR /templates

# 1) Install all your deps at build time
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 2) Copy your pipeline code
COPY lon_trans_streaming_data.py . 

# 3) Tell the launcher which script to run
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/templates/lon_trans_streaming_data.py"

# 4) Crucial: tell Dataflow to use your image as the SDK container
ENV FLEX_TEMPLATE_PYTHON_CONTAINER_IMAGE="gcr.io/lon-trans-streaming-pipeline/lon_trans_streaming_data:latest"

# Deploy commands

# gcloud builds submit --tag gcr.io/lon-trans-streaming-pipeline/lon_trans_streaming_data:latest .

# gcloud dataflow flex-template build \
# gs://templates-streaming/templates/daily_transactions_summaries.json \
# --image=gcr.io/lon-trans-streaming-pipeline/lon_trans_streaming_data:latest \
# --sdk-language=PYTHON \
# --metadata-file=metadata.json


