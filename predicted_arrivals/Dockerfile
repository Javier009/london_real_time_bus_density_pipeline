# Use a Python base image us-central1-docker.pkg.dev/real-time-data-pipeline-457520/cloud-run-source-deploy
FROM python:3.12

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file (if you have one, otherwise create an empty one)
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy your Python script
COPY arrivals_process.py .

# Set the command to run your script
CMD ["python3", "arrivals_process.py"]

#  gcloud builds submit --tag gcr.io/lon-trans-streaming-pipeline/bus-density-image:latest .


#  gcloud run jobs deploy bus-density-image \
#   --image gcr.io/lon-trans-streaming-pipeline/bus-density-image:latest \
#   --region us-central1 \
#   --set-env-vars TFL_APP_KEY='132c49c6367b496ba654bc8092f0610a' 