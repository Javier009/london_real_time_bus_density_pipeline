# Use the official Google Cloud SDK image as a base
FROM google/cloud-sdk:latest

# Set a working directory
WORKDIR /app

# Copy your application code into the container
COPY job_status.py .

# (Optional) Install language-specific dependencies if your function has them
# Example for Python:
# COPY requirements.txt .
# RUN pip install -r requirements.txt

# Expose the port Cloud Run expects (defaults to 8080)
EXPOSE 8080

# Command to run your Cloud Run function
# Replace with the actual command to start your function (e.g., python main.py, node server.js)
CMD ["python", "job_status.py"]

#  gcloud builds submit --tag gcr.io/lon-trans-streaming-pipeline/arrivals-job-status:latest .


# gcloud run jobs deploy arrivals-job-status \
# --image gcr.io/lon-trans-streaming-pipeline/arrivals-job-status:latest \
# --region us-central1