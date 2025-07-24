# Use a Python base image us-central1-docker.pkg.dev/real-time-data-pipeline-457520/cloud-run-source-deploy
FROM python:3.12

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file (if you have one, otherwise create an empty one)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy  Python script
COPY app.py .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Expose port
EXPOSE 8080

# Set the command to run your script
CMD streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.enableCORS=false


# gcloud builds submit --tag gcr.io/lon-trans-streaming-pipeline/streamlite-ui:latest .


# gcloud run jobs deploy streamlite-ui --image gcr.io/lon-trans-streaming-pipeline/streamlite-ui:latest --region us-central1 
#   --allow-unauthenticated
