from google.cloud import storage, bigquery, pubsub_v1
import csv
import requests
from datetime import datetime
from io import StringIO
import time
from flask import Flask

def fetch_naptan_ids(Request):
    # 1) ---->  Read Table and fetch unique naptan Ids already captured

    PROJECT_ID = 'lon-trans-streaming-pipeline'

    BQ_client = bigquery.Client(project=PROJECT_ID)
    data_set = 'bus_density_streaming_pipeline'
    table = 'stopspoint_coordinates'

    # SQL query — adjust table name
    query = f"""
        SELECT DISTINCT naptanId
        FROM `{PROJECT_ID}.{data_set}.{table}`
    """
    query_job = BQ_client.query(query)
    results = query_job.result()

    existingNaptanIds = [row["naptanId"] for row in results]


    # 2) ----> API Call of buss arrivals tp get NaptanIds data

    arrivals_url = "https://api.tfl.gov.uk/Mode/bus/Arrivals"
    app_key = '132c49c6367b496ba654bc8092f0610a'
    url_append = f'?app_key={app_key}' 
    print(arrivals_url + url_append)
    arrivals_response = requests.get(arrivals_url + url_append)

    if arrivals_response:
        arrivals_response_json = arrivals_response.json()
        naptanids = list(set([record['naptanId'] for record in arrivals_response_json]))

    naptan_to_capture = [n for n in naptanids if n not in existingNaptanIds]

    # 3) ---> StopPoint API CALL

    iteration = 0
    time_sleep_iterator = 0 
    batch_result = [('naptanId', 'commonName', 'latitude', 'longitud')]

    # GCS cconfiguration
    BUCKET_NAME = 'bus_stop_points'

    # Pub/Sub configuration
    TOPIC_ID = 'naptan_ids_recieved_trigger_again'
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    message_str = f"NaptanId Collection Processes finished, run again"
    message_bytes = message_str.encode("utf-8")

    if naptan_to_capture:

        for naptan_id in naptan_to_capture[0:300]: # Will process some at a time not all, eventutally data will be smaller

            # API CALL FOR COORDINATES MAPPING
            station_map_url = f'https://api.tfl.gov.uk/StopPoint/{naptan_id}'
            res = requests.get(station_map_url)

            if res.status_code == 200:
                station = res.json()
                commonName = station['commonName']
                latitude =  station['lat']
                longitud = station['lon']
                batch_result.append((naptan_id,commonName,latitude,longitud))
                print(f'Iteration {iteration} ✅')
                iteration += 1
                
                if time_sleep_iterator == 5:
                    time.sleep(5)
                    time_sleep_iterator = 0
                else:
                    time_sleep_iterator += 1
            
            else:
                print(f'Some errors in iteration {iteration} found, NaptanId: {naptan_id}, status code: {res.status_code}')
                iteration += 1
                continue

        # Store in GCS bucket

        now = datetime.now()
        DESTINATION_PATH =  f"naptan_data/year={now.year}/month={now.month:02}/day={now.day:02}/hour={now.hour:02}/minute={now.minute:02}/naptan_snapshot.csv"
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(DESTINATION_PATH)

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(batch_result)

        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

        print(f'Stored {len(batch_result)-1} in GCS and now sending a message to pubsub to trigger process again')

        time.sleep(60)
        # Send message tu pub/sub    
        future = publisher.publish(topic_path, message_bytes)
        print(f"✅ Message published. Message ID: {future.result()}")
        return f'✅  Naptan Ids captured and sent to BigQuery', 200 
    else:
        print('No new Naptan Ids to capture')
        time.sleep(60)
        future = publisher.publish(topic_path, message_bytes)
        print(f"✅ Message published. Message ID: {future.result()}")
        return f'✅  No new Naptan Ids found', 200 