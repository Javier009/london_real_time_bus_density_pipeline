import pandas as pd
import numpy as np
import time
from datetime import datetime
import pytz
import os 
from io import StringIO
import requests
from google.cloud import bigquery, storage, pubsub_v1
from sklearn.metrics.pairwise import haversine_distances


def fetch_cluster_mapping_table(project_id, data_set, BQ_client, cluster_stations_table):

    query = f"""
        SELECT *
        FROM `{project_id}.{data_set}.{cluster_stations_table}`
    """
    result =  BQ_client.query(query)

    clusterized_stations_df = pd.DataFrame([{'naptanId': row[0],
                                            'commonName': row[1],
                                            'latitude': row[2],
                                            'longitude': row[3],
                                            'clusterAgglomerative': row[4]} 
                                            for row in result])    
    return clusterized_stations_df

def bus_arrival_api_call(APP_KEY):

    arrivals_url = "https://api.tfl.gov.uk/Mode/bus/Arrivals"
    # app_key = os.environ.get('TFL_APP_KEY')
    url_append = f'?app_key={APP_KEY}' 

    arrivals_response = requests.get(arrivals_url + url_append)
    arrivals_response_status_code = arrivals_response.status_code

    if arrivals_response_status_code == 200:
        arrivals_pred_df = pd.DataFrame(arrivals_response.json())
        return arrivals_pred_df
    else:
        print('No Data due to errors')
        return pd.DataFrame()
    
def enrich_data(BQ_client, project_id, data_set, arrivals_pred_df, clusterized_stations_df):

    # 1) --> Enrich data with coordinates and clusters

    arrivals_pred_df_enriched = arrivals_pred_df[['vehicleId', 'naptanId', 'lineId', 'timestamp', 'timeToStation']].\
        merge(clusterized_stations_df[['naptanId', 'latitude','longitude', 'clusterAgglomerative']], how='left', on='naptanId')


    # 2) --> Fetch data points that were not found since coordinates havent been assigned to a cluster yet 

    not_found_naptan_df = arrivals_pred_df_enriched[arrivals_pred_df_enriched['clusterAgglomerative'].isnull()]
    not_found_naptan_df = not_found_naptan_df[not_found_naptan_df['naptanId'] != 'null'][['vehicleId', 'naptanId', 'lineId', 'timestamp', 'timeToStation']]

    if len(not_found_naptan_df) > 0: # ---> If there are not found naptanIds
        print('There are some new Naptan Ids with no clusters')

        # Remove them from the enrieched data frame -> They will be appended later
        arrivals_pred_df_enriched = arrivals_pred_df_enriched[~arrivals_pred_df_enriched['naptanId'].isin([n for n in not_found_naptan_df['naptanId']])]

        # Read raw coordinates table to estimate cluster based on haversine_distances

        # not_found_naptan_list = list(not_found_naptan_df['naptanId'])
        
        stations_raw_table = 'stopspoint_coordinates'
        not_found_naptan_df_list = [n for n in not_found_naptan_df['naptanId']]

        if len(not_found_naptan_df_list) > 1:
            raw_stations_coorsinates_query = f"""
                SELECT *
                FROM `{project_id}.{data_set}.{stations_raw_table}`        
                WHERE naptanId IN {tuple(not_found_naptan_df_list)}
            """
        else:
            raw_stations_coorsinates_query = f"""
                SELECT *
                FROM `{project_id}.{data_set}.{stations_raw_table}`        
                WHERE naptanId IN ({not_found_naptan_df_list[0]})
            """

        raw_stations_coorinates_result =  BQ_client.query(raw_stations_coorsinates_query)
        
        raw_stations_coorinates_df = pd.DataFrame([{'naptanId': row[0], 
                                                    'commonName': row[1], 
                                                    'latitude': row[2], 
                                                    'longitude': row[3]} 
                                                    for row in raw_stations_coorinates_result])
        
        if len(raw_stations_coorinates_df) > 0: # --> If Not found NaptanIds are in Coordinates raw table 
            print('Found those NaptanIds as they are already part of raw coordinates')

            # Apply haversine_distances to each not found coordinate

            clustered_coords = np.radians(clusterized_stations_df[['latitude', 'longitude']].values)
            cluster_labels = clusterized_stations_df['clusterAgglomerative'].values

            proximity_clusters = [] 

            for i in range(0,len(raw_stations_coorinates_df)):
            
                latitude = raw_stations_coorinates_df.iloc[i]['latitude']
                longitude = raw_stations_coorinates_df.iloc[i]['longitude']

                # New point (also in radians)

                new_point = np.radians([[latitude,longitude]])

                # Compute distances to all points
                distances = haversine_distances(clustered_coords, new_point) * 6371.0088  # km

                # Find the closest point and append to list of proximity clusters
                closest_index = np.argmin(distances)
                closest_cluster = cluster_labels[closest_index]

                proximity_clusters.append(closest_cluster)

                # print(f"Closest cluster: {closest_cluster}")

            raw_stations_coorinates_df['clusterAgglomerative'] = proximity_clusters

            # Now merge with not found naptan Ids:
            not_found_naptan_df_enriched = not_found_naptan_df.merge(raw_stations_coorinates_df[['naptanId', 'latitude'	, 'longitude', 'clusterAgglomerative']], how='left', on='naptanId')

            # Append the naptans with enriched data to the original data frame
            final_arrivals_pred_df_enriched = pd.concat([arrivals_pred_df_enriched, not_found_naptan_df_enriched], axis=0, ignore_index=True)

            # Finally, Drop null values in the final enriched in the data frame --> If not found after harvesine proximity that means they are not in the raw coordinates table 

            final_arrivals_pred_df_enriched = final_arrivals_pred_df_enriched.dropna(subset='clusterAgglomerative')
            final_arrivals_pred_df_enriched = final_arrivals_pred_df_enriched [['vehicleId', 'naptanId', 'timeToStation', 'latitude', 'longitude', 'clusterAgglomerative']]
        
        else:
            print('The new naptanIds are not in raw Coordinats table so they wont be considered')
            final_arrivals_pred_df_enriched = arrivals_pred_df_enriched.dropna(subset='clusterAgglomerative')


    else:
        print('No Null naptanIds found')
        final_arrivals_pred_df_enriched  =  arrivals_pred_df_enriched

    # Finally add pull time w/London time zone
    london_timezone = pytz.timezone('Europe/London')
    london_current_time = datetime.now(london_timezone)
    final_arrivals_pred_df_enriched['pull_time'] = london_current_time.strftime('%Y-%m-%d %H:%M:%S')    

    return final_arrivals_pred_df_enriched


def create_temp_file(Storage_client, final_arrivals_pred_df_enriched, destination_bucket, destination_path):
    try:
        csv_buffer = StringIO()
        final_arrivals_pred_df_enriched.to_csv(csv_buffer, index=False)
        bucket = Storage_client.bucket(destination_bucket)
        blob = bucket.blob(destination_path)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
        print('New data has been succusfully fetched, enriched and uploaded to temporary file location')
        return True
    except:
        print('Temporary file creation failed')
        return False


def move_files(Storage_client, bucket_name, source_blob_name, destination_blob_name):

    attempts = 1
    success = False 
    max_attempts = 5

    while success == False:
        if attempts <= max_attempts:
            try:
                # client = storage.Client()
                bucket = Storage_client.bucket(bucket_name)
                source_blob = bucket.blob(source_blob_name)
                # Copy to new location
                bucket.copy_blob(source_blob, bucket, destination_blob_name)
                # Delete original
                source_blob.delete()
                success = True
                print(f'Moved and deleted temporary file succesfully in {attempts} attempts')

            except Exception as e:
                print((f"Move failed: {e} retrying"))
                attempts += 1
        else:        
            print((f"Ran out of attempts function failed"))        
            break    
        
    if success:
        return ("Files moved successfully", 200)
    else:
        return (f"Move failed", 500)
    
def publish_completion_message(project_id, topic_id, message_data):

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # Data must be a bytestring
    data = message_data.encode("utf-8")

    try:
        future = publisher.publish(topic_path, data)
        message_id = future.result()
        print(f"Published message to {topic_path}. Message ID: {message_id}, triggering pipeline again")
        return True
    except Exception as e:
        print(f"Failed to publish message to Pub/Sub: {e}, triggering pipeline again")
        return False


def main():
    # 1) --> GCP Configuration set up
    PROJECT_ID = 'lon-trans-streaming-pipeline'

    # BigQuery Configuration
    BQ_client = bigquery.Client(project=PROJECT_ID)
    DATA_SET = 'bus_density_streaming_pipeline'

    # Cloud storage configuration
    Storage_client = storage.Client(project=PROJECT_ID)    

    # 2) --> Read Clusters Table
    cluster_stations_table = 'stopspoint_coordinates_aggloclusters_enriched'
    clusterized_stations_df  = fetch_cluster_mapping_table(project_id=PROJECT_ID, 
                                                           data_set=DATA_SET, 
                                                           BQ_client=BQ_client, 
                                                           cluster_stations_table=cluster_stations_table)
    
    # 3) --> Bus arrival API CALL
    API_KEY = os.environ.get('TFL_APP_KEY')
    arrivals_pred_df = bus_arrival_api_call(API_KEY)

    # 3.5) Pub sub topic id
    PUBSUB_TOPIC_ID = 'london-transport-data-topic'

    if len(arrivals_pred_df) > 0:
        # 4) --> Enrich data (Clusters + Nulls values handling)
        final_arrivals_pred_df_enriched = enrich_data(BQ_client=BQ_client,
                                                      project_id=PROJECT_ID,
                                                      data_set=DATA_SET,
                                                      arrivals_pred_df=arrivals_pred_df,
                                                      clusterized_stations_df=clusterized_stations_df)
        
        # 5) --> Send the most recent data to GCS Temporary file
        BUCKET = 'arrivals_data'
        TEMP_FILE_PATH = "temporary/arrivals_most_recent_snapshot.csv"
        temp_file_success = create_temp_file(Storage_client = Storage_client,
                                            final_arrivals_pred_df_enriched = final_arrivals_pred_df_enriched,
                                            destination_bucket=BUCKET,
                                            destination_path=TEMP_FILE_PATH)
        time.sleep(10)

        # 6) --> Move files
        if temp_file_success:
            
            destination_blob_name = "latest/arrivals_most_recent_snapshot.csv"
            job_final_stage = move_files(Storage_client=Storage_client,
                       bucket_name=BUCKET,
                       source_blob_name=TEMP_FILE_PATH, 
                       destination_blob_name=destination_blob_name)
        else:
            print('Error creating TEMPORARY FILE, finishing job...')

         # 7) --> Publish Pub/Sub message based on job completion status
                
        if job_final_stage[1] ==200:
            message = f"Cloud Run Job 'bus-density-image' completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} PDT. Triggering Pipeline One more time"
            publish_completion_message(PROJECT_ID, PUBSUB_TOPIC_ID, message)
        else:
            message = f"Cloud Run Job 'bus-density-image' failed to complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} PDT. Triggering Pipeline One more time"
            publish_completion_message(PROJECT_ID, PUBSUB_TOPIC_ID, message)
    else:
        print('Error with TFL GET request finishing job ...')
        publish_completion_message(PROJECT_ID, PUBSUB_TOPIC_ID, message)

if __name__ == '__main__':
    main()