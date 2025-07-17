import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import os 
from io import StringIO
import requests
from google.cloud import bigquery, storage
from sklearn.metrics.pairwise import haversine_distances


def main():
    # 1) --> GCP Configuration set up

    PROJECT_ID = 'lon-trans-streaming-pipeline'
    # BigQuery Configuration
    BQ_client = bigquery.Client(project=PROJECT_ID)
    DATA_SET = 'bus_density_streaming_pipeline'

    # Cloud storage configuration
    Storage_client = storage.Client(project=PROJECT_ID)
    DESTINATION_BUCKET = 'arrivals_data'
    DESTINATION_PATH =  f"temporary/arrivals_most_recent_snapshot.csv"
    bucket = Storage_client.bucket(DESTINATION_BUCKET)
    blob = bucket.blob(DESTINATION_PATH)

    # 2) --> Read Clusters Table

    cluster_stations_table = 'stopspoint_coordinates_aggloclusters_enriched'

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATA_SET}.{cluster_stations_table}`
    """
    result =  BQ_client.query(query)

    clusterized_stations_df = pd.DataFrame([{'naptanId': row[0],
                                            'commonName': row[1],
                                            'latitude': row[2],
                                            'longitude': row[3],
                                            'clusterAgglomerative': row[4]} 
                                            for row in result])

    # 3) --> Bus arrival API call

    arrivals_url = "https://api.tfl.gov.uk/Mode/bus/Arrivals"
    app_key = os.environ.get('TFL_APP_KEY')
    url_append = f'?app_key={app_key}' 

    arrivals_response = requests.get(arrivals_url + url_append)
    arrivals_response_status_code = arrivals_response.status_code

    if arrivals_response_status_code == 200:
        arrivals_pred_df = pd.DataFrame(arrivals_response.json())
    else:
        print('No Data due to errors')

    # 4) --> Enrich data with coordinates and clusters

    arrivals_pred_df_enriched = arrivals_pred_df[['vehicleId', 'naptanId', 'lineId', 'timestamp', 'timeToStation']].\
        merge(clusterized_stations_df[['naptanId', 'latitude','longitude', 'clusterAgglomerative']], how='left', on='naptanId')


    # 5) --> Fetch data points that were not found since coordinates havent been assigned to a cluster yet 

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
                FROM `{PROJECT_ID}.{DATA_SET}.{stations_raw_table}`        
                WHERE naptanId IN {tuple(not_found_naptan_df_list)}
            """
        else:
            raw_stations_coorsinates_query = f"""
                SELECT *
                FROM `{PROJECT_ID}.{DATA_SET}.{stations_raw_table}`        
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

    # Send the most recent data to GCS Temporary file --> IWill copy into official file after extraction and enrichment is complete
    csv_buffer = StringIO()
    final_arrivals_pred_df_enriched.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
    print('New data has been succusfully fetched, enriched and uploaded to temporary file location')

if __name__ == '__main__':
    main()