from google.cloud import storage
import time
from flask import Flask

def move_files(Request):
    
    bucket_name = "arrivals_data"
    source_blob_name = "temporary/arrivals_most_recent_snapshot.csv"
    destination_blob_name = "latest/arrivals_most_recent_snapshot.csv"

    print('----SOME TIME FOR FUNCTION TO INITIALIZE ----')
    time.sleep(10)

    attempts = 1
    success = False 
    max_attempts = 5

    while success == False:
        if attempts <= max_attempts:
            try:
                client = storage.Client()
                bucket = client.bucket(bucket_name)
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
        return ("Files moved successfullu", 200)
    else:
        return (f"Move failed", 500)
