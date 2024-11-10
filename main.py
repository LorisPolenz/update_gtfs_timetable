import os
import zipfile
import requests
import pandas as pd
from io import BytesIO
from minio import Minio

# The initial URL that redirects
URL = 'https://opentransportdata.swiss/de/dataset/timetable-2024-gtfs2020/permalink'

# Initialize the S3 client
s3_client = Minio(os.getenv('S3_ENDPOINT'),
                  os.getenv('S3_ACCESS_KEY'),
                  os.getenv('S3_SECRET_KEY'),
                  secure=True)


# Make a request without following the redirect
response = requests.get(URL, allow_redirects=False)

# Check if the response is a redirect (status code 302 or other 3xx codes)
if response.status_code in [301, 302, 303, 307, 308]:
    redirect_url = response.headers.get('Location')
else:
    raise Exception(f'Unexpected status code: {response.status_code}')

dir_name = redirect_url.split('/')[-1].replace('.zip', '') + '/'

res = s3_client.list_objects(
    bucket_name=os.getenv('S3_BUCKET'),
    prefix=dir_name,
    recursive=False
)

if any(res):
    exit(0)

# Get the GTFS timetable
response_timetable = requests.get(redirect_url)
response_timetable.raise_for_status()

# Unzip the file and push files to S3 bucket with the same directory structure
with zipfile.ZipFile(BytesIO(response_timetable.content)) as z:
    for filename in z.namelist():
        if filename.endswith('/'):  # Skip directories
            continue

        if filename in ['agency.txt', 'calendar.txt', 'calendar_dates.txt', 'shapes.txt', 'stop_times.txt', 'transfers.txt', 'feed_info.txt']:
            continue

        with z.open(filename) as f:
            parquet_buffer = BytesIO()
            pd.read_csv(f).to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # Define Parquet filename for S3
            parquet_filename = filename.replace('.txt', '.parquet')
            s3_path = dir_name + parquet_filename

            s3_client.put_object(
                bucket_name=os.getenv('S3_BUCKET'),
                object_name=s3_path,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            print(f"Uploaded {s3_path}")
