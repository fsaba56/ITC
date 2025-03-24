import boto3
import csv
import json

# Initialize the Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

# Define Kinesis stream name
stream_name = 'TFL_LIVE_STREAM_saba '

# Open the CSV file
with open(r'C:\Users\fsaba\Downloads\TFL_Underground_Record.csv', mode='r') as file:
    csv_reader = csv.DictReader(file)
    
    # Iterate through each row in the CSV
    for row in csv_reader:
        # Convert row to JSON (or any format you prefer)
        record = {
            'Record_id': row['Record_id'],
            'TimeDetails': row['TimeDetails'],
            'Line': row['Line'],
            'Status': row['Status'],
            'Reason': row['Reason']
        }

        # Convert record to JSON string
        record_json = json.dumps(record)
        
        # Put record into the Kinesis stream
        response = kinesis_client.put_records(
            StreamName=stream_name,
            Data=record_json,  # The data to be uploaded
            PartitionKey=row['Record_id']  # A unique key to distribute data across shards
        )

        # Print response for debugging purposes
        print(f"Record sent to Kinesis: {response}")
