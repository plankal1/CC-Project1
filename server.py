from flask import Flask, request
import boto3
import csv
import os
from concurrent.futures import ThreadPoolExecutor

# ASU_ID = "1229248802"
S3_BUCKET = "1229248802-in-bucket" 
SIMPLEDB_DOMAIN = "1229248802-simpleDB"
CSV_FILE = "classification_face_images_1000.csv"
s3_client = boto3.client("s3", region_name="us-east-1")
sdb_client = boto3.client("sdb", region_name="us-east-1")

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=200)

def create_simpledb_domain():
    sdb_client.create_domain(DomainName=SIMPLEDB_DOMAIN)
    print(f"Created SimpleDB domain: {SIMPLEDB_DOMAIN}")

def upload_classification_data():
    if not os.path.exists(CSV_FILE):
        print(f"CSV file '{CSV_FILE}' not found! Skipping upload.")
        return
    
    with open(CSV_FILE, "r") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            filename, prediction = row[0].strip(), row[1].strip()
            print(filename)
            print(prediction)
            sdb_client.put_attributes(
                DomainName=SIMPLEDB_DOMAIN,
                ItemName=filename,
                Attributes=[{"Name": "prediction", "Value": prediction, "Replace": True}]
            )
            print(f"Uploaded: {filename} -> {prediction}")

create_simpledb_domain()
upload_classification_data()

@app.route("/", methods=["POST"])
def upload_file():
    if "inputFile" not in request.files:
        return "Error: No inputFile found", 400

    file = request.files["inputFile"]
    filename = file.filename
    filename = filename.split('.')[0]
    
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=filename, Body=file)
    except Exception as e:
        return f"Error uploading to S3: {str(e)}", 500

    future = executor.submit(get_classification, filename)
    prediction_result = future.result()

    return f"{filename}:{prediction_result}", 200

def get_classification(filename):
    response = sdb_client.get_attributes(DomainName=SIMPLEDB_DOMAIN, ItemName=filename, AttributeNames=["prediction"])
    attributes = response.get("Attributes", [])
    if attributes:
        return attributes[0]["Value"]
    return "Unknown"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
