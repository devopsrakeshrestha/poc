import time
import redis
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify
import boto3

# Initialize Flask app
app = Flask(__name__)

# Initialize S3 client
s3 = boto3.client('s3')

# Hardcoded S3 bucket name
bucket_name = 'poc-deleteme'  # Replace 'poc-deleteme' with your actual bucket name
checkpoint_key = 'crawler_checkpoint.txt'  # Key for storing checkpoint in S3
processed_key = 'processed_items.txt'  # Key for storing processed items in S3

# Initialize Redis client
redis_host = 'localhost'  # Redis service hostname in Kubernetes
redis_port = 19136  # Redis default port
redis_password = 'IPtVDK5JYNsaqpKBS8qbLuShTYoVozEF'  # Replace 'your_redis_password' with the actual Redis password
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)


# Define crawling job lock key
lock_key = 'crawling_job_lock'

@app.route('/healthz')
def health_check():
    # Perform a simple health check
    return jsonify({"status": "ok"})

def acquire_lock():
    # Attempt to acquire the lock
    return redis_client.set(lock_key, 'locked', ex=10, nx=True)  # Lock expires in 10 seconds

def release_lock():
    # Release the lock
    redis_client.delete(lock_key)

def load_checkpoint():
    try:
        response = s3.get_object(Bucket=bucket_name, Key=checkpoint_key)
        checkpoint = response['Body'].read().decode('utf-8')
        return int(checkpoint)
    except s3.exceptions.NoSuchKey:
        return None
    except ValueError:
        return None

def save_checkpoint(checkpoint):
    try:
        s3.put_object(Body=str(checkpoint), Bucket=bucket_name, Key=checkpoint_key)
    except Exception as e:
        print(f"Failed to save checkpoint to S3: {str(e)}")

def load_processed_items():
    try:
        response = s3.get_object(Bucket=bucket_name, Key=processed_key)
        processed_items = response['Body'].read().decode('utf-8').split('\n')
        return set(processed_items)
    except s3.exceptions.NoSuchKey:
        return set()
    except Exception as e:
        print(f"Failed to load processed items from S3: {str(e)}")
        return set()

def save_processed_item(item):
    try:
        s3.put_object(Body=item, Bucket=bucket_name, Key=processed_key, ContentType='text/plain')
    except Exception as e:
        print(f"Failed to save processed item to S3: {str(e)}")

def crawl_wikipedia():
    if acquire_lock():
        try:
            # Load checkpoint and processed items
            checkpoint = load_checkpoint()
            processed_items = load_processed_items()

            if checkpoint is not None:
                # Resume from the checkpoint
                print("Resuming crawler from checkpoint:", checkpoint)
                page = checkpoint
            else:
                # Start fresh crawling
                print("Starting fresh crawling...")
                page = 1

            # Extract data from Wikipedia and save it to S3
            base_url = "https://en.wikipedia.org/wiki/List_of_best-selling_books"
            total_records = 0
            data = []
            while total_records < 10:
                url = f"{base_url}?page={page}"
                response = requests.get(url)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    table = soup.find("table", class_="wikitable")
                    if table:
                        rows = table.find_all("tr")[1:]
                        for row in rows:
                            columns = row.find_all("td")
                            if len(columns) >= 4:
                                title = columns[0].text.strip()
                                author = columns[1].text.strip()
                                date = columns[3].text.strip()
                                item = f"{title}, {author}, {date}"
                                if item not in processed_items:
                                    data.append({"Title": title, "Author": author, "Publication Date": date})
                                    total_records += 1
                                    save_processed_item(item)
                                    if total_records >= 10:
                                        break
                    else:
                        print(f"Table not found on page {page}.")
                        break
                    
                    page += 1
                else:
                    print(f"Failed to fetch data from Wikipedia. Status Code: {response.status_code}")
                    break

            # Save data to S3
            save_to_s3(data)
            save_checkpoint(page)
        finally:
            release_lock()
    else:
        print("Another container is already processing the crawling job.")

def save_to_s3(data):
    if data:
        try:
            json_data = "\n".join([f"{item['Title']}, {item['Author']}, {item['Publication Date']}" for item in data])
            s3.put_object(Body=json_data, Bucket=bucket_name, Key='top_1000_books.txt')
            print("Data saved to S3.")
        except Exception as e:
            print(f"Failed to save data to S3: {str(e)}")

if __name__ == "__main__":
    crawl_wikipedia()
    app.run(host='0.0.0.0', port=8080)  # Run Flask app with health check endpoint
