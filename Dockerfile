# Use the official Python image as a base image
FROM python:3.8-slim

# Set environment variables for AWS access key ID and secret access key
ENV AWS_ACCESS_KEY_ID=" "
ENV AWS_SECRET_ACCESS_KEY=" "
ENV BUCKET_NAME=" "

# Set the working directory in the container
WORKDIR /app


# Copy the Python script into the container
COPY scrape_wikipedia.py .


# Install necessary dependencies
RUN pip install Flask requests beautifulsoup4 boto3

# Install the AWS CLI for uploading files to S3
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    groff \
    && rm -rf /var/lib/apt/lists/* \
    && pip install awscli

# Run the Python script when the container launches
CMD ["python", "scrape_wikipedia.py"]
