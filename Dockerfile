# Use the official Spark base image
FROM bitnami/spark:latest

# Set the working directory
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Set the entrypoint
ENTRYPOINT ["python3", "main.py"]
