FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./app/io_worker.py .

# Create directories and set permissions
RUN mkdir -p /data/downloads && chmod -R 777 /data
VOLUME ["/data"]

# Expose port
EXPOSE 8001

# Command to run the application
CMD ["uvicorn", "io_worker:app", "--host", "0.0.0.0", "--port", "8001"]