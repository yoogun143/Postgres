FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create database.ini file with Docker-specific configuration
RUN mkdir -p /app/helper
RUN echo "[postgresql]\n\
host=db\n\
database=\${POSTGRES_DB:-gmap_db}\n\
user=\${POSTGRES_USER:-postgres}\n\
password=\${POSTGRES_PASSWORD:-postgres}\n\
port=5432" > /app/helper/database.ini

# Expose the port Streamlit runs on
EXPOSE 8501

# Command to run the application
CMD ["streamlit", "run", "app_gmap.py", "--server.address=0.0.0.0"]
