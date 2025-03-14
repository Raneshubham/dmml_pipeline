# Use official Python image
FROM python:3.10

# Set working directory
WORKDIR /app

# Copy files to container
COPY . .

# Install dependencies
RUN pip install -r requirements.txt

# Prefect requires this environment variable
ENV PREFECT_API_URL=http://127.0.0.1:4200/api

# Set entrypoint for Prefect
CMD ["python", "main.py"]
