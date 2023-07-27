# Use the official Python image as the base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the application code and requirements.txt into the container
COPY pipeline.py .
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o arquivo de configuração para o contêiner
COPY config.ini .

# Install Prefect
# RUN pip install prefect==0.15.9

# Run your Python application
CMD ["python", "pipeline.py"]