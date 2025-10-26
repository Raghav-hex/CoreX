# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install Python dependencies
RUN pip install "fastapi[all]" websockets redis

# The command to run when the container launches
# It's the same uvicorn command we ran manually before
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]