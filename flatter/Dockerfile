# Dockerfile Documentation
# This Dockerfile creates a Python environment with some specific packages installed and sets up the necessary directories for the application.

FROM python:3.8-slim-buster
WORKDIR /app
COPY flatter ./code
COPY data ./data
RUN pip install pyarrow pandas fastapi uvicorn
WORKDIR /app/code
# CMD ["python", "Flatter.py"]
CMD ["python", "-m", "uvicorn", "api:app", "--reload", "--port", "8000"]

# Instructions to build and run the Docker image:
    # docker build -t {container-name} -f .\flatter\Dockerfile . 
    # docker run -d {container-name}
