# Dockerfile Documentation

# This Dockerfile creates a Python environment with some specific packages installed and sets up the necessary directories for the application.

# Base Image
FROM python:3.8-slim-buster
# We are using the official Python Docker image with tag 3.8-slim-buster as our base image.

# Set the working directory to /app
WORKDIR /app
# This instruction sets the working directory for any instructions that follow in the Dockerfile.

# Copy directories into the Docker image
COPY flatter ./code
COPY data ./data
# These instructions copy the local 'flatter' directory to '/app/code' and 'data' directory to '/app/data' in the Docker image.

# Install Python packages
RUN pip install pyarrow pandas
# This instruction installs the necessary Python packages: pyarrow and pandas.

# Set the working directory to /app/code
WORKDIR /app/code
# This instruction sets the working directory to '/app/code' for any instructions that follow in the Dockerfile.

# Default command to execute    
CMD ["python", "Flatter.py"]
# This instruction sets the default command to execute when a container is created from the image. In this case, it runs 'Flatter.py' using Python.


# Instructions to build and run the Docker image:
    # On-assessing-the-quality-of-transforming-tree-like-structured-into-table-like-structured-data> docker build -t {container-name} -f .\flatter\Dockerfile . 
    # On-assessing-the-quality-of-transforming-tree-like-structured-into-table-like-structured-data> docker run -d {container-name}
