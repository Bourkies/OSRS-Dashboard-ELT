# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file first to leverage Docker layer caching.
# This means dependencies are only re-installed if requirements.txt changes.
COPY src/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's source code
COPY src/ /app/src/

# Copy the static image assets into the image. This is more reliable
# than a volume mount for static content.
COPY images/ /app/images/

# The command to run when the container launches.
# This executes the main ETL runner script.
CMD [ "python", "src/run_all_etl.py" ]
