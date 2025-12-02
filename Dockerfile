#
# Dockerfile to build a custom Airflow image.
#
# We start from the official Apache Airflow image. It's recommended to use a
# specific version for reproducibility. You can find available tags here:
# https://hub.docker.com/r/apache/airflow/tags
#
ARG AIRFLOW_VERSION=2.8.1
FROM apache/airflow:${AIRFLOW_VERSION}

#
# Set the user to root temporarily to install OS-level packages or create directories.
# Airflow's default user is 'airflow' (UID 50000), which does not have sudo privileges.
#
USER root

# Example of installing OS packages if needed:
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     package-name \
#  && apt-get clean && rm -rf /var/lib/apt/lists/*

# Revert to the airflow user.
USER airflow

# Copy the requirements file and install Python dependencies.
# This is done before copying DAGs to leverage Docker layer caching.
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Download NLTK data required for sentiment analysis
# This prevents hanging during DAG parsing and runtime downloads
RUN python -c "import nltk; nltk.download('vader_lexicon', quiet=True)"