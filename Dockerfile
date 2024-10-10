FROM quay.io/astronomer/astro-runtime:12.1.0

# Set the environment variable for Google Application Credentials
ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/local/airflow/include/key/service_account.json"

# Copy your service account key into the container
COPY include/key/service_account.json /usr/local/airflow/include/key/service_account.json