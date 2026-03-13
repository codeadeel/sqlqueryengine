# [ Agent | SQL Query Engine @ PostgreSQL ]: NL-to-SQL inference service
FROM ubuntu@sha256:c35e29c9450151419d9448b0fd75374fec4fff364a27f176fb458d472dfc9e54 AS queryenginepostgresql
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /home/ubuntu

# %%
# System dependencies
RUN apt-get update && apt-get install --no-install-recommends -y python3-minimal python3-pip libpq-dev --fix-missing

# %%
# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt --break-system-packages --ignore-installed wheel

# %%
# Copy application source code
COPY run.py .
COPY sqlQueryEngine/ ./sqlQueryEngine/

# %%
# Start the server
RUN chmod +x ./run.py
ENTRYPOINT ["python3", "./run.py"]
