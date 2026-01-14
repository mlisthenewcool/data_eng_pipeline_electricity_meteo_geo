# https://airflow.apache.org/docs/docker-stack/build.html
FROM apache/airflow:latest-python3.13

# ------------------------------
# ---- PIP WORKFLOW: 4.22 GB ---
# ------------------------------
USER root
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get autoremove --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create the data directory and assign ownership to the 'airflow' user
# before volume mounting. This ensures the application has the necessary
# permissions to create subdirectories at runtime, preventing failures in
# dest_path.parent.mkdir(parents=True, exist_ok=True)."
RUN mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data

USER airflow
RUN pip install --no-cache-dir --upgrade uv
RUN uv python upgrade

# NOTE: use psycopg with system tools instead of binary ?
# NOTE: use `--mount=type=cache` instead of --no-cache ?
RUN uv pip install --no-cache \
    aiofiles aiohttp duckdb loguru polars "psycopg[binary]" py7zr pydantic \
    python-json-logger pyyaml tqdm

# Use with caution, might break things ?
RUN uv pip list --outdated --format=json | \
    python -c "import json, sys; print('\n'.join([x['name'] for x in json.load(sys.stdin)]))" | \
    xargs -n1 uv pip install --no-cache -U

# optional, check results with `docker buildx history logs`
RUN uv pip list
RUN uv pip list --outdated


# ------------------------------
# ---- UV WORKFLOW: 5.88 GB ----
# ------------------------------
## upgrade & clean up to keep image small
## (rather use apt-get than apt for scripts because they are considered more stable)
## install dependencies for mysqlclient & python-ldap to be able to use uv instead of pip
#USER root
#RUN apt-get update \
#    && apt-get upgrade -y \
#    && apt-get install -y --no-install-recommends build-essential pkg-config \
#    libmariadb-dev libldap2-dev libsasl2-dev \
#    && apt-get autoremove --purge \
#    && apt-get clean && rm -rf /var/lib/apt/lists/*
#
#
#USER airflow
#RUN pip install --no-cache-dir --upgrade uv
## Should already be up to date
##RUN uv python upgrade
#
#RUN pip freeze > requirements.txt
#RUN uv init
#RUN uv add -r requirements.txt
#RUN uv add \
#    aiofiles aiohttp duckdb loguru polars "psycopg[binary]" py7zr pydantic \
#    python-json-logger pyyaml tqdm
#RUN uv sync --upgrade
#RUN uv tree --depth 1
#RUN uv pip list --outdated