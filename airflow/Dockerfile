FROM apache/airflow:latest-python3.13

# upgrade & clean up to keep image small
USER root
RUN apt update && apt upgrade -y && apt clean && rm -rf /var/lib/apt/lists/*


# upgrade uv
USER airflow
RUN pip install --no-cache-dir --upgrade uv
# Should already be up to date
#RUN uv python upgrade

# not working : from pip to uv
#RUN pip freeze > requirements.txt
#RUN uv init
#RUN uv add -r requirements.txt
#RUN uv sync --upgrade
#RUN uv add "psycopg[binary]"

# todo: inject dependencies automaticaly
RUN uv pip install --no-cache-dir \
    aiofiles aiohttp duckdb loguru polars "psycopg[binary]" py7zr pydantic \
    python-json-logger pyyaml tqdm

# Use with caution, might break things ?
RUN uv pip list --outdated --format=json | \
    python -c "import json, sys; print('\n'.join([x['name'] for x in json.load(sys.stdin)]))" | \
    xargs -n1 uv pip install --no-cache-dir -U

# optional, check result
RUN uv pip list --outdated