FROM apache/superset:4.1.4

USER root

COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY scripts /app/scripts
RUN chmod +x /app/scripts/*.sh

ENV PYTHONPATH="/app/pythonpath:/config" \
    SUPERSET_CONFIG_PATH="/config/superset_config.py"

USER superset
