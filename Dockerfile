# Imagem do Airflow do projeto GS Big Data (FIAP).
# Base oficial do Airflow 3 + dependências extras + código da coleta (src/ e dags/).
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# ARGs declarados antes do FROM precisam ser repetidos para valer depois dele.
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Dependências extras, travadas pelas constraints oficiais para não quebrar o Airflow.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Biblioteca de coleta (gs_big_data) e pasta de pouso dos arquivos.
ENV PYTHONPATH=/opt/airflow/src \
    GS_CHEGADA_DIR=/opt/airflow/chegada

# No docker-compose estas pastas são montadas do host (edição ao vivo);
# copiar aqui deixa a imagem utilizável sozinha (docker run).
COPY --chown=airflow:0 src/ /opt/airflow/src/
COPY --chown=airflow:0 dags/ /opt/airflow/dags/
RUN mkdir -p /opt/airflow/chegada
