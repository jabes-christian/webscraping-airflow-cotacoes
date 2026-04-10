# ===========================================================
# Dockerfile — Imagem customizada do Airflow
# Use quando precisar de dependências Python específicas
#
# Para usar:
#   1. Comente "image:" no docker-compose.yml
#   2. Descomente "build: ." no docker-compose.yml
#   3. Execute: docker compose build
# ===========================================================

FROM apache/airflow:2.9.1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
