FROM python:3.12-slim

RUN groupadd -r dptrb && useradd -r -g dptrb dptrb

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion_service.py .

USER dptrb

ENTRYPOINT ["python", "ingestion_service.py"]
