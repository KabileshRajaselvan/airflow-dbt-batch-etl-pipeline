FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY seed_source_db.py seed_minio.py ./

CMD ["sh", "-c", "python seed_source_db.py && python seed_minio.py"]
