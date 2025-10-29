FROM python:3.11-slim

WORKDIR /app

COPY consumer.py .

RUN pip install --no-cache-dir boto3

ENTRYPOINT ["python", "consumer.py"]

CMD ["--help"]