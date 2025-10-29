FROM python:3.11-slim

WORKDIR /app
COPY consumer.py .
RUN pip install --no-cache-dir boto3
ENTRYPOINT ["python", "consumer.py"]

CMD ["-wb", "usu-cs5270-landon-web", "-rq", "https://sqs.us-east-1.amazonaws.com/730335658944/cs5270-requests"]
