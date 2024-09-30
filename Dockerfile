FROM python:3.11-slim-buster

WORKDIR /xmlparser_elastic_postgres
COPY . .

CMD ["python", "main.py"]
