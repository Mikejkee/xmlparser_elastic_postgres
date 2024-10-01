FROM python:3.11-slim-buster

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

WORKDIR /xmlparser_elastic_postgres
COPY . .
