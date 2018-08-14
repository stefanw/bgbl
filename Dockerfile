FROM python:3.6

COPY requirements.txt /code/requirements.txt
WORKDIR /code
RUN pip install -r requirements.txt

COPY . /code/
ENV PYTHONPATH /code
