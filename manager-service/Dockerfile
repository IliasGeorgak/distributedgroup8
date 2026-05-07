FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r auth_requirements.txt

COPY . .

WORKDIR /usr/src/app
