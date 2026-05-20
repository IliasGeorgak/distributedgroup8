FROM python:3.11-slim

WORKDIR /usr/src/app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker/ .

EXPOSE 8000

CMD ["python", "worker.py"]