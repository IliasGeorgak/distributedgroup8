
FROM python:3.11-slim

ARG WORKER_COMMAND="python worker.py"
ENV WORKER_COMMAND=${WORKER_COMMAND}

WORKDIR /usr/src/app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker/ .

CMD ["sh", "-c", "${WORKER_COMMAND}"]
