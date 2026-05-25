FROM python:3.11-slim

WORKDIR /usr/src/app

COPY manager-service/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY manager-service/ .

ARG MANAGER_PORT=8000
ENV MANAGER_PORT=${MANAGER_PORT}

EXPOSE ${MANAGER_PORT}

CMD ["sh", "-c", "python -m uvicorn app.main:app --host 0.0.0.0 --port ${MANAGER_PORT}"]
