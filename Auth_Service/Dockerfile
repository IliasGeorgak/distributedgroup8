FROM python:3

WORKDIR /usr/src/app

COPY auth_requirements.txt ./
RUN pip install --no-cache-dir -r auth_requirements.txt

COPY . .

WORKDIR /usr/src/app
ARG AUTH_PORT=8080
ENV AUTH_PORT=${AUTH_PORT}
EXPOSE ${AUTH_PORT}

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${AUTH_PORT}"]
