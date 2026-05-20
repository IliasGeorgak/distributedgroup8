FROM python:3.11-slim

WORKDIR /usr/src/app

COPY manager-service/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY manager-service/ .


EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
