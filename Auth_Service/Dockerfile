FROM python:3

WORKDIR /usr/src/app

COPY auth_requirements.txt ./
RUN pip install --no-cache-dir -r auth_requirements.txt

COPY . .

WORKDIR /usr/src/app
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8080"]
#CMD ["sleep", "999999"]
