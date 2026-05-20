FROM python:3

WORKDIR /usr/src/app

COPY UI_requirements.txt ./
RUN pip install --no-cache-dir -r UI_requirements.txt

# COPY /core ./core
# COPY /schemas ./schemas
COPY app ./app

WORKDIR /usr/src/app
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8081"]
#CMD ["sleep", "999999"]
