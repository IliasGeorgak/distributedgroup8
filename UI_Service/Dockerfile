FROM python:3

WORKDIR /usr/src/app

COPY UI_requirements.txt ./
RUN pip install --no-cache-dir -r UI_requirements.txt

COPY /dependencies ./dependencies
COPY /schemas ./schemas
COPY main.py .

WORKDIR /usr/src/app
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8081"]
#CMD ["sleep", "999999"]
