FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt man_requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r man_requirements.txt python-multipart

COPY . .

WORKDIR /usr/src/app
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
