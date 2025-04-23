FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir --compile -r requirements.txt

COPY . /app

RUN mkdir -p /app/data /app/output && \
    chmod +x /app/main.py

ENV RUNNING_IN_CONTAINER=true

EXPOSE 8080

CMD ["python", "main.py"]