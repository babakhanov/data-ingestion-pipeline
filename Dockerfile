FROM alpine:latest

WORKDIR /app

ENV DATABASE_URL=postgresql://postgres:mysecretpassword@db:5432

COPY requirements.txt /app/requirements.txt

RUN apk add --no-cache python3 py3-pip rust cargo gcc python3-dev libc-dev
RUN python3 -m venv /venv

ENV PATH="/venv/bin:$PATH"

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# CMD ["python", "main.py"]
