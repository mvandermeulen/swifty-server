# syntax=docker/dockerfile:1.4
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN addgroup --system swifty && adduser --system --ingroup swifty swifty

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chown -R swifty:swifty /app
USER swifty

EXPOSE 8000

ENV UVICORN_WORKERS=4 \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8000

CMD ["sh", "-c", "uvicorn main:app --host ${UVICORN_HOST} --port ${UVICORN_PORT} --workers ${UVICORN_WORKERS}"]
