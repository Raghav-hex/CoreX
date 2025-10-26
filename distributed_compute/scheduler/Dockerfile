# scheduler/Dockerfile

FROM python:3.11-slim
WORKDIR /app

# [UPDATED] Add the new libraries here
RUN pip install "fastapi[all]" websockets redis "psycopg[binary]" asyncpg sqlalchemy

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]