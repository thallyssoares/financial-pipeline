FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .

COPY assets/ assets/
COPY tests/ tests/
COPY definitions.py resources.py ./

RUN pip install --no-cache-dir -e ".[dev]"

ENV DAGSTER_HOME=/app/.dagster_home

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
