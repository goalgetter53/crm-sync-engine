FROM python:3.11-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install ".[fake,postgres]"

COPY examples ./examples
RUN useradd --create-home app
USER app

# Default: run the sync CLI. docker-compose overrides this for the fake server.
ENTRYPOINT ["crm-sync"]
CMD ["--help"]
