FROM python:3.13-bookworm

ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive
ENV POETRY_VIRTUALENVS_CREATE=false
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -

RUN pip install --no-cache-dir prefect prefect-docker

COPY pyproject.toml poetry.lock* ./

RUN poetry install --no-interaction --no-ansi --without dev --no-root

COPY src .

