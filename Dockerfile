FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y ffmpeg libmagic1 \
    && pip install --no-cache-dir uv==0.6.14 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

RUN groupadd --system transcript \
    && useradd --system --gid transcript --home-dir /app transcript \
    && chown -R transcript:transcript /app

USER transcript
