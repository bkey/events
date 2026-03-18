FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY app/ ./app/

ENV PYTHONPATH=/code/app

EXPOSE 8000

CMD ["uv", "run", "python", "app/main.py"]
