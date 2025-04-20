FROM python:3.12.10-alpine
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY . ./CROUStillantDatasets

WORKDIR /CROUStillantDatasets

RUN uv sync --frozen

CMD ["crond", "-f"]
