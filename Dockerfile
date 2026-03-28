FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY ownership_graph/ ./ownership_graph/
COPY data/ ./data/

RUN pip install --no-cache-dir .

ENTRYPOINT ["ownership-graph-build"]
