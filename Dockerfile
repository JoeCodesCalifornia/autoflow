# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# Install the package first (thin layer) then copy examples so rebuilds of
# pipeline files stay cheap.
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY examples ./examples
COPY docs ./docs

ENTRYPOINT ["autoflow"]
CMD ["--help"]
