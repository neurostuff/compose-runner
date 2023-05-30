FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY setup.cfg .

COPY pyproject.toml .

RUN pip install .

COPY . .

RUN pip install .

ENTRYPOINT ["compose-run"]
