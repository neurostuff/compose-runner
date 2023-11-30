FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# just install dependencies (less likely to change)
RUN pip install -r requirements.txt

COPY . .

# install the package (more likely to change, leverage caching!)
RUN pip install .

ENTRYPOINT ["compose-run"]
