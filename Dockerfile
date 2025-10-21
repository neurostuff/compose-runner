FROM python:3.13-slim

ARG COMPOSE_RUNNER_VERSION
ENV COMPOSE_RUNNER_VERSION=${COMPOSE_RUNNER_VERSION}
LABEL org.opencontainers.image.title="compose-runner ecs task"
LABEL org.opencontainers.image.version=${COMPOSE_RUNNER_VERSION}

RUN test -n "$COMPOSE_RUNNER_VERSION" || (echo "COMPOSE_RUNNER_VERSION build arg is required" && exit 1)

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .

# install build backend and hatch
RUN pip install --upgrade pip && pip install hatchling hatch-vcs hatch

# export dependencies using hatch and install with pip
RUN hatch dep show requirements > requirements.txt && pip install -r requirements.txt

COPY . .

# install the package with AWS extras so the ECS task has boto3, etc.
RUN pip install '.[aws]'

ENTRYPOINT ["compose-run"]
