FROM public.ecr.aws/docker/library/python:3.13-slim

ARG COMPOSE_RUNNER_VERSION
ENV COMPOSE_RUNNER_VERSION=${COMPOSE_RUNNER_VERSION}
LABEL org.opencontainers.image.title="compose-runner ecs task"
LABEL org.opencontainers.image.version=${COMPOSE_RUNNER_VERSION}

RUN test -n "$COMPOSE_RUNNER_VERSION" || (echo "COMPOSE_RUNNER_VERSION build arg is required" && exit 1)

COPY pyproject.toml .
COPY dist/*.whl /tmp/

# Install runtime dependencies from pyproject and then install the prebuilt wheel.
RUN pip install --upgrade pip && \
    python - <<'PY' > requirements.txt
import tomllib
from pathlib import Path

project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
requirements = list(project["dependencies"])
requirements.extend(project.get("optional-dependencies", {}).get("aws", []))
print("\n".join(requirements))
PY

RUN pip install -r requirements.txt && pip install --no-deps /tmp/*.whl

ENTRYPOINT ["compose-run"]
