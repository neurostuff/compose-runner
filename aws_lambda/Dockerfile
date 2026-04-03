FROM public.ecr.aws/lambda/python:3.13

WORKDIR /var/task

# System dependencies required by NiMARE/compose-runner runtime.
RUN dnf install -y \
        blas \
        lapack \
    && dnf clean all

ARG COMPOSE_RUNNER_VERSION
ENV COMPOSE_RUNNER_VERSION=${COMPOSE_RUNNER_VERSION}

COPY pyproject.toml ./
COPY dist/*.whl /tmp/

RUN test -n "$COMPOSE_RUNNER_VERSION" || (echo "COMPOSE_RUNNER_VERSION build arg is required" && exit 1) && \
    true

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

# Default handler points to the run Lambda; the polling Lambda overrides this.
CMD ["compose_runner.aws_lambda.run_handler.handler"]
