FROM public.ecr.aws/lambda/python:3.13

WORKDIR /var/task

# System dependencies required by NiMARE/compose-runner runtime.
RUN dnf install -y \
        blas \
        lapack \
        git \
    && dnf clean all

ARG COMPOSE_RUNNER_VERSION
ENV COMPOSE_RUNNER_VERSION=${COMPOSE_RUNNER_VERSION}
ENV HATCH_BUILD_VERSION=${COMPOSE_RUNNER_VERSION}
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${COMPOSE_RUNNER_VERSION}
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_COMPOSE_RUNNER=${COMPOSE_RUNNER_VERSION}

COPY pyproject.toml README.md ./
COPY compose_runner ./compose_runner

RUN test -n "$COMPOSE_RUNNER_VERSION" || (echo "COMPOSE_RUNNER_VERSION build arg is required" && exit 1) && \
    true

RUN pip install --upgrade pip && \
    pip install .

# Default handler points to the run Lambda; the polling Lambda overrides this.
CMD ["compose_runner.aws_lambda.run_handler.handler"]
