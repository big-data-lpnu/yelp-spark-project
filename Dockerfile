ARG OPENJDK_VERSION=25
ARG OPENJDK_FLAVOR=alpine
ARG UV_PYTHON_INSTALL_DIR="/opt/uv-python"
ARG UV_INSTALL_DIR="/usr/local/bin"

# Build stage
FROM eclipse-temurin:${OPENJDK_VERSION}-jdk-${OPENJDK_FLAVOR} AS builder

ARG UV_INSTALL_DIR
ARG UV_PYTHON_INSTALL_DIR
ENV UV_INSTALL_DIR=${UV_INSTALL_DIR}
ENV UV_PYTHON_INSTALL_DIR=${UV_PYTHON_INSTALL_DIR}

RUN apk update && apk add --no-cache curl ca-certificates unzip build-base
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-cache

# Runtime stage - JRE only, not full JDK
FROM eclipse-temurin:${OPENJDK_VERSION}-jre-${OPENJDK_FLAVOR} AS runtime

ARG UV_INSTALL_DIR
ARG UV_PYTHON_INSTALL_DIR
ENV UV_INSTALL_DIR=${UV_INSTALL_DIR}
ENV UV_PYTHON_INSTALL_DIR=${UV_PYTHON_INSTALL_DIR}

RUN apk add --no-cache ca-certificates

COPY --from=builder ${UV_INSTALL_DIR} ${UV_INSTALL_DIR}
COPY --from=builder ${UV_PYTHON_INSTALL_DIR} ${UV_PYTHON_INSTALL_DIR}
COPY --from=builder /app/.venv /app/.venv

WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup \
    && chown -R appuser:appgroup /app \
    && chown -R appuser:appgroup ${UV_PYTHON_INSTALL_DIR}
USER appuser

COPY --chown=appuser:appgroup . .

CMD ["uv", "run", "-m", "src.main"]
