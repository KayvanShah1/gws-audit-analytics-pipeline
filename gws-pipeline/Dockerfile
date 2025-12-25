FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /app

FROM base AS deps

ADD https://astral.sh/uv/install.sh /tmp/uv-install.sh
RUN sh /tmp/uv-install.sh && rm /tmp/uv-install.sh
ENV PATH="/root/.cargo/bin:${PATH}"

COPY gws-pipeline/pyproject.toml gws-pipeline/uv.lock* /app/gws-pipeline/

WORKDIR /app/gws-pipeline

RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# ---- Runtime image ----
FROM base AS runtime

# Copy venv from deps stage
COPY --from=deps /opt/venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app/gws-pipeline/src"

# Dagster home
RUN mkdir -p ${DAGSTER_HOME}

# Copy repo
COPY . /app

# Expose Dagster UI
EXPOSE 3000

# Run Dagster webserver (UI/API)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/workspace.yaml"]
