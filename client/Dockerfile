FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
# Except .venv
RUN rm -rf .venv

# Install the project's dependencies using pyproject.toml with dev
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=src/pyproject.toml,target=pyproject.toml \
    uv sync --no-install-project --extra dev

# Env
ENV PATH="/app/.venv/bin:$PATH"

# Expose the Streamlit port
EXPOSE 3005

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl --fail http://localhost:3005/_stcore/health || exit 1

# Entrypoint
ENTRYPOINT ["uv", "run", "streamlit", "run", "main.py", "--server.port=3005", "--server.address=0.0.0.0"]
