# Use OpenJDK as the base image
FROM openjdk:11-jdk-slim AS base

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies and essential tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libncurses5-dev \
        libncursesw5-dev \
        libreadline-dev \
        libsqlite3-dev \
        libgdbm-dev \
        libdb5.3-dev \
        libbz2-dev \
        libexpat1-dev \
        liblzma-dev \
        tk-dev \
        libffi-dev \
        curl \
        git \
        && rm -rf /var/lib/apt/lists/*

# Set the Python version to install
ENV PYTHON_VERSION=3.12.0

# Build Python from source
WORKDIR /usr/src
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && \
    rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz

# Create symlinks for Python and pip
RUN ln -s /usr/local/bin/python3.12 /usr/bin/python3 && \
    ln -s /usr/local/bin/pip3.12 /usr/bin/pip && \
    python3 --version && pip --version

# Upgrade pip
RUN pip install uv
RUN uv pip install --no-cache-dir --upgrade pip --system

# Install setuptools
RUN uv pip install --no-cache-dir setuptools --system

# Copy requirements first (to leverage cache for dependencies)
COPY requirements.txt .
RUN uv pip install --no-cache-dir -r requirements.txt --system

WORKDIR /app

# Create necessary directories
RUN mkdir -p /app/.cache

# Copy the rest of the application code
COPY . /app

# Expose the Streamlit port
EXPOSE 3005

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl --fail http://localhost:3005/_stcore/health || exit 1

# Entrypoint
ENTRYPOINT ["streamlit", "run", "main.py", "--server.port=3005", "--server.address=0.0.0.0"]

# Define volumes
VOLUME ["/app/.cache"]
