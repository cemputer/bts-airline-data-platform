FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (layer caching: dependencies rebuilt only when these change)
COPY pyproject.toml uv.lock ./

# Install dependencies without installing the project itself
RUN uv sync --frozen --no-install-project

# Copy the dashboard script
COPY analytics/streamlit/app.py ./app.py

EXPOSE 8501

# server.address=0.0.0.0 required: binding to localhost inside container blocks external access
CMD ["uv", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]