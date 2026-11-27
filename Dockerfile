# One image runs every routewatch service: the collector, the raw consumer,
# the cleaner and the web UI. compose.app.yaml chooses each one's command.
#   docker build -t routewatch:local .

FROM python:3.12.16-slim-trixie

LABEL org.opencontainers.image.title="routewatch" \
      org.opencontainers.image.description="Streams RIPE RIS Live BGP updates through Kafka into PostgreSQL and cleans them for analysis." \
      org.opencontainers.image.source="https://github.com/munguriek/p-4-da-ca01" \
      org.opencontainers.image.licenses="MIT"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

# The pinned dependencies first, read from pyproject.toml alone, so that a code
# change reuses this layer instead of reinstalling them.
COPY pyproject.toml ./
RUN python -m pip install pip==26.3 \
 && python -c "import tomllib; print(*tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies'], sep='\n')" > /tmp/requirements.txt \
 && python -m pip install --requirement /tmp/requirements.txt \
 && rm /tmp/requirements.txt

# Then the package itself; its dependencies are already installed.
COPY README.md LICENSE ./
COPY src ./src
RUN python -m pip install --no-deps . \
 && rm -rf build src/*.egg-info

COPY scripts ./scripts
COPY ui ./ui

# The code is readable by everyone, whatever the checkout's file modes, and
# writable only by root; the services run as an unprivileged user.
RUN chmod -R a+rX,go-w /app \
 && useradd --system --uid 10001 --user-group --home-dir /app --no-create-home \
      --shell /usr/sbin/nologin routewatch
USER routewatch
