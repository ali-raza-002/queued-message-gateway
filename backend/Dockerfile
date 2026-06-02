# One image, two entrypoints. The API and the worker share the same code and
# the same dependency set, so they share the same build and differ only in the
# final stage's command.
#
#   docker build --target api    -t aila-api    backend
#   docker build --target worker -t aila-worker backend

# ---------- build ----------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.txt .
# Wheels are built once here and copied into the runtime image, which keeps
# compilers out of the thing that actually gets deployed.
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install -r requirements.txt

# ---------- runtime --------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Never run as root.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin aila
COPY --from=builder /opt/venv /opt/venv

WORKDIR /srv
COPY --chown=aila:aila app ./app
COPY --chown=aila:aila worker ./worker

USER aila

# ---------- api ------------------------------------------------------------
FROM runtime AS api

EXPOSE 8000
# Uses the stdlib rather than adding curl to the image for one health probe.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 \
    CMD ["python", "-c", \
         "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/readyz', timeout=2).status==200 else 1)"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# ---------- worker ---------------------------------------------------------
FROM runtime AS worker

# STOPSIGNAL is the default, but it is the contract the graceful-shutdown path
# in worker/worker.py depends on, so it is stated rather than assumed.
STOPSIGNAL SIGTERM
CMD ["python", "-m", "worker"]
