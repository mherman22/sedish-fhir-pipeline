# Deployable pipeline image: SQLMesh transform + loader, run as a continuous poll loop.
# config.yaml is rendered from env at start, so the image carries no connection details.
FROM python:3.12-slim
WORKDIR /app
# sqlmesh pinned: >=0.235 floated to a newer release that imports dateparser without pulling
# tzlocal (ModuleNotFoundError at startup). 0.235.4 is the known-good version.
RUN pip install --no-cache-dir "sqlmesh==0.235.4" "pymysql>=1.1.1" cryptography
COPY . /app
ENV HOME=/tmp
ENTRYPOINT ["/app/docker-entrypoint.sh"]
