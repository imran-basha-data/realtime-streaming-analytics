# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS build
WORKDIR /src
COPY requirements.txt ./
# Optional CA bundle for builds behind a TLS-intercepting proxy:
#   docker build --secret id=ca_bundle,src=/path/to/ca.pem .
RUN --mount=type=secret,id=ca_bundle,required=false \
    if [ -f /run/secrets/ca_bundle ]; then export PIP_CERT=/run/secrets/ca_bundle; fi \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN --mount=type=secret,id=ca_bundle,required=false \
    if [ -f /run/secrets/ca_bundle ]; then export PIP_CERT=/run/secrets/ca_bundle; fi \
    && pip install --no-cache-dir --no-deps --prefix=/install .

FROM python:3.12-slim
LABEL org.opencontainers.image.title="realtime-streaming-analytics" \
      org.opencontainers.image.source="https://github.com/imran-basha-data/realtime-streaming-analytics" \
      org.opencontainers.image.licenses="MIT"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=build /install /usr/local
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001
EXPOSE 8000 9108
# One image, several roles: topics | migrate | consume | produce | api
ENTRYPOINT ["stream"]
CMD ["--help"]
