#!/usr/bin/env sh
set -e

if [ "${DEMO_MODE}" = "1" ]; then
  echo "Demo mode ON: starting without OpenTelemetry (no access log)"
  uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}" --no-access-log
else
  if [ -n "${OTEL_EXPORTER_OTLP_TRACES_ENDPOINT}" ] || [ -n "${OTEL_EXPORTER_OTLP_ENDPOINT}" ]; then
    echo "Starting with OpenTelemetry instrumentation (Python config)"
  else
    echo "No OTEL endpoint set: starting without OpenTelemetry (no access log)"
  fi
  uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}" --no-access-log
fi
