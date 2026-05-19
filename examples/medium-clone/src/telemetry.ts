import { context, SpanStatusCode, trace, type Span } from "@opentelemetry/api";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { DocumentLoadInstrumentation } from "@opentelemetry/instrumentation-document-load";
import { FetchInstrumentation } from "@opentelemetry/instrumentation-fetch";
import { UserInteractionInstrumentation } from "@opentelemetry/instrumentation-user-interaction";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchLogRecordProcessor, LoggerProvider } from "@opentelemetry/sdk-logs";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { WebTracerProvider } from "@opentelemetry/sdk-trace-web";

type TelemetryAttributes = Record<string, string | number | boolean | null | undefined>;

const serviceName = "medium-clone-browser";
const sessionId = crypto.randomUUID();
const telemetryScope = "medium-clone.app";

export const telemetryCollectorUrl =
  import.meta.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL ??
  (import.meta.env.DEV ? "http://127.0.0.1:54418" : undefined);

function otlpEndpoint(signal: "logs" | "traces") {
  if (!telemetryCollectorUrl) return null;
  const trimmed = telemetryCollectorUrl.trim().replace(/\/$/, "");
  if (!trimmed) return null;
  if (trimmed.endsWith(`/v1/${signal}`)) return trimmed;
  if (trimmed.endsWith("/v1/logs")) {
    return `${trimmed.slice(0, -"/v1/logs".length)}/v1/${signal}`;
  }
  if (trimmed.endsWith("/v1/traces")) {
    return `${trimmed.slice(0, -"/v1/traces".length)}/v1/${signal}`;
  }
  return `${trimmed}/v1/${signal}`;
}

function cleanAttributes(
  attributes: TelemetryAttributes,
): Record<string, string | number | boolean> {
  const cleaned: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(attributes)) {
    if (value === undefined) continue;
    cleaned[key] = value === null ? "null" : value;
  }
  return cleaned;
}

function baseAttributes(attributes: TelemetryAttributes = {}) {
  return cleanAttributes({
    "app.session_id": sessionId,
    "app.route": window.location.hash || "#/",
    "app.url_path": window.location.pathname,
    ...attributes,
  });
}

function normalizeError(error: unknown) {
  return {
    "error.message": error instanceof Error ? error.message : String(error),
    "error.name": error instanceof Error ? error.name : typeof error,
  };
}

const resource = resourceFromAttributes({
  "service.name": serviceName,
  "service.namespace": "examples",
  "service.instance.id": sessionId,
  "deployment.environment": import.meta.env.MODE,
});

const traceEndpoint = otlpEndpoint("traces");
const logEndpoint = otlpEndpoint("logs");

const tracerProvider = traceEndpoint
  ? new WebTracerProvider({
      resource,
      spanProcessors: [
        new BatchSpanProcessor(
          new OTLPTraceExporter({
            url: traceEndpoint,
          }),
          {
            scheduledDelayMillis: 250,
            exportTimeoutMillis: 2_000,
          },
        ),
      ],
    })
  : null;

if (tracerProvider) {
  tracerProvider.register();
  registerInstrumentations({
    instrumentations: [
      new DocumentLoadInstrumentation(),
      new FetchInstrumentation({
        ignoreUrls: [/\/v1\/(?:logs|traces|metrics)$/],
        clearTimingResources: true,
      }),
      new UserInteractionInstrumentation({
        eventNames: ["click", "submit"],
      }),
    ],
  });
}

const loggerProvider = logEndpoint
  ? new LoggerProvider({
      resource,
      processors: [
        new BatchLogRecordProcessor(
          new OTLPLogExporter({
            url: logEndpoint,
          }),
          {
            scheduledDelayMillis: 250,
            exportTimeoutMillis: 2_000,
          },
        ),
      ],
    })
  : null;

const tracer = trace.getTracer(telemetryScope);
const logger = loggerProvider?.getLogger(telemetryScope);

export function shortId(id: string | null | undefined) {
  return id ? id.slice(0, 8) : "";
}

export function logEvent(
  name: string,
  attributes: TelemetryAttributes = {},
  severity: "INFO" | "ERROR" = "INFO",
) {
  const attrs = baseAttributes(attributes);
  logger?.emit({
    severityText: severity,
    severityNumber: severity === "ERROR" ? 17 : 9,
    body: name,
    attributes: attrs,
  });

  const span = trace.getSpan(context.active());
  span?.addEvent(name, attrs);
}

export function logError(name: string, error: unknown, attributes: TelemetryAttributes = {}) {
  const attrs = {
    ...attributes,
    ...normalizeError(error),
  };
  logEvent(name, attrs, "ERROR");

  const span = trace.getSpan(context.active());
  if (span) {
    if (error instanceof Error) {
      span.recordException(error);
    }
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: attrs["error.message"],
    });
  }
}

export function startOperation(name: string, attributes: TelemetryAttributes = {}) {
  const operationId = crypto.randomUUID();
  const startedAt = performance.now();
  const attrs = baseAttributes({
    ...attributes,
    "operation.id": operationId,
  });
  const span = tracer.startSpan(name, { attributes: attrs });

  logEvent(`${name}.started`, {
    ...attributes,
    "operation.id": operationId,
  });

  return {
    id: operationId,
    step(step: string, stepAttributes: TelemetryAttributes = {}) {
      const stepAttrs = baseAttributes({
        ...attributes,
        ...stepAttributes,
        "operation.id": operationId,
        "operation.elapsed_ms": Math.round(performance.now() - startedAt),
      });
      span.addEvent(step, stepAttrs);
      logEvent(`${name}.${step}`, stepAttrs);
    },
    done(doneAttributes: TelemetryAttributes = {}) {
      const durationMs = Math.round(performance.now() - startedAt);
      const doneAttrs = baseAttributes({
        ...attributes,
        ...doneAttributes,
        "operation.id": operationId,
        "operation.duration_ms": durationMs,
      });
      span.setAttributes(doneAttrs);
      span.setStatus({ code: SpanStatusCode.OK });
      logEvent(`${name}.completed`, doneAttrs);
      span.end();
    },
    error(error: unknown, errorAttributes: TelemetryAttributes = {}) {
      const durationMs = Math.round(performance.now() - startedAt);
      const errorAttrs = baseAttributes({
        ...attributes,
        ...errorAttributes,
        ...normalizeError(error),
        "operation.id": operationId,
        "operation.duration_ms": durationMs,
      });
      span.setAttributes(errorAttrs);
      if (error instanceof Error) {
        span.recordException(error);
      }
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: String(errorAttrs["error.message"]),
      });
      logEvent(`${name}.failed`, errorAttrs, "ERROR");
      span.end();
    },
  };
}

export function installBrowserTelemetry() {
  logEvent("medium.app.boot", {
    "telemetry.collector_configured": Boolean(telemetryCollectorUrl),
    "telemetry.otel_enabled": Boolean(tracerProvider && loggerProvider),
  });

  window.addEventListener("error", (event) => {
    logError("medium.browser.error", event.error ?? event.message, {
      "error.filename": event.filename,
      "error.line": event.lineno,
      "error.column": event.colno,
    });
  });

  window.addEventListener("unhandledrejection", (event) => {
    logError("medium.browser.unhandled_rejection", event.reason);
  });

  window.addEventListener("pagehide", () => {
    void tracerProvider?.forceFlush();
    void loggerProvider?.forceFlush();
  });
}
