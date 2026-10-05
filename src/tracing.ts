import { SpanStatusCode, TraceFlags, trace, type Link, type Span } from "@opentelemetry/api";

/** Spans go to whatever OpenTelemetry SDK the application registers; without one they cost nothing. */
export const tracer = () => trace.getTracer("durable-tasks", "0.1.0");

/** Attempts run long after the trigger, often in another process, so they link to it rather than nest under it. */
export function linkTo(traceId: string | null, spanId: string | null): Link[] {
    if (traceId === null || spanId === null) return [];
    return [{ context: { traceId, spanId, traceFlags: TraceFlags.SAMPLED, isRemote: true } }];
}

export function recordFailure(span: Span, error: unknown): void {
    span.recordException(error instanceof Error ? error : new Error(String(error)));
    span.setStatus({ code: SpanStatusCode.ERROR, message: error instanceof Error ? error.message : String(error) });
}
