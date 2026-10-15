import { AsyncLocalStorage } from "node:async_hooks";

export interface CallContext {
  traceId: string;
  spanId: string;
  task: { id: string | null };
  input: unknown;
}

// null 表示监控查询，整个查询链都不生成观测记录。
export const callContext = new AsyncLocalStorage<CallContext | null>();

export function taskTraceFields() {
  const context = callContext.getStore();
  return { traceId: context?.traceId ?? null, originSpanId: context?.spanId ?? null };
}

export function captureRequestBody(body: unknown) {
  const context = callContext.getStore();
  if (context) context.input = { ...context.input as object, body };
}
