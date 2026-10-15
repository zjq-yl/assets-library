import crypto from "node:crypto";
import { ZodError, type ZodType } from "zod";
import { ApiV1Error } from "@/server/api/errors";
import type {
  ApiErrorDetail,
  ApiV1ErrorCode,
  ApiV1ErrorResponse,
} from "@/shared/contracts";
import { apiV1ErrorCodeSchema } from "@/shared/contracts";
import { userIdSchema } from "@/shared/contracts";
import { callContext, captureRequestBody } from "@/server/observability/context";
import { observe } from "@/server/observability/trace";

const MAX_JSON_BODY_BYTES = 1024 * 1024;

type RequestId = ReturnType<typeof crypto.randomUUID>;

function requestId(request: Request): RequestId {
  const supplied = request.headers.get("x-request-id");
  return supplied &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      supplied,
    )
    ? (supplied as RequestId)
    : crypto.randomUUID();
}

function inheritedServiceError(error: unknown): ApiV1Error | null {
  if (!error || typeof error !== "object") return null;
  const candidate = error as {
    code?: unknown;
    message?: unknown;
    status?: unknown;
    details?: unknown;
  };
  if (
    !apiV1ErrorCodeSchema.safeParse(candidate.code).success ||
    typeof candidate.message !== "string" ||
    typeof candidate.status !== "number" ||
    candidate.status < 400 ||
    candidate.status > 599
  ) {
    return null;
  }
  return new ApiV1Error(
    candidate.code as ApiV1ErrorCode,
    candidate.message,
    candidate.status,
    Array.isArray(candidate.details)
      ? (candidate.details as ApiErrorDetail[])
      : undefined,
  );
}

export function apiV1ErrorResponse(
  error: unknown,
  id: RequestId = crypto.randomUUID(),
) {
  const normalized =
    error instanceof ApiV1Error
      ? error
      : error instanceof ZodError
        ? new ApiV1Error(
            "invalid_request",
            error.issues[0]?.message ?? "请求字段无效。",
            400,
          )
        : inheritedServiceError(error) ??
          new ApiV1Error("internal_error", "系统处理失败，请稍后重试。", 500);
  const payload: ApiV1ErrorResponse = {
    error: {
      code: normalized.code,
      message: normalized.message,
      ...(normalized.details ? { details: normalized.details } : {}),
    },
    request_id: id,
  };
  return Response.json(payload, {
    status: normalized.status,
    headers: {
      "cache-control": "no-store",
      "x-request-id": id,
    },
  });
}

export async function withApiV1(
  request: Request,
  handler: () => Promise<Response> | Response,
  authorize: (request: Request) => void = () => undefined,
) {
  const id = requestId(request);
  const url = new URL(request.url);
  const execute = async () => { try {
    authorize(request);
    const response = await handler();
    const headers = new Headers(response.headers);
    headers.set("x-request-id", id);
    const traceId = callContext.getStore()?.traceId;
    if (traceId) headers.set("x-trace-id", traceId);
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  } catch (error) {
    const response = apiV1ErrorResponse(error, id);
    const traceId = callContext.getStore()?.traceId;
    if (traceId) response.headers.set("x-trace-id", traceId);
    return response;
  } };
  // 排除监控的查询接口，包含其内部服务函数，避免轮询产生无限观测噪声。
  if (url.pathname.startsWith("/api/v1/monitor/") ||
    (request.method === "GET" && (url.pathname === "/api/v1/tasks" || /\/tasks\/[^/]+\/(monitor|logs)$/.test(url.pathname)))) {
    return callContext.run(null, execute);
  }
  const taskId = url.pathname.match(/\/(?:tasks|uploads)\/([0-9a-f-]{36})(?:\/|$)/i)?.[1] ?? null;
  return observe(`${request.method} ${url.pathname}`, {
    method: request.method, path: url.pathname,
    query: Object.fromEntries(url.searchParams), headers: Object.fromEntries(request.headers),
    request_id: id, body: request.body ? { capture: "JSON 由参数解析记录；媒体流仅记录类型和长度。" } : null,
  }, execute, { kind: "api", taskId });
}

export async function parseJson<T>(request: Request, schema: ZodType<T>) {
  const declaredLength = Number(request.headers.get("content-length") ?? "0");
  if (Number.isFinite(declaredLength) && declaredLength > MAX_JSON_BODY_BYTES) {
    throw new ApiV1Error(
      "invalid_request",
      "JSON 请求体不得超过 1 MiB。",
      413,
    );
  }
  let value: unknown;
  try {
    const text = await request.text();
    if (Buffer.byteLength(text, "utf8") > MAX_JSON_BODY_BYTES) {
      throw new ApiV1Error(
        "invalid_request",
        "JSON 请求体不得超过 1 MiB。",
        413,
      );
    }
    value = JSON.parse(text);
    captureRequestBody(value);
  } catch (error) {
    captureRequestBody({ invalid_json: true });
    if (error instanceof ApiV1Error) throw error;
    throw new ApiV1Error("invalid_request", "请求体必须是有效的 JSON。", 400);
  }
  return schema.parse(value);
}

export async function parseOptionalJson<T>(
  request: Request,
  schema: ZodType<T>,
  emptyValue: unknown = {},
) {
  const text = await request.text();
  if (!text.trim()) return schema.parse(emptyValue);
  if (Buffer.byteLength(text, "utf8") > MAX_JSON_BODY_BYTES) {
    throw new ApiV1Error(
      "invalid_request",
      "JSON 请求体不得超过 1 MiB。",
      413,
    );
  }
  try {
    const value = JSON.parse(text);
    captureRequestBody(value);
    return schema.parse(value);
  } catch (error) {
    if (error instanceof ZodError) throw error;
    throw new ApiV1Error("invalid_request", "请求体必须是有效的 JSON。", 400);
  }
}

export function parseUuid(value: string, field: string) {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
    throw new ApiV1Error("invalid_request", `${field} 必须是有效的 UUID。`, 400);
  }
  return value;
}

/** 路径参数可能仍含百分号编码；在服务层之前统一解码、去空白并限长。 */
export function parseUserIdPath(value: string) {
  let decoded: string;
  try {
    decoded = decodeURIComponent(value);
  } catch {
    throw new ApiV1Error("invalid_request", "user_id 路径编码无效。", 400);
  }
  const parsed = userIdSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new ApiV1Error(
      "invalid_request",
      parsed.error.issues[0]?.message ?? "user_id 无效。",
      400,
    );
  }
  return parsed.data;
}
