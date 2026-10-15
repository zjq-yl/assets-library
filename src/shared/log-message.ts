/** 从调用异常、API 错误响应或日志上下文中提取可直接阅读的原因。 */
export function errorSummary(value: unknown, depth = 0): string {
  if (!value || depth > 4) return "";
  if (typeof value === "string") return value;
  if (typeof value !== "object") return "";
  const row = value as Record<string, unknown>;
  if (row.error) return errorSummary(row.error, depth + 1);
  const message = typeof row.message === "string" ? row.message : typeof row.error_message === "string" ? row.error_message : "";
  const code = typeof row.code === "string" ? row.code : typeof row.error_code === "string" ? row.error_code : "";
  return [code, message].filter(Boolean).join(" · ");
}
