export function parseJobPayload(text: string): Record<string, unknown> {
  let value: unknown;
  try { value = JSON.parse(text); }
  catch { throw new Error("Enter valid JSON for the payload."); }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("The payload must be a JSON object, for example {\"customerId\":\"123\"}.");
  }
  return value as Record<string, unknown>;
}

export function newSubmissionKey(): string {
  if (typeof crypto.randomUUID === "function") return crypto.randomUUID();
  // getRandomValues also works when a private dashboard is served over plain HTTP.
  return Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
