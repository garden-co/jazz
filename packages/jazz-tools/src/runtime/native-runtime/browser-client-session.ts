import { canonicalAuthorSubject } from "../author-id.js";
import type { ClientBindingRequest } from "./browser-client-binding-protocol.js";

/**
 * A browser binding uses the worker's admitted owner and current claims. Page
 * session capabilities are realm-local and must never be imported as authority.
 * Validate any supplied principal, then let the ordinary client use its owner.
 */
export function admitBrowserClientRequest(
  request: ClientBindingRequest,
  author: string,
): ClientBindingRequest {
  function session(value: unknown): void {
    if (value === undefined || value === null) return;
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    if (!parsed || typeof parsed !== "object") throw new Error("Invalid client session");
    if (canonicalAuthorSubject(parsed.issuer, parsed.user_id, parsed.account_id) !== author)
      throw new Error("Client request identity differs from the admitted worker owner");
  }
  function context(value?: string | null): string | null | undefined {
    if (!value) return value;
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
      throw new Error("Invalid client write context");
    if (parsed.attribution !== undefined)
      throw new Error("Browser clients cannot select backend attribution");
    if (parsed.session !== undefined) session(parsed.session);
    if (parsed.issuer !== undefined || parsed.user_id !== undefined) session(parsed);
    // Keep transaction identity, branch target and timestamps intact.
    for (const key of [
      "session",
      "issuer",
      "user_id",
      "account_id",
      "claims",
      "authMode",
      "__jazz_trusted_reserved_session",
    ])
      delete parsed[key];
    return JSON.stringify(parsed);
  }
  if (request.type === "client-subscribe") {
    session(request.args[1]);
    return { ...request, args: [request.args[0], undefined, request.args[2], request.args[3]] };
  }
  if (request.type !== "client-call") return request;
  const call = request.call;
  switch (call.method) {
    case "query":
      session(call.args[1]);
      return {
        ...request,
        call: { ...call, args: [call.args[0], undefined, call.args[2], call.args[3]] },
      };
    case "beginTransaction":
      return {
        ...request,
        call: { ...call, args: [call.args[0], call.args[1], context(call.args[2])] },
      };
    case "insert":
      return {
        ...request,
        call: { ...call, args: [call.args[0], call.args[1], context(call.args[2]), call.args[3]] },
      };
    case "delete":
      return {
        ...request,
        call: { ...call, args: [call.args[0], call.args[1], context(call.args[2])] },
      };
    case "restore":
    case "update":
    case "upsert":
      return {
        ...request,
        call: { ...call, args: [call.args[0], call.args[1], call.args[2], context(call.args[3])] },
      };
    case "updateLargeValues":
      return {
        ...request,
        call: {
          ...call,
          args: [call.args[0], call.args[1], call.args[2], call.args[3], context(call.args[4])],
        },
      };
    case "streamingMutation":
      return {
        ...request,
        call: {
          ...call,
          args: [
            call.args[0],
            call.args[1],
            call.args[2],
            call.args[3],
            call.args[4],
            context(call.args[5]),
            call.args[6],
          ],
        },
      };
    case "requestInsertPermissionAdvice":
    case "requestReadPermissionAdvice":
    case "requestDeletePermissionAdvice":
      session(call.args[2]);
      return {
        ...request,
        call: { ...call, args: [call.args[0], call.args[1], undefined] },
      } as ClientBindingRequest;
    case "requestUpdatePermissionAdvice":
      session(call.args[3]);
      return {
        ...request,
        call: { ...call, args: [call.args[0], call.args[1], call.args[2], undefined] },
      };
    default:
      return request;
  }
}
