export interface InspectorHttpAuthority {
  adminSecret?: string;
  inspectorToken?: string;
}

export function inspectorAuthorityHeaders(
  authority: InspectorHttpAuthority,
): Record<string, string> {
  if (authority.adminSecret && authority.inspectorToken)
    throw new Error("Inspector credentials cannot be combined with root authority");
  if (authority.inspectorToken) return { "X-Jazz-Inspector-Token": authority.inspectorToken };
  if (authority.adminSecret) return { "X-Jazz-Admin-Secret": authority.adminSecret };
  throw new Error("Diagnostic credential is required");
}
