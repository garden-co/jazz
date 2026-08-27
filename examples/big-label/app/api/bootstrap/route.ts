import { auth, jazzIssuer } from "../../../src/lib/auth";
import { ensurePersonalOrganization } from "../../../src/lib/bootstrap";
import { userIdentity } from "jazz-tools";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  // Tenant rows use Jazz's canonical `(iss, sub)` identity, exactly as
  // `session.user` does in policies. Better Auth's provider-local id remains
  // private to its own tables.
  const organization = await ensurePersonalOrganization(
    userIdentity(jazzIssuer, session.user.id),
    session.user.name,
  );
  return Response.json({ organizationId: organization.id });
}
