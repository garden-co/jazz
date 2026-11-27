import { headers } from "next/headers";
import { auth } from "../src/lib/auth";
import { BigLabel } from "./big-label";

export const dynamic = "force-dynamic";

/** Reads the Better Auth session on the server, so the app opens Zero straight away. */
export default async function Page() {
  const session = await auth.api.getSession({ headers: await headers() });
  return <BigLabel user={session ? { id: session.user.id, email: session.user.email } : null} />;
}
