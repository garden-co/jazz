import { getUser } from "../src/lib/auth";
import OperationsPage from "./operations-page";

// Every request reads the session cookie.
export const dynamic = "force-dynamic";

export default async function Page() {
  // The Managed Better Auth session, read on the server (as in Neon's Next.js example).
  const user = await getUser();
  return <OperationsPage user={user ? { id: user.id, email: user.email } : null} />;
}
