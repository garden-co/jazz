import { getAuth } from "../../../../src/lib/auth";

// Managed Better Auth's API on the app's origin. The catch-all must be named `path`.
type Context = { params: Promise<{ path: string[] }> };
let handlers: ReturnType<ReturnType<typeof getAuth>["handler"]> | undefined;
const h = () => (handlers ??= getAuth().handler());

export const GET = (request: Request, context: Context) => h().GET(request, context);
export const POST = (request: Request, context: Context) => h().POST(request, context);
