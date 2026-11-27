import { randomBytes, scrypt, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";
import { init, id } from "@instantdb/admin";
import schema from "../../instant.schema";
import { normalizeEmail } from "./emails";

// The Instant admin SDK, for the server only: it skips permission checks.
export const adminDb = init({
  appId: process.env.NEXT_PUBLIC_INSTANT_APP_ID!,
  adminToken: process.env.INSTANT_APP_ADMIN_TOKEN!,
  schema,
});
export type AdminDb = typeof adminDb;
/** The admin SDK acting as one user: `instant.perms.ts` applies. */
export type CallerDb = ReturnType<AdminDb["asUser"]>;

// Email + password, the custom-auth route from Instant's docs: this server
// checks the password and mints an Instant token with `createToken`. Password
// hashes live in the admin-only `credentials` namespace.
const hash = promisify(scrypt) as (
  password: string,
  salt: Buffer,
  length: number,
) => Promise<Buffer>;

export class AuthError extends Error {}

export async function signUp(input: { name: string; email: string; password: string }) {
  const email = normalizeEmail(input.email);
  if (input.password.length < 8) throw new AuthError("Password is too short");
  const { credentials } = await adminDb.query({ credentials: { $: { where: { email } } } });
  if (credentials.length > 0) throw new AuthError("User already exists. Use another email.");
  const salt = randomBytes(16);
  const passwordHash = `${salt.toString("hex")}:${(await hash(input.password, salt, 64)).toString("hex")}`;
  await adminDb.transact(
    adminDb.tx.credentials[id()].create({ email, name: input.name.trim() || email, passwordHash }),
  );
  return { token: await adminDb.auth.createToken({ email }) };
}

export async function signIn(input: { email: string; password: string }) {
  const email = normalizeEmail(input.email);
  const { credentials } = await adminDb.query({ credentials: { $: { where: { email } } } });
  const [salt, expected] = credentials[0]?.passwordHash.split(":") ?? [];
  const actual = salt ? await hash(input.password, Buffer.from(salt, "hex"), 64) : undefined;
  if (!actual || !timingSafeEqual(actual, Buffer.from(expected!, "hex")))
    throw new AuthError("Invalid email or password");
  return { token: await adminDb.auth.createToken({ email }) };
}

/** A JSON route around one of the above; a refused sign-in answers 401. */
export function authRoute(handler: (body: any) => Promise<{ token: string }>) {
  return async (request: Request) => {
    try {
      return Response.json(await handler(await request.json()));
    } catch (cause) {
      if (cause instanceof AuthError)
        return Response.json({ message: cause.message }, { status: 401 });
      throw cause;
    }
  };
}

/**
 * The signed-in caller of an API route, from the Instant token the browser
 * sends, with their sign-up name, and a db that acts as them. Null when
 * nobody is signed in, so routes answer 401.
 */
export async function requestAccount(request: Request) {
  const token = request.headers.get("authorization")?.replace(/^Bearer /, "");
  const user = token ? await adminDb.auth.verifyToken(token).catch(() => null) : null;
  if (!user?.email) return null;
  const { credentials } = await adminDb.query({
    credentials: { $: { where: { email: user.email } } },
  });
  return {
    accountId: user.id,
    displayName: credentials[0]?.name || user.email,
    email: user.email,
    db: adminDb.asUser({ token: token! }),
  };
}

/** Whether a transaction failed on one of the schema's unique attributes. */
export function isUniqueConflict(error: unknown) {
  return (error as { body?: { type?: string } })?.body?.type === "record-not-unique";
}

/** Whether the server refused a write because the rules don't allow it. */
export function isPermissionDenied(error: unknown) {
  return String((error as { message?: string })?.message).includes("Permission denied");
}
