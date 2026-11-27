import { randomBytes, scrypt, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";
import { init, id } from "@instantdb/admin";
import schema from "@/instant.schema";

// The Instant admin SDK, for the server only: it skips permission checks.
export const adminDb = init({
  appId: process.env.NEXT_PUBLIC_INSTANT_APP_ID!,
  adminToken: process.env.INSTANT_APP_ADMIN_TOKEN!,
  schema,
});

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
  const email = input.email.trim().toLowerCase();
  // Industry-standard minimum; tune to whatever your product requires.
  if (input.password.length < 8) throw new AuthError("Password is too short");
  const { credentials } = await adminDb.query({ credentials: { $: { where: { email } } } });
  if (credentials.length > 0) throw new AuthError("User already exists. Use another email.");
  const salt = randomBytes(16);
  const passwordHash = `${salt.toString("hex")}:${(await hash(input.password, salt, 64)).toString("hex")}`;
  await adminDb.transact(adminDb.tx.credentials[id()].create({ email, passwordHash }));
  const token = await adminDb.auth.createToken({ email });
  const user = await adminDb.auth.getUser({ email });
  await adminDb.transact(adminDb.tx.$users[user!.id].update({ name: input.name.trim() }));
  return { token };
}

export async function signIn(input: { email: string; password: string }) {
  const email = input.email.trim().toLowerCase();
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
