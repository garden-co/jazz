// Email + password for Instant, the custom-auth route from Instant's docs:
// this server checks the password and mints an Instant token with the admin
// SDK (`createToken`). Password hashes live in the admin-only `credentials`
// namespace; no client rule can read it.
import { randomBytes, scrypt, timingSafeEqual } from "node:crypto";
import { promisify } from "node:util";
import { init, id } from "@instantdb/admin";
import schema from "../instant.schema";

const hash = promisify(scrypt) as (
  password: string,
  salt: Buffer,
  length: number,
) => Promise<Buffer>;

export const adminDb = init({
  appId: process.env.VITE_INSTANT_APP_ID!,
  adminToken: process.env.INSTANT_APP_ADMIN_TOKEN!,
  schema,
});

export class AuthError extends Error {}

async function hashPassword(password: string) {
  const salt = randomBytes(16);
  return `${salt.toString("hex")}:${(await hash(password, salt, 64)).toString("hex")}`;
}

async function verifyPassword(password: string, stored: string) {
  const [salt, expected] = stored.split(":");
  const actual = await hash(password, Buffer.from(salt!, "hex"), 64);
  return timingSafeEqual(actual, Buffer.from(expected!, "hex"));
}

export async function signUp(input: { name: string; email: string; password: string }) {
  const email = input.email.trim().toLowerCase();
  if (input.password.length < 8) throw new AuthError("Password must be at least 8 characters");
  const { credentials } = await adminDb.query({ credentials: { $: { where: { email } } } });
  if (credentials.length > 0) throw new AuthError("User already exists. Use another email.");
  await adminDb.transact(
    adminDb.tx.credentials[id()].create({
      email,
      passwordHash: await hashPassword(input.password),
    }),
  );
  const token = await adminDb.auth.createToken({ email });
  const user = await adminDb.auth.getUser({ email });
  await adminDb.transact(adminDb.tx.$users[user!.id].update({ name: input.name.trim() }));
  return { token };
}

export async function signIn(input: { email: string; password: string }) {
  const email = input.email.trim().toLowerCase();
  const { credentials } = await adminDb.query({ credentials: { $: { where: { email } } } });
  const stored = credentials[0];
  if (!stored || !(await verifyPassword(input.password, stored.passwordHash)))
    throw new AuthError("Invalid email or password");
  return { token: await adminDb.auth.createToken({ email }) };
}
