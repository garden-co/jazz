const DEVELOPMENT_AUTH_SECRET = "band-chat-development-secret";

export function resolveBandChatAuthSecret(env: {
  BETTER_AUTH_SECRET?: string;
  NODE_ENV?: string;
}): string {
  if (env.BETTER_AUTH_SECRET?.trim()) return env.BETTER_AUTH_SECRET;
  if (env.NODE_ENV === "production") {
    throw new Error(
      "BandChat configuration error: BETTER_AUTH_SECRET is required in production; set it in the deployment environment.",
    );
  }
  return DEVELOPMENT_AUTH_SECRET;
}
