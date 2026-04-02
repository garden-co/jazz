import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient({
  plugins: [jwtClient()],
});

export async function getJwtFromBetterAuth(): Promise<string | null> {
  try {
    const token = await authClient.token();
    if (token.error) {
      console.error("Error getting JWT token:", token.error.message);
      return null;
    }

    return token.data.token;
  } catch (error) {
    console.error("Error getting JWT token:", error);
    return null;
  }
}
