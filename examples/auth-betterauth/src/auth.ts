/**
 * BetterAuth client configuration.
 *
 * This client connects to the BetterAuth server for authentication
 * and retrieves JWT tokens for use with Jazz/groove-server.
 */

import { createAuthClient } from "better-auth/client";
import { jwtClient } from "better-auth/client/plugins";

// Auth server URL
const AUTH_URL = "http://localhost:3001";

// Create the BetterAuth client with JWT plugin
export const authClient = createAuthClient({
  baseURL: AUTH_URL,
  plugins: [jwtClient()],
});

// Types for auth state
export interface User {
  id: string;
  email: string;
  name: string | null;
  subscriptionTier?: string;
  roles?: string[];
}

export interface AuthState {
  user: User | null;
  token: string | null;
  loading: boolean;
  error: string | null;
}

/**
 * Get the current JWT token for use with Jazz.
 * This token contains claims that can be used in Jazz policies.
 */
export async function getJazzToken(): Promise<string | null> {
  try {
    // Use the JWT plugin's token method
    const result = await authClient.token();
    if (result.error || !result.data) {
      console.error("Failed to get JWT token:", result.error);
      return null;
    }
    return result.data.token;
  } catch (error) {
    console.error("Failed to get Jazz token:", error);
    return null;
  }
}

/**
 * Sign up a new user.
 */
export async function signUp(
  email: string,
  password: string,
  name: string,
): Promise<{ user: User } | { error: string }> {
  try {
    const result = await authClient.signUp.email({
      email,
      password,
      name,
    });

    if (result.error) {
      return { error: result.error.message || "Sign up failed" };
    }

    return {
      user: {
        id: result.data!.user.id,
        email: result.data!.user.email,
        name: result.data!.user.name,
      },
    };
  } catch (error) {
    return { error: String(error) };
  }
}

/**
 * Sign in an existing user.
 */
export async function signIn(
  email: string,
  password: string,
): Promise<{ user: User } | { error: string }> {
  try {
    const result = await authClient.signIn.email({
      email,
      password,
    });

    if (result.error) {
      return { error: result.error.message || "Sign in failed" };
    }

    return {
      user: {
        id: result.data!.user.id,
        email: result.data!.user.email,
        name: result.data!.user.name,
      },
    };
  } catch (error) {
    return { error: String(error) };
  }
}

/**
 * Sign out the current user.
 */
export async function signOut(): Promise<void> {
  await authClient.signOut();
}

/**
 * Get the current session.
 */
export async function getSession(): Promise<User | null> {
  try {
    const session = await authClient.getSession();
    if (!session?.data?.user) return null;

    return {
      id: session.data.user.id,
      email: session.data.user.email,
      name: session.data.user.name,
    };
  } catch {
    return null;
  }
}
