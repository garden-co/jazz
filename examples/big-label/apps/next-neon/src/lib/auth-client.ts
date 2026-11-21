"use client";
import { createAuthClient } from "@neon/auth/next";

// Same origin: Managed Better Auth's API is mounted at /api/auth/*.
export const authClient = createAuthClient();
