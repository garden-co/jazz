"use client";
import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";
export const authClient: any = createAuthClient({ plugins: [jwtClient() as never] });
