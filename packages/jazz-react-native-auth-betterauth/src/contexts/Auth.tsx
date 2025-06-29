import { createAuthContext } from "jazz-auth-betterauth/react";
import { useBetterAuth } from "../hooks.js";

// Create and export the AuthContext, AuthProvider, and useAuth using the factory
const { AuthProvider, useAuth } = createAuthContext(useBetterAuth);

export { AuthProvider, useAuth, createAuthContext };
