import { createAuthContext } from "jazz-auth-betterauth";
import { useBetterAuth } from "../hooks.js";

// Create and export the AuthContext, AuthProvider, and useAuth using the factory
const { AuthProvider, useAuth } = createAuthContext(useBetterAuth);

export { AuthProvider, useAuth };
