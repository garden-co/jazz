// Re-export types and utilities from jazz-react-auth-betterauth source files
export type {
  AuthClient,
  Session,
  User,
} from "jazz-react-auth-betterauth";
export { SocialProvider } from "jazz-react-auth-betterauth/src/lib/social";
export { useBetterAuth } from "./hooks.js";
export { AuthProvider, useAuth } from "./contexts/Auth.js";
