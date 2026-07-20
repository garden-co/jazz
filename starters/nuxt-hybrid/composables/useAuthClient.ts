import { createAuthClient } from "better-auth/vue";
import { jwtClient } from "better-auth/client/plugins";

const authClient = createAuthClient({ plugins: [jwtClient()] });
export const useAuthClient = () => authClient;
