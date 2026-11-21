import { getAuthUserId } from "@convex-dev/auth/server";
import { query } from "./_generated/server";

/** The signed-in account: its id, name and sign-in email. */
export const viewer = query({
  args: {},
  handler: async (ctx) => {
    const userId = await getAuthUserId(ctx);
    const user = userId ? await ctx.db.get(userId) : null;
    return user ? { id: user._id, name: user.name ?? "", email: user.email ?? "" } : null;
  },
});
