import type { MinimalClerkClient } from "./types.js";

export function getClerkUsername(
  clerkClient: Pick<MinimalClerkClient, "user">,
) {
  if (!clerkClient.user) {
    return null;
  }

  if (clerkClient.user.fullName) {
    return clerkClient.user.fullName;
  }

  if (clerkClient.user.firstName) {
    if (clerkClient.user.lastName) {
      return `${clerkClient.user.firstName} ${clerkClient.user.lastName}`;
    }

    return clerkClient.user.firstName;
  }

  if (clerkClient.user.username) {
    return clerkClient.user.username;
  }

  if (clerkClient.user.primaryEmailAddress?.emailAddress) {
    const emailUsername =
      clerkClient.user.primaryEmailAddress.emailAddress.split("@")[0];

    if (emailUsername) {
      return emailUsername;
    }
  }

  return clerkClient.user.id;
}
