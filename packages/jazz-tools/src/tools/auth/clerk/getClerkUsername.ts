import type { ClerkUser } from "./types.js";

export function getClerkUsername(user: ClerkUser) {
  if (user.fullName) {
    return user.fullName;
  }

  if (user.firstName) {
    if (user.lastName) {
      return `${user.firstName} ${user.lastName}`;
    }

    return user.firstName;
  }

  if (user.username) {
    return user.username;
  }

  if (user.primaryEmailAddress?.emailAddress) {
    const emailUsername = user.primaryEmailAddress.emailAddress.split("@")[0];

    if (emailUsername) {
      return emailUsername;
    }
  }

  return user.id;
}
