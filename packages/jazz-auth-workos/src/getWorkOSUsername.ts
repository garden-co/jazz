import type { MinimalWorkOSClient } from "./types.js";

export function getWorkOSUsername(
  workosClient: Pick<MinimalWorkOSClient, "user">,
) {
  if (!workosClient.user) {
    return null;
  }

  if (workosClient.user.firstName) {
    if (workosClient.user.lastName) {
      return `${workosClient.user.firstName} ${workosClient.user.lastName}`;
    }

    return workosClient.user.firstName;
  }


  if (workosClient.user.email) {
    const emailUsername = workosClient.user.email.split('@')[0];

    if (emailUsername) {
      return emailUsername;
    }
  }

  return workosClient.user.id;
}