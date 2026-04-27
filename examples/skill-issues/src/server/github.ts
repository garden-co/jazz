export interface GitHubDeviceStart {
  device_code: string;
  user_code: string;
  verification_uri: string;
  interval: number;
}

export interface GitHubToken {
  accessToken: string;
}

export interface GitHubUser {
  id: string;
  login: string;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("GitHub returned an invalid response.");
  }
  return value as Record<string, unknown>;
}

function stringField(record: Record<string, unknown>, name: string): string {
  const value = record[name];
  if (typeof value !== "string" || !value) {
    throw new Error(`GitHub response missing ${name}.`);
  }
  return value;
}

function numberField(record: Record<string, unknown>, name: string): number {
  const value = record[name];
  if (typeof value !== "number") {
    throw new Error(`GitHub response missing ${name}.`);
  }
  return value;
}

export async function startDeviceAuthorization(clientId: string): Promise<GitHubDeviceStart> {
  const response = await fetch("https://github.com/login/device/code", {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      client_id: clientId,
      scope: "read:user",
    }),
  });
  const json = asRecord(await response.json());

  if (!response.ok || typeof json.error === "string") {
    throw new Error(
      typeof json.error_description === "string"
        ? json.error_description
        : "GitHub device authorization failed.",
    );
  }

  return {
    device_code: stringField(json, "device_code"),
    user_code: stringField(json, "user_code"),
    verification_uri: stringField(json, "verification_uri"),
    interval: numberField(json, "interval"),
  };
}

export async function exchangeDeviceCode({
  clientId,
  deviceCode,
}: {
  clientId: string;
  deviceCode: string;
}): Promise<GitHubToken> {
  const response = await fetch("https://github.com/login/oauth/access_token", {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      client_id: clientId,
      device_code: deviceCode,
      grant_type: "urn:ietf:params:oauth:grant-type:device_code",
    }),
  });
  const json = asRecord(await response.json());

  if (!response.ok || typeof json.error === "string") {
    throw new Error(
      typeof json.error_description === "string"
        ? json.error_description
        : "GitHub device code exchange failed.",
    );
  }

  return {
    accessToken: stringField(json, "access_token"),
  };
}

export async function fetchGitHubUser(accessToken: string): Promise<GitHubUser> {
  const response = await fetch("https://api.github.com/user", {
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${accessToken}`,
    },
  });
  const json = asRecord(await response.json());

  if (!response.ok) {
    throw new Error("GitHub user fetch failed.");
  }

  const id = json.id;
  if (typeof id !== "number") {
    throw new Error("GitHub response missing numeric id.");
  }

  return {
    id: String(id),
    login: stringField(json, "login"),
  };
}
