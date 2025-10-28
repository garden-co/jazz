import { afterEach, describe, expect, it, vi } from "vitest";
import { authenticateRequest, generateAuthToken } from "../coValues/request.js";
import { createJazzTestAccount } from "../testing.js";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("authenticateRequest", () => {
  it("should correctly authenticate a request", async () => {
    const me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const token = generateAuthToken();

    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          Authorization: `Jazz ${token}`,
        },
      }),
    );

    expect(error).toBeUndefined();
    expect(account?.$jazz.id).toBe(me.$jazz.id);
  });

  it("should not return an account if no token is provided", async () => {
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {}),
    );

    expect(error).toBeUndefined();
    expect(account).toBeUndefined();
  });

  it("should return an error if the token is invalid", async () => {
    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          Authorization: `Jazz invalid~invalid~invalid`,
        },
      }),
    );

    expect(error).toMatchObject(
      expect.objectContaining({
        message: "Invalid token",
        details: expect.anything(),
      }),
    );
    expect(account).toBeUndefined();
  });

  it("should return an error if the token is malformed", async () => {
    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          Authorization: `Jazz malformed`,
        },
      }),
    );

    expect(error).toMatchObject(
      expect.objectContaining({
        message: "Invalid token",
        details: expect.anything(),
      }),
    );
    expect(account).toBeUndefined();
  });

  it("should be resilient to tampering", async () => {
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const token = generateAuthToken();
    const tokenParts = token.split("~");
    tokenParts[2] = "999999999999999";
    const tamperedToken = tokenParts.join("~");

    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          Authorization: `Jazz ${tamperedToken}`,
        },
      }),
    );

    expect(error).toMatchObject(
      expect.objectContaining({
        message: "Invalid signature",
      }),
    );
    expect(account).toBeUndefined();
  });

  it("should return an error if the token is expired", async () => {
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const token = generateAuthToken();

    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          Authorization: `Jazz ${token}`,
        },
      }),
      {
        expiration: -1000,
      },
    );

    expect(error).toMatchObject(
      expect.objectContaining({
        message: "Token expired",
      }),
    );
    expect(account).toBeUndefined();
  });

  it("should treat the request as unauthenticated if the token is not in the default format, even if present.", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const token = generateAuthToken();

    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          Authorization: `${token}`,
        },
      }),
    );

    expect(console.warn).toHaveBeenCalled();
    expect(account).toBeUndefined();
    expect(error).toBeUndefined();
  });

  it("should correctly validate a request when the token is in a non standard location", async () => {
    const me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const token = generateAuthToken();

    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          ["x-jazz-auth-token"]: `${token}`,
        },
      }),
      {
        getToken: (request) => request.headers.get("x-jazz-auth-token"),
      },
    );

    expect(error).toBeUndefined();
    expect(account?.$jazz.id).toBe(me.$jazz.id);
  });

  it("should correctly validate a request when the token is generated from a non active account", async () => {
    const notAnActiveAccount = await createJazzTestAccount({
      isCurrentActiveAccount: false,
    });

    const token = generateAuthToken(notAnActiveAccount);

    const { account, error } = await authenticateRequest(
      new Request("https://api.example.com/api/user", {
        headers: {
          Authorization: `Jazz ${token}`,
        },
      }),
      {
        loadAs: notAnActiveAccount,
      },
    );

    expect(error).toBeUndefined();
    expect(account?.$jazz.id).toBe(notAnActiveAccount.$jazz.id);
  });
});
