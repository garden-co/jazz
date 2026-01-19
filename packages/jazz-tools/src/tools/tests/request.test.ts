import { http } from "msw";
import { setupServer } from "msw/node";
import { assert, describe, expect, it, vi } from "vitest";
import { afterAll, afterEach, beforeAll } from "vitest";
import { z } from "zod/v4";
import {
  JazzRequestError,
  experimental_defineRequest,
  isJazzRequestError,
} from "../coValues/request.js";
import { Account, CoPlainText, Group, co } from "../index.js";
import {
  createUnloadedCoValue,
  CoValueLoadingState,
  exportCoValue,
  importContentPieces,
  Inaccessible,
} from "../internal.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";

const server = setupServer();

const ignoreWasmRequests = (request: Request, print: any) => {
  if (request.url.includes("application/wasm")) {
    return;
  }
  print.warning();
};

beforeAll(() =>
  server.listen({
    onUnhandledRequest: ignoreWasmRequests,
  }),
);
afterEach(() => server.resetHandlers());
afterEach(() => vi.restoreAllMocks());
afterAll(() => server.close());

async function setupAccounts() {
  const me = await createJazzTestAccount();
  const worker = await createJazzTestAccount();

  const workerPieces = await exportCoValue(Account, worker.$jazz.id, {
    loadAs: worker,
  });

  importContentPieces(workerPieces ?? [], me);

  return { me, worker };
}

describe("experimental_defineRequest", () => {
  describe("full request/response cycle", () => {
    it("should accept the CoMap init as the request payload and as response callback return value", async () => {
      const { me, worker } = await setupAccounts();

      const group = Group.create(me);
      group.addMember("everyone", "writer");

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
          age: z.number(),
        },
        response: {
          bio: z.string(),
          avatar: z.string().optional(),
        },
      });

      let receivedUser: unknown;
      let receivedMadeBy: unknown;
      let requestOwner: Group;

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          try {
            return await userRequest.handle(
              request,
              worker,
              async (user, madeBy) => {
                receivedUser = user.toJSON();
                requestOwner = user.$jazz.owner;
                receivedMadeBy = madeBy.$jazz.id;

                // Return a plain object (CoMapInit) instead of a CoMap instance
                return {
                  bio: `Profile for ${user.name}`,
                  avatar: `https://example.com/avatars/${user.email}.jpg`,
                };
              },
            );
          } catch (error) {
            console.error(error);
            throw error;
          }
        }),
      );

      // Send a plain object (CoMapInit) instead of a CoMap instance
      const response = await userRequest.send(
        {
          name: "John Doe",
          email: "john@example.com",
          age: 30,
        },
        { owner: me },
      );

      // Verify the response is a proper CoMap instance
      expect(response.bio).toEqual("Profile for John Doe");
      expect(response.avatar).toEqual(
        "https://example.com/avatars/john@example.com.jpg",
      );

      expect(
        requestOwner!.members.map((m) => [m.account.$jazz.id, m.role]),
      ).toEqual([
        [me.$jazz.id, "admin"],
        [worker.$jazz.id, "writer"],
      ]);

      expect(
        response.$jazz.owner.members.map((m) => [m.account.$jazz.id, m.role]),
      ).toEqual([
        [worker.$jazz.id, "admin"],
        [me.$jazz.id, "reader"],
      ]);

      // Verify the server received the correct data
      expect(receivedUser).toMatchObject({
        name: "John Doe",
        email: "john@example.com",
        age: 30,
      });
      expect(receivedMadeBy).toEqual(me.$jazz.id);
    });

    it("should push the response content directly to the client", async () => {
      const { me, worker } = await setupAccounts();

      const group = Group.create(me);
      group.addMember("everyone", "writer");

      const Address = co.map({
        street: co.plainText(),
        city: co.plainText(),
      });

      const Person = co.map({
        name: z.string(),
        address: Address,
      });

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
          age: z.number(),
        },
        response: {
          schema: {
            person: Person,
          },
          resolve: {
            person: {
              address: {
                street: true,
                city: true,
              },
            },
          },
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          try {
            return await userRequest.handle(
              request,
              worker,
              async (user, madeBy) => {
                const group = Group.create(me);
                group.addMember(madeBy, "writer");

                const person = Person.create(
                  {
                    name: user.name,
                    address: Address.create(
                      {
                        street: CoPlainText.create("123 Main St", group),
                        city: CoPlainText.create("New York", group),
                      },
                      group,
                    ),
                  },
                  group,
                );

                return {
                  person,
                };
              },
            );
          } catch (error) {
            console.error(error);
            throw error;
          }
        }),
      );

      // Send a plain object (CoMapInit) instead of a CoMap instance
      const response = await userRequest.send(
        {
          name: "John Doe",
          email: "john@example.com",
          age: 30,
        },
        { owner: me },
      );

      // Verify the response is a proper CoMap instance
      expect(response.person.name).toEqual("John Doe");
      expect(response.person.address.street.toString()).toEqual("123 Main St");
      expect(response.person.address.city.toString()).toEqual("New York");
    });
  });

  it("should handle errors on child covalues gracefully", async () => {
    const { me, worker } = await setupAccounts();

    await linkAccounts(me, worker);

    const Address = co.map({
      street: co.plainText(),
      city: co.plainText(),
    });

    const Person = co.map({
      name: z.string(),
      address: Address,
    });

    const privateToWorker = Group.create(worker);
    const privateToMe = Group.create(me);
    const publicGroup = Group.create(me).makePublic();
    const address = Address.create(
      {
        street: CoPlainText.create("123 Main St", privateToWorker),
        city: CoPlainText.create("New York", privateToMe),
      },
      publicGroup,
    );
    const person = Person.create(
      {
        name: "John",
        address,
      },
      publicGroup,
    );

    const personRequest = experimental_defineRequest({
      url: "https://api.example.com/api/person",
      workerId: worker.$jazz.id,
      request: {
        schema: {
          person: Person,
        },
        resolve: { person: { address: { street: true } } },
      },
      response: {
        schema: {
          person: Person,
        },
        resolve: { person: { address: { street: true, city: true } } },
      },
    });

    server.use(
      http.post("https://api.example.com/api/person", async ({ request }) => {
        return personRequest.handle(
          request,
          worker,
          async ({ person }, madeBy) => {
            person.address.street.$jazz.owner.addMember(madeBy, "reader");

            // The request should handle the error gracefully when trying to resolve
            // child covalues that the worker doesn't have access to
            return { person };
          },
        );
      }),
    );

    // Send the request - this should not throw even though the worker
    // doesn't have access to the address's child covalues
    const response = await personRequest.send({ person }, { owner: me });

    // Verify the response is still a proper Person instance
    expect(response.person.name).toEqual("John");
    expect(response.person.address.street.toString()).toBe("123 Main St");
    expect(response.person.address.city.toString()).toBe("New York");
  });

  it("should accept void responses", async () => {
    const { me, worker } = await setupAccounts();

    const group = Group.create(me);
    group.addMember("everyone", "writer");

    const userRequest = experimental_defineRequest({
      url: "https://api.example.com/api/user",
      workerId: worker.$jazz.id,
      request: {
        name: z.string(),
        email: z.string(),
        age: z.number(),
      },
      response: {},
    });

    let receivedUser: unknown;
    let receivedMadeBy: unknown;

    server.use(
      http.post("https://api.example.com/api/user", async ({ request }) => {
        try {
          return await userRequest.handle(
            request,
            worker,
            async (user, madeBy) => {
              receivedUser = user.toJSON();
              receivedMadeBy = madeBy.$jazz.id;
            },
          );
        } catch (error) {
          console.error(error);
          throw error;
        }
      }),
    );

    // Send a plain object (CoMapInit) instead of a CoMap instance
    await userRequest.send(
      {
        name: "John Doe",
        email: "john@example.com",
        age: 30,
      },
      { owner: me },
    );

    // Verify the server received the correct data
    expect(receivedUser).toMatchObject({
      name: "John Doe",
      email: "john@example.com",
      age: 30,
    });
    expect(receivedMadeBy).toEqual(me.$jazz.id);
  });

  it("should accept group as workerId", async () => {
    const { me, worker } = await setupAccounts();

    await linkAccounts(me, worker);

    // Create a group that will act as the worker
    const workerGroup = Group.create(worker);

    const userRequest = experimental_defineRequest({
      url: "https://api.example.com/api/user",
      workerId: workerGroup.$jazz.id, // Use group ID instead of account ID
      request: {
        name: z.string(),
        email: z.string(),
        age: z.number(),
      },
      response: {
        bio: z.string(),
        avatar: z.string().optional(),
      },
    });

    let receivedUser: unknown;
    let receivedMadeBy: unknown;

    server.use(
      http.post("https://api.example.com/api/user", async ({ request }) => {
        try {
          return await userRequest.handle(
            request,
            worker, // The worker account handles the request
            async (user, madeBy) => {
              receivedUser = user.toJSON();
              receivedMadeBy = madeBy.$jazz.id;

              return {
                bio: `Profile for ${user.name}`,
                avatar: `https://example.com/avatars/${user.email}.jpg`,
              };
            },
          );
        } catch (error) {
          console.error(error);
          throw error;
        }
      }),
    );

    // Send a request - this should work with group as workerId
    const response = await userRequest.send(
      {
        name: "John Doe",
        email: "john@example.com",
        age: 30,
      },
      { owner: me },
    );

    // Verify the response is a proper CoMap instance
    expect(response.bio).toEqual("Profile for John Doe");
    expect(response.avatar).toEqual(
      "https://example.com/avatars/john@example.com.jpg",
    );

    // Verify the response owner structure - should include the worker account
    expect(
      response.$jazz.owner.members.map((m) => [m.account.$jazz.id, m.role]),
    ).toEqual([
      [worker.$jazz.id, "admin"],
      [me.$jazz.id, "reader"],
    ]);

    // Verify the server received the correct data
    expect(receivedUser).toMatchObject({
      name: "John Doe",
      email: "john@example.com",
      age: 30,
    });
    expect(receivedMadeBy).toEqual(me.$jazz.id);
  });
});

describe("JazzRequestError handling", () => {
  describe("System-defined errors in request.ts", () => {
    it("should throw error when request payload is invalid", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          return userRequest.handle(request, worker, async (user, madeBy) => {
            return { bio: "test" };
          });
        }),
      );

      // Mock fetch to return invalid JSON
      const originalFetch = global.fetch;
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ invalid: "payload" }),
      });

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 400,
          "details": [ZodError: [
          {
            "code": "invalid_value",
            "values": [
              "success"
            ],
            "path": [
              "type"
            ],
            "message": "Invalid input: expected \\"success\\""
          }
        ]],
          "message": "Response payload is not valid",
        }
      `);

      global.fetch = originalFetch;
    });

    it("should throw error when request payload is already handled", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          // Mock to make it possible to call json() twice
          const body = await request.json();
          vi.spyOn(request, "json").mockResolvedValue(body);

          // First call should succeed
          await userRequest.handle(request, worker, async () => {
            return { bio: "test" };
          });

          // Second call with same ID should fail
          return userRequest.handle(request, worker, async () => {
            return { bio: "test" };
          });
        }),
      );

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 400,
          "details": undefined,
          "message": "Request payload is already handled",
        }
      `);
    });

    it("should throw error when authentication token is expired", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          const body = await request.json();

          assert(typeof body === "object");
          assert(body);

          body.createdAt = Date.now() - 1000 * 70; // 70 seconds ago
          vi.spyOn(request, "json").mockResolvedValue(body);

          return userRequest.handle(request, worker, async () => {
            return { bio: "test" };
          });
        }),
      );

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 401,
          "details": undefined,
          "message": "Authentication token is expired",
        }
      `);
    });

    it("should throw error when signature is invalid", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          const body = await request.json();

          assert(typeof body === "object");
          assert(body);

          body.authToken = "signature_zinvalid";
          vi.spyOn(request, "json").mockResolvedValue(body);

          return userRequest.handle(request, worker, async () => {
            return { bio: "test" };
          });
        }),
      );

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 401,
          "details": undefined,
          "message": "Invalid signature",
        }
      `);
    });

    it("should throw error when creator account not found", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          vi.spyOn(Account, "load").mockResolvedValue(
            createUnloadedCoValue(
              "some-covalue-id",
              CoValueLoadingState.UNAVAILABLE,
            ) as Inaccessible<Account>,
          );

          return userRequest.handle(request, worker, async () => {
            return { bio: "test" };
          });
        }),
      );

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 400,
          "details": undefined,
          "message": "Creator account not found",
        }
      `);

      vi.restoreAllMocks();
    });

    it("should throw error when there are not enough permissions to resolve the request payload", async () => {
      const { me, worker } = await setupAccounts();

      // Link the accounts to ensure that the request payload is loaded
      await linkAccounts(me, worker);

      const User = co.map({
        name: z.string(),
        email: z.string(),
      });

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          schema: {
            user: User,
          },
          resolve: {
            user: true,
          },
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          return userRequest.handle(request, worker, async (user, madeBy) => {
            return { bio: "test" };
          });
        }),
      );

      await expect(
        userRequest.send(
          {
            user: User.create(
              {
                name: "John Doe",
                email: "john@example.com",
              },
              me,
            ),
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 400,
          "details": undefined,
          "message": "Value not found",
        }
      `);

      vi.restoreAllMocks();
    });

    it("should throw error when the request payload is not found", async () => {
      const { me, worker } = await setupAccounts();

      const User = co.map({
        name: z.string(),
        email: z.string(),
      });

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          schema: {
            user: User,
          },
          resolve: {
            user: true,
          },
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          return userRequest.handle(request, worker, async (user, madeBy) => {
            return { bio: "test" };
          });
        }),
      );

      const group = Group.create(me);
      group.makePublic();

      const user = User.create(
        {
          name: "John Doe",
          email: "john@example.com",
        },
        group,
      );

      await expect(
        userRequest.send(
          {
            user,
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 400,
          "details": undefined,
          "message": "Value not found",
        }
      `);

      vi.restoreAllMocks();
    });

    it("should throw error when the server returns a non-200 status code", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          return new Response("Request failed", { status: 500 });
        }),
      );

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 500,
          "details": undefined,
          "message": "Request failed",
        }
      `);
    });

    it("should throw error when HTTP request fails", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.close();

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toThrow("fetch failed");

      server.listen({
        onUnhandledRequest: ignoreWasmRequests,
      });
    });
  });

  describe("User-defined errors from examples", () => {
    it("should handle user-defined errors", async () => {
      const { me, worker } = await setupAccounts();

      const userRequest = experimental_defineRequest({
        url: "https://api.example.com/api/user",
        workerId: worker.$jazz.id,
        request: {
          name: z.string(),
          email: z.string(),
        },
        response: {
          bio: z.string(),
        },
      });

      server.use(
        http.post("https://api.example.com/api/user", async ({ request }) => {
          return userRequest.handle(request, worker, async (user, madeBy) => {
            throw new JazzRequestError("Custom server error", 400, {
              detail: "Some details",
            });
          });
        }),
      );

      await expect(
        userRequest.send(
          {
            name: "John Doe",
            email: "john@example.com",
          },
          { owner: me },
        ),
      ).rejects.toMatchInlineSnapshot(`
        {
          "code": 400,
          "details": {
            "detail": "Some details",
          },
          "message": "Custom server error",
        }
      `);
    });
  });

  describe("JazzRequestError class", () => {
    it("should create JazzRequestError with correct properties", () => {
      const error = new JazzRequestError("Test error", 400, { detail: "test" });

      expect(error.message).toBe("Test error");
      expect(error.code).toBe(400);
      expect(error.details).toEqual({ detail: "test" });
      expect(error.isJazzRequestError).toBe(true);
    });

    it("should serialize to JSON correctly", () => {
      const error = new JazzRequestError("Test error", 400, { detail: "test" });
      const json = error.toJSON();

      expect(json).toEqual({
        message: "Test error",
        code: 400,
        details: { detail: "test" },
      });
    });

    it("should be identified by isJazzRequestError function", () => {
      const error = new JazzRequestError("Test error", 400);
      const regularError = new Error("Regular error");

      expect(isJazzRequestError(error)).toBe(true);
      expect(isJazzRequestError(regularError)).toBe(false);
      expect(isJazzRequestError({ isJazzRequestError: true })).toBe(true);
      expect(isJazzRequestError(null)).toBe(false);
    });
  });
});
