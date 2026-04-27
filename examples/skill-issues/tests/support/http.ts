import type { Express } from "express";

export async function requestJson(
  app: Express,
  method: string,
  path: string,
  body?: unknown,
): Promise<{ statusCode: number; body: unknown }> {
  const server = app.listen(0);

  try {
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("Could not determine test server address.");
    }

    const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
      method,
      headers: {
        "content-type": "application/json",
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });

    return {
      statusCode: response.status,
      body: await response.json(),
    };
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }
}
