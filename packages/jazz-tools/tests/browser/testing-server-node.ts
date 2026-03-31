import { TestingServer } from "jazz-napi";

interface StartedTestingServer {
  server: TestingServer;
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

let testingServerPromise: Promise<StartedTestingServer> | null = null;

async function startTestingServer(): Promise<StartedTestingServer> {
  const server = await TestingServer.start();
  return {
    server,
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret,
  };
}

async function getOrStartTestingServer(): Promise<StartedTestingServer> {
  if (!testingServerPromise) {
    testingServerPromise = startTestingServer().catch((error) => {
      testingServerPromise = null;
      throw error;
    });
  }

  return testingServerPromise;
}

export async function testingServerInfo(): Promise<{
  appId: string;
  serverUrl: string;
  adminSecret: string;
}> {
  const { appId, serverUrl, adminSecret } = await getOrStartTestingServer();
  return { appId, serverUrl, adminSecret };
}

export async function testingServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
): Promise<string> {
  const { server } = await getOrStartTestingServer();
  return server.jwtForUser(userId, claims);
}

export async function stopTestingServer(): Promise<void> {
  const runningServer = testingServerPromise;
  testingServerPromise = null;

  if (!runningServer) {
    return;
  }

  try {
    const { server } = await runningServer;
    await server.stop();
  } catch {
    // Swallow all errors: either startup never produced a server (nothing to stop),
    // or stop() itself failed (nothing recoverable during teardown).
  }
}
