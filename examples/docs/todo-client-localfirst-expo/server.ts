// The app's backend: a small Node server that runs Jazz (createJazz) and
// serves sync under /jazz. The app has no sign-in, so every device is a
// local-first guest. Run it with `pnpm backend`, then point the app at it
// with EXPO_PUBLIC_JAZZ_SERVER_URL (see the README).
import { createServer } from "node:http";
import { acceptLocalFirstAuth, createJazz, sqlite } from "jazz-tools/backend";
import { app } from "./schema";
import permissions from "./permissions";

async function main() {
  const port = Number(process.env.PORT ?? 1625);
  const jazz = createJazz({
    app,
    permissions,
    store: sqlite({ path: ".jazz" }),
    // No sign-in: every device is a guest, so let guests sync.
    authenticate: acceptLocalFirstAuth,
  });
  const server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await jazz.attach(server);
  // Listen on every interface so an emulator or a device on your network reaches it.
  server.listen(port, "0.0.0.0", () =>
    console.log(`Jazz on http://localhost:${port}${jazz.basePath}`),
  );
}

void main();
