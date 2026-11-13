import { actor, setup } from "rivetkit";
import { acceptDeviceAccounts, createJazz, rivet } from "jazz-tools/backend";
import { app } from "./schema.js";
import permissions from "./permissions.js";
import { jazzRequest, jazzSocket } from "./rivet-socket.js";

// Rivet owns execution, request routing, and sleep/wake. No separate HTTP server.
export const jazzServer = actor({
  createVars: () => {
    const publicUrl = process.env.JAZZ_PUBLIC_URL;
    if (!publicUrl)
      throw new Error("JAZZ_PUBLIC_URL is required for device-account authentication");
    return {
      jazz: createJazz({
        app,
        permissions,
        // Wake the core in an existing runner pool using deployment credentials.
        store: rivet({ name: "example", pool: "jazz" }),
        basePath: "/jazz",
        publicUrl,
        allowedOrigins: process.env.JAZZ_ALLOWED_ORIGINS?.split(",").filter(Boolean),
        upgradesSockets: true,
        authenticate: acceptDeviceAccounts,
      }),
    };
  },

  // Public /jazz HTTP requests route to this actor through Rivet's gateway.
  onRequest: (c, request) => c.vars.jazz.handle(request),

  // Upgrades on the same public /jazz endpoint route to Rivet's WebSocket handler.
  onWebSocket: (c, websocket) => {
    const request = c.request;
    if (!request) throw new Error("Jazz requires the WebSocket upgrade request");
    const accept = () => {
      c.vars.jazz.acceptSocket(jazzRequest(request), jazzSocket(websocket));
    };
    // Native RivetKit may hand us a socket whose open event already fired.
    if (websocket.readyState === 1) accept();
    else websocket.addEventListener("open", accept);
  },

  // Keep live Jazz sessions in memory; hibernation requires session restoration.
  options: { canHibernateWebSocket: false },
  onSleep: (c) => c.vars.jazz.close(),
  onDestroy: (c) => c.vars.jazz.close(),

  actions: {},
});

export const registry = setup({ use: { jazzServer } });
registry.start();
