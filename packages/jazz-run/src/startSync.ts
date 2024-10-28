/* istanbul ignore file -- @preserve */
import { Command, Options } from "@effect/cli";
import {
    ControlledAgent,
    DisconnectedError,
    LocalNode,
    Peer,
    PingTimeoutError,
    SyncMessage,
    WasmCrypto,
    cojsonInternals,
} from "cojson";
import { WebSocketServer } from "ws";
import { createServer } from "http";

import { createWebSocketPeer } from "cojson-transport-ws";
import { Effect } from "effect";
import { SQLiteStorage } from "cojson-storage-sqlite";
import { dirname } from "node:path";
import { mkdir } from "node:fs/promises";

const port = Options.text("port")
    .pipe(Options.withAlias("p"))
    .pipe(
        Options.withDescription(
            "Select a different port for the WebSocket server. Default is 4200",
        ),
    )
    .pipe(Options.withDefault("4200"));

const inMemory = Options.boolean("in-memory").pipe(
    Options.withDescription("Use an in-memory storage instead of file-based"),
);

const flaky = Options.boolean("flaky").pipe(
    Options.withDescription("Randomly drop messages on the WS connection"),
);

const db = Options.file("db")
    .pipe(
        Options.withDescription(
            "The path to the file where to store the data. Default is 'sync-db/storage.db'",
        ),
    )
    .pipe(Options.withDefault("sync-db/storage.db"));

export const startSync = Command.make(
    "sync",
    { port, inMemory, db, flaky },
    ({ port, inMemory, db, flaky }) => {
        return Effect.gen(function* () {
            const crypto = yield* Effect.promise(() => WasmCrypto.create());

            const server = createServer((req, res) => {
                if (req.url === "/health") {
                    res.writeHead(200);
                    res.end("ok");
                }
            });
            const wss = new WebSocketServer({ noServer: true });

            console.log("COJSON sync server listening on port " + port);

            const agentSecret = crypto.newRandomAgentSecret();
            const agentID = crypto.getAgentID(agentSecret);

            const localNode = new LocalNode(
                new ControlledAgent(agentSecret, crypto),
                crypto.newRandomSessionID(agentID),
                crypto,
            );

            if (!inMemory) {
                yield* Effect.promise(() =>
                    mkdir(dirname(db), { recursive: true }),
                );

                const storage = yield* Effect.promise(() =>
                    SQLiteStorage.asPeer({ filename: db }),
                );

                localNode.syncManager.addPeer(storage);
            }

            wss.on("connection", function connection(ws, req) {
                // ping/pong for the connection liveness
                const pinging = setInterval(() => {
                    ws.send(
                        JSON.stringify({
                            type: "ping",
                            time: Date.now(),
                            dc: "unknown",
                        }),
                    );
                }, 1500);

                ws.on("close", () => {
                    clearInterval(pinging);
                });

                const clientAddress =
                    (req.headers["x-forwarded-for"] as string | undefined)
                        ?.split(",")[0]
                        ?.trim() || req.socket.remoteAddress;

                const clientId = clientAddress + "@" + new Date().toISOString();

                let peer = createWebSocketPeer({
                    id: clientId,
                    role: "client",
                    websocket: ws,
                    expectPings: false,
                    batchingByDefault: false,
                });

                if (flaky) {
                    peer = makeTheWsPeerFlaky(peer);
                }

                localNode.syncManager.addPeer(peer);

                ws.on("error", (e) =>
                    console.error(`Error on connection ${clientId}:`, e),
                );
            });

            server.on("upgrade", function upgrade(req, socket, head) {
                if (req.url !== "/health") {
                    wss.handleUpgrade(req, socket, head, function done(ws) {
                        wss.emit("connection", ws, req);
                    });
                }
            });

            server.listen(parseInt(port));

            // Keep the server up
            yield* Effect.never;
        });
    },
);

function makeTheWsPeerFlaky(peer: Peer) {
    const incoming = new cojsonInternals.Channel<
        SyncMessage | DisconnectedError | PingTimeoutError
    >();

    async function handleIncoming() {
        for await (const msg of peer.incoming) {
            if (Math.random() > 0.8) {
                void incoming.push(msg);
            }
        }
    }

    void handleIncoming();

    return {
        ...peer,
        incoming,
    };
}
