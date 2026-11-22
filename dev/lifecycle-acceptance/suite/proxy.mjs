// What stands between a client and the server: a TCP proxy that can be cut.
//   up:     passes bytes through
//   refuse: nothing listens on the port, as when the server's host is down (ECONNREFUSED)
//   hole:   takes connections and bytes and never answers, as a dead network does
import { randomInt } from "node:crypto";
import { createConnection, createServer } from "node:net";

// Below Linux's ephemeral range (32768-60999). A port from it can be taken while the proxy refuses:
// by any outgoing connection, or by the client itself (a connect to a dead local port can connect
// to itself), and then up() couldn't listen again.
const [LOWEST, HIGHEST] = [10_000, 32_768];
const reserved = new Set();

export class Proxy {
  #server;
  #sockets = new Set();
  #target;
  mode = "up";
  port = 0;
  forwarded = 0;

  static async start(targetPort) {
    const proxy = new Proxy();
    proxy.#target = targetPort;
    for (let attempt = 1; ; attempt++) {
      proxy.port = randomInt(LOWEST, HIGHEST);
      if (reserved.has(proxy.port)) continue;
      reserved.add(proxy.port);
      try {
        await proxy.#listen();
        return proxy;
      } catch (error) {
        reserved.delete(proxy.port);
        if (error.code !== "EADDRINUSE" || attempt === 50) throw error;
      }
    }
  }

  #listen() {
    this.#server = createServer((socket) => this.#connection(socket));
    return new Promise((resolve, reject) => {
      this.#server.once("error", reject);
      this.#server.listen(this.port, "127.0.0.1", () => resolve());
    });
  }

  #track(socket) {
    this.#sockets.add(socket);
    socket.on("error", () => socket.destroy());
    socket.on("close", () => this.#sockets.delete(socket));
  }

  #connection(socket) {
    this.#track(socket);
    if (this.mode === "refuse") return socket.destroy();
    if (this.mode === "hole") return socket.resume();
    const upstream = createConnection(this.#target, "127.0.0.1");
    this.#track(upstream);
    for (const [from, to] of [[socket, upstream], [upstream, socket]]) {
      from.on("data", (bytes) => {
        if (this.mode !== "up") return;
        this.forwarded += bytes.length;
        to.write(bytes);
      });
      from.on("close", () => {
        // In a black hole a connection's end doesn't arrive either.
        if (this.mode !== "hole") to.destroy();
      });
    }
  }

  async #stopListening() {
    if (!this.#server?.listening) return;
    const closed = new Promise((resolve) => this.#server.close(resolve));
    for (const socket of this.#sockets) socket.destroy();
    await closed;
  }

  get url() {
    return `http://127.0.0.1:${this.port}/jazz`;
  }

  async refuse() {
    this.mode = "refuse";
    await this.#stopListening();
  }

  async hole() {
    if (!this.#server.listening) await this.#listen();
    this.mode = "hole";
  }

  async up() {
    // Connections from the outage are dead: the client makes new ones.
    for (const socket of this.#sockets) socket.destroy();
    this.mode = "up";
    if (!this.#server.listening) await this.#listen();
  }

  async stop() {
    this.mode = "refuse";
    await this.#stopListening();
    reserved.delete(this.port);
  }
}
