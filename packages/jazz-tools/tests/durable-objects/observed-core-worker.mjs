// Expose constructor identity only in the fixture: a successful write through
// a still-resident core cannot establish hibernation.
import { createJazzCore, routeJazzRequest } from "jazz-tools/durable-object";

export class JazzCore extends createJazzCore({ blobs: "sqlite" }) {
  generation = crypto.randomUUID();

  acceptHibernatingWebSocket(ws) {
    this.ctx.acceptWebSocket(ws, [crypto.randomUUID()]);
  }

  webSocketMessage(ws, data) {
    this.firstEvent ??= this.ctx.getTags(ws)[0];
    return super.webSocketMessage(ws, data);
  }

  async fetch(request) {
    this.firstEvent ??= new URL(request.url).pathname;
    if (new URL(request.url).pathname === "/test/sync-frame") {
      const [ws] = this.ctx.getWebSockets(new URL(request.url).searchParams.get("id"));
      await super.webSocketMessage(ws, await request.arrayBuffer());
      return new Response(null, { status: 204 });
    }
    if (new URL(request.url).pathname === "/test/upload-receipt") {
      const receipts = this.ctx.storage.sql.exec("SELECT 1 FROM _jazz_lv_receipt LIMIT 1");
      return Response.json(receipts.toArray().length > 0);
    }
    if (new URL(request.url).pathname === "/test/drop-output") {
      const ids = await request.json();
      this.dropped = Object.fromEntries(ids.map((id) => [id, 0]));
      for (const id of ids) {
        const [ws] = this.ctx.getWebSockets(id);
        if (!ws) throw new Error("missing test socket");
        ws.send = () => {
          this.dropped[id]++;
        };
      }
      return new Response(null, { status: 204 });
    }
    if (new URL(request.url).pathname === "/test/shutdown") {
      await this.ctx.storage.sync();
      this.ctx.abort("test shutdown after durable commit");
    }
    if (new URL(request.url).pathname === "/test/sockets") {
      return Response.json({
        generation: this.generation,
        firstEvent: this.firstEvent,
        dropped: this.dropped,
        ids: this.ctx
          .getWebSockets()
          .flatMap((ws) => this.ctx.getTags(ws))
          .sort(),
        attachments: Object.fromEntries(
          this.ctx
            .getWebSockets()
            .map((ws) => [this.ctx.getTags(ws)[0], Array.from(ws.deserializeAttachment() ?? [])]),
        ),
        admitted: [...this.sockets.values()].flatMap((ws) => this.ctx.getTags(ws)).sort(),
      });
    }
    if (new URL(request.url).pathname === "/test/generation") {
      return new Response(this.generation);
    }
    return super.fetch(request);
  }
}

export default { fetch: routeJazzRequest };
