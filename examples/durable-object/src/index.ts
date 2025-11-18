import "jazz-tools/load-edge-wasm";
import { DurableObject } from "cloudflare:workers";
import { startWorker } from "jazz-tools/worker";
import { getDurableObjectSqlStorage } from "cojson-storage-do-sqlite";
import { co, Group, z } from "jazz-tools";

const KvMap = co.record(z.string(), z.string());

export class KVStore extends DurableObject {
  private jazzConn: Awaited<ReturnType<typeof startWorker>> | null = null;

  private async getJazzConnection() {
    if (this.jazzConn) {
      return this.jazzConn;
    }
    this.jazzConn = await startWorker({
      accountID: this.env.JAZZ_WORKER_ACCOUNT,
      accountSecret: this.env.JAZZ_WORKER_SECRET,
      syncServer: `wss://cloud.jazz.tools/?key=${this.env.JAZZ_API_KEY}`,
      asActiveAccount: false,
      storage: getDurableObjectSqlStorage(this.ctx.storage),
    });
    return this.jazzConn;
  }

  private async getKvMap() {
    return this.ctx.blockConcurrencyWhile(async () => {
      const jazzConn = await this.getJazzConnection();
      const mapId = await this.ctx.storage.get<string>("kvMapId");
      if (mapId) {
        const kvMap = await KvMap.load(mapId, { loadAs: jazzConn.worker });
        if (!kvMap) {
          throw new Error(`Failed to load KvMap with ID ${mapId}`);
        }
        return kvMap;
      }
      const group = Group.create({ owner: jazzConn.worker });
      group.addMember(jazzConn.worker, "admin");
      const kvMap = KvMap.create({}, { owner: group });
      await this.ctx.storage.put("kvMapId", kvMap.$jazz.id);
      return kvMap;
    });
  }

  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    const kvMap = await this.getKvMap();
    if (request.method === "GET") {
      const value = kvMap[path];
      return new Response(value);
    } else if (request.method === "POST") {
      const value = await request.text();
      kvMap.$jazz.set(path, value);
      return new Response("OK");
    } else {
      return new Response("Method not allowed", { status: 405 });
    }
  }
}

export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    if (path === "/") {
      if (request.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
      }
      const id = env.KVStore.newUniqueId();
      return Response.json({
        doId: id.toString(),
      });
    } else {
      if (!(request.method === "POST" || request.method === "GET")) {
        return new Response("Method not allowed", { status: 405 });
      }
      const [_empty, doId, keyId] = path.split("/");
      if (!doId || !keyId) {
        return new Response("Bad request", { status: 400 });
      }
      const id = env.KVStore.idFromString(doId);
      const stub = env.KVStore.get(id);
      url.pathname = `/${keyId}`;
      const newRequest = new Request(url, {
        method: request.method,
        headers: request.headers,
        ...(request.method === "POST" ? { body: request.body } : {}),
      });
      return stub.fetch(newRequest);
    }
  },
} satisfies ExportedHandler<Env>;
