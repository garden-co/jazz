import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";
import { FastifyHttpOptions, FastifyHttpsOptions, FastifyHttp2Options, FastifyHttp2SecureOptions } from "fastify";
import * as http from "http";
import * as https from "https";
import * as http2 from "http2";
import path from "path";
import {
    CoValue,
    MutationEvent,
    covalues,
    events,
    addCoValue,
    updateCoValue,
    updateCoValueBinary,
    BenchmarkStore,
    RequestTimer,
    shutdown,
    PORT,
    FastifyResponseWrapper,
} from "../util";
import logger from "../util/logger";
import { tlsCert } from "../util/tls";
import { FileStreamManager, UploadBody } from "./filestream-manager";

const fileManager = new FileStreamManager();

interface Client {
    userAgentId: string;
    res: FastifyReply;
}
let clients: Client[] = [];
let exportFileName: string;
let isHttp2Server = false;

function broadcast(uuid: string): void {
    const event = events.get(uuid) as MutationEvent;
    const { type, ...data } = event;
    logger.debug(
        `[Broadcast to ${clients.length} clients] Mutation event of type: '${type}' was found for: ${uuid}.`,
    );

    clients.forEach((client) => {
        client.res.raw.write(`event: ${type}\n`);
        client.res.raw.write(`data: ${JSON.stringify(data)}\n\n`);
        // (client.res.raw as ServerResponse).flush?.(); // For HTTP/2
    });
}


interface CoValueParams {
    uuid: string;
}

interface AllQuery {
    all?: string;
}

async function routes(fastify: FastifyInstance, options = {}) {

    fastify.register(import('@fastify/static'), {
        root: path.join(__dirname, '../../public/client/http'),
        prefix: '/',
    });

    fastify.register(import('@fastify/static'), {
        root: path.join(__dirname, "../../node_modules/@faker-js/faker/dist/esm"),
        prefix: '/faker',
        decorateReply: false
    })

    // Allow testing from plain HTTP/1.1 (without SSL)
    fastify.addHook('preHandler', (request, reply, done) => {
        reply.header('Access-Control-Allow-Origin', 'http://localhost:3001'); // Allow requests from http://localhost:3001
        reply.header('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
        reply.header('Access-Control-Allow-Credentials', 'true'); // Allow cookies
        if (request.method === 'OPTIONS') {
            reply.code(200).send();
            return;
        }
        done();
    });

    const benchmarkStore = new BenchmarkStore();
    fastify.addHook('preHandler', (request, reply, done) => {
        if ((request.method === 'GET' && request.url.startsWith("/covalue/") && request.url.indexOf("/subscribe") === -1)
            || request.method === 'POST' || request.method === 'PATCH') {
            const timer = new RequestTimer(benchmarkStore.requestId());

            reply.raw.on('close', () => {
                timer.method(request.method).path(request.url).status(reply.statusCode).end();
                benchmarkStore.addRequestLog(timer.toRequestLog());
            });
        }

        done();
    });


    fastify.get<{ Querystring: AllQuery }>("/covalue", async (request: FastifyRequest<{ Querystring: AllQuery }>, reply: FastifyReply) => {
        const { all } = request.query;

        if (all) {
            return Object.values(covalues);
        } else {
            return Object.keys(covalues);
        }
    });

    fastify.get<{ Params: CoValueParams }>("/covalue/:uuid", async (request: FastifyRequest<{ Params: CoValueParams }>, reply: FastifyReply) => {
        const { uuid } = request.params;

        const covalue = covalues[uuid];
        if (!covalue) {
            return reply.status(404).send({ m: "CoValue not found" });
        }

        return covalue;
    });

    fastify.get<{ Params: CoValueParams }>("/covalue/:uuid/binary", async (request: FastifyRequest<{ Params: CoValueParams }>, reply: FastifyReply) => {
        const { uuid } = request.params;

        const covalue: CoValue = covalues[uuid];
        if (!covalue) {
            return reply.status(404).send({ m: "CoValue not found" });
        }

        const filePath = covalue.url?.path as string;
        if (!filePath) {
            return reply.status(404).send({ m: "CoValue binary file not found" });
        }

        await fileManager.chunkFileDownload(
            {
                filePath,
                range: request.headers.range,
            },
            {
                type: "http",
                res: reply.raw,
            },
        );
    });

    fastify.post<{ Body: CoValue }>("/covalue", async (request: FastifyRequest<{ Body: CoValue }>, reply: FastifyReply) => {
        const covalue = request.body;

        if (!covalue) {
            return reply.status(400).send({ m: "CoValue cannot be blank" });
        }

        addCoValue(covalues, covalue);
        reply.status(201).send({ m: "OK" });
    });

    fastify.post<{ Body: UploadBody }>("/covalue/binary", async (request: FastifyRequest<{ Body: UploadBody }>, reply: FastifyReply) => {
        const payload = request.body;
        await fileManager.chunkFileUpload(payload, new FastifyResponseWrapper(reply));
    });

    fastify.patch<{ Params: CoValueParams, Body: Partial<CoValue> }>("/covalue/:uuid", async (request: FastifyRequest<{ Params: CoValueParams, Body: Partial<CoValue> }>, reply: FastifyReply) => {
        const { uuid } = request.params;
        const partialCovalue = request.body;

        const covalue = covalues[uuid];
        if (!covalue) {
            return reply.status(404).send({ m: "CoValue not found" });
        }

        updateCoValue(covalue, partialCovalue);

        // broadcast the mutation to subscribers
        broadcast(uuid);
        reply.status(200).send();
    });

    fastify.patch<{ Params: CoValueParams, Body: Partial<CoValue> }>("/covalue/:uuid/binary", async (request: FastifyRequest<{ Params: CoValueParams, Body: Partial<CoValue> }>, reply: FastifyReply) => {
        const { uuid } = request.params;
        const partialCovalue = request.body;

        const covalue = covalues[uuid];
        if (!covalue) {
            return reply.status(404).send({ m: "CoValue not found" });
        }

        updateCoValueBinary(covalue, partialCovalue);

        // broadcast the mutation to subscribers
        broadcast(uuid);
        reply.status(200).send();
    });


    interface SubscribeParams {
        uuid: string;
        ua: string;
    }
    fastify.get<{ Params: SubscribeParams }>("/covalue/:uuid/subscribe/:ua", async (request: FastifyRequest<{ Params: SubscribeParams }>, reply: FastifyReply) => {
        const { uuid, ua } = request.params;
        const headers: Record<string, string> = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
        };

        if (!isHttp2Server) {
            // Only valid for HTTP/1.1 & below. Invalid in HTTP/2 & HTTP/3. See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive
            headers["Connection"] = "keep-alive";
        }
        reply.raw.writeHead(200, headers);

        logger.debug(`[Client-#${ua}] Opening an event stream on: ${uuid}.`);
        const client: Client = {
            userAgentId: ua,
            res: reply,
        };
        clients.push(client);

        reply.raw.on("close", () => {
            logger.debug(`[Client-#${ua}] Closed the event stream for: ${uuid}.`);
            clients = clients.filter((client) => client.userAgentId !== ua);
            reply.raw.end();
        });
    });

    fastify.post("/stop", async (request: FastifyRequest, reply: FastifyReply) => {
        shutdown(new FastifyResponseWrapper(reply), benchmarkStore, exportFileName, async () => {
            if (PORT === "3001") {
                // also shutdown Caddy on TLS port 3000 via the `/stop` endpoint of the admin URL
                const caddyAdminUrl = "http://localhost:2019/stop";
                try {
                    await fetch(`${caddyAdminUrl}`, { method: 'POST' });
                    logger.info("Caddy server shutdown successfully.");
                } catch (error) {
                    logger.error("Error shutting down Caddy server:", error);
                }
            }
        });
    });

}

function createFastifyServer(
  isHttp2: boolean,
  useTLS: boolean
): FastifyInstance {
  let fastify: any;

  if (isHttp2 && useTLS) {
    // HTTP/2 with TLS
    fastify = Fastify({
        logger: false,
        http2: true,
        https: tlsCert,
        bodyLimit: 50 * 1024 * 1024 // 50MB,
    } as FastifyHttp2SecureOptions<http2.Http2SecureServer>);
  } else if (isHttp2) {
    // HTTP/2 without TLS
    fastify = Fastify({
        logger: false,
        http2: true,
        bodyLimit: 50 * 1024 * 1024 // 50MB,
    } as FastifyHttp2Options<http2.Http2Server>);
  } else if (useTLS) {
    // HTTP/1.1 with TLS
    fastify = Fastify({
        logger: false,
        https: tlsCert,
        bodyLimit: 50 * 1024 * 1024 // 50MB,
    } as FastifyHttpsOptions<https.Server>);
  } else {
    // Default: HTTP/1.1 without TLS
    fastify = Fastify({
        logger: false,
        bodyLimit: 50 * 1024 * 1024 // 50MB,
    } as FastifyHttpOptions<http.Server>);
  }

  return fastify;
}

export async function createWebServer(isHttp2: boolean, useTLS: boolean = true) {
    const fastify = createFastifyServer(isHttp2, useTLS);
    fastify.register(routes);

    try {
        if (isHttp2) {
            isHttp2Server = true;
            exportFileName = "A3_NodeServer-HTTP2-SSE.csv";
            await fastify.listen({ port: +PORT, host: '0.0.0.0' });
            logger.info(
                `HTTP/2 + TLSv1.3 Server is running on: https://localhost:${PORT}`,
            );
        } else {
            if (useTLS) {
                exportFileName = "A2_NodeServer-HTTP1-SSE.csv";
                await fastify.listen({ port: +PORT, host: '0.0.0.0' });
                logger.info(
                    `HTTP/1.1 + TLSv1.3 Server is running on: https://localhost:${PORT}`,
                );
            } else {
                exportFileName = "C1_Node+CaddyServer-HTTP3-SSE.csv";
                await fastify.listen({ port: +PORT, host: '0.0.0.0' });
                logger.info(
                    `HTTP/1.1 Server is running on: http://localhost:${PORT}`,
                );
            }
        }

    } catch (err) {
        fastify.log.error(err);
        process.exit(1);
    }
}
