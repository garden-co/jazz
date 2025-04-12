# 1.0. Introduction
This README contains my findings while building a microbenchmark to explore the efficiency of WebSockets versus HTTP1.1/2/3 + SSE as specified in [this research task](https://github.com/garden-co/jazz/issues/301).

## 1.1. Overview
Out of the total of 9 different web server + protocol combos, only 6 web server + protocol combos were feasible.

Below is a snapsot of web server + protocol combos that were built:

| # | Server      | Layer 7 | Layer 6 | Layer 4 | Built | Status |
|----|------------|--------|------------|--------|--------|--------|
| A1 | Node.js    | WebSocket (WS) |  TLSv1.3 (Optional) | TCP | ✔️ | **Complete** |
| A2 |     | HTTP/1.1 + Server-Sent Events (SSE) |  TLSv1.3 (Optional) | TCP | ✔️ | **Complete** |
| A3 |     | HTTP/2 + SSE |  TLSv1.3 (Optional) | TCP | ✔️ | **Complete** |
| A4 |     | HTTP/3 + SSE |  TLSv1.3 (Mandatory) | UDP (QUIC) | ❌ | **N/A** |
| B1 | uWebSockets.js | WebSocket |  TLSv1.3 (Optional) | TCP | ✔️ | **Complete** |
| B2 |     | HTTP/1 + SSE |  TLSv1.3 (Optional) | TCP | ✔️ | **Complete** |
| B3 |     | HTTP/2 + SSE |  TLSv1.3 (Optional) | TCP | ❌ | **N/A** |
| B4 |     | HTTP/3 + SSE |  TLSv1.3 (Mandatory) | UDP (QUIC) | ❌ | **N/A** |
| C1 | Node.js + Caddy    | HTTP/3 + SSE |  TLSv1.3 (Mandatory) | UDP (QUIC) | ✔️ | **Complete** | 

## 1.2. Server + Protocol Combos
As can be seen in the table above, 6 different web servers + protocol combos were built for the benchmarks:
1. Node.js (WS) code-named `node-ws`;
2. Node.js (HTTP/1.1 + SSE) code-named `node-http1`;
3. Node.js (HTTP/2 + SSE) code-named `node-http2`;
4. uWebSockets.js (WS) code-named `uws-ws`;
5. uWebSockets.js (HTTP/1.1 + SSE) code-named `uws-http1`;
6. Caddy + Node.js (HTTP/3 + SSE) code-named `caddy-http3`.

## 1.3. Server Memory Limit
On the development machine I used, which has 16GB of RAM installed, the memory limit for the `node` process defaults to 4GB. 

For the benchmarks proper, each web server is started with a higher memory limit of 8GB. This is set in `package.json` via `--max-old-space-size=8192` for all 6 web servers, but if running on a machine with higher installed memory, you can raise it as needed.

## 1.4. Server Test Data 
I used [faker.js](https://v7.fakerjs.dev/) to generate test data served by each web server. 

To keep things fast, the generated test data are not stored in a database. Instead, everything is served from memory, with the exception of binary files used to simulate binary CoValue downloads and uploads. These are stored on disk at: `public/downloads` and `public/uploads`.

Each web server listens on https://localhost:3000/ and serves a default `index` page from the file `public/client/{ws|http}/index.html`, depending on whether the server's transport is based on websockets or plain HTTP + SSE. 

The default `index` page hosts all JavaScript code that will be driven by [`playwright`](https://playwright.dev/) and [`artillery`](https://www.artillery.io/) load tests.

The default page includes a minified version of `faker.js` which is also used to create test data in the browser. The only downside is this [caveat](https://v7.fakerjs.dev/guide/usage.html#browser) from the `faker.js` documentation:
> Using the browser is great for experimenting 👍. However, due to all of the strings Faker uses to generate fake data, Faker is a large package. It's > 5 MiB minified. Please avoid deploying the full Faker in your web app.

## 1.5. Server Network Conditions
With respect to simulating various network conditions (I, II, III, IV), I explored:

OS-level throttling using third-party tools like:
- [Network Link Conditioner for MacOS](https://apple.stackexchange.com/questions/24066/how-to-simulate-slow-internet-connections-on-the-mac)
- [NetLimiter for Windows](https://www.netlimiter.com/)

Or doing the throttling at the browser-level using:
- [Chrome DevTools](https://developer.chrome.com/docs/devtools/network#throttle) 

I eventually came across Shopify's [`toxiproxy`](https://github.com/Shopify/toxiproxy) which has first-class support for programmatic use and would have been perfect for our needs. Unfortunately, `toxiproxy` was designed to [proxy TCP traffic](https://github.com/Shopify/toxiproxy?tab=readme-ov-file#2-populating-toxiproxy), meaning it would work for TCP-based protocols like HTTP/1.1 and HTTP/2, but not for HTTP/3, which is based on UDP (QUIC), so I had to abandon the idea.

I was able to achieve the desired OS-level programmatic throttling using a combination of macOS' `pfctl` (packet filter control) and `dnctl` (dummynet control) CLI tools. Note that some programmatic invocations of `pfctl` and `dnctl` by scripts used in the benchmarks will require sudo privileges.

The only downside is that the benchmarks would need to be adapted to use `netem` (network emulator) and `tc` (traffic control), if there's a need to execute it on a production-class machine running Linux.

## 1.6. Summary
This is a high-level summary of the different components that make up the microbenchmark.

The next chapter will take a deeper look at some of the decisions that went into the design of the web servers and highlight a couple of implementation notes.

---

# 2.0. Server Design
> If you're doing back pressure, the 'right way' depends on whether your system is 'open' or 'closed'.

—[Marc Brooker](https://x.com/MarcJBrooker/status/1910429156367814732)

## 2.0.1. Head-of-Line Blocking (HOLB)
The most demanding aspect of the benchmarks are the requirements involving:
* simulating bulk _download_ of (binary) CoValues that lead to HOLB;
* simulating bulk _upload_ of (binary) CoValues that lead to HOLB;

because either case will produce a lot of network I/O for HOLB to show up as network congestion in the benchmarks i.e. extremely high server response times, which in the benchmark report is recorded as high latency.

To avoid the common benchmarking mistake where each target is exercised differently, I abstracted all logic related to the handling of binary CoValues (i.e. streaming downloads and uploads of 50MB binary files in 100KB chunks) into a single class named `FileStreamManager`. 

This class is used internally by all 6 web servers and can be found in the file `src/node-js/filestream-manager.ts`.

## 2.1. Node.js
### 2.1.1. WS
This server uses the blazing fast [`ws`](https://github.com/websockets/ws) WebSocket library. As per the [docs](https://github.com/websockets/ws?tab=readme-ov-file#opt-in-for-performance), the optional [`bufferutil`](https://github.com/websockets/bufferutil) binary addon was installed alongside the `ws` module to improve the performance of certain masking and unmasking operations.

The code can be found in `src/node-js/websocket.ts` and it is used for only 1 protocol combo:
* WS with TLS (used by `node-ws`).

### 2.1.2. HTTP/1.1 + SSE
#### Express
For the web servers that use HTTP + SSE as their transport, I initially wanted to use [`lib/http.js`](https://nodejs.org/api/http.html) directly, as the spec suggested. It quickly became clear I would have to handle lots of boring stuff like serving static files, file-upload and other minutiae by myself, and I'd have to repeat the same code for HTTP/1.1 and HTTP/2, so I ditched the idea and switched to [Express](https://github.com/expressjs/express) so I could iterate quickly.

Express doesn't have [first-class support](https://github.com/expressjs/express/issues/5462) for HTTP/2 yet, but HTTP/2 can still be had using [SPDY](https://github.com/expressjs/express/issues/5462#issuecomment-1936586792). 

The original Express code is in `src/node-js/http-sse-express.ts` and it handled both HTTP/1.1 with TLS and HTTP/2 with TLS.

#### Fastify
[Fastify](https://github.com/fastify/fastify) claims to be [significantly faster](https://github.com/fastify/fastify?tab=readme-ov-file#benchmarks) than Express so once the first HTTP + SSE server was feature-complete, I converted the Express middleware code to Fastify using an LLM, after multiple attempts[^1].

After the conversion, the only changes that were needed were due to differences between how Express and Fastify handled protocol errors. 

Even though I was successful in combining the `spdy` [npm package](https://www.npmjs.com/package/spdy) with Express, the resulting HTTP/2 implementation didn't raise protocol errors when connection-specific header fields like [`Connection: keep-alive`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Connection), [`Keep-Alive: <value>`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Keep-Alive) or [`Transfer-Encoding: chunked`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Transfer-Encoding) are accidentally included by a developer. 

Those headers are valid in HTTP/1.1 but invalid in HTTP/2, but it was only Fastify that flagged their presence as protocol errors.

The Fastify code can be found in `src/node-js/http-sse-fastify.ts` and it is used to bootstrap 3 different protocol combos: 
* HTTP/1.1 with TLS + SSE (used by `node-http1`);
* HTTP/2 with TLS + SSE (used by `node-http2`); 
* HTTP/1.1 without TLS + SSE (used by `caddy-http3`)[^2];

## 2.2. µWebSockets.js (µWS.js)
### 2.2.1. WS
Out of all 6 web servers, this server was the most challenging to get right. By far.

#### No Free Lunch
First, the µWS project touts it high performance credentials and it is able to do this because it offers far more knobs for tuning, compared to other WebSocket libraries.

For instance, if µWS.js receives a payload from a client whose size exceeds the default value of [`maxPayloadLength`](https://unetworking.github.io/uWebSockets.js/generated/interfaces/WebSocketBehavior.html#maxPayloadLength), the connection is immediately closed. 

Currently, `maxPayloadLength` defaults to `16 * 1024` i.e. `16KB`, but I didn't realize this until I started experiencing WebSocket connection closures. These unexplained connection closures didn't come up at all when I first tested the code against the `ws` WebSocket library. 

In retrospect, the type of head-scratching was bound to occur because the default is lower than the limit needed for these benchmarks i.e. download and upload of binary CoValues are expected to happen in chunks of `100KB`.

Second, the project's backpressure [example](https://github.com/uNetworking/uWebSockets.js/blob/master/examples/Backpressure.js) is too basic to be representative of how backpressure may arise in the real-world, or how it may be handled, without ugly hacks.

So, it is not suprising that all major LLMs struggle with generating realistic µWS code compared to other libraries. I had to do a lot of trial and error to avoid several foot guns that came up during development.

The code can be found in `src/uws-js/websocket.ts` and it is used for only 1 protocol combo:
* WS with TLS (used by `uws-ws`).

### 2.2.2. HTTP/1.1 + SSE
To eke out the most performance in the µWS.js server (HTTP/1.1 + SSE) code-named `uws-http1`, I used its [corking](https://github.com/uNetworking/uWebSockets/blob/d437169851a21785c7c3beaaeb5cb83c88665230/misc/READMORE.md#corking) mechanism. Corking, when used correctly, allows µWS.js to efficiently combine multiple individual syscalls that invoke `send()` into a single network syscall to `send()`.

The code can be found in `src/uws-js/http1-sse.ts` and it is used for only 1 protocol combo:
* HTTP/1.1 with TLS + SSE (used by `uws-http1`).

---

# 3.0. Running the Code
```bash
cd /tmp
git clone https://github.com/ayewo/jazz/ -b single-threaded-server-benchmarks --single-branch benchmarks
cd /tmp/benchmarks/

# 1. setup local SSL certs
brew install mkcert
mkdir -p /tmp/benchmarks/experiments/servers/cert
cd /tmp/benchmarks/experiments/servers/cert
mkcert localhost 127.0.0.1
mkcert -install

# 2. put test files in the paths expected by playwright
mkdir -p /tmp/benchmarks/experiments/servers/tests/fixtures
cd /tmp/benchmarks/experiments/servers/tests/fixtures
dd if=/dev/urandom of=sample_file.bin bs=1m count=50
zip binary-sample.zip sample_file.bin

cd /tmp/benchmarks/experiments/servers
mkdir -p public/downloads
mkdir -p public/uploads
cp tests/fixtures/binary-sample.zip public/downloads/sample.zip

# 3. install Chromium for playwright tests
pnpm playwright install chromium

# 4. start each web server on port 3000, one at a time, then each structured text and binary benchmark
pnpm clean
pnpm bench
```

--- 

# 4.0. Footnotes
[^1]:  I was successful after 4 attempts with 3 different models. The Express-to-Fastify conversion was done using a zero-shot prompt against: 
(a) Claude Sonnet 3.5, 
(b) DeepSeek, 
(c) Claude Sonnet 3.5 again, before switching to 
(d) Google Gemini (on February 17, 2025). 
Gemini's conversion was the closest, stylistically and semantically, to the original Express code. It also had the least amount of compilation errors from the TypeScript compiler, but I still had to refine it using Claude and some manual digging to provide type hints to the 3 different calls to `Fastify()` that create HTTP/2 with TLS, HTTP/1.1 with TLS and HTTP/1.1 without TLS.

[^2]: In this protocol combo, Fastify is on HTTP/1.1 behind Caddy on HTTP/3. Caddy handles the termination of all TLS connections.
