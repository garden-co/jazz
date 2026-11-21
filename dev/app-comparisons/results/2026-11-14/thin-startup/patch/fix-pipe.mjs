// Fix 3 (prototype): pipeline the thin handshake.
// Today: socket open -> prelude + hello -> wait for Welcome -> schema ->
// wait for its answer -> subscribe -> answer: three round trips after the
// socket opens before the first result. Here the schema request goes right
// behind the hello, and live queries right behind the schema, without
// waiting for either answer (the server handles a connection's frames in
// order). One round trip after open. Unanswered or refused, the existing
// paths apply: a schema error is fatal, a lost connection reopens everything.
export function apply({ edit }) {
  // Carrier: frames that may go out before the server's Welcome.
  edit(
    "runtime/native-runtime/websocket.js",
    `            this.socket.send(encodeWebSocketFrameBatch([encodeWireClientHello(this.localFeatures)]));`,
    `            this.socket.send(encodeWebSocketFrameBatch([encodeWireClientHello(this.localFeatures)]));
            this.resolveHelloSent?.();`,
  );
  edit(
    "runtime/native-runtime/websocket.js",
    `    async send(frame) {
        await this.sendBatch([frame]);
    }`,
    `    async send(frame) {
        await this.sendBatch([frame]);
    }
    /** thin-startup fix 3: send right behind the hello, before the Welcome. */
    async sendBatchEarly(frames) {
        this.helloSent ??= new Promise((resolve) => (this.resolveHelloSent = resolve));
        await this.helloSent;
        if (this.closing) return;
        this.socket.send(encodeWebSocketFrameBatch(frames));
    }`,
  );
  // Thin runtime: bind the schema at open; subscribe while it's in flight.
  edit(
    "thin/thin-runtime.js",
    `        this.carrier = carrier;
        void carrier.ready().then((negotiation) => {`,
    `        this.carrier = carrier;
        carrier.helloSent ??= new Promise((resolve) => (carrier.resolveHelloSent = resolve));
        this.pipelined = carrier;
        this.bindSchema(carrier);
        void carrier.ready().then((negotiation) => {`,
  );
  edit(
    "thin/thin-runtime.js",
    `                return;
            }
            this.bindSchema(carrier);
        }, () => {`,
    `                return;
            }
        }, () => {`,
  );
  edit(
    "thin/thin-runtime.js",
    `                for (const live of this.liveQueries.values())
                    if (live.callback)
                        this.openLive(live);`,
    `                this.pipelined = undefined;
                for (const live of this.liveQueries.values())
                    if (live.callback && live.requestId === undefined)
                        this.openLive(live);`,
  );
  edit(
    "thin/thin-runtime.js",
    `        // Parts of one frame go back to back.
        void carrier?.sendBatch(frames);`,
    `        // Parts of one frame go back to back.
        if (carrier && !this.bound && this.pipelined === carrier) void carrier.sendBatchEarly(frames);
        else void carrier?.sendBatch(frames);`,
  );
  edit(
    "thin/thin-runtime.js",
    `        live.callback = onUpdate;
        if (this.bound)
            this.openLive(live);`,
    `        live.callback = onUpdate;
        if (this.bound || (this.carrier && this.pipelined === this.carrier))
            this.openLive(live);`,
  );
}
