/* eslint-disable no-restricted-globals */
let generation = 0;
self.onconnect = (event) => {
  const port = event.ports[0];
  port.onmessage = (msg) => {
    const d = msg.data;
    if (d?.t === "CHECK_CAPABILITY") {
      port.postMessage({ t: "CAPABILITY_RESULT", supported: true });
      return;
    }
    if (d?.t !== "CONNECT") return;
    if (d.protocolVersion !== 1) {
      port.postMessage({ t: "LEADER_FAULT", reason: "version-mismatch" });
      return;
    }
    generation += 1;
    const ch = new MessageChannel();
    ch.port1.onmessage = (inner) => {
      if (inner.data?.type === "follower-sync") {
        ch.port1.postMessage({ type: "leader-sync", payload: inner.data.payload ?? [] });
      }
    };
    ch.port1.start();
    port.postMessage({ t: "PEER_PORT", port: ch.port2, generation }, [ch.port2]);
  };
  port.start();
};
