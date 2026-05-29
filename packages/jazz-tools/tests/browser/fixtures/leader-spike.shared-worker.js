// packages/jazz-tools/tests/browser/fixtures/leader-spike.shared-worker.js
/* eslint-disable no-restricted-globals */
self.onconnect = (event) => {
  const tabPort = event.ports[0];
  tabPort.onmessage = (msg) => {
    if (msg.data?.t === "HELLO") {
      const ch = new MessageChannel();
      ch.port1.onmessage = (inner) => {
        if (inner.data?.ping) {
          ch.port1.postMessage({ pong: inner.data.ping });
        }
      };
      ch.port1.start();
      tabPort.postMessage({ t: "PEER_PORT" }, [ch.port2]);
    }
  };
  tabPort.start();
};
