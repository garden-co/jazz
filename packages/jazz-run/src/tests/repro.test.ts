import { expect, test } from "vitest";
import { repro1, repro1Deps } from "./repro1";
import { repro2, repro2Deps } from "./repro2";
import { repro3, repro3Deps } from "./repro3";
import { repro4, repro4Deps } from "./repro4";
import { startWorker } from "jazz-tools/worker";
import { emptyKnownState } from "cojson";

export function knownStateFromContent(content: any) {
  const knownState = emptyKnownState(content.id);
  knownState.header = Boolean(content.header);

  for (const [sessionID, session] of Object.entries(content.new)) {
    knownState.sessions[sessionID as any] =
      session.after + session.newTransactions.length;
  }

  return knownState;
}

test("repro1", async () => {
  const { worker } = await startWorker({
    accountID: "co_zFysfHGNxGE4MCZ4uJ2rfmXWACv",
    accountSecret:
      "sealerSecret_zvL8vxFWeJ1jroSg438XnSgVZFuEfgTZQ5haKdz1g4jM/signerSecret_z2UebSG8X5sPXa2pWaUvBHXKJu7R1AiWorZB2s9MQEgn3",
  });

  for (const msg of repro1Deps) {
    worker.$jazz.localNode.syncManager.handleNewContent(msg as any, "import");
  }

  worker.$jazz.localNode.syncManager.handleNewContent(
    repro1.msg as any,
    "import",
  );

  await worker.$jazz.localNode.load(repro1.msg.id as any);
  expect(
    worker.$jazz.localNode.getCoValue(repro1.msg.id as any).knownState(),
  ).toEqual(knownStateFromContent(repro1.msg));
});

// test("repro2", async () => {
//     const client = setupTestNode();

//     for (const msg of repro2Deps) {
//         client.node.syncManager.handleNewContent(msg, "import");
//     }

//     client.node.syncManager.handleNewContent(repro2.msg, "import");
//     expect(client.node.getCoValue(repro2.msg.id).knownState()).toEqual(knownStateFromContent(repro2.msg));
// });

// test.skip("repro3", async () => {
//     const client = setupTestNode();

//     for (const msg of repro3Deps) {
//         client.node.syncManager.handleNewContent(msg, "import");
//     }

//     client.node.syncManager.handleNewContent(repro3.msg, "import");
// });

// test("repro4", async () => {
//     const client = setupTestNode();

//     for (const msg of repro4Deps) {
//         client.node.syncManager.handleNewContent(msg, "import");
//     }

//     client.node.syncManager.handleNewContent(repro4.msg, "import");
// });

// test("repro5", async () => {
//     const client = setupTestNode();
//     const hash = {
//         "hash": "hash_zFNbMbmJwCJFptNpTcWVPMDKC9WnsJPr8ZaRyP56uA7qV",
//         "id": "co_zJf2JaWR3Qw12MfqdJHo4WiJm3",
//         "sessionID": "co_zFdU3Ftg3FVE7YC3vEJFbhxwYZa_session_zF1ohMmNWs23",
//         "signerID": "signer_z4H3bLup1CzuVqyWH7TUqwWWbsZYtxXSSu1N3ujGwWiVw",
//         "newSignature": "signature_zMeMJDY5LUauqwfnKCZNjh3UhpY59CKPbLpdivWh35AkiaiHaMtP55NHSuSJcbRGLgJcBTQfA1osM1HvPCgV1oWX"
//     }

//     const result = client.node.crypto.verify(hash.newSignature, hash.hash, hash.signerID);
//     expect(result).toBe(true);
// });

// test("repro6", async () => {
//     const client = setupTestNode();
//     const hash = {
//         "hash": "hash_zAFCVR2crgMo8notS2NBXrFGsXtdjKA3Kww4VuWcGnHaF",
//         "id": "co_zJf2JaWR3Qw12MfqdJHo4WiJm3",
//         "sessionID": "co_zFdU3Ftg3FVE7YC3vEJFbhxwYZa_session_zF1ohMmNWs23",
//         "signerID": "signer_z4H3bLup1CzuVqyWH7TUqwWWbsZYtxXSSu1N3ujGwWiVw",
//         "newSignature": "signature_z62WwpeNyqersowRnvYD4tntLcoPUys8gENYRhZR3ifwBr5oP1k7Rut5FUouZajG2b9BSgDMwJhhh2CMc33Vixwrn"
//     }

//     const result = client.node.crypto.verify(hash.newSignature, hash.hash, hash.signerID);
//     expect(result).toBe(true);
// });

// test("repro7", async () => {
//     const client = setupTestNode();
//     const hash = {
//         "hash": "hash_z4Sa6g8ZWcftnTCwrFQTecpGTxziUReEazq1p8Ufhgrup",
//         "id": "co_zhJhLv1CLrDGhiqSgp33P5BC8wC",
//         "sessionID": "co_zQHdjFivigkt3drWwa8JRrcCBKL_session_zA52iaLZm9rg",
//         "signerID": "signer_z5NTHV8TG8GnxsQKkJerpdB9TZw8AEsS5MrTBHQriPH5E",
//         "newSignature": "signature_z4U2BVA1bQYsEse4qjW4opryFH22ehgtczEtphqGou34gnUmqTHTcNsVCqrLjr7HFJzzNeSApYK8gNoqmENsyUVwe"
//     }

//     const result = client.node.crypto.verify(hash.newSignature, hash.hash, hash.signerID);
//     expect(result).toBe(true);
// });
