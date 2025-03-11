// @vitest-environment happy-dom

import {
  TestJSCrypto,
  createJazzTestAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { CloudAuth } from "..";

const crypto = await TestJSCrypto.create();

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("JazzCloudAuth", () => {
  describe("Key sharding", () => {
    it("should return the pre-split keys after merging", () => {
      const accountSecret = crypto.newRandomAgentSecret();
      const [signer0, signer1] = CloudAuth.splitSignerSecret(
        accountSecret,
        crypto,
      );
      const [sealer0, sealer1] = CloudAuth.splitSealerSecret(
        accountSecret,
        crypto,
      );
      const signerSecret = CloudAuth.mergeSignerSecret(
        signer0,
        signer1,
        crypto,
      );
      const sealerSecret = CloudAuth.mergeSealerSecret(
        sealer0,
        sealer1,
        crypto,
      );
      expect(signerSecret).toEqual(crypto.getAgentSignerSecret(accountSecret));
      expect(sealerSecret).toEqual(crypto.getAgentSealerSecret(accountSecret));
    });
  });
});
