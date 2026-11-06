import { describe, expect, it } from "vitest";
import { defaultCoreTarballUrl } from "./vercel-store.js";

describe("vercel() default core tarball", () => {
  it("is npm's tarball for a published version", () => {
    expect(defaultCoreTarballUrl("2.0.0-alpha.60")).toBe(
      "https://registry.npmjs.org/@garden-co/jazz-napi-linux-x64-gnu/-/jazz-napi-linux-x64-gnu-2.0.0-alpha.60.tgz",
    );
  });

  it("is the release asset for a release candidate, which never reaches npm", () => {
    expect(defaultCoreTarballUrl("2.0.0-alpha.60-rc.4")).toBe(
      "https://github.com/garden-co/jazz/releases/download/v2.0.0-alpha.60-rc.4/garden-co-jazz-napi-linux-x64-gnu-2.0.0-alpha.60-rc.4.tgz",
    );
  });
});
