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

  it("is npm's tarball for a published beta", () => {
    expect(defaultCoreTarballUrl("2.0.0-beta.0")).toBe(
      "https://registry.npmjs.org/@garden-co/jazz-napi-linux-x64-gnu/-/jazz-napi-linux-x64-gnu-2.0.0-beta.0.tgz",
    );
  });

  it.each(["2.0.0-beta.1-rc.2", "2.0.0-beta.1.2-rc.0", "2.0.0-alpha.65.66-rc.0"])(
    "is the release asset for the candidate %s",
    (version) => {
      expect(defaultCoreTarballUrl(version)).toBe(
        `https://github.com/garden-co/jazz/releases/download/v${version}/garden-co-jazz-napi-linux-x64-gnu-${version}.tgz`,
      );
    },
  );
});
