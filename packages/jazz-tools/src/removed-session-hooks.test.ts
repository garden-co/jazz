import { describe, expect, it } from "vitest";
import * as react from "./react/index.js";
import * as reactCore from "./react-core/index.js";
import * as vue from "./vue/index.js";

// useSession() (Svelte getSession()) was deprecated in alpha.66 and removed in
// beta.0: the session's user is useJazzSession().user (Svelte getJazzSession()).
describe("permission-session hooks removed in beta.0", () => {
  it("are no longer exported", () => {
    for (const [entry, mod] of [
      ["jazz-tools/react", react],
      ["jazz-tools/react-core", reactCore],
      ["jazz-tools/vue", vue],
    ] as const) {
      expect("useSession" in mod, entry).toBe(false);
      expect("useJazzSession" in mod, entry).toBe(true);
    }
  });
});
