import { describe, expect, it, suite } from "vitest";
import { AGENTS_MD_URL, getFrameworkSpecificDocsUrl } from "../src/utils.js";
import { frameworks } from "../src/config.js";

describe("Framework specific docs fetch", () => {
  suite("should only generate urls for frameworks which exist", () => {
    const llmsTxtFrameworks = [
      "react",
      "svelte",
      "react-native",
      "react-native-expo",
      "vanilla",
    ];
    const acceptableUrls = llmsTxtFrameworks.map(
      (f) => `https://jazz.tools/${f}/llms-full.txt`,
    );

    for (const framework of frameworks) {
      it("should generate a valid URL for " + framework.value, () => {
        const frameworkDocsUrl = getFrameworkSpecificDocsUrl(framework.value);
        expect(frameworkDocsUrl).toBeDefined();
        expect(acceptableUrls.includes(frameworkDocsUrl)).toBeTruthy();
      });
    }
  });
});

describe("AGENTS.md fetch", () => {
  it("should point to the correct URL", () => {
    expect(AGENTS_MD_URL).toBe("https://jazz.tools/AGENTS.md");
  });

  it("should be a valid HTTPS URL", () => {
    const url = new URL(AGENTS_MD_URL);
    expect(url.protocol).toBe("https:");
    expect(url.hostname).toBe("jazz.tools");
    expect(url.pathname).toBe("/AGENTS.md");
  });

  it.skipIf(!process.env.TEST_LIVE_URLS)(
    "should be fetchable and return markdown content",
    async () => {
      const response = await fetch(AGENTS_MD_URL);
      expect(response.ok).toBe(true);

      const content = await response.text();
      expect(content).toContain("# AGENTS.md");
      expect(content).toContain("Jazz");
    },
  );

  it.skipIf(!process.env.TEST_LIVE_URLS)(
    "should contain skills listing",
    async () => {
      const response = await fetch(AGENTS_MD_URL);
      const content = await response.text();

      expect(content).toContain(".skills/jazz-schema-design/SKILL.md");
      expect(content).toContain(".skills/jazz-performance/SKILL.md");
      expect(content).toContain(".skills/jazz-permissions-security/SKILL.md");
      expect(content).toContain(".skills/jazz-testing/SKILL.md");
      expect(content).toContain(".skills/jazz-ui-development/SKILL.md");
    },
  );

  it.skipIf(!process.env.TEST_LIVE_URLS)(
    "should contain docs index with URL-based root",
    async () => {
      const response = await fetch(AGENTS_MD_URL);
      const content = await response.text();

      expect(content).toContain("root:https://jazz.tools/docs");
      expect(content).not.toContain("root:homepage/homepage/public/docs");
    },
  );
});
