import { describe, expect, it, suite } from "vitest";
import { getFrameworkSpecificDocsUrl } from "../src/utils.js";
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
