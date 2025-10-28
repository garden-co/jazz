import { expect, onTestFinished, test } from "vitest";
import { runTransform } from "../src/transform";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const currentDir = fileURLToPath(new URL(".", import.meta.url));

test("runTransform", () => {
  const fixturesBefore = readFileSync(
    `${currentDir}/fixtures/test-example.ts`,
    "utf-8",
  );
  runTransform(`${currentDir}/fixtures`);

  const result = readFileSync(
    `${currentDir}/fixtures/test-example.ts`,
    "utf-8",
  );

  onTestFinished(() => {
    writeFileSync(`${currentDir}/fixtures/test-example.ts`, fixturesBefore);
  });

  expect(result).toMatchSnapshot();
});
