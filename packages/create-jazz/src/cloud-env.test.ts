import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { writeHostedEnv } from "./cloud-env.js";

const HOSTED_KEYS = [
  "NEXT_PUBLIC_JAZZ_APP_ID",
  "NEXT_PUBLIC_JAZZ_SERVER_URL",
  "JAZZ_ADMIN_SECRET",
  "BACKEND_SECRET",
] as const;

const TODO_COMMENT = "# TODO: provision at https://v2.dashboard.jazz.tools";

let dir: string;
let warnSpy: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "cloud-env-test-"));
  warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
});

afterEach(() => {
  warnSpy.mockRestore();
});

function readEnv(d: string): string {
  return readFileSync(join(d, ".env"), "utf8");
}

describe("writeHostedEnv", () => {
  describe("slice 1: fresh dir, all values supplied", () => {
    it("writes all four keys with supplied values and no TODO comment", () => {
      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "app_abc",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
          JAZZ_ADMIN_SECRET: "admin_secret",
          BACKEND_SECRET: "backend_secret",
        },
      });

      const content = readEnv(dir);
      expect(content).toContain("NEXT_PUBLIC_JAZZ_APP_ID=app_abc");
      expect(content).toContain("NEXT_PUBLIC_JAZZ_SERVER_URL=https://jazz.example.com");
      expect(content).toContain("JAZZ_ADMIN_SECRET=admin_secret");
      expect(content).toContain("BACKEND_SECRET=backend_secret");
      expect(content).not.toContain(TODO_COMMENT);
      expect(content.endsWith("\n")).toBe(true);
    });
  });

  describe("slice 2: fresh dir, no values supplied", () => {
    it("writes all four keys with empty values and the TODO comment at the top", () => {
      writeHostedEnv({ dir, values: {} });

      const content = readEnv(dir);
      const lines = content.split("\n");
      expect(lines[0]).toBe(TODO_COMMENT);
      for (const key of HOSTED_KEYS) {
        expect(content).toContain(`${key}=`);
      }
      expect(content.endsWith("\n")).toBe(true);
    });
  });

  describe("slice 3: pre-existing file with all four keys filled in", () => {
    it("preserves existing values when supplied values differ", () => {
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=original_app\n" +
        "NEXT_PUBLIC_JAZZ_SERVER_URL=https://original.example.com\n" +
        "JAZZ_ADMIN_SECRET=original_admin\n" +
        "BACKEND_SECRET=original_backend\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "new_app",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://new.example.com",
          JAZZ_ADMIN_SECRET: "new_admin",
          BACKEND_SECRET: "new_backend",
        },
      });

      const content = readEnv(dir);
      expect(content).toContain("NEXT_PUBLIC_JAZZ_APP_ID=original_app");
      expect(content).toContain("NEXT_PUBLIC_JAZZ_SERVER_URL=https://original.example.com");
      expect(content).toContain("JAZZ_ADMIN_SECRET=original_admin");
      expect(content).toContain("BACKEND_SECRET=original_backend");
      expect(content).not.toContain("new_app");
    });
  });

  describe("slice 4: pre-existing file with two of the four keys filled in", () => {
    it("preserves existing two, appends missing two, adds TODO if any added value is empty", () => {
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=existing_app\n" + "JAZZ_ADMIN_SECRET=existing_admin\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "new_app",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
          JAZZ_ADMIN_SECRET: "new_admin",
          BACKEND_SECRET: "backend_secret",
        },
      });

      const content = readEnv(dir);
      expect(content).toContain("NEXT_PUBLIC_JAZZ_APP_ID=existing_app");
      expect(content).toContain("JAZZ_ADMIN_SECRET=existing_admin");
      expect(content).toContain("NEXT_PUBLIC_JAZZ_SERVER_URL=https://jazz.example.com");
      expect(content).toContain("BACKEND_SECRET=backend_secret");
      expect(content).not.toContain(TODO_COMMENT);
    });

    it("adds TODO comment when an appended value ends up empty", () => {
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=existing_app\n" + "JAZZ_ADMIN_SECRET=existing_admin\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "new_app",
          JAZZ_ADMIN_SECRET: "new_admin",
          // NEXT_PUBLIC_JAZZ_SERVER_URL and BACKEND_SECRET not supplied
        },
      });

      const content = readEnv(dir);
      expect(content).toContain(TODO_COMMENT);
      expect(content).toContain("NEXT_PUBLIC_JAZZ_SERVER_URL=");
      expect(content).toContain("BACKEND_SECRET=");
    });
  });

  describe("slice 5: pre-existing file with unrelated keys", () => {
    it("preserves unrelated keys and appends the four hosted keys", () => {
      const existing = "BETTER_AUTH_SECRET=abc123\nOTHER=value\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "app_abc",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
          JAZZ_ADMIN_SECRET: "admin_secret",
          BACKEND_SECRET: "backend_secret",
        },
      });

      const content = readEnv(dir);
      const lines = content.split("\n").filter((l) => l.length > 0);
      expect(lines[0]).toBe("BETTER_AUTH_SECRET=abc123");
      expect(lines[1]).toBe("OTHER=value");
      for (const key of HOSTED_KEYS) {
        expect(content).toContain(`${key}=`);
      }
      expect(content).not.toContain(TODO_COMMENT);
    });
  });

  describe("slice 11: empty-value placeholders treated as present (P2.2)", () => {
    it("preserves empty placeholders and emits console.warn when all four keys are present but empty", () => {
      const placeholder =
        TODO_COMMENT +
        "\n" +
        "NEXT_PUBLIC_JAZZ_APP_ID=\n" +
        "NEXT_PUBLIC_JAZZ_SERVER_URL=\n" +
        "JAZZ_ADMIN_SECRET=\n" +
        "BACKEND_SECRET=\n";
      writeFileSync(join(dir, ".env"), placeholder);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "app_real",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
          JAZZ_ADMIN_SECRET: "admin_real",
          BACKEND_SECRET: "backend_real",
        },
      });

      const content = readEnv(dir);
      expect(content).toBe(placeholder);

      expect(warnSpy).toHaveBeenCalledOnce();
      const warnArg: string = warnSpy.mock.calls[0][0];
      expect(warnArg).toContain("NEXT_PUBLIC_JAZZ_APP_ID");
      expect(warnArg).toContain("NEXT_PUBLIC_JAZZ_SERVER_URL");
      expect(warnArg).toContain("JAZZ_ADMIN_SECRET");
      expect(warnArg).toContain("BACKEND_SECRET");
    });
  });

  describe("slice 10: warn when skipping keys already present in file", () => {
    it("emits console.warn naming skipped keys when supplied values differ from existing ones", () => {
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=original\n" +
        "NEXT_PUBLIC_JAZZ_SERVER_URL=https://original.example.com\n" +
        "JAZZ_ADMIN_SECRET=original_admin\n" +
        "BACKEND_SECRET=original_backend\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "new_app",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://new.example.com",
          JAZZ_ADMIN_SECRET: "new_admin",
          BACKEND_SECRET: "new_backend",
        },
      });

      expect(warnSpy).toHaveBeenCalledOnce();
      const warnArg: string = warnSpy.mock.calls[0][0];
      expect(warnArg).toContain("NEXT_PUBLIC_JAZZ_APP_ID");
      expect(warnArg).toContain("NEXT_PUBLIC_JAZZ_SERVER_URL");
      expect(warnArg).toContain("JAZZ_ADMIN_SECRET");
      expect(warnArg).toContain("BACKEND_SECRET");
      expect(warnArg).toContain(".env");
    });

    it("does not warn when supplied values are empty", () => {
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=original\n" +
        "NEXT_PUBLIC_JAZZ_SERVER_URL=https://original.example.com\n" +
        "JAZZ_ADMIN_SECRET=original_admin\n" +
        "BACKEND_SECRET=original_backend\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({ dir, values: {} });

      expect(warnSpy).not.toHaveBeenCalled();
    });

    it("does not warn when supplied value matches the existing value", () => {
      const existing = "NEXT_PUBLIC_JAZZ_APP_ID=same_value\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "same_value",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
          JAZZ_ADMIN_SECRET: "admin_secret",
          BACKEND_SECRET: "backend_secret",
        },
      });

      expect(warnSpy).not.toHaveBeenCalled();
    });
  });

  describe("slice 9: TODO comment position with unrelated keys present", () => {
    it("places TODO comment between existing content and the additions block, not at file top", () => {
      const existing = "BETTER_AUTH_SECRET=abc123\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({ dir, values: {} });

      const content = readEnv(dir);
      const lines = content.split("\n").filter((l) => l.length > 0);
      expect(lines[0]).toBe("BETTER_AUTH_SECRET=abc123");
      const commentIndex = lines.indexOf(TODO_COMMENT);
      expect(commentIndex).toBeGreaterThan(0);
      const firstHostedKeyIndex = lines.findIndex((l) => HOSTED_KEYS.some((k) => l.startsWith(k)));
      expect(commentIndex).toBeLessThan(firstHostedKeyIndex);
    });
  });

  describe("slice 8: CRLF input normalisation", () => {
    it("parses CRLF files without trailing CR on values", () => {
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=existing_app\r\n" + "JAZZ_ADMIN_SECRET=existing_admin\r\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
          BACKEND_SECRET: "backend_secret",
        },
      });

      const content = readEnv(dir);
      // Existing values must be preserved without CR
      expect(content).toContain("NEXT_PUBLIC_JAZZ_APP_ID=existing_app");
      expect(content).not.toContain("existing_app\r");
      // New keys appended
      expect(content).toContain("NEXT_PUBLIC_JAZZ_SERVER_URL=https://jazz.example.com");
      expect(content).toContain("BACKEND_SECRET=backend_secret");
      // Output must end with LF only
      expect(content.endsWith("\n")).toBe(true);
      expect(content.includes("\r")).toBe(false);
    });

    it("treats CRLF empty values (KEY=\\r\\n) as present, not missing", () => {
      const existing =
        "NEXT_PUBLIC_JAZZ_APP_ID=\r\n" +
        "NEXT_PUBLIC_JAZZ_SERVER_URL=\r\n" +
        "JAZZ_ADMIN_SECRET=\r\n" +
        "BACKEND_SECRET=\r\n";
      writeFileSync(join(dir, ".env"), existing);

      writeHostedEnv({
        dir,
        values: {
          NEXT_PUBLIC_JAZZ_APP_ID: "should_be_ignored",
          NEXT_PUBLIC_JAZZ_SERVER_URL: "should_be_ignored",
          JAZZ_ADMIN_SECRET: "should_be_ignored",
          BACKEND_SECRET: "should_be_ignored",
        },
      });

      const content = readEnv(dir);
      expect(content).not.toContain("should_be_ignored");
    });
  });

  describe("slice 7: newline injection in values", () => {
    it("throws when a value contains a newline character", () => {
      expect(() =>
        writeHostedEnv({
          dir,
          values: {
            NEXT_PUBLIC_JAZZ_APP_ID: "app_abc\nINJECTED=evil",
            NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
            JAZZ_ADMIN_SECRET: "admin_secret",
            BACKEND_SECRET: "backend_secret",
          },
        }),
      ).toThrow(
        "Refusing to write hosted env: value for NEXT_PUBLIC_JAZZ_APP_ID contains an illegal newline character.",
      );
    });

    it("throws when a value contains a carriage return", () => {
      expect(() =>
        writeHostedEnv({
          dir,
          values: {
            NEXT_PUBLIC_JAZZ_APP_ID: "app_abc",
            NEXT_PUBLIC_JAZZ_SERVER_URL: "https://jazz.example.com",
            JAZZ_ADMIN_SECRET: "admin_secret\rinjected",
            BACKEND_SECRET: "backend_secret",
          },
        }),
      ).toThrow(
        "Refusing to write hosted env: value for JAZZ_ADMIN_SECRET contains an illegal newline character.",
      );
    });
  });

  describe("slice 6: TODO comment exact text and position", () => {
    it("comment is exactly the expected string and appears before all key lines", () => {
      writeHostedEnv({ dir, values: {} });

      const content = readEnv(dir);
      const lines = content.split("\n");
      const commentIndex = lines.indexOf(TODO_COMMENT);
      expect(commentIndex).toBe(0);

      const firstKeyIndex = lines.findIndex((l) => HOSTED_KEYS.some((k) => l.startsWith(k)));
      expect(commentIndex).toBeLessThan(firstKeyIndex);
    });
  });
});
