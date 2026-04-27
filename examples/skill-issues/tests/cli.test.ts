import { chmod, mkdir, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { configPath } from "../src/config.js";
import { runCli } from "../src/cli.js";

describe("skill issues CLI", () => {
  it("initializes local-first auth config", async () => {
    const home = await mkdtemp(join(tmpdir(), "skill-issues-cli-"));

    const result = await runCli(["auth", "init"], {
      cwd: home,
      env: {
        SKILL_ISSUES_APP_ID: "app-id",
        SKILL_ISSUES_SERVER_URL: "https://cloud.example",
      },
    });

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Initialized skill issues auth");

    const config = JSON.parse(
      await readFile(join(home, ".skill-issues", "config.json"), "utf8"),
    ) as unknown;

    expect(config).toMatchObject({
      appId: "app-id",
      serverUrl: "https://cloud.example",
    });
    expect(config).toHaveProperty("localFirstSecret");
    expect(typeof (config as { localFirstSecret?: unknown }).localFirstSecret).toBe("string");
  });

  it("rejects unknown commands", async () => {
    const home = await mkdtemp(join(tmpdir(), "skill-issues-cli-"));

    const result = await runCli(["wat"], { cwd: home, env: {} });

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("Unknown command: wat");
  });

  it("restricts an existing config file to owner-only permissions", async () => {
    const home = await mkdtemp(join(tmpdir(), "skill-issues-cli-"));
    const path = configPath(home);
    await mkdir(join(home, ".skill-issues"), { recursive: true });
    await writeFile(path, "{}\n");
    await chmod(path, 0o644);

    const result = await runCli(["auth", "init"], {
      cwd: home,
      env: {
        SKILL_ISSUES_APP_ID: "app-id",
        SKILL_ISSUES_SERVER_URL: "https://cloud.example",
      },
    });

    expect(result.exitCode).toBe(0);
    expect((await stat(path)).mode & 0o777).toBe(0o600);
  });

  it("completes GitHub auth with a verifier service", async () => {
    const home = await mkdtemp(join(tmpdir(), "skill-issues-cli-"));
    await mkdir(join(home, ".skill-issues"), { recursive: true });
    await writeFile(
      configPath(home),
      JSON.stringify({
        appId: "app-id",
        serverUrl: "https://cloud.example",
        localFirstSecret: "local-first-secret",
      }),
    );

    const calls: string[] = [];
    const stdoutWrites: string[] = [];
    const completions: Array<{ verifierUrl: string; deviceCode: string; jazzProof: string }> = [];
    const result = await runCli(
      ["auth", "github", "--verifier-url", "https://verifier.example"],
      {
        cwd: home,
        env: {
          GITHUB_CLIENT_ID: "github-client-id",
        },
        writeStdout(text) {
          calls.push("write");
          stdoutWrites.push(text);
        },
      },
      {
        async startDeviceAuthorization(clientId) {
          calls.push("start");
          expect(clientId).toBe("github-client-id");
          return {
            device_code: "device-code-123",
            user_code: "ABCD-1234",
            verification_uri: "https://github.com/login/device",
            interval: 5,
          };
        },
        async waitForGitHubAuthorization(device) {
          calls.push("wait");
          expect(device.device_code).toBe("device-code-123");
        },
        createLocalFirstProof(secret) {
          expect(secret).toBe("local-first-secret");
          return "proof-token";
        },
        async completeGitHubVerification(verifierUrl, payload) {
          calls.push("complete");
          completions.push({ verifierUrl, ...payload });
          return {
            id: "alice-jazz-id",
            githubLogin: "alice",
          };
        },
      },
    );

    expect(result.exitCode).toBe(0);
    expect(stdoutWrites).toEqual([
      "Open https://github.com/login/device and enter code ABCD-1234.\n",
    ]);
    expect(result.stdout).toBe("Verified GitHub user alice.\n");
    expect(result.stdout).not.toContain("https://github.com/login/device");
    expect(calls).toEqual(["start", "write", "wait", "complete"]);
    expect(completions).toEqual([
      {
        verifierUrl: "https://verifier.example",
        deviceCode: "device-code-123",
        jazzProof: "proof-token",
      },
    ]);
  });
});
