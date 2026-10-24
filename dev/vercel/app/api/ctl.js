// Control endpoint for the spike: shard lifecycle and commands inside the
// shard's sandbox. Guarded by SPIKE_CTL_SECRET; never part of a real app.
import { Drive, Sandbox } from "@vercel/sandbox";
import { drive, ensureCore, startupTimeline } from "../lib/core.js";

const json = (body, status = 200) =>
  new Response(JSON.stringify(body, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });

export async function GET(request) {
  const q = new URL(request.url).searchParams;
  if (!process.env.SPIKE_CTL_SECRET || q.get("secret") !== process.env.SPIKE_CTL_SECRET) {
    return json({ error: "forbidden" }, 403);
  }
  const name = q.get("name") ?? "core1";
  const t0 = performance.now();
  try {
    switch (q.get("op")) {
      case "drive": {
        const d = await drive(q.get("drive"), Number(q.get("gib") ?? 10));
        return json({ drive: d.name, region: d.region, ms: performance.now() - t0 });
      }
      case "mount-build": {
        // Attach the binary drive read-write to the build sandbox for its next session.
        const sbx = await Sandbox.get({ name: "jazz-build", resume: false });
        await sbx.update({
          mounts: { "/opt/jazz": { drive: q.get("drive") ?? "jazz-bin", mode: "read-write" } },
        });
        return json({ ok: true });
      }
      case "up": {
        const r = await ensureCore({
          name,
          data: q.get("data") ?? `${name}-data`,
          vcpus: Number(q.get("vcpus") ?? 1),
          timeoutMs: Number(q.get("timeout_ms") ?? 3_600_000),
          image: q.get("image") ?? undefined,
          shell: q.get("shell") !== "0",
        });
        const profile =
          q.get("profile") === "1" && r.command ? await startupTimeline(r.command) : undefined;
        return json({
          base: r.base,
          ok: r.ok,
          started: r.started,
          session: r.session,
          timings: r.timings,
          epochs: r.epochs,
          profile,
        });
      }
      case "config": {
        const sbx = await Sandbox.get({ name, resume: false });
        await sbx.update({ timeout: Number(q.get("timeout_ms")) });
        return json({ ok: true, timeout: sbx.timeout });
      }
      case "stop": {
        const sbx = await Sandbox.get({ name, resume: false });
        const r = await sbx.stop();
        return json({ ms: performance.now() - t0, status: r?.status, snapshot: r?.snapshot?.id });
      }
      case "status": {
        const sbx = await Sandbox.get({ name, resume: false });
        let session = null;
        try {
          const c = sbx.currentSession();
          session = { id: c.sessionId ?? c.id, status: c.status, timeout: c.timeout };
        } catch {}
        return json({
          status: sbx.status,
          session,
          timeout: sbx.timeout,
          totals: {
            durationMs: sbx.totalDurationMs,
            cpuMs: sbx.totalActiveCpuDurationMs,
            ingress: sbx.totalIngressBytes,
            egress: sbx.totalEgressBytes,
          },
        });
      }
      case "exec": {
        const sbx = await Sandbox.get({ name });
        // vcr=1 hands the command this function's project-scoped OIDC token,
        // which Vercel Container Registry accepts for pushes (user "oidc").
        const env =
          q.get("vcr") === "1"
            ? { VCR_TOKEN: request.headers.get("x-vercel-oidc-token") ?? "" }
            : {};
        const cmd = await sbx.runCommand({
          cmd: "bash",
          args: ["-lc", q.get("cmd")],
          sudo: q.get("sudo") === "1",
          env,
        });
        return json({
          exit: cmd.exitCode,
          stdout: await cmd.stdout(),
          stderr: await cmd.stderr(),
          ms: performance.now() - t0,
        });
      }
      case "drives": {
        const out = [];
        for await (const d of (await Drive.list({})).drives ?? []) out.push(d);
        return json(out);
      }
      default:
        return json({ error: "unknown op" }, 400);
    }
  } catch (e) {
    return json({ error: String(e?.stack ?? e), ms: performance.now() - t0 }, 500);
  }
}
