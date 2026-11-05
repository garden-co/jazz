/**
 * A very rough capacity and cost model for running Jazz on each platform,
 * for the homepage calculator. Every number is a named assumption with a
 * default, so the calculator's debug panel can vary each one; none of it is
 * a quote.
 *
 * Engine defaults come from the most recent example-app measurements:
 * - DocShelf ceiling bench (2026-10-24): native ~2 ms of CPU per 10-row write,
 *   ~3.5 us per connected session per write, ~1.25 KB of memory per
 *   (session, watched row). On Durable Object SQLite: ~8 ms per write and
 *   ~6 us per session, in 128 MB.
 * - Sharding experiments (2026-10-30): read and write throughput scale with
 *   roughly 0.7x the shard count.
 */

export const frequencyOptions = [
  {
    value: "monthly",
    label: "Monthly",
    activeDaysPerMonth: 1,
    visitsPerUserPerMonth: 1,
    structuredStorageMultiplier: 0.75,
    blobFetchRatio: 0.04,
  },
  {
    value: "weekly",
    label: "Weekly",
    activeDaysPerMonth: 4,
    visitsPerUserPerMonth: 4,
    structuredStorageMultiplier: 1,
    blobFetchRatio: 0.1,
  },
  {
    value: "daily",
    label: "Daily",
    activeDaysPerMonth: 20,
    visitsPerUserPerMonth: 20,
    structuredStorageMultiplier: 1,
    blobFetchRatio: 0.22,
  },
  {
    value: "multiple-daily",
    label: "Multiple times daily",
    activeDaysPerMonth: 22,
    visitsPerUserPerMonth: 44,
    structuredStorageMultiplier: 1,
    blobFetchRatio: 0.4,
  },
] as const;

export const realtimeOptions = [
  {
    value: "mostly-form-like",
    label: "Mostly form-like apps",
    /** How the calculator names the choice once it is picked. */
    chosen: "Form-like interaction",
    recipientsPerWrite: 1,
    activeShare: 0.25,
    averageSessionMinutes: 3,
    activeWindowHours: 10,
    writesPerActiveMinute: 0.5,
    rowsWatchedPerSession: 60,
    structuredStoragePerUserMb: 0.5,
    structuredEgressMbPerVisit: 0.5,
    egressFanoutMultiplier: 1,
  },
  {
    value: "collaborative",
    label: "Collaborative / shared state",
    chosen: "Collaborative interaction",
    recipientsPerWrite: 4,
    activeShare: 0.7,
    averageSessionMinutes: 20,
    activeWindowHours: 10,
    writesPerActiveMinute: 4,
    rowsWatchedPerSession: 240,
    structuredStoragePerUserMb: 1.5,
    structuredEgressMbPerVisit: 2,
    egressFanoutMultiplier: 2.5,
  },
  {
    value: "live",
    label: "Live streams / fan-out",
    chosen: "Live fan-out interaction",
    recipientsPerWrite: 50,
    activeShare: 0.9,
    averageSessionMinutes: 45,
    activeWindowHours: 3,
    writesPerActiveMinute: 6,
    rowsWatchedPerSession: 400,
    structuredStoragePerUserMb: 3,
    structuredEgressMbPerVisit: 4,
    egressFanoutMultiplier: 10,
  },
] as const;

export type FrequencyOption = (typeof frequencyOptions)[number]["value"];
export type RealtimeOption = (typeof realtimeOptions)[number]["value"];

/** One tunable number in the model. */
export type Param = {
  key: string;
  label: string;
  min: number;
  max: number;
  step: number;
  default: number;
};

export type ParamGroup = { id: string; title: string; params: Param[] };

export type Assumptions = Record<string, number>;

const param = (key: string, label: string, value: number, min: number, max: number, step: number) =>
  ({ key, label, default: value, min, max, step }) satisfies Param;

function frequencyParams(option: (typeof frequencyOptions)[number]) {
  const k = `freq.${option.value}`;
  return [
    param(`${k}.visits`, "Visits per user per month", option.visitsPerUserPerMonth, 1, 100, 1),
    param(`${k}.activeDays`, "Active days per month", option.activeDaysPerMonth, 1, 30, 1),
    param(
      `${k}.storageMultiplier`,
      "Structured storage multiplier",
      option.structuredStorageMultiplier,
      0.1,
      3,
      0.05,
    ),
    param(
      `${k}.blobFetchRatio`,
      "Share of blobs fetched per month",
      option.blobFetchRatio,
      0,
      2,
      0.01,
    ),
  ];
}

function realtimeParams(option: (typeof realtimeOptions)[number]) {
  const k = `rt.${option.value}`;
  return [
    param(`${k}.sessionMinutes`, "Session length (min)", option.averageSessionMinutes, 1, 240, 1),
    param(`${k}.windowHours`, "Active window per day (h)", option.activeWindowHours, 1, 24, 0.5),
    param(
      `${k}.writesPerMinute`,
      "Writes per active minute",
      option.writesPerActiveMinute,
      0,
      60,
      0.1,
    ),
    param(
      `${k}.rowsWatched`,
      "Rows watched per session",
      option.rowsWatchedPerSession,
      10,
      5000,
      10,
    ),
    param(
      `${k}.storagePerUserMb`,
      "Structured storage per user (MB)",
      option.structuredStoragePerUserMb,
      0.1,
      200,
      0.1,
    ),
    param(
      `${k}.egressMbPerVisit`,
      "Sync egress per visit (MB)",
      option.structuredEgressMbPerVisit,
      0,
      50,
      0.1,
    ),
    param(`${k}.fanout`, "Egress fan-out multiplier", option.egressFanoutMultiplier, 1, 50, 0.5),
    param(`${k}.recipients`, "Sessions receiving each write", option.recipientsPerWrite, 0, 500, 1),
    param(
      `${k}.activeShare`,
      "Share of sessions active in a minute",
      option.activeShare,
      0.05,
      1,
      0.05,
    ),
  ];
}

const globalParams = [
  param("global.peakFactor", "Peak over average concurrency", 2, 1, 10, 0.1),
  param("global.comfortable", "Target peak utilization", 0.7, 0.3, 1, 0.05),
  param("global.shardEfficiency", "Shard throughput efficiency", 0.7, 0.3, 1, 0.05),
  param("global.parallel", "Share of per-write work on other cores", 0, 0, 0.95, 0.05),
  param("global.sqliteShare", "Share of a write that is serial SQLite", 0.45, 0.1, 1, 0.05),
  param("global.kbPerWatchedRow", "Memory per watched row (KB)", 1.25, 0.05, 10, 0.05),
  param("global.rowsPerWrite", "Rows per write", 10, 1, 100, 1),
  param("global.idleGraceMinutes", "Idle time before sleeping (min)", 5, 0, 60, 1),
  param("global.proRamGb", "Jazz Pro up to total RAM (GB)", 4, 0, 192, 1),
];

type PlatformId = "vercel" | "cloudflare" | "server" | "aws";

/** One instance size a platform offers. */
/** `ramGb` is what the platform provisions; `memoryGb` what Jazz can use of it. */
export type Size = { label: string; vcpus: number; ramGb: number; memoryGb: number };

export type Platform = {
  id: PlatformId;
  name: string;
  comingSoon?: boolean;
  params: Param[];
  sizes: (a: Assumptions) => Size[];
  /** Largest structured data one instance may hold, or Infinity. */
  diskGb: (a: Assumptions) => number;
  /** Monthly platform cost for `shards` instances of `size` at this demand. */
  cost: (d: Demand, a: Assumptions, size: Size, shards: number, cpuUtilization: number) => number;
  costNote: string;
  /** The properties besides cost that shape the choice, keyed like `traitRows`. */
  traits: Record<TraitKey, string>;
  /** The button under the prices: a guide to follow, or none yet. */
  cta: { label: string; href?: string };
  /** Trait text that depends on the assumptions, such as a toggle. */
  traitsFor?: (a: Assumptions) => Partial<Record<TraitKey, string>>;
};

/**
 * Rows shown under every platform, in this order. Rows marked `more` stay
 * behind the shared "More details" button.
 */
export const traitRows = [
  { key: "coldWake", label: "Cold wake (p50 / p95)" },
  { key: "security", label: "Security" },
  { key: "durability", label: "Durability" },
  { key: "idle", label: "When idle", more: true },
  { key: "regions", label: "Regions", more: true },
  { key: "scale", label: "One instance up to", more: true },
  { key: "transfer", label: "Transfer", more: true },
  { key: "operations", label: "You operate", more: true },
] as const;

export type TraitKey = (typeof traitRows)[number]["key"];

const engineParams = (id: PlatformId, writeMs: number, recipientUs: number, sessionUs: number) => [
  param(`${id}.writeMs`, "CPU per write (ms)", writeMs, 0.1, 50, 0.1),
  param(`${id}.recipientUs`, "CPU per receiving session (µs)", recipientUs, 0, 2000, 10),
  param(`${id}.sessionUs`, "CPU per idle session per write (µs)", sessionUs, 0, 50, 0.5),
];

const over = (value: number, included: number) => Math.max(value - included, 0);
const HOURS_PER_MONTH = 730;

/**
 * What a Vercel deployment uses in a month, and the bill on the plan it needs.
 *
 * The Sandbox bills memory while it is up and active CPU while it works. The
 * sync proxy is a Fluid function: it holds the websockets while sessions are
 * connected, which is the same active window, and bills its configured memory
 * for that time plus a sliver of active CPU per message. Fluid runs many
 * connections in one instance, so the proxy grows with sessions, not shards.
 *
 * Small apps that sleep when idle fit Hobby's allowances (Sandbox: 5 CPU
 * hours, 420 GB-hours, 45-minute sessions; functions: 4 CPU hours,
 * 360 GB-hours; 100 GB transfer) and pay nothing. Everything else is Pro,
 * whose $20 includes $20 of usage.
 */
function vercelBill(d: Demand, a: Assumptions, size: Size, shards: number, cpu: number) {
  const hibernate = a["vercel.hibernate"] >= 0.5;
  const sandboxHours = hibernate ? d.upHours : HOURS_PER_MONTH;
  const sandboxGbHours = shards * size.ramGb * sandboxHours;
  const sandboxCpuHours = shards * size.vcpus * Math.max(cpu * 0.4, 0.01) * d.upHours;

  const proxies = Math.max(1, Math.ceil(d.peakSessions / a["vercel.proxySessions"]));
  const proxyGbHours = proxies * a["vercel.proxyMemoryGb"] * d.upHours;
  const messages = d.monthlyWrites * (1 + d.recipientsPerWrite);
  const proxyCpuHours = (messages * a["vercel.proxyMsPerMessage"]) / 3_600_000;

  const fitsHobby =
    hibernate &&
    size.vcpus <= 4 &&
    sandboxCpuHours <= 5 &&
    sandboxGbHours <= 420 &&
    proxyCpuHours <= 4 &&
    proxyGbHours <= 360 &&
    d.structuredGb <= 15 &&
    d.blobGb <= a["vercel.hobbyBlobGb"] &&
    d.egressGb <= 100;
  if (fitsHobby) return { plan: "Hobby" as const, cost: 0 };

  const usage =
    sandboxGbHours * a["vercel.memoryGbHour"] +
    (sandboxCpuHours + proxyCpuHours) * a["vercel.cpuHour"] +
    proxyGbHours * a["vercel.fluidMemoryGbHour"] +
    d.structuredGb * a["vercel.driveGb"] +
    d.blobGb * a["vercel.blobGb"] +
    over(d.egressGb, a["vercel.includedEgressGb"]) * a["vercel.egressGb"];
  return { plan: "Pro" as const, cost: Math.max(a["vercel.plan"], usage) };
}

export const platforms: Platform[] = [
  {
    id: "cloudflare",
    cta: { label: "Coming soon" },
    name: "Cloudflare Durable Objects",
    comingSoon: true,
    params: [
      ...engineParams("cloudflare", 8, 440, 6),
      param("cloudflare.memoryGb", "Memory for sessions (GB)", 0.1, 0.05, 0.128, 0.005),
      param("cloudflare.diskGb", "Storage per object (GB)", 10, 1, 50, 1),
      param("cloudflare.plan", "Workers Paid ($/mo)", 5, 0, 50, 1),
      param("cloudflare.durationMGbS", "Duration ($/M GB-s)", 12.5, 0, 50, 0.5),
      param("cloudflare.requestsM", "Requests ($/M)", 0.15, 0, 1, 0.01),
      param("cloudflare.rowsWrittenM", "Rows written ($/M)", 1, 0, 5, 0.05),
      param("cloudflare.rowsReadM", "Rows read ($/M)", 0.001, 0, 0.01, 0.0005),
      param("cloudflare.storageGb", "SQLite storage ($/GB-month)", 0.2, 0, 1, 0.01),
      param("cloudflare.r2Gb", "R2 storage ($/GB-month)", 0.015, 0, 0.1, 0.001),
    ],
    sizes: (a) => [
      { label: "Durable Object", vcpus: 1, ramGb: 0.128, memoryGb: a["cloudflare.memoryGb"] },
    ],
    diskGb: (a) => a["cloudflare.diskGb"],
    cost: (d, a, _size, shards) => {
      const gbSeconds = shards * 0.128 * d.upHours * 3600;
      // Workers Free runs SQLite-backed objects with daily caps (reset at
      // 00:00 UTC; past one, that kind of operation fails until the reset):
      // 100k requests, 13,000 GB-s, 100k rows written, 5M rows read, 5 GB in
      // total and 1 GB per object. R2's 10 GB free tier holds large values.
      const perDay = (monthly: number) => monthly / 30;
      const requestCount = d.monthlyVisits + d.monthlyWrites / 20;
      const rowsWrittenCount = d.monthlyWrites * a["global.rowsPerWrite"] * 2;
      const rowsReadCount = d.monthlyWrites * 100 + d.monthlyVisits * d.rowsWatchedPerSession;
      const fitsFree =
        perDay(requestCount) <= 100_000 &&
        perDay(gbSeconds) <= 13_000 &&
        perDay(rowsWrittenCount) <= 100_000 &&
        perDay(rowsReadCount) <= 5_000_000 &&
        d.structuredGb <= 5 &&
        d.structuredGb / shards <= 1 &&
        d.blobGb <= 10;
      if (fitsFree) return 0;
      const duration = (over(gbSeconds, 400_000) / 1e6) * a["cloudflare.durationMGbS"];
      const requests = (over(requestCount, 1e6) / 1e6) * a["cloudflare.requestsM"];
      const rowsWritten = (over(rowsWrittenCount, 50e6) / 1e6) * a["cloudflare.rowsWrittenM"];
      const rowsRead = (over(rowsReadCount, 25e9) / 1e6) * a["cloudflare.rowsReadM"];
      const storage =
        over(d.structuredGb, 5) * a["cloudflare.storageGb"] +
        over(d.blobGb, 10) * a["cloudflare.r2Gb"];
      return a["cloudflare.plan"] + duration + requests + rowsWritten + rowsRead + storage;
    },
    costNote:
      "Workers Free while the app fits its daily caps (100k requests, 100k rows written, 13,000 GB-s, 5 GB stored), otherwise Workers Paid at $5 plus Durable Object duration, requests, rows and storage past its allowances. R2 holds large values. Egress is free.",
    traits: {
      coldWake: "~50 ms / ~250 ms (estimated)",
      idle: "Idle WebSockets are parked at no Durable Object cost",
      security: "Cloudflare WAF and DDoS protection, with a fast-patching security team",
      durability: "Every write stored across multiple data centers before it's acknowledged",
      regions: "Global; rows and shards can have region affinity",
      scale: "128 MB memory / 10 GB per object",
      transfer: "Free",
      operations: "Nothing beyond your Workers project",
    },
  },
  {
    id: "vercel",
    cta: { label: "Deploy on Vercel", href: "/docs/setup/deployment/vercel" },
    name: "Vercel Sandbox",
    params: [
      ...engineParams("vercel", 2, 110, 3.5),
      param("vercel.maxVcpus", "Largest Sandbox (vCPUs)", 8, 1, 8, 1),
      param("vercel.hibernate", "Hibernate when idle (1 = yes)", 1, 0, 1, 1),
      param("vercel.plan", "Pro plan ($/mo, usage credit included)", 20, 0, 100, 1),
      param("vercel.memoryGbHour", "Sandbox memory ($/GB-hour)", 0.0212, 0, 0.05, 0.0002),
      param("vercel.cpuHour", "Active CPU ($/vCPU-hour)", 0.128, 0, 0.5, 0.002),
      param("vercel.proxyMemoryGb", "Sync proxy function memory (GB)", 1, 0.5, 4, 0.5),
      param("vercel.fluidMemoryGbHour", "Function memory ($/GB-hour)", 0.0106, 0, 0.05, 0.0002),
      param("vercel.proxySessions", "Sessions per proxy instance", 2000, 100, 20000, 100),
      param("vercel.proxyMsPerMessage", "Proxy CPU per message (ms)", 0.1, 0, 2, 0.05),
      param("vercel.driveGb", "Drive storage ($/GB-month)", 0.05, 0, 0.5, 0.01),
      param("vercel.hobbyBlobGb", "Hobby Blob storage (GB)", 1, 0, 10, 0.5),
      param("vercel.blobGb", "Blob storage ($/GB-month)", 0.023, 0, 0.2, 0.001),
      param("vercel.egressGb", "Transfer ($/GB)", 0.15, 0, 0.5, 0.01),
      param("vercel.includedEgressGb", "Included transfer (GB)", 1024, 0, 5000, 64),
    ],
    sizes: (a) =>
      [1, 2, 4, 8]
        .filter((vcpus) => vcpus <= a["vercel.maxVcpus"])
        .map((vcpus) => ({
          label: `${vcpus} vCPU / ${vcpus * 2} GB Sandbox`,
          vcpus,
          ramGb: vcpus * 2,
          memoryGb: vcpus * 2 - 0.5,
        })),
    diskGb: () => Infinity,
    cost: (d, a, size, shards, cpu) => vercelBill(d, a, size, shards, cpu).cost,
    costNote:
      "Hobby (free) while the app fits its allowances and sleeps when idle, otherwise Pro: $20 a month, which includes $20 of usage. Usage is Sandbox memory (2 GB per vCPU) and active CPU while awake, the sync proxy function, Drive and Blob storage, and transfer.",
    traits: {
      coldWake: "~0.75 s / ~1 s",
      idle: "Active-CPU pricing; Jazz hibernates a Sandbox after a few idle minutes",
      security: "Vercel Firewall and DDoS protection, with a fast-patching security team",
      durability: "Persistent Drive, Blob for large values",
      regions: "19 regions; pick one per store (default iad1)",
      scale: "8 vCPU / 16 GB",
      transfer: "1 TB included, then $0.15/GB",
      operations: "Nothing beyond your Vercel project",
    },
    traitsFor: (a) =>
      a["vercel.hibernate"] >= 0.5
        ? {}
        : {
            coldWake: "None, always on",
            idle: "Always on: memory billed around the clock, CPU only while active",
          },
  },
  {
    id: "aws",
    cta: { label: "Host yourself", href: "/docs/setup/deployment/own-server" },
    name: "AWS EC2",
    params: [
      ...engineParams("aws", 2, 110, 3.5),
      param("aws.maxVcpus", "Largest instance (vCPUs)", 48, 2, 48, 2),
      param("aws.perVcpuHour", "On-demand ($/vCPU-hour, 4 GB each, m7g)", 0.0408, 0.01, 0.1, 0.001),
      param("aws.discount", "Savings Plan discount", 0, 0, 0.6, 0.05),
      param("aws.ebsGb", "EBS gp3 ($/GB-month)", 0.08, 0, 0.3, 0.01),
      param("aws.snapshotGb", "Snapshots ($/GB-month)", 0.05, 0, 0.2, 0.01),
      param("aws.s3Gb", "S3 ($/GB-month)", 0.023, 0, 0.1, 0.001),
      param("aws.egressGb", "Data transfer out ($/GB)", 0.09, 0, 0.2, 0.005),
      param("aws.includedEgressGb", "Free transfer out (GB)", 100, 0, 1000, 10),
    ],
    sizes: (a) =>
      [2, 4, 8, 16, 32, 48]
        .filter((vcpus) => vcpus <= a["aws.maxVcpus"])
        .map((vcpus) => ({
          label: `${vcpus} vCPU / ${vcpus * 4} GB EC2 instance`,
          vcpus,
          ramGb: vcpus * 4,
          memoryGb: vcpus * 4 - 2,
        })),
    // EBS volumes grow with the data.
    diskGb: () => Infinity,
    cost: (d, a, size, shards) => {
      const instances =
        shards * size.vcpus * a["aws.perVcpuHour"] * HOURS_PER_MONTH * (1 - a["aws.discount"]);
      // Each shard keeps its data on EBS with room to grow, plus snapshots.
      const ebsGb = Math.max(d.structuredGb * 1.5, shards * 20);
      const storage = ebsGb * a["aws.ebsGb"] + d.structuredGb * a["aws.snapshotGb"];
      const s3 = d.blobGb * a["aws.s3Gb"];
      const egress = over(d.egressGb, a["aws.includedEgressGb"]) * a["aws.egressGb"];
      return instances + storage + s3 + egress;
    },
    costNote:
      "Always-on Graviton instances (on-demand unless discounted), EBS with snapshots, S3 for large values, and data transfer out. Any region.",
    traits: {
      coldWake: "None, always on",
      idle: "Billed around the clock",
      security: "Shield Standard included; WAF, OS patches and TLS are up to you",
      durability: "EBS replicated within one zone, plus snapshots",
      regions: "Any AWS region",
      scale: "48 vCPU / 192 GB (or larger)",
      transfer: "100 GB free, then $0.09/GB",
      operations: "The OS and instance; AWS runs the hardware",
    },
  },
  {
    id: "server",
    cta: { label: "Host yourself", href: "/docs/setup/deployment/own-server" },
    name: "Your own server",
    params: [
      ...engineParams("server", 2, 110, 3.5),
      param("server.maxVcpus", "Largest server (vCPUs)", 48, 2, 48, 2),
      param("server.perVcpu", "Dedicated server ($/vCPU-month, 4 GB each)", 8, 2, 30, 0.5),
      param("server.backups", "Backups (share of server price)", 0.2, 0, 1, 0.05),
      param("server.diskGbPerVcpu", "Included disk per vCPU (GB)", 35, 5, 200, 5),
      param("server.volumeGb", "Extra volumes ($/GB-month)", 0.05, 0, 0.5, 0.01),
      param("server.objectTb", "Object Storage ($/TB-month)", 6, 0, 50, 0.5),
      param("server.includedTb", "Included traffic per server (TB)", 20, 0, 100, 1),
      param("server.egressTb", "Extra traffic ($/TB)", 1.2, 0, 20, 0.1),
    ],
    sizes: (a) =>
      [2, 4, 8, 16, 32, 48]
        .filter((vcpus) => vcpus <= a["server.maxVcpus"])
        .map((vcpus) => ({
          label: `${vcpus} vCPU / ${vcpus * 4} GB Hetzner server`,
          vcpus,
          ramGb: vcpus * 4,
          memoryGb: vcpus * 4 - 2,
        })),
    // Attach volumes as the data grows.
    diskGb: () => Infinity,
    cost: (d, a, size, shards) => {
      const servers = shards * size.vcpus * a["server.perVcpu"] * (1 + a["server.backups"]);
      const disk = shards * size.vcpus * a["server.diskGbPerVcpu"];
      const volumes = over(d.structuredGb, disk) * a["server.volumeGb"];
      const objectStorage = d.blobGb > 0 ? Math.ceil(d.blobGb / 1024) * a["server.objectTb"] : 0;
      const egress =
        (over(d.egressGb, shards * a["server.includedTb"] * 1024) / 1024) * a["server.egressTb"];
      return servers + volumes + objectStorage + egress;
    },
    costNote:
      "Dedicated servers with backups, volumes past the included disk, Object Storage for large values, and traffic past each server's included allowance.",
    traits: {
      coldWake: "None, always on",
      idle: "Flat monthly price",
      security: "Firewall, OS patches and TLS are up to you",
      durability: "Server disks plus the backups you schedule",
      regions: "Wherever your provider runs (Hetzner: EU, US, Singapore)",
      scale: "48 vCPU / 192 GB (or larger)",
      transfer: "20 TB per server included",
      operations: "The OS, backups, monitoring and failover",
    },
  },
];

/** The groups of assumptions that apply to the chosen usage. */
export function paramGroups(frequency: FrequencyOption, realtime: RealtimeOption): ParamGroup[] {
  const f = frequencyOptions.find((option) => option.value === frequency) ?? frequencyOptions[2];
  const r = realtimeOptions.find((option) => option.value === realtime) ?? realtimeOptions[1];
  return [
    { id: "frequency", title: `Usage: ${f.label}`, params: frequencyParams(f) },
    { id: "realtime", title: `Usage: ${r.label}`, params: realtimeParams(r) },
    { id: "global", title: "Engine, headroom and sharding", params: globalParams },
    ...platforms.map((platform) => ({
      id: platform.id,
      title: platform.name,
      params: platform.params,
    })),
  ];
}

export const defaultAssumptions: Assumptions = Object.fromEntries(
  [
    ...frequencyOptions.flatMap(frequencyParams),
    ...realtimeOptions.flatMap(realtimeParams),
    ...globalParams,
    ...platforms.flatMap((platform) => platform.params),
  ].map((p) => [p.key, p.default]),
);

export type Usage = {
  mau: number;
  frequency: FrequencyOption;
  realtime: RealtimeOption;
  blobStoragePerUserMb: number;
  /** Jazz Pro: shards up to `global.proRamGb` of RAM in total, per month. */
  licensePro: number;
  /** Jazz Scale: base price per month. */
  licenseScaleBase: number;
  /** Jazz Scale: price per 1,000 peak active sessions per month, on top of the base. */
  licensePer1k: number;
};

function nonNegative(value: number) {
  return Number.isFinite(value) ? Math.max(value, 0) : 0;
}

/** What an app at this usage asks of its backend, independent of platform. */
export function estimateDemand(usage: Usage, a: Assumptions) {
  const f = (name: string) => a[`freq.${usage.frequency}.${name}`];
  const r = (name: string) => a[`rt.${usage.realtime}.${name}`];
  const users = nonNegative(usage.mau);

  const monthlyVisits = users * f("visits");
  const dailyVisits = monthlyVisits / f("activeDays");
  const sessionHours = r("sessionMinutes") / 60;
  const averageConcurrent = (dailyVisits * sessionHours) / r("windowHours");
  const peakSessions = users > 0 ? Math.max(1, averageConcurrent * a["global.peakFactor"]) : 0;
  const peakWritesPerSecond = (peakSessions * r("writesPerMinute")) / 60;
  const monthlyWrites = monthlyVisits * r("sessionMinutes") * r("writesPerMinute");

  // Hours a scale-to-zero instance is up: the active window on active days,
  // shrinking towards zero when visits are too sparse to keep it warm.
  const idleGraceHours = a["global.idleGraceMinutes"] / 60;
  const visitsPerWindowHour = dailyVisits / r("windowHours");
  const warmShare = 1 - Math.exp(-visitsPerWindowHour * (sessionHours + idleGraceHours));
  const upHours = Math.min(f("activeDays") * r("windowHours") * warmShare, HOURS_PER_MONTH);

  const structuredGb = (users * r("storagePerUserMb") * f("storageMultiplier")) / 1024;
  const blobGb = (users * nonNegative(usage.blobStoragePerUserMb)) / 1024;
  const egressGb =
    (monthlyVisits * r("egressMbPerVisit") * r("fanout")) / 1024 + blobGb * f("blobFetchRatio");

  return {
    monthlyVisits,
    peakSessions,
    /** Sessions that wrote, subscribed or received an update in the peak minute. */
    peakActiveSessions: peakSessions * r("activeShare"),
    peakWritesPerSecond,
    monthlyWrites,
    upHours,
    structuredGb,
    blobGb,
    egressGb,
    rowsWatchedPerSession: r("rowsWatched"),
    recipientsPerWrite: r("recipients"),
  };
}

export type Demand = ReturnType<typeof estimateDemand>;

/**
 * The busiest share of one shard's capacity with `shards` shards of `size`.
 * The SQLite part of a write (and, with no parallel engine, all of it) runs on
 * one core, so a bigger box only helps with the rest and with memory.
 */
function utilization(platform: Platform, size: Size, d: Demand, a: Assumptions, shards: number) {
  const p = (name: string) => a[`${platform.id}.${name}`];
  const sessions = d.peakSessions / shards;
  const recipients = Math.min(d.recipientsPerWrite, sessions);
  const msPerWrite =
    p("writeMs") + (recipients * p("recipientUs") + sessions * p("sessionUs")) / 1000;
  const parallel = a["global.parallel"];
  const serialMs = Math.max(msPerWrite * (1 - parallel), p("writeMs") * a["global.sqliteShare"]);
  const writes = d.peakWritesPerSecond / (shards > 1 ? a["global.shardEfficiency"] * shards : 1);
  const cpuSerial = (writes * serialMs) / 1000;
  const cpuTotal = (writes * msPerWrite) / 1000 / size.vcpus;
  const memoryGb = (sessions * d.rowsWatchedPerSession * a["global.kbPerWatchedRow"]) / 1024 / 1024;
  const memory = memoryGb / size.memoryGb;
  const disk = d.structuredGb / shards / platform.diskGb(a);
  const cpu = Math.max(cpuSerial, cpuTotal);
  return { cpu, worst: Math.max(cpu, memory, disk) };
}

/** The fewest shards of `size` that keep every shard under `limit` of its capacity. */
function shardsFor(platform: Platform, size: Size, d: Demand, a: Assumptions, limit: number) {
  const fits = (shards: number) => utilization(platform, size, d, a, shards).worst <= limit;
  if (fits(1)) return 1;
  let high = 2;
  while (!fits(high)) {
    high *= 2;
    if (high > 1_000_000) return Infinity;
  }
  let low = high / 2;
  while (high - low > 1) {
    const mid = Math.floor((low + high) / 2);
    if (fits(mid)) high = mid;
    else low = mid;
  }
  return high;
}

export type TierId = "oss" | "pro" | "scale";

/** The ways to license Jazz, as the homepage lists them. Custom is never computed. */
export const jazzTiers: Record<
  TierId | "custom",
  {
    name: string;
    price: string;
    /** A second price line, such as a usage charge. */
    priceExtra?: string;
    summary: string;
    /** A short footnote under the summary. */
    note?: string;
  }
> = {
  oss: {
    name: "Jazz OSS (MIT)",
    price: "Free",
    summary:
      "A single server instance of any size, however much you use it. Jazz deploys with your backend on your own cloud account, so you only pay your platform.",
  },
  pro: {
    name: "Jazz Pro (source-available)",
    price: "Free",
    summary:
      "When an app outgrows one instance, shard it across several for free, up to 4 GB of RAM in total",
  },
  scale: {
    name: "Jazz Scale",
    price: "$200/mo",
    priceExtra: "+ $50/mo per 1,000 peak active sessions",
    summary:
      "Unlimited sharded instances, to take your database to millions of users, globally distributed",
    note: "Peak active sessions: the monthly 95th percentile of sessions active per minute. A session is active when it writes, subscribes or receives an update. As Jazz gets faster your platform bill shrinks while the license stays the same. The license is a key your servers check offline: no metering and no phone-home. Going over it only logs a reminder.",
  },
  custom: {
    name: "Jazz Custom",
    price: "Let's talk",
    summary:
      "Support, SLAs and volume pricing for larger teams, and a deal for early-stage startups whose usage is ahead of their revenue",
  },
};

/** One instance is OSS; shards with little RAM in total are Pro; the rest is Scale. */
export function licenseTier(size: Size, shards: number, a: Assumptions): TierId {
  if (shards <= 1) return "oss";
  if (shards * size.ramGb <= a["global.proRamGb"]) return "pro";
  return "scale";
}

export function licenseCost(d: Demand, usage: Usage, tier: TierId) {
  if (tier === "oss") return 0;
  if (tier === "pro") return usage.licensePro;
  return usage.licenseScaleBase + Math.ceil(d.peakActiveSessions / 1000) * usage.licensePer1k;
}

export type PlatformEstimate = {
  platform: Platform;
  /** The cheapest size and shard count, license included. */
  size: Size;
  instances: number;
  /** Shards the largest size needs to run at all. */
  requiredShards: number;
  platformCost: number;
  tier: TierId;
  licenseCost: number;
  totalCost: number;
  costPerMau: number;
  peakActiveSessions: number;
  /** The most MAU one instance of the largest size serves comfortably at this usage. */
  mauPerInstance: number;
  largest: Size;
};

function mauPerInstance(platform: Platform, size: Size, usage: Usage, a: Assumptions) {
  const fits = (mau: number) =>
    utilization(platform, size, estimateDemand({ ...usage, mau }, a), a, 1).worst <=
    a["global.comfortable"];
  let low = 1;
  let high = 1e9;
  if (!fits(low)) return 0;
  while (high / low > 1.02) {
    const mid = Math.sqrt(low * high);
    if (fits(mid)) low = mid;
    else high = mid;
  }
  return low;
}

export function estimatePlatforms(
  usage: Usage,
  a: Assumptions = defaultAssumptions,
): PlatformEstimate[] {
  const d = estimateDemand(usage, a);
  return platforms.map((platform) => {
    const sizes = platform.sizes(a);
    const largest = sizes[sizes.length - 1];
    let best: {
      size: Size;
      instances: number;
      platformCost: number;
      tier: TierId;
      license: number;
    } | null = null;
    for (const size of sizes) {
      const instances = shardsFor(platform, size, d, a, a["global.comfortable"]);
      if (!Number.isFinite(instances)) continue;
      const cpu = utilization(platform, size, d, a, instances).cpu;
      const platformCost = platform.cost(d, a, size, instances, cpu);
      const tier = licenseTier(size, instances, a);
      const license = licenseCost(d, usage, tier);
      if (!best || platformCost + license < best.platformCost + best.license) {
        best = { size, instances, platformCost, tier, license };
      }
    }
    const chosen = best ?? {
      size: largest,
      instances: Infinity,
      platformCost: Infinity,
      tier: "scale" as TierId,
      license: Infinity,
    };
    const totalCost = chosen.platformCost + chosen.license;
    return {
      platform,
      size: chosen.size,
      instances: chosen.instances,
      requiredShards: shardsFor(platform, largest, d, a, 1),
      platformCost: chosen.platformCost,
      tier: chosen.tier,
      licenseCost: chosen.license,
      totalCost,
      costPerMau: usage.mau > 0 ? totalCost / usage.mau : 0,
      peakActiveSessions: d.peakActiveSessions,
      mauPerInstance: mauPerInstance(platform, largest, usage, a),
      largest,
    };
  });
}
