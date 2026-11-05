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
    averageSessionMinutes: 3,
    activeWindowHours: 10,
    writesPerActiveMinute: 0.5,
    rowsWatchedPerSession: 60,
    structuredStoragePerUserMb: 8,
    structuredEgressMbPerVisit: 0.5,
    egressFanoutMultiplier: 1,
  },
  {
    value: "collaborative",
    label: "Collaborative / shared state",
    averageSessionMinutes: 20,
    activeWindowHours: 10,
    writesPerActiveMinute: 4,
    rowsWatchedPerSession: 240,
    structuredStoragePerUserMb: 16,
    structuredEgressMbPerVisit: 2,
    egressFanoutMultiplier: 2.5,
  },
  {
    value: "live",
    label: "Live streams / fan-out",
    averageSessionMinutes: 45,
    activeWindowHours: 3,
    writesPerActiveMinute: 6,
    rowsWatchedPerSession: 400,
    structuredStoragePerUserMb: 28,
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
  ];
}

const globalParams = [
  param("global.peakFactor", "Peak over average concurrency", 2, 1, 10, 0.1),
  param("global.comfortable", "Target peak utilization", 0.7, 0.3, 1, 0.05),
  param("global.shardEfficiency", "Shard throughput efficiency", 0.7, 0.3, 1, 0.05),
  param("global.kbPerWatchedRow", "Memory per watched row (KB)", 1.25, 0.1, 10, 0.05),
  param("global.rowsPerWrite", "Rows per write", 10, 1, 100, 1),
  param("global.idleGraceMinutes", "Idle time before sleeping (min)", 5, 0, 60, 1),
];

type PlatformId = "vercel" | "cloudflare" | "server";

export type Platform = {
  id: PlatformId;
  name: string;
  comingSoon?: boolean;
  /** One shard's worth of the platform, as shown to the reader. */
  instance: string;
  params: Param[];
  /** Largest structured data one instance may hold, or Infinity. */
  diskGb: (a: Assumptions) => number;
  /** Monthly platform cost for `shards` instances at this demand. */
  cost: (d: Demand, a: Assumptions, shards: number, cpuUtilization: number) => number;
  costNote: string;
};

const engineParams = (
  id: PlatformId,
  cores: number,
  writeMs: number,
  sessionUs: number,
  memoryGb: number,
) => [
  param(`${id}.cores`, "Usable cores", cores, 0.25, 32, 0.25),
  param(`${id}.writeMs`, "CPU per write (ms)", writeMs, 0.1, 50, 0.1),
  param(`${id}.sessionUs`, "CPU per session per write (µs)", sessionUs, 0, 50, 0.5),
  param(`${id}.memoryGb`, "Memory for sessions (GB)", memoryGb, 0.05, 64, 0.05),
];

const over = (value: number, included: number) => Math.max(value - included, 0);
const HOURS_PER_MONTH = 730;

export const platforms: Platform[] = [
  {
    id: "vercel",
    name: "Vercel Sandbox",
    instance: "Sandbox",
    params: [
      ...engineParams("vercel", 1.5, 2, 3.5, 3.5),
      param("vercel.vcpus", "Sandbox vCPUs", 2, 1, 8, 1),
      param("vercel.plan", "Pro plan ($/mo)", 20, 0, 100, 1),
      param("vercel.memoryGbHour", "Memory ($/GB-hour)", 0.0106, 0, 0.05, 0.0002),
      param("vercel.cpuHour", "Active CPU ($/vCPU-hour)", 0.128, 0, 0.5, 0.002),
      param("vercel.proxy", "Sync proxy ($/instance-month)", 12, 0, 50, 1),
      param("vercel.driveGb", "Drive storage ($/GB-month)", 0.08, 0, 0.5, 0.01),
      param("vercel.blobGb", "Blob storage ($/GB-month)", 0.023, 0, 0.2, 0.001),
      param("vercel.egressGb", "Transfer ($/GB)", 0.15, 0, 0.5, 0.01),
      param("vercel.includedEgressGb", "Included transfer (GB)", 1024, 0, 5000, 64),
    ],
    diskGb: () => Infinity,
    cost: (d, a, shards, cpu) => {
      const hours = d.upHours;
      const vcpus = a["vercel.vcpus"];
      const memory = shards * vcpus * 2 * a["vercel.memoryGbHour"] * hours;
      const activeCpu = shards * vcpus * Math.max(cpu * 0.4, 0.05) * hours * a["vercel.cpuHour"];
      const proxy = shards * a["vercel.proxy"] * (hours / HOURS_PER_MONTH);
      const storage = d.structuredGb * a["vercel.driveGb"] + d.blobGb * a["vercel.blobGb"];
      const egress = over(d.egressGb, a["vercel.includedEgressGb"]) * a["vercel.egressGb"];
      return a["vercel.plan"] + memory + activeCpu + proxy + storage + egress;
    },
    costNote:
      "Pro plan, Sandbox memory (2 GB per vCPU) and active CPU while awake, the sync proxy, Drive and Blob storage, and transfer.",
  },
  {
    id: "cloudflare",
    name: "Cloudflare Durable Objects",
    comingSoon: true,
    instance: "Durable Object",
    params: [
      ...engineParams("cloudflare", 1, 8, 6, 0.1),
      param("cloudflare.diskGb", "Storage per object (GB)", 10, 1, 50, 1),
      param("cloudflare.plan", "Workers Paid ($/mo)", 5, 0, 50, 1),
      param("cloudflare.durationMGbS", "Duration ($/M GB-s)", 12.5, 0, 50, 0.5),
      param("cloudflare.requestsM", "Requests ($/M)", 0.15, 0, 1, 0.01),
      param("cloudflare.rowsWrittenM", "Rows written ($/M)", 1, 0, 5, 0.05),
      param("cloudflare.rowsReadM", "Rows read ($/M)", 0.001, 0, 0.01, 0.0005),
      param("cloudflare.storageGb", "SQLite storage ($/GB-month)", 0.2, 0, 1, 0.01),
      param("cloudflare.r2Gb", "R2 storage ($/GB-month)", 0.015, 0, 0.1, 0.001),
    ],
    diskGb: (a) => a["cloudflare.diskGb"],
    cost: (d, a, shards) => {
      const gbSeconds = shards * 0.128 * d.upHours * 3600;
      const duration = (over(gbSeconds, 400_000) / 1e6) * a["cloudflare.durationMGbS"];
      const requests =
        (over(d.monthlyVisits + d.monthlyWrites / 20, 1e6) / 1e6) * a["cloudflare.requestsM"];
      const rowsWritten =
        (over(d.monthlyWrites * a["global.rowsPerWrite"] * 2, 50e6) / 1e6) *
        a["cloudflare.rowsWrittenM"];
      const rowsRead =
        (over(d.monthlyWrites * 100 + d.monthlyVisits * d.rowsWatchedPerSession, 25e9) / 1e6) *
        a["cloudflare.rowsReadM"];
      const storage =
        over(d.structuredGb, 5) * a["cloudflare.storageGb"] +
        over(d.blobGb, 10) * a["cloudflare.r2Gb"];
      return a["cloudflare.plan"] + duration + requests + rowsWritten + rowsRead + storage;
    },
    costNote:
      "Workers Paid plan, Durable Object duration, requests, rows and storage, and R2 for large values. Egress is free.",
  },
  {
    id: "server",
    name: "Your own server",
    instance: "4 vCPU / 16 GB Hetzner server",
    params: [
      ...engineParams("server", 2.5, 2, 3.5, 14),
      param("server.monthly", "Server ($/mo)", 32, 5, 200, 1),
      param("server.backups", "Backups ($/server-month)", 6.4, 0, 50, 0.1),
      param("server.diskGb", "Included disk (GB)", 140, 20, 1000, 10),
      param("server.volumeGb", "Extra volumes ($/GB-month)", 0.05, 0, 0.5, 0.01),
      param("server.objectTb", "Object Storage ($/TB-month)", 6, 0, 50, 0.5),
      param("server.includedTb", "Included traffic per server (TB)", 20, 0, 100, 1),
      param("server.egressTb", "Extra traffic ($/TB)", 1.2, 0, 20, 0.1),
    ],
    // Attach volumes as the data grows.
    diskGb: () => Infinity,
    cost: (d, a, shards) => {
      const servers = shards * (a["server.monthly"] + a["server.backups"]);
      const volumes = over(d.structuredGb, shards * a["server.diskGb"]) * a["server.volumeGb"];
      const objectStorage = d.blobGb > 0 ? Math.ceil(d.blobGb / 1024) * a["server.objectTb"] : 0;
      const egress =
        (over(d.egressGb, shards * a["server.includedTb"] * 1024) / 1024) * a["server.egressTb"];
      return servers + volumes + objectStorage + egress;
    },
    costNote:
      "Dedicated servers with backups, volumes past the included disk, Object Storage for large values, and traffic past each server's included allowance.",
  },
];

/** The groups of assumptions that apply to the chosen usage. */
export function paramGroups(frequency: FrequencyOption, realtime: RealtimeOption): ParamGroup[] {
  const f = frequencyOptions.find((option) => option.value === frequency) ?? frequencyOptions[2];
  const r = realtimeOptions.find((option) => option.value === realtime) ?? realtimeOptions[1];
  return [
    { id: "frequency", title: `Usage: ${f.label}`, params: frequencyParams(f) },
    { id: "realtime", title: `Usage: ${r.label}`, params: realtimeParams(r) },
    { id: "global", title: "Peaks, headroom and sharding", params: globalParams },
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
  /** Jazz Pro license per shard, per month. */
  licensePerShard: number;
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
    peakWritesPerSecond,
    monthlyWrites,
    upHours,
    structuredGb,
    blobGb,
    egressGb,
    rowsWatchedPerSession: r("rowsWatched"),
  };
}

export type Demand = ReturnType<typeof estimateDemand>;

/** The busiest share of one shard's CPU, memory and disk with `shards` shards. */
function utilization(platform: Platform, d: Demand, a: Assumptions, shards: number) {
  const p = (name: string) => a[`${platform.id}.${name}`];
  const sessions = d.peakSessions / shards;
  const msPerWrite = p("writeMs") + (sessions * p("sessionUs")) / 1000;
  const scale = shards > 1 ? a["global.shardEfficiency"] * shards : 1;
  const cpu = (d.peakWritesPerSecond * msPerWrite) / (p("cores") * 1000 * scale);
  const memoryGb = (sessions * d.rowsWatchedPerSession * a["global.kbPerWatchedRow"]) / 1024 / 1024;
  const memory = memoryGb / p("memoryGb");
  const disk = d.structuredGb / shards / platform.diskGb(a);
  return { cpu, worst: Math.max(cpu, memory, disk) };
}

/** The fewest shards that keep every shard under `limit` of its capacity. */
function shardsFor(platform: Platform, d: Demand, a: Assumptions, limit: number) {
  const fits = (shards: number) => utilization(platform, d, a, shards).worst <= limit;
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

function mauPerInstance(platform: Platform, usage: Usage, a: Assumptions) {
  const fits = (mau: number) =>
    utilization(platform, estimateDemand({ ...usage, mau }, a), a, 1).worst <=
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

export type PlatformEstimate = {
  platform: Platform;
  /** Shards needed to run at all, and to run with headroom. */
  requiredShards: number;
  recommendedShards: number;
  /** Instances the costs assume: the recommended count. */
  instances: number;
  platformCost: number;
  licenseCost: number;
  totalCost: number;
  costPerMau: number;
  /** The most MAU one instance serves comfortably at this usage. */
  mauPerInstance: number;
};

export function estimatePlatforms(
  usage: Usage,
  a: Assumptions = defaultAssumptions,
): PlatformEstimate[] {
  const d = estimateDemand(usage, a);
  return platforms.map((platform) => {
    const requiredShards = shardsFor(platform, d, a, 1);
    const recommendedShards = shardsFor(platform, d, a, a["global.comfortable"]);
    const instances = Number.isFinite(recommendedShards) ? recommendedShards : requiredShards;
    const cpu = utilization(platform, d, a, instances).cpu;
    const platformCost = platform.cost(d, a, instances, cpu);
    // One instance is free under MIT; sharding licenses every shard.
    const licenseCost = instances > 1 ? instances * usage.licensePerShard : 0;
    const totalCost = platformCost + licenseCost;
    return {
      platform,
      requiredShards,
      recommendedShards,
      instances,
      platformCost,
      licenseCost,
      totalCost,
      costPerMau: usage.mau > 0 ? totalCost / usage.mau : 0,
      mauPerInstance: mauPerInstance(platform, usage, a),
    };
  });
}
