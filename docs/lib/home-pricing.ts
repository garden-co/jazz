/**
 * A very rough capacity and cost model for running Jazz on each platform,
 * for the homepage calculator. Every constant here is a first guess meant to
 * be tuned; none of it is a quote.
 *
 * Engine numbers come from the most recent example-app measurements:
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
    sessionLabel: "~3 minute sessions over the active window",
    averageSessionMinutes: 3,
    structuredEgressMbPerVisit: 0.5,
    egressFanoutMultiplier: 1,
    structuredStoragePerUserMb: 8,
    activeWindowHours: 10,
    writesPerActiveMinute: 0.5,
    rowsWatchedPerSession: 60,
  },
  {
    value: "collaborative",
    label: "Collaborative / shared state",
    sessionLabel: "~20 minute sessions over the active window",
    averageSessionMinutes: 20,
    structuredEgressMbPerVisit: 2,
    egressFanoutMultiplier: 2.5,
    structuredStoragePerUserMb: 16,
    activeWindowHours: 10,
    writesPerActiveMinute: 4,
    rowsWatchedPerSession: 240,
  },
  {
    value: "live",
    label: "Live streams / fan-out",
    sessionLabel: "~45 minute sessions in a concentrated event window",
    averageSessionMinutes: 45,
    structuredEgressMbPerVisit: 4,
    egressFanoutMultiplier: 10,
    structuredStoragePerUserMb: 28,
    activeWindowHours: 3,
    writesPerActiveMinute: 6,
    rowsWatchedPerSession: 400,
  },
] as const;

export type FrequencyOption = (typeof frequencyOptions)[number]["value"];
export type RealtimeOption = (typeof realtimeOptions)[number]["value"];

const MB_PER_GB = 1024;
const HOURS_PER_MONTH = 730;
const KB_PER_GB = 1024 * 1024;
/** Rows touched by one write transaction (the bench's 10-row write). */
const ROWS_PER_WRITE = 10;
/** Peak concurrency over the active window's average. */
const PEAK_FACTOR = 2;
/** Throughput of N shards relative to one, per shard (sharding experiments). */
const SHARD_EFFICIENCY = 0.7;
/** Above this share of an instance's capacity, another shard is recommended. */
const COMFORTABLE_UTILIZATION = 0.7;

export type Usage = {
  mau: number;
  frequency: FrequencyOption;
  realtime: RealtimeOption;
  blobStoragePerUserMb: number;
};

function nonNegative(value: number) {
  return Number.isFinite(value) ? Math.max(value, 0) : 0;
}

/** What an app at this usage asks of its backend, independent of platform. */
export function estimateDemand({ mau, frequency, realtime, blobStoragePerUserMb }: Usage) {
  const f = frequencyOptions.find((option) => option.value === frequency) ?? frequencyOptions[2];
  const r = realtimeOptions.find((option) => option.value === realtime) ?? realtimeOptions[1];
  const users = nonNegative(mau);

  const monthlyVisits = users * f.visitsPerUserPerMonth;
  const dailyVisits = monthlyVisits / f.activeDaysPerMonth;
  const sessionHours = r.averageSessionMinutes / 60;
  const averageConcurrent = (dailyVisits * sessionHours) / r.activeWindowHours;
  const peakSessions = users > 0 ? Math.max(1, averageConcurrent * PEAK_FACTOR) : 0;
  const peakWritesPerSecond = (peakSessions * r.writesPerActiveMinute) / 60;
  const monthlyWrites = monthlyVisits * r.averageSessionMinutes * r.writesPerActiveMinute;

  // Hours a scale-to-zero instance is up: the active window on active days,
  // shrinking towards zero when visits are too sparse to keep it warm.
  const idleGraceHours = 5 / 60;
  const visitsPerWindowHour = dailyVisits / r.activeWindowHours;
  const warmShare = 1 - Math.exp(-visitsPerWindowHour * (sessionHours + idleGraceHours));
  const upHours = Math.min(f.activeDaysPerMonth * r.activeWindowHours * warmShare, HOURS_PER_MONTH);

  const structuredGb =
    (users * r.structuredStoragePerUserMb * f.structuredStorageMultiplier) / MB_PER_GB;
  const blobGb = (users * nonNegative(blobStoragePerUserMb)) / MB_PER_GB;
  const egressGb =
    (monthlyVisits * r.structuredEgressMbPerVisit * r.egressFanoutMultiplier) / MB_PER_GB +
    blobGb * f.blobFetchRatio;

  return {
    monthlyVisits,
    peakSessions,
    peakWritesPerSecond,
    monthlyWrites,
    upHours,
    structuredGb,
    blobGb,
    egressGb,
    rowsWatchedPerSession: r.rowsWatchedPerSession,
  };
}

export type Demand = ReturnType<typeof estimateDemand>;

export type Platform = {
  id: "vercel" | "cloudflare" | "server";
  name: string;
  comingSoon?: boolean;
  /** One shard's worth of the platform, as shown to the reader. */
  instance: string;
  /** Cores the engine can keep busy. */
  usableCores: number;
  writeMs: number;
  sessionUsPerWrite: number;
  memoryGb: number;
  diskGb: number;
  /** Monthly platform cost for `shards` instances at this demand. */
  cost: (demand: Demand, shards: number, cpuUtilization: number) => number;
  costNote: string;
};

const over = (value: number, included: number) => Math.max(value - included, 0);

export const platforms: Platform[] = [
  {
    id: "vercel",
    name: "Vercel Sandbox",
    instance: "2 vCPU / 4 GB Sandbox",
    usableCores: 1.5,
    writeMs: 2,
    sessionUsPerWrite: 3.5,
    memoryGb: 3.5,
    // The Sandbox's Drive grows with the data.
    diskGb: Infinity,
    cost: (d, shards, cpu) => {
      const hours = d.upHours;
      const memory = shards * 4 * 0.0106 * hours;
      const activeCpu = shards * 2 * Math.max(cpu * 0.4, 0.05) * hours * 0.128;
      const proxy = shards * 12 * (hours / HOURS_PER_MONTH);
      const storage = d.structuredGb * 0.08 + d.blobGb * 0.023;
      const egress = over(d.egressGb, 1024) * 0.15;
      return 20 + memory + activeCpu + proxy + storage + egress;
    },
    costNote:
      "Pro plan, Sandbox memory and active CPU while awake, the sync proxy, Blob storage and transfer.",
  },
  {
    id: "cloudflare",
    name: "Cloudflare Durable Objects",
    comingSoon: true,
    instance: "Durable Object",
    usableCores: 1,
    writeMs: 8,
    sessionUsPerWrite: 6,
    memoryGb: 0.1,
    // Durable Object SQLite holds at most 10 GB per object.
    diskGb: 10,
    cost: (d, shards) => {
      const gbSeconds = shards * 0.128 * d.upHours * 3600;
      const duration = (over(gbSeconds, 400_000) / 1e6) * 12.5;
      const requests = (over(d.monthlyVisits + d.monthlyWrites / 20, 1e6) / 1e6) * 0.15;
      const rowsWritten = (over(d.monthlyWrites * ROWS_PER_WRITE * 2, 50e6) / 1e6) * 1;
      const rowsRead =
        (over(d.monthlyWrites * 100 + d.monthlyVisits * d.rowsWatchedPerSession, 25e9) / 1e6) *
        0.001;
      const storage = over(d.structuredGb, 5) * 0.2 + over(d.blobGb, 10) * 0.015;
      return 5 + duration + requests + rowsWritten + rowsRead + storage;
    },
    costNote:
      "Workers Paid plan, Durable Object duration, requests, rows and storage, and R2 for large values. Egress is free.",
  },
  {
    id: "server",
    name: "Your own server",
    instance: "4 vCPU / 16 GB Hetzner server",
    usableCores: 2.5,
    writeMs: 2,
    sessionUsPerWrite: 3.5,
    memoryGb: 14,
    // Attach volumes as the data grows.
    diskGb: Infinity,
    cost: (d, shards) => {
      const servers = shards * 32;
      const backups = shards * 6.4;
      const volumes = over(d.structuredGb, shards * 140) * 0.05;
      const objectStorage = d.blobGb > 0 ? Math.ceil(d.blobGb / 1024) * 6 : 0;
      const egress = (over(d.egressGb, shards * 20 * 1024) / 1024) * 1.2;
      return servers + backups + volumes + objectStorage + egress;
    },
    costNote:
      "Dedicated servers with backups, volumes past the 160 GB disk, Object Storage for large values, and traffic past the included 20 TB each.",
  },
];

/** The busiest share of one shard's CPU, memory and disk with `shards` shards. */
function utilization(platform: Platform, demand: Demand, shards: number) {
  const sessions = demand.peakSessions / shards;
  const msPerWrite = platform.writeMs + (sessions * platform.sessionUsPerWrite) / 1000;
  const capacity = platform.usableCores * 1000 * (shards > 1 ? SHARD_EFFICIENCY * shards : 1);
  const cpu = (demand.peakWritesPerSecond * msPerWrite) / capacity;
  const memory = (sessions * demand.rowsWatchedPerSession * 1.25) / KB_PER_GB / platform.memoryGb;
  const disk = demand.structuredGb / shards / platform.diskGb;
  return { cpu, worst: Math.max(cpu, memory, disk) };
}

/** The fewest shards that keep every shard under `limit` of its capacity. */
function shardsFor(platform: Platform, demand: Demand, limit: number) {
  const fits = (shards: number) => utilization(platform, demand, shards).worst <= limit;
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

export type PlatformEstimate = {
  platform: Platform;
  /** Shards needed to run at all, and to run with headroom. */
  requiredShards: number;
  recommendedShards: number;
  monthlyCost: number;
  /** The most MAU one instance serves comfortably at this usage. */
  mauPerInstance: number;
};

function mauPerInstance(platform: Platform, usage: Usage) {
  const fits = (mau: number) =>
    utilization(platform, estimateDemand({ ...usage, mau }), 1).worst <= COMFORTABLE_UTILIZATION;
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

export function estimatePlatforms(usage: Usage): PlatformEstimate[] {
  const demand = estimateDemand(usage);
  return platforms.map((platform) => {
    const requiredShards = shardsFor(platform, demand, 1);
    const recommendedShards = shardsFor(platform, demand, COMFORTABLE_UTILIZATION);
    const shards = Number.isFinite(recommendedShards) ? recommendedShards : requiredShards;
    const cpu = utilization(platform, demand, shards).cpu;
    return {
      platform,
      requiredShards,
      recommendedShards,
      monthlyCost: platform.cost(demand, shards, cpu),
      mauPerInstance: mauPerInstance(platform, usage),
    };
  });
}
