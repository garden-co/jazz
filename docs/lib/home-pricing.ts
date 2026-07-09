export const selfServePricing = {
  compute: 0.039,
  storagePerGbMonth: 0.45,
  egressPerGb: 0.09,
} as const;

export const selfServeFreeTier = {
  computeCuHours: (24 * 30) / 2,
  storageGbMonth: 1,
  egressGb: 5,
} as const;

export const pricingMeters = [
  {
    name: "Compute",
    price: "$0.039",
    unit: "per hour of 2GB RAM instance",
    note: "Auto-scaling available soon",
    included: "1GB RAM instance always included",
  },
  {
    name: "Storage",
    price: "$0.45",
    unit: "per GB/month",
    note: "Includes backups and caches.",
    included: "1GB/month included",
  },
  {
    name: "Egress",
    price: "$0.09",
    unit: "per GB out",
    note: "Passed through at cost.",
    included: "5GB/month included",
  },
] as const;

export const frequencyOptions = [
  {
    value: "multiple-daily",
    label: "Multiple times daily",
    activeDaysPerMonth: 22,
    visitsPerUserPerMonth: 44,
    structuredStorageMultiplier: 1,
    blobFetchRatio: 0.4,
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
    value: "weekly",
    label: "Weekly",
    activeDaysPerMonth: 4,
    visitsPerUserPerMonth: 4,
    structuredStorageMultiplier: 1,
    blobFetchRatio: 0.1,
  },
  {
    value: "monthly",
    label: "Monthly",
    activeDaysPerMonth: 1,
    visitsPerUserPerMonth: 1,
    structuredStorageMultiplier: 0.75,
    blobFetchRatio: 0.04,
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
  },
  {
    value: "collaborative",
    label: "Collaborative / shared state",
    sessionLabel: "~20 minute sessions over the active window",
    averageSessionMinutes: 20,
    structuredEgressMbPerVisit: 2,
    egressFanoutMultiplier: 2.5,
    structuredStoragePerUserMb: 16,
  },
  {
    value: "live",
    label: "Live streams / fan-out",
    sessionLabel: "~45 minute sessions in a concentrated event window",
    averageSessionMinutes: 45,
    structuredEgressMbPerVisit: 4,
    egressFanoutMultiplier: 10,
    structuredStoragePerUserMb: 28,
  },
] as const;

export type FrequencyOption = (typeof frequencyOptions)[number]["value"];
export type RealtimeOption = (typeof realtimeOptions)[number]["value"];

const MB_PER_GB = 1024;
const HOURS_PER_DAY = 24;
const HOURS_PER_MONTH = 24 * 30;
const IDLE_SHUTDOWN_GRACE_MINUTES = 15;
const SIMULTANEOUS_USERS_PER_INSTANCE = 200;

function clampNonNegative(value: number) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(value, 0);
}

export function estimatePricing({
  mau,
  frequency,
  realtime,
  blobStoragePerUserMb,
}: {
  mau: number;
  frequency: FrequencyOption;
  realtime: RealtimeOption;
  blobStoragePerUserMb: number;
}) {
  const selectedFrequency =
    frequencyOptions.find((option) => option.value === frequency) ?? frequencyOptions[1];
  const selectedRealtime =
    realtimeOptions.find((option) => option.value === realtime) ?? realtimeOptions[1];

  const monthlyActiveUsers = clampNonNegative(mau);
  const blobStorageMb = clampNonNegative(blobStoragePerUserMb);

  const monthlyVisits = monthlyActiveUsers * selectedFrequency.visitsPerUserPerMonth;
  const dailyVisits =
    selectedFrequency.activeDaysPerMonth > 0
      ? monthlyVisits / selectedFrequency.activeDaysPerMonth
      : 0;
  const sessionFootprintHours =
    (selectedRealtime.averageSessionMinutes + IDLE_SHUTDOWN_GRACE_MINUTES) / 60;
  const dailyComputeUptimeHours =
    HOURS_PER_DAY * (1 - Math.exp(-(dailyVisits * sessionFootprintHours) / HOURS_PER_DAY));
  const estimatedSingleInstanceUptimeHours = Math.min(
    selectedFrequency.activeDaysPerMonth * dailyComputeUptimeHours,
    HOURS_PER_MONTH,
  );
  const estimatedSimultaneousUsers =
    estimatedSingleInstanceUptimeHours > 0
      ? (monthlyVisits * sessionFootprintHours) / estimatedSingleInstanceUptimeHours
      : 0;
  const computeInstanceMultiplier = Math.max(
    1,
    Math.ceil(estimatedSimultaneousUsers / SIMULTANEOUS_USERS_PER_INSTANCE),
  );
  const estimatedComputeUptimeHours =
    estimatedSingleInstanceUptimeHours * computeInstanceMultiplier;

  const structuredStorageGb =
    (monthlyActiveUsers *
      selectedRealtime.structuredStoragePerUserMb *
      selectedFrequency.structuredStorageMultiplier) /
    MB_PER_GB;
  const blobStorageGb = (monthlyActiveUsers * blobStorageMb) / MB_PER_GB;
  const storageGbMonth = structuredStorageGb + blobStorageGb;

  const structuredEgressGb =
    (monthlyVisits *
      selectedRealtime.structuredEgressMbPerVisit *
      selectedRealtime.egressFanoutMultiplier) /
    MB_PER_GB;
  const blobEgressGb = blobStorageGb * selectedFrequency.blobFetchRatio;
  const egressGb = structuredEgressGb + blobEgressGb;

  const billableComputeUptimeHours = Math.max(
    estimatedComputeUptimeHours - selfServeFreeTier.computeCuHours,
    0,
  );
  const billableStorageGbMonth = Math.max(storageGbMonth - selfServeFreeTier.storageGbMonth, 0);
  const billableEgressGb = Math.max(egressGb - selfServeFreeTier.egressGb, 0);
  const isWithinFreeTier =
    billableComputeUptimeHours === 0 && billableStorageGbMonth === 0 && billableEgressGb === 0;

  const computeCost = billableComputeUptimeHours * selfServePricing.compute;
  const storageCost = billableStorageGbMonth * selfServePricing.storagePerGbMonth;
  const egressCost = billableEgressGb * selfServePricing.egressPerGb;
  const totalMonthlyCost = computeCost + storageCost + egressCost;

  return {
    monthlyVisits,
    dailyVisits,
    sessionFootprintHours,
    dailyComputeUptimeHours,
    estimatedSingleInstanceUptimeHours,
    estimatedSimultaneousUsers,
    computeInstanceMultiplier,
    estimatedComputeUptimeHours,
    structuredStorageGb,
    blobStorageGb,
    storageGbMonth,
    structuredEgressGb,
    blobEgressGb,
    egressGb,
    billableComputeUptimeHours,
    billableStorageGbMonth,
    billableEgressGb,
    isWithinFreeTier,
    computeCost,
    storageCost,
    egressCost,
    totalMonthlyCost,
  };
}
