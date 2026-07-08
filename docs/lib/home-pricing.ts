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
    unit: "per CU/h",
    note: "1 CU ≈ 2 GB RAM ",
    included: "360 CU-hours/month included",
  },
  {
    name: "Storage",
    price: "$0.45",
    unit: "per GB-month",
    note: "Includes backups and caches.",
    included: "1GB-month included",
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
    activeMinutesPerActiveDay: 90,
    blobFetchRatio: 0.4,
    peakConcurrencyFraction: 0.12,
  },
  {
    value: "daily",
    label: "Daily",
    activeDaysPerMonth: 20,
    activeMinutesPerActiveDay: 30,
    blobFetchRatio: 0.22,
    peakConcurrencyFraction: 0.06,
  },
  {
    value: "weekly",
    label: "Weekly",
    activeDaysPerMonth: 4,
    activeMinutesPerActiveDay: 45,
    blobFetchRatio: 0.1,
    peakConcurrencyFraction: 0.025,
  },
  {
    value: "monthly",
    label: "Monthly",
    activeDaysPerMonth: 1,
    activeMinutesPerActiveDay: 60,
    blobFetchRatio: 0.04,
    peakConcurrencyFraction: 0.01,
  },
] as const;

export const realtimeOptions = [
  {
    value: "mostly-form-like",
    label: "Mostly form-like apps",
    cadenceLabel: "~1 interaction every 30s while active",
    interactionRateHz: 1 / 30,
    structuredEgressKbPerInteraction: 3,
    egressFanoutMultiplier: 1,
    structuredStoragePerUserMb: 8,
  },
  {
    value: "collaborative",
    label: "Collaborative / shared state",
    cadenceLabel: "~1 interaction every second while active",
    interactionRateHz: 1,
    structuredEgressKbPerInteraction: 4,
    egressFanoutMultiplier: 2.5,
    structuredStoragePerUserMb: 16,
  },
  {
    value: "live",
    label: "Live streams / fan-out",
    cadenceLabel: "~30 interactions every second while active",
    interactionRateHz: 30,
    structuredEgressKbPerInteraction: 2,
    egressFanoutMultiplier: 10,
    structuredStoragePerUserMb: 28,
  },
] as const;

export type FrequencyOption = (typeof frequencyOptions)[number]["value"];
export type RealtimeOption = (typeof realtimeOptions)[number]["value"];

const MB_PER_GB = 1024;
const KB_PER_GB = 1024 * 1024;
const SECONDS_PER_HOUR = 60 * 60;
const HOURS_PER_MONTH = 24 * 30;

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

  const monthlyActiveSecondsPerUser =
    selectedFrequency.activeDaysPerMonth * selectedFrequency.activeMinutesPerActiveDay * 60;
  const monthlyActiveSeconds = monthlyActiveUsers * monthlyActiveSecondsPerUser;
  const monthlyActiveHours = monthlyActiveSeconds / SECONDS_PER_HOUR;
  const estimatedComputeUptimeHours = Math.min(monthlyActiveHours, HOURS_PER_MONTH);
  const monthlyInteractions = monthlyActiveSeconds * selectedRealtime.interactionRateHz;

  const structuredStorageGb =
    (monthlyActiveUsers * selectedRealtime.structuredStoragePerUserMb) / MB_PER_GB;
  const blobStorageGb = (monthlyActiveUsers * blobStorageMb) / MB_PER_GB;
  const storageGbMonth = structuredStorageGb + blobStorageGb;

  const structuredEgressGb =
    (monthlyInteractions *
      selectedRealtime.structuredEgressKbPerInteraction *
      selectedRealtime.egressFanoutMultiplier) /
    KB_PER_GB;
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

  const peakConcurrentUsers = monthlyActiveUsers * selectedFrequency.peakConcurrencyFraction;

  return {
    monthlyActiveSecondsPerUser,
    monthlyInteractions,
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
    peakConcurrentUsers,
    computeCost,
    storageCost,
    egressCost,
    totalMonthlyCost,
  };
}
