export const selfServePricing = {
  ioPerMillionOps: 0.15,
  storagePerGbMonth: 0.45,
  egressPerGb: 0.09,
} as const;

export const pricingMeters = [
  {
    name: "I/O",
    price: "$0.15",
    unit: "per 1M I/O operations",
    note: "Reads and writes against our SSDs.",
  },
  {
    name: "Storage",
    price: "$0.45",
    unit: "per GB-month",
    note: "Includes replication and caches.",
  },
  {
    name: "Egress",
    price: "$0.09",
    unit: "per GB out",
    note: "Passed through at cost.",
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
    ioOpsPerInteraction: 70,
    ioAmplification: 1.1,
    structuredEgressKbPerInteraction: 3,
    egressFanoutMultiplier: 1,
    structuredStoragePerUserMb: 8,
    peakBurstMultiplier: 1.3,
  },
  {
    value: "collaborative",
    label: "Collaborative / shared state",
    cadenceLabel: "~1 interaction every second while active",
    interactionRateHz: 1,
    ioOpsPerInteraction: 85,
    ioAmplification: 1.8,
    structuredEgressKbPerInteraction: 4,
    egressFanoutMultiplier: 2.5,
    structuredStoragePerUserMb: 16,
    peakBurstMultiplier: 1.6,
  },
  {
    value: "live",
    label: "Live streams / fan-out",
    cadenceLabel: "~30 interactions every second while active",
    interactionRateHz: 30,
    ioOpsPerInteraction: 18,
    ioAmplification: 2.4,
    structuredEgressKbPerInteraction: 2,
    egressFanoutMultiplier: 10,
    structuredStoragePerUserMb: 28,
    peakBurstMultiplier: 2.5,
  },
] as const;

export type FrequencyOption = (typeof frequencyOptions)[number]["value"];
export type RealtimeOption = (typeof realtimeOptions)[number]["value"];

const MB_PER_GB = 1024;
const OPS_PER_MILLION = 1_000_000;
const KB_PER_GB = 1024 * 1024;

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
  const monthlyInteractions = monthlyActiveSeconds * selectedRealtime.interactionRateHz;

  const monthlyIoOperations =
    monthlyInteractions * selectedRealtime.ioOpsPerInteraction * selectedRealtime.ioAmplification;

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

  const ioCost = (monthlyIoOperations / OPS_PER_MILLION) * selfServePricing.ioPerMillionOps;
  const storageCost = storageGbMonth * selfServePricing.storagePerGbMonth;
  const egressCost = egressGb * selfServePricing.egressPerGb;
  const totalMonthlyCost = ioCost + storageCost + egressCost;

  const activeWindowSeconds = monthlyActiveSeconds;
  const averageActiveIops = activeWindowSeconds > 0 ? monthlyIoOperations / activeWindowSeconds : 0;
  const peakConcurrentUsers = monthlyActiveUsers * selectedFrequency.peakConcurrencyFraction;
  const peakIops =
    peakConcurrentUsers *
    selectedRealtime.interactionRateHz *
    selectedRealtime.ioOpsPerInteraction *
    selectedRealtime.ioAmplification *
    selectedRealtime.peakBurstMultiplier;

  return {
    monthlyActiveSecondsPerUser,
    monthlyInteractions,
    monthlyIoOperations,
    structuredStorageGb,
    blobStorageGb,
    storageGbMonth,
    structuredEgressGb,
    blobEgressGb,
    egressGb,
    averageActiveIops,
    peakConcurrentUsers,
    peakIops,
    ioCost,
    storageCost,
    egressCost,
    totalMonthlyCost,
  };
}
