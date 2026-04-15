"use client";

import { useState } from "react";
import {
  estimatePricing,
  frequencyOptions,
  realtimeOptions,
  type FrequencyOption,
  type RealtimeOption,
} from "@/lib/home-pricing";

const compactFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 1,
  notation: "compact",
});

const mauTicks = [
  { value: 1, label: "10" },
  { value: 2, label: "100" },
  { value: 3, label: "1k" },
  { value: 4, label: "10k" },
  { value: 5, label: "100k" },
  { value: 6, label: "1M" },
] as const;

const frequencyTicks = [
  { value: 0, label: "Multi-daily" },
  { value: 1, label: "Daily" },
  { value: 2, label: "Weekly" },
  { value: 3, label: "Monthly" },
] as const;

const realtimeTicks = [
  { value: 0, label: "Form-like" },
  { value: 1, label: "Collaborative" },
  { value: 2, label: "Live" },
] as const;

function formatCurrency(value: number) {
  if (value >= 1000)
    return compactFormatter
      .format(value)
      .replace(/[A-Z]$/, (match) => match.toUpperCase())
      .replace(/^/, "$");
  if (value >= 100) return `$${value.toFixed(0)}`;
  if (value >= 10) return `$${value.toFixed(1)}`;
  return `$${value.toFixed(2)}`;
}

function trimTrailingZero(value: string) {
  return value.replace(/\.0(?=[A-Za-z]$)/, "");
}

function formatCount(value: number) {
  if (value >= 1_000_000) {
    return trimTrailingZero(`${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}M`);
  }
  if (value >= 1_000) {
    return trimTrailingZero(`${(value / 1_000).toFixed(value >= 10_000 ? 0 : 1)}k`);
  }
  return `${Math.round(value)}`;
}

function formatOps(value: number) {
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return `${Math.round(value)}`;
}

function formatData(value: number) {
  if (value >= 100) return `${value.toFixed(0)} GB`;
  if (value >= 10) return `${value.toFixed(1)} GB`;
  if (value >= 1) return `${value.toFixed(2)} GB`;
  return `${(value * 1024).toFixed(0)} MB`;
}

function formatIops(value: number) {
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k`;
  return `${Math.round(value)}`;
}

function parseNumber(value: string) {
  const next = Number(value);
  if (!Number.isFinite(next)) return 0;
  return Math.max(next, 0);
}

function TickSlider({
  label,
  valueLabel,
  min,
  max,
  step = 1,
  value,
  onChange,
  ticks,
}: {
  label: string;
  valueLabel: string;
  min: number;
  max: number;
  step?: number;
  value: number;
  onChange: (value: number) => void;
  ticks: ReadonlyArray<{ value: number; label: string }>;
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-end justify-between gap-4">
        <span className="text-sm font-medium">{label}</span>
        <span className="text-sm leading-relaxed text-fd-muted-foreground">{valueLabel}</span>
      </div>
      <input
        className="w-full"
        max={max}
        min={min}
        step={step}
        type="range"
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
      <div
        className="grid gap-2 text-xs leading-relaxed text-fd-muted-foreground"
        style={{ gridTemplateColumns: `repeat(${ticks.length}, minmax(0, 1fr))` }}
      >
        {ticks.map((tick) => {
          const isActive = Math.abs(value - tick.value) < step / 2 + 0.001;

          return (
            <button
              key={tick.label}
              className={isActive ? "text-fd-foreground" : undefined}
              type="button"
              onClick={() => onChange(tick.value)}
            >
              {tick.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}

export function PricingCalculator() {
  const [mauLogValue, setMauLogValue] = useState(3);
  const [frequencyIndex, setFrequencyIndex] = useState(1);
  const [realtimeIndex, setRealtimeIndex] = useState(0);
  const [blobStoragePerUserMb, setBlobStoragePerUserMb] = useState(32);

  const mau = Math.max(1, Math.round(10 ** mauLogValue));
  const frequency = frequencyOptions[frequencyIndex]?.value ?? "daily";
  const realtime = realtimeOptions[realtimeIndex]?.value ?? "mostly-form-like";
  const selectedFrequency =
    frequencyOptions.find((option) => option.value === frequency) ?? frequencyOptions[1];
  const selectedRealtime =
    realtimeOptions.find((option) => option.value === realtime) ?? realtimeOptions[0];

  const estimate = estimatePricing({
    mau,
    frequency,
    realtime,
    blobStoragePerUserMb,
  });
  const monthlyCostPerUser = mau > 0 ? estimate.totalMonthlyCost / mau : 0;

  return (
    <div className="grid gap-12 lg:grid-cols-[minmax(0,0.78fr)_minmax(0,1.22fr)]">
      <div className="max-w-[30rem] space-y-4">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
          Estimator
        </p>
      </div>
      <div className="space-y-12">
        <div className="grid gap-6 sm:grid-cols-2">
          <TickSlider
            label="Monthly active users"
            max={6}
            min={1}
            step={0.05}
            ticks={mauTicks}
            value={mauLogValue}
            valueLabel={`${formatCount(mau)} MAUs`}
            onChange={setMauLogValue}
          />
          <TickSlider
            label="Frequency of use"
            max={frequencyTicks.length - 1}
            min={0}
            ticks={frequencyTicks}
            value={frequencyIndex}
            valueLabel={selectedFrequency.label}
            onChange={(value) => setFrequencyIndex(Math.round(value))}
          />
          <TickSlider
            label="Real-time profile"
            max={realtimeTicks.length - 1}
            min={0}
            ticks={realtimeTicks}
            value={realtimeIndex}
            valueLabel={selectedRealtime.label}
            onChange={(value) => setRealtimeIndex(Math.round(value))}
          />
          <label className="space-y-2">
            <span className="text-sm font-medium">Blob storage per user</span>
            <div className="flex items-center gap-3">
              <input
                className="w-full border border-fd-border bg-transparent px-3 py-3 text-base outline-none transition-colors focus:border-fd-foreground"
                inputMode="decimal"
                min="0"
                step="1"
                type="number"
                value={blobStoragePerUserMb}
                onChange={(event) => setBlobStoragePerUserMb(parseNumber(event.target.value))}
              />
              <span className="text-sm text-fd-muted-foreground">MB</span>
            </div>
          </label>
        </div>
        <div className="grid gap-4 text-sm leading-relaxed text-fd-muted-foreground sm:grid-cols-2">
          <p>
            {selectedFrequency.label} assumes about{" "}
            {(selectedFrequency.activeDaysPerMonth * selectedFrequency.activeMinutesPerActiveDay) /
              60}{" "}
            active hours per user each month.
          </p>
          <p>
            {selectedRealtime.label} assumes {selectedRealtime.cadenceLabel}
          </p>
        </div>
        <div className="space-y-8 border-t pt-8">
          <div className="grid gap-6 md:grid-cols-[minmax(0,1fr)_minmax(0,0.7fr)] md:items-end">
            <div className="space-y-2">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Estimated monthly bill
              </p>
              <p className="text-5xl font-black tracking-[-0.06em]">
                {formatCurrency(estimate.totalMonthlyCost)}
              </p>
            </div>
            <div className="border-t pt-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Cost per user / mo
              </p>
              <p className="mt-2 text-3xl font-black tracking-[-0.05em]">
                {formatCurrency(monthlyCostPerUser)}
              </p>
              <p className="mt-1 text-sm leading-relaxed text-fd-muted-foreground">
                Based on {formatCount(mau)} monthly active users
              </p>
            </div>
          </div>
          <p className="max-w-[24rem] text-sm leading-relaxed text-fd-muted-foreground">
            Rough self-serve estimate based on the draft public meters on this page. Enterprise
            contracts can still diverge.
          </p>
          <div className="grid gap-x-8 gap-y-6 sm:grid-cols-2 xl:grid-cols-4">
            <div className="border-t pt-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Monthly I/O
              </p>
              <p className="mt-2 text-3xl font-black tracking-[-0.05em]">
                {formatOps(estimate.monthlyIoOperations)}
              </p>
              <p className="mt-1 text-sm leading-relaxed text-fd-muted-foreground">
                {formatCurrency(estimate.ioCost)} at $0.15 per 1M ops
              </p>
            </div>
            <div className="border-t pt-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Stored data
              </p>
              <p className="mt-2 text-3xl font-black tracking-[-0.05em]">
                {formatData(estimate.storageGbMonth)}
              </p>
              <p className="mt-1 text-sm leading-relaxed text-fd-muted-foreground">
                {formatCurrency(estimate.storageCost)} at $0.45 per GB-month
              </p>
            </div>
            <div className="border-t pt-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Egress
              </p>
              <p className="mt-2 text-3xl font-black tracking-[-0.05em]">
                {formatData(estimate.egressGb)}
              </p>
              <p className="mt-1 text-sm leading-relaxed text-fd-muted-foreground">
                {formatCurrency(estimate.egressCost)} at $0.09 per GB out
              </p>
            </div>
            {/* <div className="border-t pt-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Rough peak IOPS
              </p>
              <p className="mt-2 text-3xl font-black tracking-[-0.05em]">
                {formatIops(estimate.peakIops)}
              </p>
              <p className="mt-1 text-sm leading-relaxed text-fd-muted-foreground">
                Planning signal only, not billed
              </p>
            </div> */}
          </div>
          <div className="grid gap-4 text-sm leading-relaxed text-fd-muted-foreground sm:grid-cols-2">
            <p>
              Structured storage baseline: {formatData(estimate.structuredStorageGb)}. Blob storage:{" "}
              {formatData(estimate.blobStorageGb)}.
            </p>
            <p>
              Structured sync egress: {formatData(estimate.structuredEgressGb)}. Blob egress:{" "}
              {formatData(estimate.blobEgressGb)}.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
