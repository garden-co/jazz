"use client";

import { useState } from "react";
import {
  defaultAssumptions,
  estimatePlatforms,
  frequencyOptions,
  paramGroups,
  realtimeOptions,
  type Assumptions,
  type Param,
  type PlatformEstimate,
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
  { value: 0, label: "Monthly" },
  { value: 1, label: "Weekly" },
  { value: 2, label: "Daily" },
  { value: 3, label: "Multi-daily" },
] as const;

const realtimeTicks = [
  { value: 0, label: "Form-like" },
  { value: 1, label: "Collaborative" },
  { value: 2, label: "Live" },
] as const;

function formatCurrency(value: number) {
  if (value === 0) return "$0";
  if (value >= 1000)
    return compactFormatter
      .format(value)
      .replace(/[A-Z]$/, (match) => match.toUpperCase())
      .replace(/^/, "$");
  if (value >= 100) return `$${value.toFixed(0)}`;
  if (value >= 10) return `$${value.toFixed(1)}`;
  if (value >= 0.01) return `$${value.toFixed(2)}`;
  return `$${value.toFixed(4)}`;
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

function roundMau(value: number) {
  if (value <= 0) return "none";
  const magnitude = 10 ** Math.floor(Math.log10(value));
  return `~${formatCount(Math.floor(value / magnitude) * magnitude)}`;
}

function PlatformResult({
  estimate,
  instanceLabel,
  licensePerShard,
}: {
  estimate: PlatformEstimate;
  instanceLabel: string;
  licensePerShard: number;
}) {
  const {
    platform,
    requiredShards,
    recommendedShards,
    instances,
    platformCost,
    licenseCost,
    totalCost,
    costPerMau,
    mauPerInstance,
  } = estimate;
  const sharded = instances > 1;
  return (
    <div className="flex flex-col gap-4 border-t pt-6">
      <div>
        <p className="text-base font-medium">
          {platform.name}
          {platform.comingSoon ? (
            <span className="ml-2 align-middle text-xs font-normal text-fd-muted-foreground">
              coming soon
            </span>
          ) : null}
        </p>
        <p className="mt-1 text-sm leading-relaxed text-fd-muted-foreground">
          One {instanceLabel}: {roundMau(mauPerInstance)} MAU at this usage
        </p>
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-sm text-fd-muted-foreground">Total / mo</p>
          <p className="font-display mt-1 text-3xl font-black tracking-[-0.03em]">
            {formatCurrency(totalCost)}
          </p>
        </div>
        <div>
          <p className="text-sm text-fd-muted-foreground">Per MAU / mo</p>
          <p className="font-display mt-1 text-3xl font-black tracking-[-0.03em]">
            {formatCurrency(costPerMau)}
          </p>
        </div>
      </div>
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
        <dt className="text-fd-muted-foreground">
          Platform{sharded ? `, ${formatCount(instances)} instances` : ""}
        </dt>
        <dd className="text-right">{formatCurrency(platformCost)}</dd>
        <dt className="text-fd-muted-foreground">Jazz</dt>
        <dd className="text-right">
          {sharded
            ? `${formatCount(instances)} × $${licensePerShard} = ${formatCurrency(licenseCost)}`
            : "Free (MIT)"}
        </dd>
      </dl>
      {sharded ? (
        <p className="text-sm leading-relaxed">
          {requiredShards > 1
            ? `${formatCount(requiredShards)} shards required${
                recommendedShards > requiredShards
                  ? `, ${formatCount(recommendedShards)} recommended`
                  : ""
              }.`
            : `Fits on one, ${formatCount(recommendedShards)} shards recommended.`}{" "}
          <span className="text-fd-muted-foreground">Pricing for Jazz Pro with sharding TBA.</span>
        </p>
      ) : null}
      <p className="text-xs leading-relaxed text-fd-muted-foreground">{platform.costNote}</p>
    </div>
  );
}

function formatParam(value: number, step: number) {
  const decimals = Math.max(0, Math.min(4, -Math.floor(Math.log10(step))));
  return value.toFixed(decimals);
}

function AssumptionSlider({
  param,
  value,
  onChange,
}: {
  param: Param;
  value: number;
  onChange: (value: number) => void;
}) {
  const changed = Math.abs(value - param.default) > param.step / 2;
  return (
    <label className="grid gap-1 text-xs">
      <span className="flex justify-between gap-3">
        <span className={changed ? "font-medium" : "text-fd-muted-foreground"}>{param.label}</span>
        <span className="tabular-nums">
          {formatParam(value, param.step)}
          {changed ? (
            <span className="ml-1 text-fd-muted-foreground">
              (was {formatParam(param.default, param.step)})
            </span>
          ) : null}
        </span>
      </span>
      <input
        className="w-full"
        type="range"
        min={param.min}
        max={param.max}
        step={param.step}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </label>
  );
}

export function PricingCalculator() {
  const [mauLogValue, setMauLogValue] = useState(4);
  const [frequencyIndex, setFrequencyIndex] = useState(2);
  const [realtimeIndex, setRealtimeIndex] = useState(1);
  const [blobStoragePerUserMb, setBlobStoragePerUserMb] = useState(32);
  const [licensePerShard, setLicensePerShard] = useState(50);
  const [overrides, setOverrides] = useState<Assumptions>({});

  const mau = Math.max(1, Math.round(10 ** mauLogValue));
  const selectedFrequency = frequencyOptions[frequencyIndex] ?? frequencyOptions[2];
  const selectedRealtime = realtimeOptions[realtimeIndex] ?? realtimeOptions[1];
  const assumptions = { ...defaultAssumptions, ...overrides };
  const groups = paramGroups(selectedFrequency.value, selectedRealtime.value);
  const changedCount = Object.keys(overrides).length;

  const estimates = estimatePlatforms(
    {
      mau,
      frequency: selectedFrequency.value,
      realtime: selectedRealtime.value,
      blobStoragePerUserMb,
      licensePerShard,
    },
    assumptions,
  );
  const vcpus = assumptions["vercel.vcpus"];
  const instanceLabels: Record<string, string> = {
    vercel: `${vcpus} vCPU / ${vcpus * 2} GB Sandbox`,
  };

  return (
    <div className="space-y-8">
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
        <div className="space-y-3 sm:col-span-2">
          <div className="flex items-end justify-between gap-4">
            <span className="text-sm font-medium">Jazz Pro license per shard (draft)</span>
            <span className="text-sm text-fd-muted-foreground">
              ${licensePerShard} / shard / mo
            </span>
          </div>
          <input
            className="w-full"
            type="range"
            min={5}
            max={200}
            step={5}
            value={licensePerShard}
            onChange={(event) => setLicensePerShard(Number(event.target.value))}
          />
        </div>
      </div>
      {/* The results sit right under the sliders, so on phones both stay in
          view together. */}
      <div className="grid gap-8 sm:grid-cols-3">
        {estimates.map((estimate) => (
          <PlatformResult
            key={estimate.platform.id}
            estimate={estimate}
            instanceLabel={instanceLabels[estimate.platform.id] ?? estimate.platform.instance}
            licensePerShard={licensePerShard}
          />
        ))}
      </div>
      <p className="max-w-[40rem] text-sm leading-relaxed text-fd-muted-foreground">
        Very rough estimates from our example-app benchmarks and each platform&apos;s list prices.
        An app gets another shard once an instance passes the target peak utilization; one instance
        is free under MIT.
      </p>
      <details open className="border-t pt-6">
        <summary className="cursor-pointer text-sm font-medium">
          Model assumptions (debug){changedCount > 0 ? `, ${changedCount} changed` : ""}
        </summary>
        <div className="mt-4">
          <button
            type="button"
            className="text-sm underline disabled:opacity-50"
            disabled={changedCount === 0}
            onClick={() => setOverrides({})}
          >
            Reset all to defaults
          </button>
        </div>
        <div className="mt-6 grid gap-x-10 gap-y-8 sm:grid-cols-2 lg:grid-cols-3">
          {groups.map((group) => (
            <fieldset key={group.id} className="grid content-start gap-3">
              <legend className="mb-3 text-sm font-medium">{group.title}</legend>
              {group.params.map((p) => (
                <AssumptionSlider
                  key={p.key}
                  param={p}
                  value={assumptions[p.key]}
                  onChange={(value) =>
                    setOverrides((current) => {
                      const next = { ...current };
                      if (Math.abs(value - p.default) < p.step / 2) delete next[p.key];
                      else next[p.key] = value;
                      return next;
                    })
                  }
                />
              ))}
            </fieldset>
          ))}
        </div>
      </details>
    </div>
  );
}
