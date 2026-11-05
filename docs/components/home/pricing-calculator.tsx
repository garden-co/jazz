"use client";

import { useState } from "react";
import { Button } from "@astryxdesign/core/Button";
import {
  defaultAssumptions,
  estimatePlatforms,
  frequencyOptions,
  paramGroups,
  realtimeOptions,
  type Assumptions,
  type Param,
  type PlatformEstimate,
  jazzTiers,
  traitRows,
} from "@/lib/home-pricing";

/** Flip to true to show the license sliders and every model assumption. */
const SHOW_MODEL_DETAILS = false;

/** License list prices, used while the details are hidden. */
const LICENSE = { pro: 0, scaleBase: 200, per1k: 50 };

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

// Blob storage per user, as log10 of megabytes: 1 MB to 1 TB.
const blobTicks = [
  { value: 0, label: "1 MB" },
  { value: 1, label: "10 MB" },
  { value: 2, label: "100 MB" },
  { value: 3, label: "1 GB" },
  { value: 4, label: "10 GB" },
  { value: 5, label: "100 GB" },
  { value: 6, label: "1 TB" },
] as const;

function formatMb(mb: number) {
  if (mb >= 1e6) return `${trimTrailingZero((mb / 1e6).toFixed(1))} TB`;
  if (mb >= 1e3) return `${trimTrailingZero((mb / 1e3).toFixed(mb >= 1e4 ? 0 : 1))} GB`;
  return `${Math.round(mb)} MB`;
}

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
  return value.replace(/\.0(?=[A-Za-z]*$)/, "");
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

/** Width of the range input's thumb, which the tick labels line up with. */
const THUMB_PX = 16;

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
      <div>
        <span className="text-sm font-medium text-fd-muted-foreground">{label}</span>
        <p className="font-display mt-1 text-3xl font-black tracking-[-0.03em]">{valueLabel}</p>
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
      {/* Each label sits under its value on the track. The thumb's centre
          travels from half a thumb in at the left to half a thumb in at the
          right, so labels follow that span; the end labels align to the
          slider's edges instead of hanging past them. */}
      <div className="relative h-5 text-xs leading-relaxed text-fd-muted-foreground">
        {ticks.map((tick, index) => {
          const isActive = Math.abs(value - tick.value) < step / 2 + 0.001;
          const fraction = (tick.value - min) / (max - min);
          const first = index === 0;
          const last = index === ticks.length - 1;

          return (
            <button
              key={tick.label}
              className={`absolute top-0 whitespace-nowrap ${isActive ? "text-fd-foreground" : ""}`}
              style={
                first
                  ? { left: 0 }
                  : last
                    ? { right: 0 }
                    : {
                        left: `calc(${THUMB_PX / 2}px + ${fraction} * (100% - ${THUMB_PX}px))`,
                        transform: "translateX(-50%)",
                      }
              }
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

function visibleTraitRows(showMore: boolean) {
  return traitRows.filter((row) => showMore || !("more" in row && row.more));
}

function PlatformResult({
  estimate,
  assumptions,
  showMore,
  onHibernateChange,
}: {
  estimate: PlatformEstimate;
  assumptions: Assumptions;
  showMore: boolean;
  onHibernateChange: (hibernate: boolean) => void;
}) {
  const rows = visibleTraitRows(showMore);
  // Header, button, totals, cost breakdown, sharding note, one row per trait, and the cost
  // note with the details.
  const resultRows = (SHOW_MODEL_DETAILS ? 5 : 4) + rows.length + (showMore ? 1 : 0);
  const {
    platform,
    size,
    instances,
    requiredShards,
    platformCost,
    tier,
    licenseCost,
    totalCost,
    costPerMau,
    peakActiveSessions,
    mauPerInstance,
    largest,
  } = estimate;
  const sharded = instances > 1;
  const traits = { ...platform.traits, ...platform.traitsFor?.(assumptions) };
  const hibernate = assumptions["vercel.hibernate"] >= 0.5;
  return (
    <div
      className="grid grid-rows-subgrid gap-y-4 border-t pt-6"
      style={{ gridRow: `span ${resultRows}` }}
    >
      <div>
        <p className="text-base font-medium">{platform.name}</p>
        {SHOW_MODEL_DETAILS ? (
          <p className="mt-1 text-sm leading-relaxed text-fd-muted-foreground">
            One {largest.label}: {roundMau(mauPerInstance)} MAU at this usage
          </p>
        ) : null}
      </div>
      <div>
        <Button
          label={platform.cta.label}
          variant={platform.id === "vercel" ? "primary" : "secondary"}
          href={platform.cta.href}
          isDisabled={!platform.cta.href}
        />
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-sm text-fd-muted-foreground">Total / mo</p>
          <p className="font-display mt-1 text-3xl font-black tracking-[-0.03em]">
            {formatCurrency(totalCost)}
          </p>
        </div>
        <div className="text-right">
          <p className="text-sm text-fd-muted-foreground">Per MAU / mo</p>
          <p className="font-display mt-1 text-3xl font-black tracking-[-0.03em]">
            {formatCurrency(costPerMau)}
          </p>
        </div>
      </div>
      <dl className="grid grid-cols-[auto_1fr] content-start gap-x-4 gap-y-1 text-sm">
        <dt className="text-fd-muted-foreground">Runs on</dt>
        <dd className="text-right">
          {sharded ? `${formatCount(instances)} × ` : ""}
          {size.label}
        </dd>
        <dt className="text-fd-muted-foreground">{platform.billLabel}</dt>
        <dd className="text-right">{formatCurrency(platformCost)}</dd>
        <dt className="text-fd-muted-foreground">Jazz</dt>
        <dd className="text-right">
          {licenseCost === 0
            ? `${shortName(tier)}, free`
            : `${shortName(tier)} $${Math.round(licenseCost).toLocaleString("en-US")}`}
        </dd>
      </dl>
      {SHOW_MODEL_DETAILS ? (
        <p className="text-sm leading-relaxed">
          {sharded ? (
            <>
              {requiredShards > 1
                ? `Sharded: even the largest size needs ${formatCount(requiredShards)} shards.`
                : "Sharded: cheaper than one larger instance with headroom."}{" "}
              <span className="text-fd-muted-foreground">
                {tier === "scale"
                  ? `Jazz Scale is billed on ${formatCount(peakActiveSessions)} peak active sessions.`
                  : `Jazz Pro covers shards with up to ${assumptions["global.proRamGb"]} GB of RAM in total.`}
              </span>
            </>
          ) : null}
        </p>
      ) : null}
      {rows.map((row) => (
        <div key={row.key} className="border-t pt-3 text-sm leading-relaxed">
          <p className="text-xs text-fd-muted-foreground">{row.label}</p>
          {row.key === "coldWake" && platform.id === "vercel" ? (
            <div className="mt-0.5 flex flex-wrap items-baseline justify-between gap-x-3">
              <p>{traits[row.key]}</p>
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={hibernate}
                  onChange={(event) => onHibernateChange(event.target.checked)}
                />
                Hibernate when idle
              </label>
            </div>
          ) : (
            <p className="mt-0.5">{traits[row.key]}</p>
          )}
        </div>
      ))}
      {showMore ? (
        <p className="text-xs leading-relaxed text-fd-muted-foreground">{platform.costNote}</p>
      ) : null}
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

/** A tier's name without its license note, such as "Jazz OSS". */
const shortName = (tier: keyof typeof jazzTiers) => jazzTiers[tier].name.replace(/ \(.*\)$/, "");

export function PricingCalculator() {
  const [mauLogValue, setMauLogValue] = useState(3);
  const [frequencyIndex, setFrequencyIndex] = useState(2);
  const [realtimeIndex, setRealtimeIndex] = useState(1);
  const [blobLogMb, setBlobLogMb] = useState(0);
  const [licensePro, setLicensePro] = useState(LICENSE.pro);
  const [licenseScaleBase, setLicenseScaleBase] = useState(LICENSE.scaleBase);
  const [licensePer1k, setLicensePer1k] = useState(LICENSE.per1k);
  const [overrides, setOverrides] = useState<Assumptions>({});
  const [showMore, setShowMore] = useState(false);

  const mau = Math.max(1, Math.round(10 ** mauLogValue));
  const blobStoragePerUserMb = 10 ** blobLogMb;
  const selectedFrequency = frequencyOptions[frequencyIndex] ?? frequencyOptions[2];
  const selectedRealtime = realtimeOptions[realtimeIndex] ?? realtimeOptions[1];
  const assumptions = { ...defaultAssumptions, ...overrides };
  const groups = paramGroups(selectedFrequency.value, selectedRealtime.value);
  const changedCount = Object.keys(overrides).length;
  const setParam = (key: string, value: number, tolerance = 1e-9) =>
    setOverrides((current) => {
      const next = { ...current };
      if (Math.abs(value - defaultAssumptions[key]) < tolerance) delete next[key];
      else next[key] = value;
      return next;
    });

  const estimates = estimatePlatforms(
    {
      mau,
      frequency: selectedFrequency.value,
      realtime: selectedRealtime.value,
      blobStoragePerUserMb,
      licensePro,
      licenseScaleBase,
      licensePer1k,
    },
    assumptions,
  );

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
          valueLabel={`${formatCount(mau)} MAU`}
          onChange={setMauLogValue}
        />
        <TickSlider
          label="Frequency of use"
          max={frequencyTicks.length - 1}
          min={0}
          ticks={frequencyTicks}
          value={frequencyIndex}
          valueLabel={`${selectedFrequency.label} use`}
          onChange={(value) => setFrequencyIndex(Math.round(value))}
        />
        <TickSlider
          label="Real-time profile"
          max={realtimeTicks.length - 1}
          min={0}
          ticks={realtimeTicks}
          value={realtimeIndex}
          valueLabel={selectedRealtime.chosen}
          onChange={(value) => setRealtimeIndex(Math.round(value))}
        />
        <TickSlider
          label="Large values per user (files, images, rich text, durable streams)"
          max={6}
          min={0}
          step={0.05}
          ticks={blobTicks}
          value={blobLogMb}
          valueLabel={`${formatMb(blobStoragePerUserMb)} blob per user`}
          onChange={setBlobLogMb}
        />
        {SHOW_MODEL_DETAILS ? (
          <>
            <TickSlider
              label="Jazz Pro per month (draft)"
              max={100}
              min={0}
              step={1}
              ticks={[0, 20, 40, 60, 80, 100].map((value) => ({ value, label: `$${value}` }))}
              value={licensePro}
              valueLabel={`$${licensePro} / mo`}
              onChange={setLicensePro}
            />
            <TickSlider
              label="Jazz Scale per 1,000 peak active sessions (draft)"
              max={50}
              min={1}
              step={1}
              ticks={[1, 10, 20, 30, 40, 50].map((value) => ({ value, label: `$${value}` }))}
              value={licensePer1k}
              valueLabel={`$${licensePer1k} / mo`}
              onChange={setLicensePer1k}
            />
            <TickSlider
              label="Jazz Scale base price (draft)"
              max={1000}
              min={0}
              step={10}
              ticks={[0, 250, 500, 750, 1000].map((value) => ({ value, label: `$${value}` }))}
              value={licenseScaleBase}
              valueLabel={`$${licenseScaleBase} / mo`}
              onChange={setLicenseScaleBase}
            />
          </>
        ) : null}
      </div>
      {/* The results sit right under the sliders, so on phones both stay in
          view together. */}
      <div className="grid gap-x-8 gap-y-10 sm:grid-cols-2 xl:grid-cols-4">
        {estimates.map((estimate) => (
          <PlatformResult
            key={estimate.platform.id}
            estimate={estimate}
            assumptions={assumptions}
            showMore={showMore}
            onHibernateChange={(hibernate) => setParam("vercel.hibernate", hibernate ? 1 : 0)}
          />
        ))}
      </div>
      {showMore ? (
        <p className="max-w-[40rem] text-sm leading-relaxed text-fd-muted-foreground">
          Very rough estimates from our example-app benchmarks and each platform&apos;s list prices.
          Each platform picks the cheapest size and shard count, license included: Jazz OSS for one
          instance, Jazz Pro (${licensePro}/mo) for shards with up to{" "}
          {assumptions["global.proRamGb"]} GB of RAM in total, and Jazz Scale (${licenseScaleBase}
          /mo plus ${licensePer1k} per 1,000 peak active sessions) beyond that. A session is active
          in a minute when it writes, subscribes or gets an update. Vercel cold wakes are from our
          October measurements (0.6 to 1 s); Cloudflare&apos;s are estimates until its runtime
          ships.
        </p>
      ) : null}
      <div className="flex justify-center">
        <Button
          label={showMore ? "Fewer details" : "More details"}
          variant="secondary"
          aria-expanded={showMore}
          onClick={() => setShowMore((value) => !value)}
        />
      </div>
      {SHOW_MODEL_DETAILS ? (
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
                    onChange={(value) => setParam(p.key, value, p.step / 2)}
                  />
                ))}
              </fieldset>
            ))}
          </div>
        </details>
      ) : null}
    </div>
  );
}
