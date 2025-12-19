import type {
  DataPoint,
  Histogram,
  ResourceMetrics,
  ScopeMetrics,
} from "@opentelemetry/sdk-metrics";

type ActiveSubscription = {
  id: string;
  sources: string[];
  count: number;
};

export type PerfMetrics = {
  transport: {
    ingress: number;
    egress: number;
  };
  cojson: {
    available: number;
    loading: number;
    unknown: number;
    unavailable: number;
  };
  tools: {
    activeSubscriptions: ActiveSubscription[];
    loadTimes: {
      id: string;
      source_id?: string;
      parent_id?: string;
      resolve?: string;
      loadTime: number;
      loadFromStorage?: number;
      loadFromPeer?: number;
      transactionParsing?: number;
    }[];
  };
};

export function getActiveSubscriptions(
  metrics: ScopeMetrics[],
): ActiveSubscription[] {
  const scope = "jazz-tools";
  const name = "jazz.subscription.active";

  const dps = metrics
    .find((sm) => sm.scope.name === scope)
    ?.metrics.find((m) => m.descriptor.name === name)?.dataPoints as
    | DataPoint<number>[]
    | undefined;

  if (!dps) {
    return [];
  }

  const subs = new Map<string, ActiveSubscription>();

  for (const dp of dps) {
    if (dp.value === 0) {
      continue;
    }

    const id = dp.attributes.id as string;
    const source_id = dp.attributes.source_id as string | undefined;
    const value = dp.value;

    let sub = subs.get(id);
    if (!sub) {
      sub = { id, sources: [], count: 0 };
      subs.set(id, sub);
    }

    if (source_id) {
      sub.sources.push(source_id);
    }

    sub.count += value;
  }

  return Array.from(subs.values());
}

export function exportMetrics(
  resourceMetrics: ResourceMetrics[],
): PerfMetrics | null {
  const scopedMetrics = resourceMetrics.at(0)?.scopeMetrics;

  if (!scopedMetrics) {
    return null;
  }

  const ingress = getSumOfCounterMetric(
    scopedMetrics,
    "cojson-transport-ws",
    "jazz.usage.ingress",
  );
  const egress = getSumOfCounterMetric(
    scopedMetrics,
    "cojson-transport-ws",
    "jazz.usage.egress",
  );
  const availableCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "available",
    },
  );
  const loadingCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "loading",
    },
  );

  const unknownCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "unknown",
    },
  );

  const unavailableCoValues = getSumOfCounterMetric(
    scopedMetrics,
    "cojson",
    "jazz.covalues.loaded",
    {
      state: "unavailable",
    },
  );

  return {
    transport: {
      ingress,
      egress,
    },
    cojson: {
      available: availableCoValues,
      loading: loadingCoValues,
      unknown: unknownCoValues,
      unavailable: unavailableCoValues,
    },
    tools: {
      loadTimes: getLoadTimes(scopedMetrics),
      activeSubscriptions: getActiveSubscriptions(scopedMetrics),
    },
  };
}

function getSumOfCounterMetric(
  metrics: ScopeMetrics[],
  scope: string,
  name: string,
  attributes?: Record<string, string>,
) {
  const dp = metrics
    .find((sm) => sm.scope.name === scope)
    ?.metrics.find((m) => m.descriptor.name === name)?.dataPoints;

  if (!dp) {
    return 0;
  }

  return dp.reduce((acc, dp) => {
    if (typeof dp.value !== "number") {
      throw new Error(`Metric ${name} has a value that is not a number`);
    }

    // if attributes is defined, and the attributes do not match, skip this counter
    if (
      attributes &&
      !Object.keys(attributes).every(
        (key) => dp.attributes[key] === attributes[key],
      )
    ) {
      return acc;
    }

    return acc + dp.value;
  }, 0);
}

export function getLoadTimes(metrics: ScopeMetrics[]) {
  const scope = "jazz-tools";
  const name = "jazz.subscription.first_load";

  const dp = metrics
    .find((sm) => sm.scope.name === scope)
    ?.metrics.find((m) => m.descriptor.name === name)?.dataPoints as
    | DataPoint<Histogram>[]
    | undefined;

  if (!dp) {
    return [];
  }

  return dp.map((dp) => ({
    id: dp.attributes.id as string,
    source_id: dp.attributes.source_id as string | undefined,
    parent_id: dp.attributes.parent_id as string | undefined,
    resolve: dp.attributes.resolve as string | undefined,
    loadTime: dp.value.min ?? 0,
    loadFromStorage: dp.attributes.loadFromStorage as number | undefined,
    loadFromPeer: dp.attributes.loadFromPeer as number | undefined,
    transactionParsing: dp.attributes.transactionParsing as number | undefined,
  }));
}
