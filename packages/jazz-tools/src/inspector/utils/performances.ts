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
  resolve?: string;
};

export type LoadTimeMetric = {
  id: string;
  source_id?: string;
  parent_id?: string;
  parent_key?: string;
  resolve?: string;
  loadTime: number;
  loadFrom: "storage" | "network" | "memory";
  startTime: number;
};

export type OTelMetrics = {
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
  activeSubscriptions: ActiveSubscription[];
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
    loadTimes: LoadTimeMetric[];
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

    // In the inspector, we only want to count the top-level subscriptions
    if (source_id !== undefined) {
      continue;
    }

    const key = `${id}-${dp.attributes.resolve}`;

    let sub = subs.get(key);
    if (!sub) {
      sub = {
        id,
        sources: [],
        count: 0,
        resolve: dp.attributes.resolve as string,
      };
      subs.set(key, sub);
    }

    if (source_id) {
      sub.sources.push(source_id);
    }

    sub.count += value;
  }

  return Array.from(subs.values());
}

export function exportOTelMetrics(
  resourceMetrics: ResourceMetrics[],
): OTelMetrics | null {
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
    activeSubscriptions: getActiveSubscriptions(scopedMetrics),
  };
}

export function exportMetrics(
  resourceMetrics: ResourceMetrics[],
): PerfMetrics | null {
  const otelMetrics = exportOTelMetrics(resourceMetrics);

  if (!otelMetrics) {
    return null;
  }

  return {
    transport: otelMetrics.transport,
    cojson: otelMetrics.cojson,
    tools: {
      loadTimes: getLoadTimes(),
      activeSubscriptions: otelMetrics.activeSubscriptions,
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

export function getLoadTimes(): LoadTimeMetric[] {
  if (
    typeof performance === "undefined" ||
    !("getEntriesByType" in performance)
  ) {
    return [];
  }

  const measures = performance.getEntriesByType(
    "measure",
  ) as PerformanceMeasure[];
  const loadMeasures = measures.filter((measure) =>
    measure.name.startsWith("jazz.subscription.first_load."),
  );

  return loadMeasures
    .filter((measure) => !measure.detail?.parent_id)
    .map((measure) => {
      const detail = measure.detail || {};
      return {
        id: detail.id as string,
        source_id: detail.source_id as string | undefined,
        parent_id: detail.parent_id,
        parent_key: detail.parent_key,
        resolve: JSON.stringify(detail.resolve as any),
        loadTime: measure.duration,
        loadFrom: detail.loadFromStorage
          ? ("storage" as const)
          : detail.loadFromPeer
            ? ("network" as const)
            : ("memory" as const),
        startTime: performance.timeOrigin + measure.startTime,
      };
    });
}
