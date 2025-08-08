/**
    In order to not block other concurrently syncing CoValues we introduce a maximum size of transactions,
    since they are the smallest unit of progress that can be synced within a CoValue.
    This is particularly important for storing binary data in CoValues, since they are likely to be at least on the order of megabytes.
    This also means that we want to keep signatures roughly after each MAX_RECOMMENDED_TX size chunk,
    to be able to verify partially loaded CoValues or CoValues that are still being created (like a video live stream).
**/
export const MAX_RECOMMENDED_TX_SIZE = 100 * 1024;

export const CO_VALUE_LOADING_CONFIG = {
  MAX_RETRIES: 1,
  TIMEOUT: 30_000,
  RETRY_DELAY: 3000,
};

export function setCoValueLoadingMaxRetries(maxRetries: number) {
  CO_VALUE_LOADING_CONFIG.MAX_RETRIES = maxRetries;
}

export function setCoValueLoadingTimeout(timeout: number) {
  CO_VALUE_LOADING_CONFIG.TIMEOUT = timeout;
}

export function setCoValueLoadingRetryDelay(delay: number) {
  CO_VALUE_LOADING_CONFIG.RETRY_DELAY = delay;
}

export const SYNC_SCHEDULER_CONFIG = {
  INCOMING_MESSAGES_TIME_BUDGET: 50,
};

export function setIncomingMessagesTimeBudget(budget: number) {
  SYNC_SCHEDULER_CONFIG.INCOMING_MESSAGES_TIME_BUDGET = budget;
}

export const GARBAGE_COLLECTOR_CONFIG = {
  MAX_AGE: 1000 * 60 * 10, // 10 minutes
  INTERVAL: 1000 * 60 * 5, // 5 minutes
};

export function setGarbageCollectorMaxAge(maxAge: number) {
  GARBAGE_COLLECTOR_CONFIG.MAX_AGE = maxAge;
}

export function setGarbageCollectorInterval(interval: number) {
  GARBAGE_COLLECTOR_CONFIG.INTERVAL = interval;
}
