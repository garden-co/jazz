import type { JsonValue } from "./jsonValue.js";

export const LogLevel = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
  NONE: 4,
} as const;
export type LogLevel = (typeof LogLevel)[keyof typeof LogLevel];

type ErrorAttributes = { err: unknown };

export interface LogSystem {
  debug(message: string, attributes?: Record<string, JsonValue>): void;
  info(message: string, attributes?: Record<string, JsonValue>): void;
  warn(
    message: string,
    attributes?: Record<string, JsonValue> | ErrorAttributes,
  ): void;
  error(
    message: string,
    attributes?: Record<string, JsonValue> | ErrorAttributes,
  ): void;
}

// Default console-based logging system
export class ConsoleLogSystem implements LogSystem {
  debug(message: string, attributes?: Record<string, JsonValue>) {
    console.debug(message, attributes);
  }
  info(message: string, attributes?: Record<string, JsonValue>) {
    console.info(message, attributes);
  }
  warn(
    message: string,
    attributes?: Record<string, JsonValue> | ErrorAttributes,
  ) {
    console.warn(message, attributes);
  }
  error(
    message: string,
    attributes?: Record<string, JsonValue> | ErrorAttributes,
  ) {
    console.error(message, attributes);
  }
}

export class Logger {
  private level: LogLevel;
  private logSystem: LogSystem;

  constructor(
    level: LogLevel = LogLevel.INFO,
    logSystem: LogSystem = new ConsoleLogSystem(),
  ) {
    this.level = level;
    this.logSystem = logSystem;
  }

  setLevel(level: LogLevel) {
    this.level = level;
  }

  setLogSystem(logSystem: LogSystem) {
    this.logSystem = logSystem;
  }

  debug(message: string, attributes?: Record<string, JsonValue>) {
    if (this.level <= LogLevel.DEBUG) {
      this.logSystem.debug(message, attributes);
    }
  }

  info(message: string, attributes?: Record<string, JsonValue>) {
    if (this.level <= LogLevel.INFO) {
      this.logSystem.info(message, attributes);
    }
  }

  warn(
    message: string,
    attributes?: Record<string, JsonValue> | ErrorAttributes,
  ) {
    if (this.level <= LogLevel.WARN) {
      this.logSystem.warn(message, attributes);
    }
  }

  error(
    message: string,
    attributes?: Record<string, JsonValue> | ErrorAttributes,
  ) {
    if (this.level <= LogLevel.ERROR) {
      this.logSystem.error(message, attributes);
    }
  }
}

// Create default logger instance
export const logger = new Logger();
