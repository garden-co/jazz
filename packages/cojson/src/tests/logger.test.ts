import { describe, expect, test, vi } from "vitest";
import { LogLevel, Logger } from "../logger";

describe("Logger", () => {
  describe("Log Level Filtering", () => {
    test("should respect log level hierarchy", () => {
      const mockLogSystem = {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      const logger = new Logger(LogLevel.WARN, mockLogSystem);

      logger.debug("Debug message");
      logger.info("Info message");
      logger.warn("Warning message");
      logger.error("Error message");

      expect(mockLogSystem.debug).not.toHaveBeenCalled();
      expect(mockLogSystem.info).not.toHaveBeenCalled();
      expect(mockLogSystem.warn).toHaveBeenCalledWith(
        "Warning message",
        undefined,
      );
      expect(mockLogSystem.error).toHaveBeenCalledWith(
        "Error message",
        undefined,
      );
    });

    test("should pass additional arguments to log system", () => {
      const mockLogSystem = {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      const logger = new Logger(LogLevel.DEBUG, mockLogSystem);

      logger.debug("Debug message", { foo: "bar" });

      expect(mockLogSystem.debug).toHaveBeenCalledWith("Debug message", {
        foo: "bar",
      });
    });
  });

  describe("Log System Configuration", () => {
    test("should allow changing log level at runtime", () => {
      const mockLogSystem = {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      const logger = new Logger(LogLevel.ERROR, mockLogSystem);

      logger.warn("Warning 1"); // Should not log
      expect(mockLogSystem.warn).not.toHaveBeenCalled();

      logger.setLevel(LogLevel.WARN);
      logger.warn("Warning 2"); // Should log
      expect(mockLogSystem.warn).toHaveBeenCalledWith("Warning 2", undefined);
    });

    test("should allow changing log system at runtime", () => {
      const mockLogSystem1 = {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      const mockLogSystem2 = {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      const logger = new Logger(LogLevel.INFO, mockLogSystem1);

      logger.info("Message 1");
      expect(mockLogSystem1.info).toHaveBeenCalledWith("Message 1", undefined);
      expect(mockLogSystem2.info).not.toHaveBeenCalled();

      logger.setLogSystem(mockLogSystem2);
      logger.info("Message 2");
      expect(mockLogSystem2.info).toHaveBeenCalledWith("Message 2", undefined);
      expect(mockLogSystem1.info).toHaveBeenCalledTimes(1);
    });
  });

  describe("Default Console Log System", () => {
    test("should use console methods by default", () => {
      const consoleSpy = {
        debug: vi.spyOn(console, "debug").mockImplementation(() => {}),
        info: vi.spyOn(console, "info").mockImplementation(() => {}),
        warn: vi.spyOn(console, "warn").mockImplementation(() => {}),
        error: vi.spyOn(console, "error").mockImplementation(() => {}),
      };

      const logger = new Logger();
      logger.setLevel(LogLevel.DEBUG);
      const testMessage = "Test message";
      const testArgs = { data: "test" };

      logger.debug(testMessage, testArgs);
      logger.info(testMessage, testArgs);
      logger.warn(testMessage, testArgs);
      logger.error(testMessage, testArgs);

      expect(consoleSpy.debug).toHaveBeenCalledWith(testMessage, testArgs);
      expect(consoleSpy.info).toHaveBeenCalledWith(testMessage, testArgs);
      expect(consoleSpy.warn).toHaveBeenCalledWith(testMessage, testArgs);
      expect(consoleSpy.error).toHaveBeenCalledWith(testMessage, testArgs);

      // Cleanup
      Object.values(consoleSpy).forEach((spy) => spy.mockRestore());
    });
  });

  describe("Log Level NONE", () => {
    test("should not log anything when level is NONE", () => {
      const mockLogSystem = {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      };

      const logger = new Logger(LogLevel.NONE, mockLogSystem);

      logger.debug("Debug message");
      logger.info("Info message");
      logger.warn("Warning message");
      logger.error("Error message");

      expect(mockLogSystem.debug).not.toHaveBeenCalled();
      expect(mockLogSystem.info).not.toHaveBeenCalled();
      expect(mockLogSystem.warn).not.toHaveBeenCalled();
      expect(mockLogSystem.error).not.toHaveBeenCalled();
    });
  });
});
