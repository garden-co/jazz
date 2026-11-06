import "vitest";

declare global {
  interface Window {
    __jazz?: {
      shutdown(): Promise<void>;
    };
  }
}

export {};
