import "vitest";

declare module "vitest" {
  export interface ProvidedContext {
    jazzServerUrl: string;
  }
}

declare global {
  interface Window {
    __jazz?: {
      shutdown(): Promise<void>;
    };
  }
}

export {};
