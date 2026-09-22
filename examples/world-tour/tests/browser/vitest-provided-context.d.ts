import "vitest";

declare module "vitest" {
  export interface ProvidedContext {
    worldTourJazzServerUrl: string;
  }
}
