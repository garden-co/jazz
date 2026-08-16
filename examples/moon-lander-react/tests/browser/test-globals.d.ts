import "vitest";

declare module "vitest" {
  export interface ProvidedContext {
    jazzServerUrl: string;
  }
}
