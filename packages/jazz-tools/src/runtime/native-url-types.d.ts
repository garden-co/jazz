declare module "whatwg-url-without-unicode" {
  export const URL: typeof globalThis.URL;
}
declare module "tr46" {
  export function toASCII(
    domain: string,
    options: {
      checkHyphens: boolean;
      checkBidi: boolean;
      checkJoiners: boolean;
      useSTD3ASCIIRules: boolean;
      transitionalProcessing: boolean;
      verifyDNSLength: boolean;
      ignoreInvalidPunycode: boolean;
    },
  ): string | null;
}
