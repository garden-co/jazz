import { URL as NativeURL } from "whatwg-url-without-unicode";
import { toASCII } from "tr46";

/** The native URL parser needs the WHATWG domain-to-ASCII step restored. */
export class PlatformURL extends NativeURL {
  constructor(input: string | URL, base?: string | URL) {
    super(input, base);
    if (!this.hostname || this.hostname.startsWith("[")) return;
    const domain = toASCII(this.hostname, {
      checkHyphens: false,
      checkBidi: true,
      checkJoiners: true,
      useSTD3ASCIIRules: false,
      transitionalProcessing: false,
      verifyDNSLength: false,
      ignoreInvalidPunycode: false,
    });
    if (!domain || /[\u0000-\u0020#%/:<>?@[\\\]^|\u007f]/.test(domain)) {
      throw new TypeError("Invalid URL hostname");
    }
    // The setter runs IPv4 parsing after IDNA (including fullwidth digits).
    this.hostname = domain;
    const parts = domain.split(".");
    if (parts.at(-1) === "") parts.pop();
    const last = parts.at(-1) ?? "";
    const endsInNumber = /^[0-9]+$/.test(last) || /^0x[0-9a-f]*$/i.test(last);
    const ipv4 = /^(?:[0-9]+\.){3}[0-9]+$/.test(this.hostname);
    if ((endsInNumber && !ipv4) || (!ipv4 && this.hostname !== domain)) {
      throw new TypeError("Invalid URL hostname");
    }
  }
}
