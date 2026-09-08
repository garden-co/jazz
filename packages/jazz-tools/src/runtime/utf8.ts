/** Internal UTF-8 decoding without requiring browser globals on native hosts. */
export class Utf8Decoder {
  private readonly native: TextDecoder | undefined;
  private readonly fatal: boolean;

  constructor(options: { fatal?: boolean } = {}) {
    this.fatal = options.fatal ?? false;
    this.native =
      typeof globalThis.TextDecoder === "function"
        ? new globalThis.TextDecoder("utf-8", options)
        : undefined;
  }

  decode(input: Uint8Array | ArrayBuffer = new Uint8Array()): string {
    if (this.native) return this.native.decode(input);
    const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
    const output: string[] = [];
    const invalid = () => {
      if (this.fatal) throw new TypeError("The encoded data was not valid UTF-8");
      output.push("\ufffd");
    };
    let codePoint = 0;
    let remaining = 0;
    let lower = 0x80;
    let upper = 0xbf;
    // WHATWG UTF-8 decoding: an invalid continuation is reprocessed as a new
    // leading byte. This preserves replacement behavior as well as strict mode.
    for (let index = 0; index < bytes.length; index++) {
      const byte = bytes[index]!;
      if (remaining === 0) {
        if (byte <= 0x7f) output.push(String.fromCharCode(byte));
        else if (byte >= 0xc2 && byte <= 0xdf) {
          remaining = 1;
          codePoint = byte & 0x1f;
        } else if (byte >= 0xe0 && byte <= 0xef) {
          remaining = 2;
          codePoint = byte & 0x0f;
          if (byte === 0xe0) lower = 0xa0;
          if (byte === 0xed) upper = 0x9f;
        } else if (byte >= 0xf0 && byte <= 0xf4) {
          remaining = 3;
          codePoint = byte & 0x07;
          if (byte === 0xf0) lower = 0x90;
          if (byte === 0xf4) upper = 0x8f;
        } else invalid();
      } else if (byte < lower || byte > upper) {
        remaining = 0;
        lower = 0x80;
        upper = 0xbf;
        invalid();
        index--;
      } else {
        lower = 0x80;
        upper = 0xbf;
        codePoint = (codePoint << 6) | (byte & 0x3f);
        if (--remaining === 0) output.push(String.fromCodePoint(codePoint));
      }
    }
    if (remaining !== 0) invalid();
    // TextDecoder's default ignoreBOM=false strips only an initial UTF-8 BOM.
    if (output[0] === "\ufeff") output.shift();
    return output.join("");
  }
}
