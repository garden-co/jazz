/** Nothing that signs anyone in reaches a log or a result. */
export function redact(text) {
  return String(text)
    .replace(/jazz-auth-v\d:[A-Za-z0-9_=+/-]+/g, "jazz-auth:[redacted]")
    .replace(/eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, "[redacted token]")
    .replace(/((?:secret|password)["' ]*[:=]\s*)["']?[^\s"',}]+/gi, "$1[redacted]");
}
