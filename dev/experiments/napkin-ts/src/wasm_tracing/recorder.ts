/** Collects one span's or event's fields into display text and a field map. */
export class StringRecorder {
  display = "";
  isFollowingArgs = false;
  fields = new Map<string, string>();
  message: string | undefined;

  private recordField(name: string, debug: string, raw: string): void {
    if (this.isFollowingArgs) this.display += "\n";
    else {
      this.display += " ";
      this.isFollowingArgs = true;
    }
    this.display += `${name} = ${debug};`;
    this.fields.set(name, raw);
  }

  record(name: string, value: unknown): void {
    const debug = typeof value === "string" ? JSON.stringify(value) : String(value);
    if (name === "message") {
      this.message = trimDebugMessage(debug);
      this.display = this.display ? `${debug}\n${this.display}` : debug;
    } else this.recordField(name, debug, typeof value === "string" ? value : debug);
  }

  toString(): string {
    return this.display ? ` ${this.display}` : "";
  }
}

function trimDebugMessage(message: string): string {
  return message.startsWith('"') && message.endsWith('"') && message.length >= 2
    ? message.slice(1, -1)
    : message;
}
