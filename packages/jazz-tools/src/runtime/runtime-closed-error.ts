/**
 * @internal A runtime refused or ended a request because it was closed: not
 * an answer about the request itself. A `Db` that has handed over to a
 * successor retries such a read there.
 */
export class RuntimeClosedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RuntimeClosedError";
  }
}
