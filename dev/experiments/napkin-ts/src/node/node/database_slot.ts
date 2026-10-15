// Ownership of the Groove database during node construction and extraction.
export class DatabaseSlot {
  constructor(private database: any | undefined) {}

  take(): any {
    const db = this.database;
    if (db === undefined) throw new Error("node database slot must be populated outside rebuild");
    this.database = undefined;
    return db;
  }

  intoInner(): any {
    return this.take();
  }

  get(): any {
    if (this.database === undefined) throw new Error("node database slot must be populated");
    return this.database;
  }
}
