import type { Session } from "./context.js";
import { SYSTEM_READ_SESSION } from "./system-identity.js";

/** Per-Db identity overrides for clients shared by backend contexts. */
export class DbAccessContext {
  private constructor(
    private readonly session: Session | undefined,
    readonly attribution?: string,
  ) {}

  static forSession(session: Session): DbAccessContext {
    return new DbAccessContext(session);
  }

  /** Backend authority with writes credited to the supplied canonical author. */
  static forAttribution(attribution: string, session?: Session): DbAccessContext {
    return new DbAccessContext(session, attribution);
  }

  /** When attribution is present, this supplies provenance, not user authority. */
  get writeSession(): Session | undefined {
    return this.session;
  }

  get readSession(): Session | undefined {
    // Attribution supplies write provenance without restricting backend reads.
    return this.attribution !== undefined ? SYSTEM_READ_SESSION : this.session;
  }
}
