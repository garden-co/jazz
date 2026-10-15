import type { SortedMap } from "../../support/collections.js";
import type { AuthorSubject, Value } from "../../_extern/engine.js";
import type { SourceId } from "./input.js";
import type { PolicyDecisionRole } from "./output.js";

export type PolicyContext =
  | { kind: "System" }
  | {
      kind: "Identity";
      mode: PolicyEnforcementMode;
      permissionSubject: AuthorSubject;
      claims: SortedMap<string, Value>;
      attribution?: AuthorSubject;
    }
  | {
      kind: "AuthorizationSubplan";
      protectedSource: SourceId;
      role: PolicyDecisionRole;
      mode: PolicyEnforcementMode;
      permissionSubject: AuthorSubject;
      claims: SortedMap<string, Value>;
      attribution?: AuthorSubject;
    };

export type PolicyEnforcementMode = "PermissiveLocal" | "Enforcing";
