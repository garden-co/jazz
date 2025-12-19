import { z } from "../implementation/zodSchema/zodReExport.js";
import {
  Account,
  CoMap,
  CoProfileSchema,
  CoValueClass,
  Group,
  Simplify,
  TypeSym,
} from "../internal.js";

/** @category Identity & Permissions */
export class Profile extends CoMap<CoProfileSchema> {
  static fields = {
    name: { type: "json", field: z.string() } as const,
    inbox: { type: "json", field: z.string().optional() } as const,
    inboxInvite: { type: "json", field: z.string().optional() } as const,
  };

  declare readonly name: string;
  declare readonly inbox: string | undefined;
  declare readonly inboxInvite: string | undefined;
}
