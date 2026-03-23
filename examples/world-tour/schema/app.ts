// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface File {
  id: string;
  name: string;
  mimeType: string;
  partIds: string[];
  partSizes: number[];
}

export interface FilePart {
  id: string;
  data: Uint8Array;
}

export interface Band {
  id: string;
  name: string;
  logoFileId?: string;
}

export interface Venue {
  id: string;
  name: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  capacity?: number;
}

export interface Member {
  id: string;
  bandId: string;
  userId: string;
}

export interface Stop {
  id: string;
  bandId: string;
  venueId: string;
  date: Date;
  status: "cancelled" | "confirmed" | "tentative";
  publicDescription: string;
  privateNotes?: string;
}

export interface FileInit {
  name: string;
  mimeType: string;
  partIds: string[];
  partSizes: number[];
}

export interface FilePartInit {
  data: Uint8Array;
}

export interface BandInit {
  name: string;
  logoFileId?: string;
}

export interface VenueInit {
  name: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  capacity?: number;
}

export interface MemberInit {
  bandId: string;
  userId: string;
}

export interface StopInit {
  bandId: string;
  venueId: string;
  date: Date;
  status: "cancelled" | "confirmed" | "tentative";
  publicDescription: string;
  privateNotes?: string;
}

export interface FileWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  mimeType?: string | { eq?: string; ne?: string; contains?: string };
  partIds?: string[] | { eq?: string[]; contains?: string };
  partSizes?: number[] | { eq?: number[]; contains?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface FilePartWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  data?: Uint8Array | { eq?: Uint8Array; ne?: Uint8Array };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface BandWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  logoFileId?: string | { eq?: string; ne?: string; isNull?: boolean };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface VenueWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  city?: string | { eq?: string; ne?: string; contains?: string };
  country?: string | { eq?: string; ne?: string; contains?: string };
  lat?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  lng?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  capacity?: number | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface MemberWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  bandId?: string | { eq?: string; ne?: string };
  userId?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export interface StopWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  bandId?: string | { eq?: string; ne?: string };
  venueId?: string | { eq?: string; ne?: string };
  date?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  status?: "cancelled" | "confirmed" | "tentative" | { eq?: "cancelled" | "confirmed" | "tentative"; ne?: "cancelled" | "confirmed" | "tentative"; in?: ("cancelled" | "confirmed" | "tentative")[] };
  publicDescription?: string | { eq?: string; ne?: string; contains?: string };
  privateNotes?: string | { eq?: string; ne?: string; contains?: string };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

type AnyFileQueryBuilder<T = any> = { readonly _table: "files" } & QueryBuilder<T>;
type AnyFilePartQueryBuilder<T = any> = { readonly _table: "file_parts" } & QueryBuilder<T>;
type AnyBandQueryBuilder<T = any> = { readonly _table: "bands" } & QueryBuilder<T>;
type AnyVenueQueryBuilder<T = any> = { readonly _table: "venues" } & QueryBuilder<T>;
type AnyMemberQueryBuilder<T = any> = { readonly _table: "members" } & QueryBuilder<T>;
type AnyStopQueryBuilder<T = any> = { readonly _table: "stops" } & QueryBuilder<T>;

export interface FileInclude {
  parts?: true | FilePartInclude | AnyFilePartQueryBuilder<any>;
  bandsViaLogoFile?: true | BandInclude | AnyBandQueryBuilder<any>;
}

export interface FilePartInclude {
  filesViaParts?: true | FileInclude | AnyFileQueryBuilder<any>;
}

export interface BandInclude {
  logoFile?: true | FileInclude | AnyFileQueryBuilder<any>;
  membersViaBand?: true | MemberInclude | AnyMemberQueryBuilder<any>;
  stopsViaBand?: true | StopInclude | AnyStopQueryBuilder<any>;
}

export interface VenueInclude {
  stopsViaVenue?: true | StopInclude | AnyStopQueryBuilder<any>;
}

export interface MemberInclude {
  band?: true | BandInclude | AnyBandQueryBuilder<any>;
}

export interface StopInclude {
  band?: true | BandInclude | AnyBandQueryBuilder<any>;
  venue?: true | VenueInclude | AnyVenueQueryBuilder<any>;
}

export type FileIncludedRelations<I extends FileInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "parts"
      ? NonNullable<I["parts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? FilePart[]
          : RelationInclude extends AnyFilePartQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FilePartInclude
              ? FilePartWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "bandsViaLogoFile"
      ? NonNullable<I["bandsViaLogoFile"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Band[]
          : RelationInclude extends AnyBandQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends BandInclude
              ? BandWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type FilePartIncludedRelations<I extends FilePartInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "filesViaParts"
      ? NonNullable<I["filesViaParts"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? File[]
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends FileInclude
              ? FileWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type BandIncludedRelations<I extends BandInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "logoFile"
      ? NonNullable<I["logoFile"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? File | undefined
          : RelationInclude extends AnyFileQueryBuilder<infer QueryRow>
            ? QueryRow | undefined
            : RelationInclude extends FileInclude
              ? FileWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "membersViaBand"
      ? NonNullable<I["membersViaBand"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Member[]
          : RelationInclude extends AnyMemberQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends MemberInclude
              ? MemberWithIncludes<RelationInclude, false>[]
              : never
        : never
    : K extends "stopsViaBand"
      ? NonNullable<I["stopsViaBand"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Stop[]
          : RelationInclude extends AnyStopQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends StopInclude
              ? StopWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type VenueIncludedRelations<I extends VenueInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "stopsViaVenue"
      ? NonNullable<I["stopsViaVenue"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? Stop[]
          : RelationInclude extends AnyStopQueryBuilder<infer QueryRow>
            ? QueryRow[]
            : RelationInclude extends StopInclude
              ? StopWithIncludes<RelationInclude, false>[]
              : never
        : never
    : never;
};

export type MemberIncludedRelations<I extends MemberInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "band"
      ? NonNullable<I["band"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Band : Band | undefined
          : RelationInclude extends AnyBandQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends BandInclude
              ? R extends true ? BandWithIncludes<RelationInclude, false> : BandWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export type StopIncludedRelations<I extends StopInclude = {}, R extends boolean = false> = {
  [K in keyof I]-?:
    K extends "band"
      ? NonNullable<I["band"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Band : Band | undefined
          : RelationInclude extends AnyBandQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends BandInclude
              ? R extends true ? BandWithIncludes<RelationInclude, false> : BandWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : K extends "venue"
      ? NonNullable<I["venue"]> extends infer RelationInclude
        ? RelationInclude extends true
          ? R extends true ? Venue : Venue | undefined
          : RelationInclude extends AnyVenueQueryBuilder<infer QueryRow>
            ? R extends true ? QueryRow : QueryRow | undefined
            : RelationInclude extends VenueInclude
              ? R extends true ? VenueWithIncludes<RelationInclude, false> : VenueWithIncludes<RelationInclude, false> | undefined
              : never
        : never
    : never;
};

export interface FileRelations {
  parts: FilePart[];
  bandsViaLogoFile: Band[];
}

export interface FilePartRelations {
  filesViaParts: File[];
}

export interface BandRelations {
  logoFile: File | undefined;
  membersViaBand: Member[];
  stopsViaBand: Stop[];
}

export interface VenueRelations {
  stopsViaVenue: Stop[];
}

export interface MemberRelations {
  band: Band | undefined;
}

export interface StopRelations {
  band: Band | undefined;
  venue: Venue | undefined;
}

export type FileWithIncludes<I extends FileInclude = {}, R extends boolean = false> = File & FileIncludedRelations<I, R>;

export type FilePartWithIncludes<I extends FilePartInclude = {}, R extends boolean = false> = FilePart & FilePartIncludedRelations<I, R>;

export type BandWithIncludes<I extends BandInclude = {}, R extends boolean = false> = Band & BandIncludedRelations<I, R>;

export type VenueWithIncludes<I extends VenueInclude = {}, R extends boolean = false> = Venue & VenueIncludedRelations<I, R>;

export type MemberWithIncludes<I extends MemberInclude = {}, R extends boolean = false> = Member & MemberIncludedRelations<I, R>;

export type StopWithIncludes<I extends StopInclude = {}, R extends boolean = false> = Stop & StopIncludedRelations<I, R>;

export type FileSelectableColumn = keyof File | PermissionIntrospectionColumn | "*";
export type FileOrderableColumn = keyof File | PermissionIntrospectionColumn;

export type FileSelected<S extends FileSelectableColumn = keyof File> = ("*" extends S ? File : Pick<File, Extract<S | "id", keyof File>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FileSelectedWithIncludes<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File, R extends boolean = false> = FileSelected<S> & FileIncludedRelations<I, R>;

export type FilePartSelectableColumn = keyof FilePart | PermissionIntrospectionColumn | "*";
export type FilePartOrderableColumn = keyof FilePart | PermissionIntrospectionColumn;

export type FilePartSelected<S extends FilePartSelectableColumn = keyof FilePart> = ("*" extends S ? FilePart : Pick<FilePart, Extract<S | "id", keyof FilePart>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type FilePartSelectedWithIncludes<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart, R extends boolean = false> = FilePartSelected<S> & FilePartIncludedRelations<I, R>;

export type BandSelectableColumn = keyof Band | PermissionIntrospectionColumn | "*";
export type BandOrderableColumn = keyof Band | PermissionIntrospectionColumn;

export type BandSelected<S extends BandSelectableColumn = keyof Band> = ("*" extends S ? Band : Pick<Band, Extract<S | "id", keyof Band>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type BandSelectedWithIncludes<I extends BandInclude = {}, S extends BandSelectableColumn = keyof Band, R extends boolean = false> = BandSelected<S> & BandIncludedRelations<I, R>;

export type VenueSelectableColumn = keyof Venue | PermissionIntrospectionColumn | "*";
export type VenueOrderableColumn = keyof Venue | PermissionIntrospectionColumn;

export type VenueSelected<S extends VenueSelectableColumn = keyof Venue> = ("*" extends S ? Venue : Pick<Venue, Extract<S | "id", keyof Venue>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type VenueSelectedWithIncludes<I extends VenueInclude = {}, S extends VenueSelectableColumn = keyof Venue, R extends boolean = false> = VenueSelected<S> & VenueIncludedRelations<I, R>;

export type MemberSelectableColumn = keyof Member | PermissionIntrospectionColumn | "*";
export type MemberOrderableColumn = keyof Member | PermissionIntrospectionColumn;

export type MemberSelected<S extends MemberSelectableColumn = keyof Member> = ("*" extends S ? Member : Pick<Member, Extract<S | "id", keyof Member>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type MemberSelectedWithIncludes<I extends MemberInclude = {}, S extends MemberSelectableColumn = keyof Member, R extends boolean = false> = MemberSelected<S> & MemberIncludedRelations<I, R>;

export type StopSelectableColumn = keyof Stop | PermissionIntrospectionColumn | "*";
export type StopOrderableColumn = keyof Stop | PermissionIntrospectionColumn;

export type StopSelected<S extends StopSelectableColumn = keyof Stop> = ("*" extends S ? Stop : Pick<Stop, Extract<S | "id", keyof Stop>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export type StopSelectedWithIncludes<I extends StopInclude = {}, S extends StopSelectableColumn = keyof Stop, R extends boolean = false> = StopSelected<S> & StopIncludedRelations<I, R>;

export const wasmSchema: WasmSchema = {
  "files": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "mimeType",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "partIds",
        "column_type": {
          "type": "Array",
          "element": {
            "type": "Uuid"
          }
        },
        "nullable": false,
        "references": "file_parts"
      },
      {
        "name": "partSizes",
        "column_type": {
          "type": "Array",
          "element": {
            "type": "Integer"
          }
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "update": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        },
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "delete": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      }
    }
  },
  "file_parts": {
    "columns": [
      {
        "name": "data",
        "column_type": {
          "type": "Bytea"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "update": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        },
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "delete": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      }
    }
  },
  "bands": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "logoFileId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": true,
        "references": "files"
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "update": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        },
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "delete": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      }
    }
  },
  "venues": {
    "columns": [
      {
        "name": "name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "city",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "country",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "lat",
        "column_type": {
          "type": "Double"
        },
        "nullable": false
      },
      {
        "name": "lng",
        "column_type": {
          "type": "Double"
        },
        "nullable": false
      },
      {
        "name": "capacity",
        "column_type": {
          "type": "Integer"
        },
        "nullable": true
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "True"
        }
      },
      "insert": {
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "update": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        },
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "delete": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      }
    }
  },
  "members": {
    "columns": [
      {
        "name": "bandId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "bands"
      },
      {
        "name": "userId",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Cmp",
          "column": "userId",
          "op": "Eq",
          "value": {
            "type": "SessionRef",
            "path": [
              "user_id"
            ]
          }
        }
      },
      "insert": {
        "with_check": {
          "type": "True"
        }
      },
      "update": {},
      "delete": {}
    }
  },
  "stops": {
    "columns": [
      {
        "name": "bandId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "bands"
      },
      {
        "name": "venueId",
        "column_type": {
          "type": "Uuid"
        },
        "nullable": false,
        "references": "venues"
      },
      {
        "name": "date",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      },
      {
        "name": "status",
        "column_type": {
          "type": "Enum",
          "variants": [
            "cancelled",
            "confirmed",
            "tentative"
          ]
        },
        "nullable": false
      },
      {
        "name": "publicDescription",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "privateNotes",
        "column_type": {
          "type": "Text"
        },
        "nullable": true
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Or",
          "exprs": [
            {
              "type": "Cmp",
              "column": "status",
              "op": "Eq",
              "value": {
                "type": "Literal",
                "value": {
                  "type": "Text",
                  "value": "confirmed"
                }
              }
            },
            {
              "type": "Exists",
              "table": "members",
              "condition": {
                "type": "Cmp",
                "column": "userId",
                "op": "Eq",
                "value": {
                  "type": "SessionRef",
                  "path": [
                    "user_id"
                  ]
                }
              }
            }
          ]
        }
      },
      "insert": {
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "update": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        },
        "with_check": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      },
      "delete": {
        "using": {
          "type": "Exists",
          "table": "members",
          "condition": {
            "type": "Cmp",
            "column": "userId",
            "op": "Eq",
            "value": {
              "type": "SessionRef",
              "path": [
                "user_id"
              ]
            }
          }
        }
      }
    }
  }
};

export class FileQueryBuilder<I extends FileInclude = {}, S extends FileSelectableColumn = keyof File, R extends boolean = false> implements QueryBuilder<FileSelectedWithIncludes<I, S, R>> {
  readonly _table = "files";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: FileSelectedWithIncludes<I, S, R>;
  readonly _initType!: FileInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FileInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: FileWhereInput): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends FileSelectableColumn>(...columns: [NewS, ...NewS[]]): FileQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FileInclude>(relations: NewI): FileQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): FileQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: FileOrderableColumn, direction: "asc" | "desc" = "asc"): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "parts" | "bandsViaLogoFile"): FileQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FileWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FileQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends FileInclude = I, CloneS extends FileSelectableColumn = S, CloneR extends boolean = R>(): FileQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new FileQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class FilePartQueryBuilder<I extends FilePartInclude = {}, S extends FilePartSelectableColumn = keyof FilePart, R extends boolean = false> implements QueryBuilder<FilePartSelectedWithIncludes<I, S, R>> {
  readonly _table = "file_parts";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: FilePartSelectedWithIncludes<I, S, R>;
  readonly _initType!: FilePartInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<FilePartInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: FilePartWhereInput): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends FilePartSelectableColumn>(...columns: [NewS, ...NewS[]]): FilePartQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends FilePartInclude>(relations: NewI): FilePartQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): FilePartQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: FilePartOrderableColumn, direction: "asc" | "desc" = "asc"): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "filesViaParts"): FilePartQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: FilePartWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): FilePartQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends FilePartInclude = I, CloneS extends FilePartSelectableColumn = S, CloneR extends boolean = R>(): FilePartQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new FilePartQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class BandQueryBuilder<I extends BandInclude = {}, S extends BandSelectableColumn = keyof Band, R extends boolean = false> implements QueryBuilder<BandSelectedWithIncludes<I, S, R>> {
  readonly _table = "bands";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: BandSelectedWithIncludes<I, S, R>;
  readonly _initType!: BandInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<BandInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: BandWhereInput): BandQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends BandSelectableColumn>(...columns: [NewS, ...NewS[]]): BandQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends BandInclude>(relations: NewI): BandQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): BandQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: BandOrderableColumn, direction: "asc" | "desc" = "asc"): BandQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): BandQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): BandQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "logoFile" | "membersViaBand" | "stopsViaBand"): BandQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: BandWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): BandQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends BandInclude = I, CloneS extends BandSelectableColumn = S, CloneR extends boolean = R>(): BandQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new BandQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class VenueQueryBuilder<I extends VenueInclude = {}, S extends VenueSelectableColumn = keyof Venue, R extends boolean = false> implements QueryBuilder<VenueSelectedWithIncludes<I, S, R>> {
  readonly _table = "venues";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: VenueSelectedWithIncludes<I, S, R>;
  readonly _initType!: VenueInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<VenueInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: VenueWhereInput): VenueQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends VenueSelectableColumn>(...columns: [NewS, ...NewS[]]): VenueQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends VenueInclude>(relations: NewI): VenueQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): VenueQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: VenueOrderableColumn, direction: "asc" | "desc" = "asc"): VenueQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): VenueQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): VenueQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "stopsViaVenue"): VenueQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: VenueWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): VenueQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends VenueInclude = I, CloneS extends VenueSelectableColumn = S, CloneR extends boolean = R>(): VenueQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new VenueQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class MemberQueryBuilder<I extends MemberInclude = {}, S extends MemberSelectableColumn = keyof Member, R extends boolean = false> implements QueryBuilder<MemberSelectedWithIncludes<I, S, R>> {
  readonly _table = "members";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: MemberSelectedWithIncludes<I, S, R>;
  readonly _initType!: MemberInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<MemberInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: MemberWhereInput): MemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends MemberSelectableColumn>(...columns: [NewS, ...NewS[]]): MemberQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends MemberInclude>(relations: NewI): MemberQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): MemberQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: MemberOrderableColumn, direction: "asc" | "desc" = "asc"): MemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "band"): MemberQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: MemberWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): MemberQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends MemberInclude = I, CloneS extends MemberSelectableColumn = S, CloneR extends boolean = R>(): MemberQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new MemberQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class StopQueryBuilder<I extends StopInclude = {}, S extends StopSelectableColumn = keyof Stop, R extends boolean = false> implements QueryBuilder<StopSelectedWithIncludes<I, S, R>> {
  readonly _table = "stops";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: StopSelectedWithIncludes<I, S, R>;
  readonly _initType!: StopInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<StopInclude> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: StopWhereInput): StopQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends StopSelectableColumn>(...columns: [NewS, ...NewS[]]): StopQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends StopInclude>(relations: NewI): StopQueryBuilder<I & NewI, S, R> {
    const clone = this._clone<I & NewI, S, R>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  requireIncludes(): StopQueryBuilder<I, S, true> {
    const clone = this._clone<I, S, true>();
    clone._requireIncludes = true;
    return clone;
  }

  orderBy(column: StopOrderableColumn, direction: "asc" | "desc" = "asc"): StopQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): StopQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): StopQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "band" | "venue"): StopQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: StopWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): StopQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends StopInclude = I, CloneS extends StopSelectableColumn = S, CloneR extends boolean = R>(): StopQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new StopQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export interface GeneratedApp {
  files: FileQueryBuilder;
  file_parts: FilePartQueryBuilder;
  bands: BandQueryBuilder;
  venues: VenueQueryBuilder;
  members: MemberQueryBuilder;
  stops: StopQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  files: new FileQueryBuilder(),
  file_parts: new FilePartQueryBuilder(),
  bands: new BandQueryBuilder(),
  venues: new VenueQueryBuilder(),
  members: new MemberQueryBuilder(),
  stops: new StopQueryBuilder(),
  wasmSchema,
};
