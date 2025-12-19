import { CoreCoMapSchema } from "../schemaTypes/CoMapSchema";
import { CoreCoRecordSchema } from "../schemaTypes/CoRecordSchema";
import { CoreCoListSchema } from "../schemaTypes/CoListSchema";
import { CoreCoFeedSchema } from "../schemaTypes/CoFeedSchema";
import { CoreCoVectorSchema } from "../schemaTypes/CoVectorSchema";
import { CorePlainTextSchema } from "../schemaTypes/PlainTextSchema";
import { CoreRichTextSchema } from "../schemaTypes/RichTextSchema";
import { CoreCoOptionalSchema } from "../schemaTypes/CoOptionalSchema";
import { CoreCoDiscriminatedUnionSchema } from "../schemaTypes/CoDiscriminatedUnionSchema";
import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema";

export type ToCore<S extends CoreCoValueSchema> = S extends CoreCoMapSchema<
  infer Shape,
  infer CatchAll
>
  ? CoreCoMapSchema<Shape, CatchAll>
  : S extends CoreCoRecordSchema<infer Key, infer Value>
    ? CoreCoRecordSchema<Key, Value>
    : S extends CoreCoListSchema<infer Element>
      ? CoreCoListSchema<Element>
      : S extends CoreCoFeedSchema<infer Element>
        ? CoreCoFeedSchema<Element>
        : S extends CoreCoVectorSchema
          ? CoreCoVectorSchema
          : S extends CorePlainTextSchema
            ? CorePlainTextSchema
            : S extends CoreRichTextSchema
              ? CoreRichTextSchema
              : S extends CoreCoOptionalSchema<infer Inner>
                ? CoreCoOptionalSchema<Inner>
                : S extends CoreCoDiscriminatedUnionSchema<infer Members>
                  ? CoreCoDiscriminatedUnionSchema<Members>
                  : "TODO: MISSING TOCORE FOR " & S;
