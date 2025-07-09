import { CojsonInternalTypes, CryptoProvider, JsonValue } from "cojson";
import z from "zod/v4";
import {
  CoValue,
  CoValueClass,
  CoValueOrZodSchema,
  Loaded,
  ResolveQuery,
  anySchemaToCoSchema,
  exportCoValue,
  importContentPieces,
  loadCoValue,
} from "../internal.js";
import { Account } from "./account.js";

type CoValueKnownState = CojsonInternalTypes.CoValueKnownState;

export type ValueSchemas = Record<string, CoValueOrZodSchema>;

export type ValueResolves<Vs extends ValueSchemas> = {
  [K in keyof Vs]?: ResolveQuery<Vs[K]>;
};

export type ValuesFor<Vs extends ValueSchemas, Vr extends ValueResolves<Vs>> = {
  [K in keyof Vs]: Vr[K] extends ResolveQuery<Vs[K]>
    ? Loaded<Vs[K], Vr[K]>
    : Loaded<Vs[K]>;
};

export type CoValueRequest<Vs extends ValueSchemas, P extends z.ZodType> = {
  payload: {
    values: Record<keyof Vs, string>;
    params: z.infer<P>;
  };
  madeBy: string;
  signerID: CojsonInternalTypes.SignerID;
  signature: CojsonInternalTypes.Signature;
  knownStates?: Record<string, CoValueKnownState>;
  contentPieces?: Record<string, CojsonInternalTypes.NewContentMessage>;
};

function getCoValueClass<Vs extends ValueSchemas>(
  value: Vs,
  k: string,
): CoValueClass<CoValue> {
  const schema = value[k];

  if (!schema) {
    throw new Error(`Value ${k} not found in valueSchemas`);
  }

  return anySchemaToCoSchema(schema);
}

async function serializeRequest<
  Vs extends ValueSchemas,
  P extends z.ZodType,
  Vr extends ValueResolves<Vs>,
>(
  paramsSchema: P,
  valueSchemas: Vs,
  values: ValuesFor<Vs, {}>,
  params: z.infer<P>,
  resolve: Vr,
  owner?: Account,
) {
  const me = owner ?? Account.getMe();
  const node = me._raw.core.node;
  const agent = node.getCurrentAgent();
  const signerID = agent.currentSignerID();
  const signerSecret = agent.currentSignerSecret();

  const valuesIds: [string, string][] = [];
  const contentPieces: CojsonInternalTypes.NewContentMessage[] = [];

  for (const [k, v] of Object.entries(values)) {
    valuesIds.push([k, v.id]);

    const exported = await exportCoValue(valueSchemas[k] as any, v.id, {
      // @ts-expect-error We can't really validate this
      resolve: resolve[k],
      loadAs: me,
    });

    if (!exported) {
      throw new Error(`Failed to export value ${k}`);
    }

    for (const piece of exported) {
      contentPieces.push(piece);
    }
  }

  const exported = await exportCoValue(Account, me.id, {
    loadAs: me,
  });

  if (!exported) {
    throw new Error(`Failed to export current account`);
  }

  for (const piece of exported) {
    contentPieces.push(piece);
  }

  const payload = {
    values: valuesIds,
    contentPieces,
    params: paramsSchema.parse(params),
  };

  const signature = node.crypto.sign(signerSecret, payload as JsonValue);

  return JSON.stringify({
    payload,
    madeBy: me.id,
    signerID,
    signature,
  });
}

const requestSchema = z.object({
  payload: z.object({
    values: z.array(z.tuple([z.string(), z.string()])),
    params: z.any(),
    // TODO Setup a proper content schema for this
    contentPieces: z.array(z.json()),
  }),
  madeBy: z.string(),
  signerID: z.string(),
  signature: z.string(),
});

async function parseIncomingRequest<P extends z.ZodType>(
  request: unknown,
  paramsSchema: P,
  crypto: CryptoProvider,
  loadAs?: Account,
) {
  const requestParsed = requestSchema.parse(request);

  if (
    !crypto.verify(
      requestParsed.signature as CojsonInternalTypes.Signature,
      requestParsed.payload as JsonValue,
      requestParsed.signerID as CojsonInternalTypes.SignerID,
    )
  ) {
    throw new Error("Invalid signature");
  }

  importContentPieces(
    requestParsed.payload
      .contentPieces as CojsonInternalTypes.NewContentMessage[],
    loadAs,
  );

  const madeBy = await Account.load(requestParsed.madeBy, {
    loadAs,
  });

  if (!madeBy) {
    throw new Error("Made by account not found");
  }

  const signerID = crypto.getAgentSignerID(madeBy._raw.currentAgentID());

  if (signerID !== requestParsed.signerID) {
    throw new Error(
      "The signer ID in the request does not match the signer ID of the account",
    );
  }

  return {
    values: requestParsed.payload.values,
    contentPieces: requestParsed.payload.contentPieces,
    params: paramsSchema.parse(requestParsed.payload.params),
    madeBy,
  };
}

async function loadValues<
  Vs extends ValueSchemas,
  Vr extends ValueResolves<Vs>,
>(values: [string, string][], valuesSchema: Vs, resolve: Vr, loadAs?: Account) {
  const loaded = {};
  const promises = [];
  const errors: string[] = [];

  for (const [k, id] of values) {
    const coSchema = getCoValueClass(valuesSchema, k);

    promises.push(
      loadCoValue(coSchema, id, {
        // @ts-expect-error We can't really validate this
        resolve: resolve[k],
        loadAs: loadAs ?? Account.getMe(),
      }).then((v: unknown) => {
        if (!v) {
          errors.push(`Value ${k} not found/accessible`);
        }

        // @ts-ignore
        loaded[k] = v;

        return v;
      }),
    );
  }

  await Promise.all(promises);

  if (errors.length > 0) {
    throw new Error(errors.join("\n"));
  }

  return loaded as ValuesFor<Vs, Vr>;
}

export function experimental_defineRequest<
  Vs extends ValueSchemas,
  Vr extends ValueResolves<Vs>,
  P extends z.ZodType,
  R extends z.ZodType,
>(
  valueSchemas: Vs,
  defOptions: {
    resolve: Vr;
    paramsSchema: P;
    responseSchema: R;
    url: string;
  },
) {
  const handle = async (
    request: Request,
    as: Account,
    callback: (
      values: ValuesFor<Vs, Vr>,
      params: z.infer<P>,
      madeBy: Account,
    ) => Promise<z.infer<R>>,
  ): Promise<Response> => {
    const node = as._raw.core.node;
    const data = await parseIncomingRequest(
      await request.json(),
      defOptions.paramsSchema,
      node.crypto,
      as,
    );

    const values = await loadValues(
      data.values,
      valueSchemas,
      defOptions.resolve,
      as,
    );
    const responsePayload = defOptions.responseSchema.parse(
      await callback(values, data.params, data.madeBy),
    );

    const responseBody = JSON.stringify({
      type: "success",
      payload: responsePayload,
    });

    return new Response(responseBody, {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    });
  };

  const send = async (
    values: ValuesFor<Vs, {}>,
    params: z.infer<P>,
    options?: { assumeUnknown?: boolean; owner?: Account },
  ) => {
    const response = await fetch(defOptions.url, {
      method: "POST",
      body: await serializeRequest(
        defOptions.paramsSchema,
        valueSchemas,
        values,
        params,
        defOptions.resolve,
        options?.owner,
      ),
    });

    const responseBody = await response.json();
    const responseParsed = z
      .object({
        type: z.literal("success"),
        payload: z.any(),
      })
      .parse(responseBody);

    return defOptions.responseSchema.parse(responseParsed.payload);
  };

  return {
    send,
    handle,
  };
}
