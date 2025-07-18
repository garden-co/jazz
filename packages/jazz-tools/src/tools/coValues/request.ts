import { CoValueCore, CojsonInternalTypes, cojsonInternals } from "cojson";
import z from "zod/v4";
import {
  AnyCoMapSchema,
  AnyCoSchema,
  CoMapInitZod,
  CoMapSchema,
  CoValue,
  CoValueClass,
  Group,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  Simplify,
  anySchemaToCoSchema,
  coMapDefiner,
  exportCoValue,
  importContentPieces,
  loadCoValue,
} from "../internal.js";
import { Account } from "./account.js";

type MessageShape = Record<string, z.core.$ZodType | AnyCoSchema>;

type RequestSchemaDefinition<
  S extends MessageShape,
  R extends ResolveQuery<CoMapSchema<S>> = true,
> =
  | S
  | {
      schema: S;
      resolve?: R;
    };

interface RequestOptions<
  RequestShape extends MessageShape,
  RequestResolve extends ResolveQuery<CoMapSchema<RequestShape>>,
  ResponseShape extends MessageShape,
  ResponseResolve extends ResolveQuery<CoMapSchema<ResponseShape>>,
> {
  url: string;
  workerId: string;
  request: RequestSchemaDefinition<
    RequestShape,
    ResolveQueryStrict<CoMapSchema<RequestShape>, RequestResolve>
  >;
  response: RequestSchemaDefinition<
    ResponseShape,
    ResolveQueryStrict<CoMapSchema<ResponseShape>, ResponseResolve>
  >;
}

type MessageValuePayload<T extends MessageShape> = Simplify<CoMapInitZod<T>>;

function inputValueToCoMap<S extends MessageShape>(
  schema: AnyCoMapSchema,
  value: MessageValuePayload<S>,
  owner: Account,
  sharedWith: Account | Group,
) {
  const group = Group.create({ owner });

  group.addMember(sharedWith, "reader");

  return (schema as CoMapSchema<S>).create(value, group) as Loaded<
    CoMapSchema<S>
  >;
}

async function serializeMessagePayload(
  // Skipping type validation here to avoid excessive type complexity that affects the typecheck performance
  schema: AnyCoMapSchema,
  resolve: any,
  value: any,
  owner: Account,
  target: Account | Group,
) {
  const me = owner ?? Account.getMe();
  const node = me._raw.core.node;
  const crypto = node.crypto;

  const agent = node.getCurrentAgent();
  const signerID = agent.currentSignerID();
  const signerSecret = agent.currentSignerSecret();

  const coMap = inputValueToCoMap(schema, value, me, target);

  const contentPieces =
    (await exportCoValue(schema, coMap.id, {
      resolve,
      loadAs: me,
      bestEffortResolution: true,
    })) ?? [];

  if (!contentPieces.some((piece) => piece.id === me.id)) {
    const accountContent = await exportCoValue(Account, me.id, {
      loadAs: me,
    });

    if (!accountContent) {
      throw new JazzRequestError(`Failed to export current account`, 500);
    }

    for (const piece of accountContent) {
      contentPieces.push(piece);
    }
  }

  const createdAt = Date.now();

  const signPayload = crypto.secureHash({
    contentPieces,
    id: coMap.id,
    createdAt,
    signerID,
  });

  const authToken = crypto.sign(signerSecret, signPayload);

  return {
    contentPieces,
    id: coMap.id,
    createdAt,
    authToken,
    signerID,
  };
}

const requestSchema = z.object({
  contentPieces: z.array(z.json()),
  id: z.custom<`co_z${string}`>(
    (value) => typeof value === "string" && value.startsWith("co_z"),
  ),
  createdAt: z.number(),
  authToken: z.custom<`signature_z${string}`>(
    (value) => typeof value === "string" && value.startsWith("signature_z"),
  ),
  signerID: z.custom<`signer_z${string}`>(
    (value) => typeof value === "string" && value.startsWith("signer_z"),
  ),
});

async function handleIncomingMessage(
  // Skipping type validation here to avoid excessive type complexity that affects the typecheck performance
  schema: AnyCoMapSchema,
  resolve: any,
  request: unknown,
  loadAs: Account,
  handledMessages: Set<`co_z${string}`>,
) {
  const node = loadAs._raw.core.node;
  const crypto = node.crypto;

  const requestParsed = requestSchema.safeParse(request);

  if (!requestParsed.success) {
    throw new JazzRequestError(
      "Request payload is not valid",
      400,
      requestParsed.error,
    );
  }

  const requestData = requestParsed.data;

  if (handledMessages.has(requestData.id)) {
    throw new JazzRequestError("Request payload is already handled", 400);
  }

  if (requestData.createdAt + 1000 * 60 < Date.now()) {
    throw new JazzRequestError("Authentication token is expired", 401);
  }

  const signPayload = crypto.secureHash({
    contentPieces: requestData.contentPieces,
    id: requestData.id,
    createdAt: requestData.createdAt,
    signerID: requestData.signerID,
  });

  let validSignature = false;

  try {
    validSignature = crypto.verify(
      requestData.authToken,
      signPayload,
      requestData.signerID,
    );
  } catch (error) {
    validSignature = false;
  }

  if (!validSignature) {
    throw new JazzRequestError("Invalid signature", 401);
  }

  handledMessages.add(requestData.id);

  importContentPieces(
    requestData.contentPieces as CojsonInternalTypes.NewContentMessage[],
    loadAs,
  );

  const coValue = await node.loadCoValueCore(requestData.id);
  const accountId = getCoValueCreatorAccountId(coValue);

  const madeBy = await Account.load(accountId, {
    loadAs,
  });

  if (!madeBy) {
    throw new JazzRequestError("Creator account not found", 400);
  }

  const coSchema = anySchemaToCoSchema(schema) as CoValueClass<CoValue>;
  const value = await loadCoValue(coSchema, requestData.id, {
    resolve,
    loadAs,
  });

  if (!value) {
    throw new JazzRequestError("Value not found", 400);
  }

  return {
    value: value as unknown,
    madeBy,
  };
}

function parseSchemaAndResolve<
  S extends MessageShape,
  R extends ResolveQuery<CoMapSchema<S>>,
>(options: RequestSchemaDefinition<S, R>) {
  if ("schema" in options) {
    return {
      // Casting to reduce the type complexity
      schema: coMapDefiner(options.schema) as AnyCoMapSchema,
      resolve: options.resolve as any,
    };
  }

  return {
    schema: coMapDefiner(options) as AnyCoMapSchema,
    resolve: true as any,
  };
}

class HttpRoute<
  RequestShape extends MessageShape = z.core.$ZodLooseShape,
  RequestResolve extends ResolveQuery<CoMapSchema<RequestShape>> = any,
  ResponseShape extends MessageShape = z.core.$ZodLooseShape,
  ResponseResolve extends ResolveQuery<CoMapSchema<ResponseShape>> = any,
> {
  private processedValues = new Set<`co_z${string}`>();
  private requestDefinition: {
    schema: AnyCoMapSchema;
    resolve: any;
  };
  private responseDefinition: {
    schema: AnyCoMapSchema;
    resolve: any;
  };
  private url: string;
  private workerId: string;

  constructor(
    params: RequestOptions<
      RequestShape,
      RequestResolve,
      ResponseShape,
      ResponseResolve
    >,
  ) {
    this.requestDefinition = parseSchemaAndResolve(params.request);
    this.responseDefinition = parseSchemaAndResolve(params.response);
    this.url = params.url;
    this.workerId = params.workerId;
  }

  send = async (
    values: MessageValuePayload<RequestShape>,
    options?: { owner?: Account },
  ): Promise<Loaded<CoMapSchema<ResponseShape>, ResponseResolve>> => {
    const as = options?.owner ?? Account.getMe();

    const workerAccount = await Account.load(this.workerId, {
      loadAs: as,
    });

    if (!workerAccount) {
      throw new JazzRequestError("Worker account not found", 400);
    }

    const response = await fetch(this.url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(
        await serializeMessagePayload(
          this.requestDefinition.schema,
          this.requestDefinition.resolve,
          values,
          as,
          workerAccount,
        ),
      ),
    });

    if (!response.ok) {
      if (response.headers.has("X-Jazz-Request-Error")) {
        const error = await response.json();
        throw new JazzRequestError(error.message, error.code, error.details);
      }

      throw new JazzRequestError("Request failed", response.status);
    }

    const responseBody = await response.json();

    const responseParsed = z
      .object({
        type: z.literal("success"),
        payload: z.any(),
      })
      .safeParse(responseBody);

    if (!responseParsed.success) {
      throw new JazzRequestError(
        "Response payload is not valid",
        400,
        responseParsed.error,
      );
    }

    const data = await handleIncomingMessage(
      this.responseDefinition.schema,
      this.responseDefinition.resolve,
      responseParsed.data.payload,
      as,
      this.processedValues,
    );

    return data.value as Loaded<CoMapSchema<ResponseShape>, ResponseResolve>;
  };

  handle = async (
    request: Request,
    as: Account,
    callback: (
      value: Loaded<CoMapSchema<RequestShape>, RequestResolve>,
      madeBy: Account,
    ) =>
      | Promise<MessageValuePayload<ResponseShape>>
      | MessageValuePayload<ResponseShape>,
  ): Promise<Response> => {
    try {
      const response = await this.executeHandleRequest(request, as, callback);
      return response;
    } catch (error) {
      if (isJazzRequestError(error)) {
        return new Response(JSON.stringify(error.toJSON()), {
          status: error.code,
          headers: {
            "Content-Type": "application/json",
            "X-Jazz-Request-Error": "true",
          },
        });
      }

      throw error;
    }
  };

  executeHandleRequest = async (
    request: Request,
    as: Account,
    callback: (
      value: Loaded<CoMapSchema<RequestShape>, RequestResolve>,
      madeBy: Account,
    ) =>
      | Promise<MessageValuePayload<ResponseShape>>
      | MessageValuePayload<ResponseShape>,
  ): Promise<Response> => {
    const node = as._raw.core.node;
    const body = await request.json();
    const data = await handleIncomingMessage(
      this.requestDefinition.schema,
      this.requestDefinition.resolve,
      body,
      as,
      this.processedValues,
    );

    const tracking = node.syncManager.trackDirtyCoValues();

    const responseValue = await callback(
      data.value as Loaded<CoMapSchema<RequestShape>, RequestResolve>,
      data.madeBy,
    );

    const responsePayload = await serializeMessagePayload(
      this.responseDefinition.schema,
      this.responseDefinition.resolve,
      responseValue,
      as,
      data.madeBy,
    );

    const responseBody = JSON.stringify({
      type: "success",
      payload: responsePayload,
    });

    // TODO: Detect the defer support from the environment
    await Promise.all(
      Array.from(tracking.done(), (id) => node.syncManager.waitForSync(id)),
    );

    return new Response(responseBody, {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    });
  };

  get requestSchema(): CoMapSchema<RequestShape> {
    return this.requestDefinition.schema as CoMapSchema<RequestShape>;
  }

  get responseSchema() {
    return this.responseDefinition.schema as CoMapSchema<ResponseShape>;
  }
}

export function experimental_defineRequest<
  RequestShape extends MessageShape,
  RequestResolve extends ResolveQuery<CoMapSchema<RequestShape>>,
  ResponseShape extends MessageShape,
  ResponseResolve extends ResolveQuery<CoMapSchema<ResponseShape>>,
>(
  params: RequestOptions<
    RequestShape,
    RequestResolve,
    ResponseShape,
    ResponseResolve
  >,
) {
  return new HttpRoute(params);
}

function getCoValueCreatorAccountId(coValue: CoValueCore) {
  if (!coValue.isAvailable()) {
    throw new Error("Unable to load the request payload");
  }

  const creatorSessionId = coValue.getValidSortedTransactions().at(0)
    ?.txID.sessionID;

  if (!creatorSessionId) {
    throw new JazzRequestError(
      "Request payload is not valid, creator session ID not found",
      400,
    );
  }

  const accountId =
    cojsonInternals.accountOrAgentIDfromSessionID(creatorSessionId);

  if (!accountId.startsWith("co_z")) {
    throw new JazzRequestError(
      "Request payload is not valid, the creator is not a valid account",
      400,
    );
  }

  return accountId;
}

export class JazzRequestError {
  public readonly isJazzRequestError = true;

  constructor(
    public readonly message: string,
    public readonly code: number,
    public readonly details?: unknown,
  ) {}

  toJSON() {
    return { message: this.message, code: this.code, details: this.details };
  }
}

export function isJazzRequestError(error: unknown): error is JazzRequestError {
  return (
    error instanceof JazzRequestError ||
    (typeof error === "object" &&
      error !== null &&
      "isJazzRequestError" in error &&
      Boolean(error.isJazzRequestError))
  );
}
