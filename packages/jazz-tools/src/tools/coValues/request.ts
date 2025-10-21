import {
  CoValueCore,
  CojsonInternalTypes,
  CryptoProvider,
  RawAccount,
  RawCoMap,
  cojsonInternals,
} from "cojson";
import z from "zod/v4";
import {
  AnyZodOrCoValueSchema,
  CoMap,
  CoMapSchema,
  CoMapSchemaInit,
  CoValueClass,
  CoreCoMapSchema,
  CoValueLoadingState,
  createUnloadedCoValue,
  Group,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  Simplify,
  coMapDefiner,
  coValueClassFromCoValueClassOrSchema,
  exportCoValue,
  importContentPieces,
  loadCoValue,
} from "../internal.js";
import { isCoValueId } from "../lib/id.js";
import { Account } from "./account.js";

type MessageShape = Record<string, AnyZodOrCoValueSchema>;

type RequestSchemaDefinition<
  S extends MessageShape,
  R extends ResolveQuery<CoMapSchema<S>> = true,
> =
  | S
  | {
      schema: S;
      resolve?: R;
    };

/**
 * Configuration options for defining HTTP request/response schemas in Jazz.
 *
 * This interface defines the structure for creating typed HTTP routes with
 * request and response validation using CoMap schemas.
 *
 * @template RequestShape - The shape of the request message schema (must extend MessageShape)
 * @template RequestResolve - The resolve query type for the request CoMap schema
 * @template ResponseShape - The shape of the response message schema (must extend MessageShape)
 * @template ResponseResolve - The resolve query type for the response CoMap schema
 */
interface RequestOptions<
  RequestShape extends MessageShape,
  RequestResolve extends ResolveQuery<CoMapSchema<RequestShape>>,
  ResponseShape extends MessageShape,
  ResponseResolve extends ResolveQuery<CoMapSchema<ResponseShape>>,
> {
  /**
   * The URL endpoint for the HTTP route.
   * This is used by the client to send requests to the server.
   */
  url: string;

  /**
   * The id of the worker Account or Group.
   */
  workerId: string;

  /**
   * Schema definition for the request payload.
   * Can be either a direct schema object or an object with schema and optional resolve properties.
   * The schema defines the structure and validation rules for incoming requests.
   */
  request: RequestSchemaDefinition<
    RequestShape,
    ResolveQueryStrict<CoMapSchema<RequestShape>, RequestResolve>
  >;

  /**
   * Schema definition for the response payload.
   * Can be either a direct schema object or an object with schema and optional resolve properties.
   * The schema defines the structure and validation rules for outgoing responses.
   */
  response: RequestSchemaDefinition<
    ResponseShape,
    ResolveQueryStrict<CoMapSchema<ResponseShape>, ResponseResolve>
  >;
}

type AsNullablePayload<T extends MessageShape> = T extends Record<string, never>
  ? undefined
  : never;
type MessageValuePayload<T extends MessageShape> =
  | Simplify<CoMapSchemaInit<T>>
  | AsNullablePayload<T>;

function createMessageEnvelope<S extends MessageShape>(
  schema: CoreCoMapSchema,
  value: MessageValuePayload<S>,
  owner: Account,
  sharedWith: Account | Group,
  type: "request" | "response",
): Loaded<CoMapSchema<S>> {
  const group = Group.create({ owner });

  if (type === "request") {
    group.addMember(sharedWith, "writer");
  } else {
    group.addMember(sharedWith, "reader");
  }

  // @ts-expect-error - CoreCoMapSchema doesn't have static methods
  return schema.create(value ?? {}, group);
}

/**
 * Function that exports the input CoValue in a serializable format and prepares the information
 * required for the other side to safely verify the identity of the sender.
 */
async function serializeMessagePayload({
  type,
  schema,
  resolve,
  value,
  owner,
  target,
}: {
  // Skipping type validation here to avoid excessive type complexity that affects the typecheck performance
  type: "request" | "response";
  schema: CoreCoMapSchema;
  resolve: any;
  value: any;
  owner: Account;
  target: Account | Group;
}) {
  const me = owner ?? Account.getMe();
  const node = me.$jazz.localNode;
  const crypto = node.crypto;

  const agent = node.getCurrentAgent();
  const signerID = agent.currentSignerID();
  const signerSecret = agent.currentSignerSecret();

  const envelope = createMessageEnvelope(schema, value, me, target, type);

  const contentPieces =
    (await exportCoValue(schema, envelope.$jazz.id, {
      resolve,
      loadAs: me,
      bestEffortResolution: true,
    })) ?? [];

  const createdAt = Date.now();

  const signPayload = crypto.secureHash({
    contentPieces,
    id: envelope.$jazz.id,
    createdAt,
    signerID,
  });

  const authToken = crypto.sign(signerSecret, signPayload);

  return {
    contentPieces,
    id: envelope.$jazz.id,
    createdAt,
    authToken,
    signerID,
  };
}

const coIdSchema = z.custom<`co_z${string}`>(isCoValueId);
const signatureSchema = z.custom<`signature_z${string}`>(
  (value) => typeof value === "string" && value.startsWith("signature_z"),
);

const requestSchema = z.object({
  contentPieces: z.array(z.json()),
  id: coIdSchema,
  createdAt: z.number(),
  authToken: signatureSchema,
  signerID: z.custom<`signer_z${string}`>(
    (value) => typeof value === "string" && value.startsWith("signer_z"),
  ),
});

/**
 * Function that parses the message payload, verifies the identity of the sender and loads the data.
 *
 * @returns The data from the message.
 */
async function handleMessagePayload({
  type,
  schema,
  resolve,
  request,
  loadAs,
}: {
  type: "request" | "response";
  // Skipping type validation here to avoid excessive type complexity that affects the typecheck performance
  schema: CoreCoMapSchema;
  resolve: any;
  request: unknown;
  loadAs: Account;
}) {
  const node = loadAs.$jazz.localNode;
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

  if (type === "request") {
    const core = await node.loadCoValueCore(requestData.id, undefined, true);

    // Check if the message has already been handled to prevent replay attacks
    if (core.isAvailable()) {
      const content = core.getCurrentContent() as RawCoMap;

      if (content.get("$handled") === loadAs.$jazz.id) {
        throw new JazzRequestError("Request payload is already handled", 400);
      }
    }

    // Check if the message is expired as extra protection
    if (requestData.createdAt + 1000 * 60 < Date.now()) {
      throw new JazzRequestError("Authentication token is expired", 401);
    }
  }

  // Verify the signature of the message to prevent tampering
  const signPayload = crypto.secureHash({
    contentPieces: requestData.contentPieces,
    id: requestData.id,
    createdAt: requestData.createdAt,
    signerID: requestData.signerID,
  });

  if (
    !safeVerifySignature(
      crypto,
      signPayload,
      requestData.signerID,
      requestData.authToken,
    )
  ) {
    throw new JazzRequestError("Invalid signature", 401);
  }

  let contentPieces =
    requestData.contentPieces as CojsonInternalTypes.NewContentMessage[];

  if (type === "request") {
    const coValueContent = contentPieces.find(
      (piece) => piece.id === requestData.id,
    );

    if (coValueContent && coValueContent.header) {
      const validValues = cojsonInternals.getDependedOnCoValues(
        coValueContent.header,
        coValueContent,
      );
      validValues.add(requestData.id);
      contentPieces = contentPieces.filter((piece) =>
        validValues.has(piece.id),
      );
    } else {
      contentPieces = [];
    }
  }

  importContentPieces(contentPieces, loadAs);

  const coValue = await node.loadCoValueCore(requestData.id);
  const accountId = getCoValueCreatorAccountId(coValue);

  const madeBy = await Account.load(accountId, {
    loadAs,
  });

  if (!madeBy.$isLoaded) {
    throw new JazzRequestError("Creator account not found", 400);
  }

  const coSchema = coValueClassFromCoValueClassOrSchema(
    schema,
  ) as CoValueClass<CoMap>;
  const value = await loadCoValue<CoMap, true>(coSchema, requestData.id, {
    resolve,
    loadAs,
  });

  if (!value.$isLoaded) {
    throw new JazzRequestError("Value not found", 400);
  }

  if (type === "request") {
    value.$jazz.raw.set("$handled", loadAs.$jazz.id);
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
      // Using a type cast to reduce the type complexity
      schema: coMapDefiner(options.schema) as CoreCoMapSchema,
      resolve: options.resolve as any,
    };
  }

  return {
    schema: coMapDefiner(options) as CoreCoMapSchema,
    resolve: true as any,
  };
}

export class HttpRoute<
  RequestShape extends MessageShape = z.core.$ZodLooseShape,
  RequestResolve extends ResolveQuery<CoMapSchema<RequestShape>> = any,
  ResponseShape extends MessageShape = z.core.$ZodLooseShape,
  ResponseResolve extends ResolveQuery<CoMapSchema<ResponseShape>> = any,
> {
  private requestDefinition: {
    schema: CoreCoMapSchema;
    resolve: any;
  };
  private responseDefinition: {
    schema: CoreCoMapSchema;
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

    if (params.workerId === undefined) {
      throw new TypeError("Worker ID is required");
    }
  }

  async send(
    values: MessageValuePayload<RequestShape>,
    options?: { owner?: Account },
  ): Promise<Loaded<CoMapSchema<ResponseShape>, ResponseResolve>> {
    const as = options?.owner ?? Account.getMe();

    const target = await loadWorkerAccountOrGroup(this.workerId, as);
    if (!target.$isLoaded) {
      throw new JazzRequestError("Worker account not found", 400);
    }

    const response = await fetch(this.url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(
        await serializeMessagePayload({
          type: "request",
          schema: this.requestDefinition.schema,
          resolve: true, // export only the envelope
          value: values,
          owner: as,
          target,
        }),
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

    const data = await handleMessagePayload({
      type: "response",
      schema: this.responseDefinition.schema,
      resolve: this.responseDefinition.resolve,
      request: responseParsed.data.payload,
      loadAs: as,
    });

    return data.value as Loaded<CoMapSchema<ResponseShape>, ResponseResolve>;
  }

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
      // Serialize the error to make it possible to handle it on the client side
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
    const node = as.$jazz.localNode;
    const body = await request.json();
    const data = await handleMessagePayload({
      type: "request",
      schema: this.requestDefinition.schema,
      resolve: this.requestDefinition.resolve,
      request: body,
      loadAs: as,
    });

    const tracking = node.syncManager.trackDirtyCoValues();

    const responseValue = await callback(
      data.value as Loaded<CoMapSchema<RequestShape>, RequestResolve>,
      data.madeBy,
    );

    const responsePayload = await serializeMessagePayload({
      type: "response",
      schema: this.responseDefinition.schema,
      resolve: this.responseDefinition.resolve,
      value: responseValue,
      owner: as,
      target: data.madeBy,
    });

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

/**
 * Define a request route.
 *
 * @param params - The parameters for the request route.
 * @returns The request route.
 *
 * @see {@link https://jazz.tools/docs/react/server-side/http-requests}
 */
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

  if (!isCoValueId(accountId)) {
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

function safeVerifySignature(
  crypto: CryptoProvider,
  signPayload: `hash_z${string}`,
  signerID: `signer_z${string}`,
  authToken: `signature_z${string}`,
) {
  try {
    return crypto.verify(authToken, signPayload, signerID);
  } catch (error) {
    return false;
  }
}

async function loadWorkerAccountOrGroup(id: string, loadAs: Account) {
  const node = loadAs.$jazz.localNode;
  const coValue = await node.loadCoValueCore(id as `co_z${string}`);

  if (!coValue.isAvailable()) {
    return createUnloadedCoValue(id, CoValueLoadingState.UNAVAILABLE);
  }

  const content = coValue.getCurrentContent();

  if (content instanceof RawAccount) {
    return Account.load(content.id, {
      loadAs,
    });
  }

  return Group.load(content.id, {
    loadAs,
  });
}

function defaultGetToken(request: Request) {
  const headerValue = request.headers.get("Authorization");
  if (headerValue?.startsWith("Jazz ")) {
    return headerValue.replace("Jazz ", "");
  }

  if (headerValue) {
    console.warn(
      "An Authorization header was found, but it did not start with 'Jazz '. If this is intentional, you can specify the location of the token using the `getToken` option.",
    );
  }

  return undefined;
}

/**
 * Authenticates a Request by verifying a signed authentication token.
 *
 * - If a token is not provided, the returned account is `undefined` and no error is returned.
 * - If a valid token is provided, the signer account is returned.
 * - If an invalid token is provided, an error is returned detailing the validation error, and the returned account is `undefined`.
 *
 * @see {@link generateAuthToken} for generating a token.
 *
 * Note: This function does not perform any authorization checks, it only verifies if - **when provided** - a token is valid. It is up to the caller to perform any additional authorization checks, if needed.
 *
 * @param request - The request to authenticate.
 * @param options - The options for the authentication.
 * @param options.expiration - The expiration time of the token in milliseconds, defaults to 1 minute.
 * @param options.loadAs - The account to load the token from, defaults to the current active account.
 * @param options.getToken - If specified, this function will be used to get the token from the request. By default the token is expected to be in the `Authorization` header in the form of `Jazz <token>`.
 * @returns The account if it is valid, otherwise an error.
 *
 * @example
 * ```ts
 * const { account, error } = await authenticateRequest(request);
 * if (error) {
 *   return new Response(JSON.stringify(error), { status: 401 });
 * }
 * ```
 */
export async function authenticateRequest(
  request: Request,
  options?: {
    expiration?: number;
    loadAs?: Account;
    getToken?: (request: Request) => string | undefined | null;
  },
): Promise<
  | {
      account?: Account;
      error?: never;
    }
  | {
      account?: never;
      error: { message: string; details?: unknown };
    }
> {
  const token = options?.getToken?.(request) ?? defaultGetToken(request);

  if (!token) {
    return {};
  }

  const { account, error } = await parseAuthToken(token, {
    loadAs: options?.loadAs,
    expiration: options?.expiration ?? 1000 * 60,
  });

  if (error) {
    return { error };
  }

  return { account, error };
}

/**
 * Generates an authentication token for a given account. This token can be used to authenticate a request. See {@link authenticateRequest} for more details.
 *
 * @param as - The account to generate the token for, defaults to the current active account.
 * @returns The authentication token.
 *
 * @example Make a fetch request with the token
 * ```ts
 * const token = generateAuthToken();
 * const response = await fetch(url, {
 *   headers: {
 *     Authorization: `Jazz ${token}`,
 *   },
 * });
 * ```
 */

export function generateAuthToken(as?: Account) {
  const account = as ?? Account.getMe();
  const node = account.$jazz.localNode;
  const crypto = node.crypto;

  const agent = node.getCurrentAgent();
  const signerSecret = agent.currentSignerSecret();

  const createdAt = Date.now();

  const signPayload = crypto.secureHash({
    id: account.$jazz.id,
    createdAt,
  });

  const authToken = crypto.sign(signerSecret, signPayload);

  return `${authToken}~${account.$jazz.id}~${createdAt}`;
}

export async function parseAuthToken(
  authToken: string,
  options?: { loadAs?: Account; expiration?: number },
): Promise<
  | { account: Account; error?: never }
  | { account?: never; error: { message: string; details?: unknown } }
> {
  const expiration = options?.expiration ?? 1_000 * 60; // 1 minute

  const parsed = z
    .tuple([signatureSchema, coIdSchema, z.string().transform(Number)])
    .safeParse(authToken.split("~"));

  if (!parsed.success) {
    return {
      error: {
        message: "Invalid token",
        details: parsed.error,
      },
    };
  }

  const [signature, id, createdAt] = parsed.data;

  if (createdAt + expiration < Date.now()) {
    return {
      error: {
        message: "Token expired",
      },
    };
  }

  const account = await Account.load(id, { loadAs: options?.loadAs });

  if (!account.$isLoaded) {
    return {
      error: {
        message: "Failed to load account",
        details: { id },
      },
    };
  }

  const node = account.$jazz.localNode;
  const crypto = node.crypto;

  // Verify the signature of the message to prevent tampering
  const signPayload = crypto.secureHash({
    id: account.$jazz.id,
    createdAt: Number(createdAt),
  });

  const agentID = account.$jazz.raw.currentAgentID();
  const signerID = crypto.getAgentSignerID(agentID);

  if (!crypto.verify(signature, signPayload, signerID)) {
    return {
      error: {
        message: "Invalid signature",
      },
    };
  }

  return {
    account,
  };
}
