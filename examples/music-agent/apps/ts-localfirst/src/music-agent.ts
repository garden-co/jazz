import type { Db, StreamingValueSource } from "jazz-tools";
import { app } from "../schema.js";

export type Role = "user" | "assistant" | "tool";

export type TranscriptTurn = {
  id: string;
  conversationId: string;
  role: Role;
  ordinal: number;
  body: string;
};

export type ToolCall = {
  id: string;
  turnId: string;
  name: string;
  argumentsJson: string;
  resultJson: string;
};

export type Attachment = {
  id: string;
  turnId: string;
  filename: string;
  mediaType: string;
  byteLength: number;
  payload: Uint8Array;
};

/**
 * Application-shaped persistence boundary. The in-memory implementation below
 * is deterministic for E2E; JazzMusicStore maps the same operations to Jazz.
 */
export interface MusicStore {
  createConversation(title: string): Promise<string>;
  addTurn(turn: Omit<TranscriptTurn, "id">): Promise<string>;
  streamTurn(
    turn: Omit<TranscriptTurn, "id" | "body">,
    body: StreamingValueSource,
  ): Promise<string>;
  addToolCall(call: Omit<ToolCall, "id">): Promise<string>;
  addAttachment(
    attachment: Omit<Attachment, "id" | "payload">,
    payload: StreamingValueSource,
  ): Promise<string>;
  transcript(conversationId: string): Promise<TranscriptTurn[]>;
  readAttachmentRange(id: string, start: number, end: number): Promise<Uint8Array>;
}

/** A Jazz adapter using the public typed Db surface, including streaming Text and Bytea writes. */
export class JazzMusicStore implements MusicStore {
  constructor(private readonly db: Db) {}

  async createConversation(title: string): Promise<string> {
    return this.db.insert(app.conversations, { title, created_at: new Date() }).value.id;
  }

  async addTurn(turn: Omit<TranscriptTurn, "id">): Promise<string> {
    return this.db.insert(app.turns, {
      conversation_id: turn.conversationId,
      role: turn.role,
      ordinal: turn.ordinal,
      body: turn.body,
      created_at: new Date(),
    }).value.id;
  }

  async streamTurn(
    turn: Omit<TranscriptTurn, "id" | "body">,
    body: StreamingValueSource,
  ): Promise<string> {
    const write = await this.db.insertStreaming(app.turns, {
      conversation_id: turn.conversationId,
      role: turn.role,
      ordinal: turn.ordinal,
      body,
      created_at: new Date(),
    });
    return write.value.id;
  }

  async addToolCall(call: Omit<ToolCall, "id">): Promise<string> {
    return this.db.insert(app.tool_calls, {
      turn_id: call.turnId,
      name: call.name,
      arguments_json: call.argumentsJson,
      result_json: call.resultJson,
    }).value.id;
  }

  async addAttachment(
    attachment: Omit<Attachment, "id" | "payload">,
    payload: StreamingValueSource,
  ): Promise<string> {
    const write = await this.db.insertStreaming(app.attachments, {
      turn_id: attachment.turnId,
      filename: attachment.filename,
      media_type: attachment.mediaType,
      payload,
      byte_length: attachment.byteLength,
    });
    return write.value.id;
  }

  async transcript(conversationId: string): Promise<TranscriptTurn[]> {
    const rows = await this.db.all(
      app.turns.where({ conversation_id: conversationId }).orderBy("ordinal", "asc"),
    );
    return rows.map((row) => ({
      id: row.id,
      conversationId: row.conversation_id,
      role: row.role as Role,
      ordinal: row.ordinal,
      body: row.body,
    }));
  }

  async readAttachmentRange(_id: string, _start: number, _end: number): Promise<Uint8Array> {
    // Typed Db currently has no range-read method. Deliberately do not access
    // its private JazzClient; use the public JazzClient API when it is promoted
    // to the typed facade.
    throw new Error("Typed Db range reads are not available yet");
  }
}

/** Provider-free agent with an intentionally stable transcript for E2E. */
export class DeterministicMusicAgent {
  constructor(private readonly store: MusicStore) {}

  async answer(conversationId: string, prompt: string): Promise<TranscriptTurn[]> {
    const existing = await this.store.transcript(conversationId);
    const userId = await this.store.addTurn({
      conversationId,
      role: "user",
      ordinal: existing.length,
      body: prompt,
    });
    const assistantOrdinal = existing.length + 1;
    const assistantId = await this.store.streamTurn(
      { conversationId, role: "assistant", ordinal: assistantOrdinal },
      chunks([
        "I found a focused listening path for ",
        prompt,
        ". ",
        "Starting with the live cut.",
      ]),
    );
    await this.store.addToolCall({
      turnId: assistantId,
      name: "music.search",
      argumentsJson: JSON.stringify({ query: prompt }),
      resultJson: JSON.stringify({ track: "Midnight Practice", duration_seconds: 248 }),
    });
    await this.store.addTurn({
      conversationId,
      role: "tool",
      ordinal: assistantOrdinal + 1,
      body: `music.search selected Midnight Practice for ${userId}`,
    });
    return this.store.transcript(conversationId);
  }
}

export async function* chunks(parts: readonly string[]): AsyncIterable<string> {
  for (const part of parts) yield part;
}

/** Use this for Bytea attachment streams; Jazz deliberately rejects text chunks for bytes. */
export async function* byteChunks(parts: readonly Uint8Array[]): AsyncIterable<Uint8Array> {
  for (const part of parts) yield part;
}

/** Deterministic in-memory E2E store; it models materialization and ranges. */
export class MemoryMusicStore implements MusicStore {
  private readonly turns: TranscriptTurn[] = [];
  private readonly toolCalls: ToolCall[] = [];
  private readonly attachments = new Map<string, Attachment>();
  private nextId = 1;

  async createConversation(_title: string): Promise<string> {
    return this.id("conversation");
  }

  async addTurn(turn: Omit<TranscriptTurn, "id">): Promise<string> {
    const id = this.id("turn");
    this.turns.push({ id, ...turn });
    return id;
  }

  async streamTurn(
    turn: Omit<TranscriptTurn, "id" | "body">,
    body: StreamingValueSource,
  ): Promise<string> {
    const chunks = await collect(body);
    return this.addTurn({ ...turn, body: new TextDecoder().decode(chunks) });
  }

  async addToolCall(call: Omit<ToolCall, "id">): Promise<string> {
    const id = this.id("tool");
    this.toolCalls.push({ id, ...call });
    return id;
  }

  async addAttachment(
    attachment: Omit<Attachment, "id" | "payload">,
    payload: StreamingValueSource,
  ): Promise<string> {
    const id = this.id("attachment");
    const materialized = await collect(payload);
    this.attachments.set(id, {
      id,
      ...attachment,
      byteLength: materialized.length,
      payload: materialized,
    });
    return id;
  }

  async transcript(conversationId: string): Promise<TranscriptTurn[]> {
    return this.turns
      .filter((turn) => turn.conversationId === conversationId)
      .sort((left, right) => left.ordinal - right.ordinal);
  }

  async readAttachmentRange(id: string, start: number, end: number): Promise<Uint8Array> {
    return this.attachments.get(id)?.payload.slice(start, end) ?? new Uint8Array();
  }

  toolCallCount(): number {
    return this.toolCalls.length;
  }

  private id(kind: string): string {
    return `${kind}-${this.nextId++}`;
  }
}

async function collect(source: StreamingValueSource): Promise<Uint8Array> {
  const encoder = new TextEncoder();
  const pieces: Uint8Array[] = [];
  if (Symbol.asyncIterator in source) {
    for await (const chunk of source as AsyncIterable<Uint8Array | string>) {
      pieces.push(typeof chunk === "string" ? encoder.encode(chunk) : chunk);
    }
  } else {
    const reader = (source as ReadableStream<Uint8Array | string>).getReader();
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      pieces.push(typeof next.value === "string" ? encoder.encode(next.value) : next.value);
    }
  }
  const length = pieces.reduce((total, piece) => total + piece.length, 0);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const piece of pieces) {
    result.set(piece, offset);
    offset += piece.length;
  }
  return result;
}
