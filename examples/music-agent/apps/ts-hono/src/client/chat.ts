import type { Db } from "jazz-tools";
import { app, type Conversation, type TimedTurn, type ToolCall } from "@/schema";
import { regenerateReply, requestReply, resumeReply } from "@/src/ui/agent-api";
import { branchPath, latestLeaf, siblingsOf } from "@/src/ui/transcript";
import { h } from "./dom";
import { markdown } from "./markdown";

type AttachmentSummary = {
  id: string;
  turnId: string;
  filename: string;
  mediaType: string;
  byteLength: number;
};
type Artist = { id: string; name: string };

const NEW_TITLE = "New conversation";

/**
 * MusicAgent's chat, with plain DOM. Every list comes from a Jazz
 * subscription and is redrawn when it changes; replies stream in the same
 * way, row by row, as the server writes them.
 */
export function chatScreen(options: {
  db: Db;
  account: string;
  userName: string;
  agentLabel: string;
  onSignOut(): void;
}): { element: HTMLElement; dispose(): void } {
  const { db, account, userName, agentLabel } = options;
  let artist: Artist | undefined;
  let conversations: Conversation[] = [];
  let selectedId: string | undefined;
  let stopConversation: (() => void) | undefined;

  const heading = h("h2", {}, "MusicAgent");
  const subheading = h("p", { class: "secondary" }, `Booked by ${userName}`);
  const newButton = h(
    "button",
    { type: "button", class: "primary", disabled: true },
    "New conversation",
  );
  const list = h("ul", { class: "conversations" });
  const main = h("section", { class: "conversation", "aria-label": "Conversation" });
  const element = h(
    "div",
    { class: "chat-shell" },
    h(
      "header",
      { class: "top" },
      h("h1", {}, "MusicAgent"),
      h(
        "div",
        { class: "row" },
        h("span", { class: "badge" }, agentLabel),
        h(
          "button",
          { type: "button", class: "ghost", on: { click: options.onSignOut } },
          "Sign out",
        ),
      ),
    ),
    h(
      "nav",
      { class: "side", "aria-label": "Conversations" },
      heading,
      subheading,
      newButton,
      list,
    ),
    main,
  );

  newButton.addEventListener("click", () => {
    if (!artist) return;
    const { value } = db.insert(app.conversations, {
      ownerAccount: account,
      artistId: artist.id,
      title: NEW_TITLE,
    });
    select(value.id);
  });

  function renderNav() {
    heading.textContent = artist?.name ?? "MusicAgent";
    newButton.disabled = !artist;
    list.replaceChildren(
      ...conversations.map((conversation) =>
        h(
          "li",
          {},
          h(
            "button",
            {
              type: "button",
              "aria-current": conversation.id === selected()?.id ? "true" : undefined,
              on: { click: () => select(conversation.id) },
            },
            conversation.title,
          ),
        ),
      ),
    );
  }

  const selected = () => conversations.find((c) => c.id === selectedId) ?? conversations[0];

  function select(id: string | undefined) {
    selectedId = id;
    renderNav();
    const conversation = selected();
    if (conversation?.id === view?.id) return view?.update(conversation);
    stopConversation?.();
    view = conversation && artist ? conversationView(db, conversation, agentLabel) : undefined;
    stopConversation = view?.dispose;
    main.replaceChildren(
      view?.element ?? h("p", { class: "status", role: "status" }, "Loading conversations…"),
    );
  }
  let view: ReturnType<typeof conversationView> | undefined;

  const stops = [
    db.subscribe(app.artists, (rows) => {
      artist = rows[0];
      select(selectedId);
    }),
    db.subscribe(app.conversations.orderBy("$createdAt", "desc"), (rows) => {
      conversations = rows;
      select(selectedId);
    }),
  ];
  select(undefined);

  return {
    element,
    dispose() {
      for (const stop of stops) stop();
      stopConversation?.();
    },
  };
}

function conversationView(db: Db, initial: Conversation, agentLabel: string) {
  let conversation = initial;
  const where = { conversationId: conversation.id };
  let turns: TimedTurn[] = [];
  let toolCalls: ToolCall[] = [];
  let attachments: AttachmentSummary[] = [];
  // Audio players are reused across redraws, so playback survives streaming.
  const players = new Map<string, HTMLElement>();

  const messages = h("ol", { class: "messages", "aria-live": "polite" });
  const status = h("p", { class: "error", role: "alert", hidden: true });
  const input = h("textarea", { rows: 2, "aria-label": "Message input" });
  const picker = h("input", { type: "file", accept: "audio/*", multiple: true, hidden: true });
  const chosen = h("ul", { class: "files" });
  const send = h("button", { type: "submit", class: "primary" }, "Send");
  let files: File[] = [];
  const composer = h(
    "form",
    {
      class: "composer",
      on: {
        submit: (event) => {
          event.preventDefault();
          const text = input.value.trim();
          if (!text && !files.length) return;
          const outgoing = files;
          input.value = "";
          files = [];
          renderFiles();
          act(() => sendTurn(text, outgoing));
        },
      },
    },
    input,
    chosen,
    h(
      "div",
      { class: "row" },
      h(
        "button",
        { type: "button", class: "ghost", on: { click: () => picker.click() } },
        "Attach audio",
      ),
      picker,
      send,
    ),
    status,
  );
  picker.addEventListener("change", () => {
    files = [...files, ...Array.from(picker.files ?? [])];
    picker.value = "";
    renderFiles();
  });
  function renderFiles() {
    chosen.replaceChildren(
      ...files.map((file, index) =>
        h(
          "li",
          {},
          file.name,
          h(
            "button",
            {
              type: "button",
              class: "ghost",
              "aria-label": `Remove ${file.name}`,
              on: {
                click: () => {
                  files = files.filter((_, i) => i !== index);
                  renderFiles();
                },
              },
            },
            "×",
          ),
        ),
      ),
    );
  }

  const element = h("div", { class: "conversation-body" }, messages, composer);

  function act(action: () => Promise<unknown>) {
    status.hidden = true;
    action().catch((cause) => {
      status.textContent = cause instanceof Error ? cause.message : String(cause);
      status.hidden = false;
    });
  }

  async function sendTurn(text: string, outgoing: File[]) {
    const head = branchPath(turns, conversation.headTurnId).at(-1);
    // The browser writes the user's turn and audio into Jazz itself, then asks
    // the server to answer it. The turn and the head move are one transaction.
    const turnWrite = await db.transaction((tx) => {
      const turn = tx.insert(app.turns, {
        conversationId: conversation.id,
        parentId: head?.id,
        role: "user",
        body: text,
        status: "complete",
      });
      tx.update(app.conversations, conversation.id, {
        headTurnId: turn.id,
        ...(conversation.title === NEW_TITLE && text ? { title: titleFrom(text) } : {}),
      });
      return turn.id;
    });
    const userTurnId = turnWrite.value;
    // Audio streams in after the turn it belongs to, outside the transaction.
    const uploads = await Promise.all(
      outgoing.map((file) =>
        db.insertStreaming(app.attachments, {
          conversationId: conversation.id,
          turnId: userTurnId,
          filename: file.name,
          mediaType: file.type || "application/octet-stream",
          byteLength: file.size,
          payload: file.stream(),
        }),
      ),
    );
    await turnWrite.wait({ tier: "global" });
    await Promise.all(uploads.map((upload) => upload.wait({ tier: "global" })));
    await requestReply(userTurnId);
  }

  /** Show another branch: its newest leaf becomes the conversation's head. */
  const showBranch = (turnId: string) =>
    db.update(app.conversations, conversation.id, { headTurnId: latestLeaf(turns, turnId) });

  function render() {
    const path = branchPath(turns, conversation.headTurnId);
    const head = path.at(-1);
    const replying =
      head?.role === "assistant" && (head.status === "queued" || head.status === "streaming");
    input.disabled = replying;
    send.disabled = replying;
    input.placeholder = replying
      ? "Replying. This keeps going if you close the tab."
      : `Ask ${agentLabel === "Claude" ? "Claude" : "the agent"} about venues, dates or a setlist`;
    if (!path.length) {
      messages.replaceChildren(
        h(
          "li",
          { class: "empty" },
          h("h2", {}, "What are we booking?"),
          h(
            "p",
            { class: "secondary" },
            "Ask for venues in a city, the free dates on the calendar, or a setlist for a slot. Attach a rough mix to include it in the pitch.",
          ),
        ),
      );
      return;
    }
    messages.replaceChildren(
      ...path.map((turn) =>
        turn.role === "user"
          ? userMessage(turn, turn === head)
          : assistantMessage(turn, siblingsOf(turns, turn)),
      ),
    );
  }

  function userMessage(turn: TimedTurn, isHead: boolean) {
    return h(
      "li",
      { class: "message user" },
      ...attachments.filter((file) => file.turnId === turn.id).map(player),
      h("div", { class: "bubble" }, turn.body),
      h("time", { class: "secondary" }, turn.$createdAt.toLocaleTimeString()),
      // A user turn at the head with no reply (the request failed) can ask again.
      isHead &&
        h(
          "button",
          { type: "button", on: { click: () => act(() => requestReply(turn.id)) } },
          "Ask for a reply",
        ),
    );
  }

  function assistantMessage(turn: TimedTurn, siblings: TimedTurn[]) {
    const working = turn.status === "queued" || turn.status === "streaming";
    const index = siblings.indexOf(turn);
    const calls = toolCalls.filter((call) => call.turnId === turn.id);
    const regenerate = () => act(() => regenerateReply(turn.id));
    const recovery = (title: string, description: string, tone: string) =>
      h(
        "div",
        { class: `banner ${tone}`, role: "status" },
        h("strong", {}, title),
        h("p", {}, description),
        h(
          "div",
          { class: "row" },
          h(
            "button",
            {
              type: "button",
              class: "primary",
              on: { click: () => act(() => resumeReply(turn.id)) },
            },
            "Resume",
          ),
          h("button", { type: "button", on: { click: regenerate } }, "Regenerate"),
        ),
      );
    return h(
      "li",
      { class: "message assistant" },
      calls.length > 0 &&
        h(
          "ul",
          { class: "tool-calls", "aria-label": "Tool calls" },
          ...calls.map((call) =>
            h(
              "li",
              {},
              h("code", {}, call.name),
              " ",
              h("span", { class: "secondary" }, call.status),
            ),
          ),
        ),
      h(
        "div",
        { class: "bubble assistant-body" },
        (turn.body && markdown(turn.body)) ||
          (working
            ? h(
                "span",
                { role: "status" },
                turn.status === "queued" ? "Waiting for the agent…" : "Thinking…",
              )
            : h("span", { class: "secondary" }, "No reply was written.")),
      ),
      h(
        "div",
        { class: "row" },
        h("span", { class: "secondary" }, turn.provider ?? "Agent"),
        h("time", { class: "secondary" }, turn.$createdAt.toLocaleTimeString()),
        siblings.length > 1 &&
          h(
            "span",
            { class: "row" },
            h(
              "button",
              {
                type: "button",
                class: "ghost",
                "aria-label": "Previous reply",
                disabled: index === 0,
                on: { click: () => showBranch(siblings[index - 1]!.id) },
              },
              "‹",
            ),
            h("span", { class: "secondary" }, `${index + 1} of ${siblings.length}`),
            h(
              "button",
              {
                type: "button",
                class: "ghost",
                "aria-label": "Next reply",
                disabled: index === siblings.length - 1,
                on: { click: () => showBranch(siblings[index + 1]!.id) },
              },
              "›",
            ),
          ),
        !working &&
          h(
            "button",
            {
              type: "button",
              class: "ghost",
              title: "Answer again as a new branch",
              on: { click: regenerate },
            },
            "Regenerate",
          ),
      ),
      turn.status === "interrupted" &&
        recovery(
          "Reply interrupted",
          "The server stopped while writing this reply. Resume it from where it stopped, or regenerate it as a new branch.",
          "warning",
        ),
      turn.status === "failed" && recovery("Reply failed", turn.error ?? "", "error"),
    );
  }

  /**
   * The audio element asks for byte ranges as it plays and seeks; the server
   * answers each with a partial read of the Jazz `bytes` column.
   */
  function player(file: AttachmentSummary) {
    let card = players.get(file.id);
    if (!card) {
      card = h(
        "figure",
        { class: "card attachment" },
        h(
          "figcaption",
          {},
          file.filename,
          " ",
          h("span", { class: "secondary" }, formatBytes(file.byteLength)),
        ),
        file.mediaType.startsWith("audio/") &&
          h("audio", {
            controls: true,
            preload: "metadata",
            src: `/api/attachments/${file.id}`,
            "aria-label": `Play ${file.filename}`,
          }),
      );
      players.set(file.id, card);
    }
    return card;
  }

  const stops = [
    db.subscribe(app.turns.where(where).select("*", "$createdAt"), (rows) => {
      turns = rows as TimedTurn[];
      render();
    }),
    db.subscribe(app.toolCalls.where(where).orderBy("ordinal", "asc"), (rows) => {
      toolCalls = rows;
      render();
    }),
    // Attachment metadata only: the audio bytes are fetched page by page by the player.
    db.subscribe(
      app.attachments.where(where).select("turnId", "filename", "mediaType", "byteLength"),
      (rows) => {
        attachments = rows as AttachmentSummary[];
        render();
      },
    ),
  ];
  render();

  return {
    id: conversation.id,
    element,
    update(next: Conversation) {
      conversation = next;
      render();
    },
    dispose() {
      for (const stop of stops) stop();
    },
  };
}

function titleFrom(text: string) {
  const line = text.split("\n")[0]!.trim();
  return line.length > 48 ? `${line.slice(0, 47)}…` : line;
}

function formatBytes(bytes: number) {
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(bytes / 1000))} kB`;
}
