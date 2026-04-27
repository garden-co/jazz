import { FormEvent, useEffect, useMemo, useState } from "react";
import type { IssueItem, ItemKind, ItemStatus, ListedItem } from "../repository.js";

type KindFilter = "all" | ItemKind;
type StatusFilter = "all" | ItemStatus;
type DraftItem = IssueItem;

const emptyDraft: DraftItem = {
  kind: "issue",
  slug: "",
  title: "",
  description: "",
};

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: {
      "content-type": "application/json",
      ...init?.headers,
    },
    ...init,
  });
  const body: unknown = await response.json();

  if (!response.ok) {
    const message =
      typeof body === "object" && body !== null && "error" in body && typeof body.error === "string"
        ? body.error
        : "Request failed.";
    throw new Error(message);
  }

  return body as T;
}

async function fetchItems(): Promise<ListedItem[]> {
  return api<ListedItem[]>("/api/items");
}

async function createItem(item: IssueItem): Promise<void> {
  await api<{ ok: true }>("/api/items", {
    method: "POST",
    body: JSON.stringify(item),
  });
}

async function assignMe(slug: string): Promise<void> {
  await api<{ ok: true }>(`/api/items/${encodeURIComponent(slug)}/assign-me`, {
    method: "POST",
  });
}

async function setStatus(slug: string, status: ItemStatus): Promise<void> {
  await api<{ ok: true }>(`/api/items/${encodeURIComponent(slug)}/status`, {
    method: "POST",
    body: JSON.stringify({ status }),
  });
}

async function exportTodo(): Promise<void> {
  await api<{ ok: true }>("/api/export", {
    method: "POST",
  });
}

function statusLabel(status: ItemStatus): string {
  return status.replace("_", " ");
}

export function App() {
  const [items, setItems] = useState<ListedItem[]>([]);
  const [selectedSlug, setSelectedSlug] = useState<string | null>(null);
  const [kindFilter, setKindFilter] = useState<KindFilter>("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [draft, setDraft] = useState<DraftItem>(emptyDraft);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  async function reload(nextSelectedSlug = selectedSlug) {
    const nextItems = await fetchItems();
    setItems(nextItems);
    setSelectedSlug(
      nextSelectedSlug && nextItems.some((item) => item.slug === nextSelectedSlug)
        ? nextSelectedSlug
        : (nextItems[0]?.slug ?? null),
    );
  }

  useEffect(() => {
    reload().catch((loadError: unknown) => {
      setError(loadError instanceof Error ? loadError.message : String(loadError));
    });
  }, []);

  const filteredItems = useMemo(
    () =>
      items.filter(
        (item) =>
          (kindFilter === "all" || item.kind === kindFilter) &&
          (statusFilter === "all" || item.state.status === statusFilter),
      ),
    [items, kindFilter, statusFilter],
  );
  const selectedItem =
    filteredItems.find((item) => item.slug === selectedSlug) ?? filteredItems[0] ?? null;

  async function run(action: () => Promise<void>, success: string) {
    setBusy(true);
    setError("");
    setMessage("");
    try {
      await action();
      setMessage(success);
    } catch (actionError) {
      setError(actionError instanceof Error ? actionError.message : String(actionError));
    } finally {
      setBusy(false);
    }
  }

  async function submitCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await run(async () => {
      await createItem(draft);
      await reload(draft.slug);
      setDraft(emptyDraft);
    }, "Item saved.");
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <h1>Skill Issues</h1>
          <p>{items.length} tracked items</p>
        </div>
        <button
          type="button"
          className="secondary"
          disabled={busy}
          onClick={() => run(exportTodo, "Exported to todo/.")}
        >
          Export todo
        </button>
      </header>

      <section className="workspace" aria-label="Skill issues workspace">
        <aside className="sidebar" aria-label="Issue list">
          <div className="filters">
            <label>
              Kind
              <select
                value={kindFilter}
                onChange={(event) => setKindFilter(event.target.value as KindFilter)}
              >
                <option value="all">All</option>
                <option value="issue">Issues</option>
                <option value="idea">Ideas</option>
              </select>
            </label>
            <label>
              Status
              <select
                value={statusFilter}
                onChange={(event) => setStatusFilter(event.target.value as StatusFilter)}
              >
                <option value="all">All</option>
                <option value="open">Open</option>
                <option value="in_progress">In progress</option>
                <option value="done">Done</option>
              </select>
            </label>
          </div>

          <div className="item-list">
            {filteredItems.map((item) => (
              <button
                type="button"
                key={item.slug}
                className={item.slug === selectedItem?.slug ? "item-row selected" : "item-row"}
                onClick={() => setSelectedSlug(item.slug)}
              >
                <span className="item-title">{item.title}</span>
                <span className="item-meta">
                  {item.kind} · {statusLabel(item.state.status)}
                </span>
              </button>
            ))}
            {filteredItems.length === 0 ? (
              <p className="empty">No items match these filters.</p>
            ) : null}
          </div>
        </aside>

        <section className="detail" aria-label="Selected item">
          {selectedItem ? (
            <>
              <div className="detail-header">
                <div>
                  <span className="eyebrow">{selectedItem.kind}</span>
                  <h2>{selectedItem.title}</h2>
                  <p className="slug">{selectedItem.slug}</p>
                </div>
                <select
                  value={selectedItem.state.status}
                  disabled={busy}
                  onChange={(event) =>
                    run(async () => {
                      await setStatus(selectedItem.slug, event.target.value as ItemStatus);
                      await reload(selectedItem.slug);
                    }, "Status updated.")
                  }
                >
                  <option value="open">Open</option>
                  <option value="in_progress">In progress</option>
                  <option value="done">Done</option>
                </select>
              </div>
              <p className="description">{selectedItem.description}</p>
              <div className="actions">
                <button
                  type="button"
                  disabled={busy}
                  onClick={() =>
                    run(async () => {
                      await assignMe(selectedItem.slug);
                      await reload(selectedItem.slug);
                    }, "Assigned to current user.")
                  }
                >
                  Assign me
                </button>
                <span className="assignee">
                  {selectedItem.assignee
                    ? `Assigned to ${selectedItem.assignee.githubLogin}`
                    : "Unassigned"}
                </span>
              </div>
            </>
          ) : (
            <p className="empty">Select or create an item.</p>
          )}

          <form className="create-form" onSubmit={submitCreate}>
            <h2>Create item</h2>
            <div className="form-grid">
              <label>
                Kind
                <select
                  value={draft.kind}
                  onChange={(event) => setDraft({ ...draft, kind: event.target.value as ItemKind })}
                >
                  <option value="issue">Issue</option>
                  <option value="idea">Idea</option>
                </select>
              </label>
              <label>
                Slug
                <input
                  value={draft.slug}
                  onChange={(event) => setDraft({ ...draft, slug: event.target.value })}
                  placeholder="sync-status"
                  required
                />
              </label>
              <label className="wide">
                Title
                <input
                  value={draft.title}
                  onChange={(event) => setDraft({ ...draft, title: event.target.value })}
                  placeholder="Readable item title"
                  required
                />
              </label>
              <label className="wide">
                Description
                <textarea
                  value={draft.description}
                  onChange={(event) => setDraft({ ...draft, description: event.target.value })}
                  rows={5}
                  required
                />
              </label>
            </div>
            <button type="submit" disabled={busy}>
              Save item
            </button>
          </form>
        </section>
      </section>

      {message ? <p className="notice">{message}</p> : null}
      {error ? <p className="notice error">{error}</p> : null}
    </main>
  );
}
