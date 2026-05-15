import { useEffect, useMemo, useState } from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { toast } from "sonner";
import { app, CATEGORIES, HERO_COLORS, STATUSES } from "../schema.js";
import type { Article, Branch } from "../schema.js";

type ArticleDiff = {
  kind: "insert" | "update" | "delete" | "unchanged" | "error";
  changed: string[];
  conflicts: string[];
};

type DiffArticle = Article & {
  $diff?: ArticleDiff | null;
};

type ArticleFormValues = {
  title: string;
  slug: string;
  excerpt: string;
  body: string;
  status: (typeof STATUSES)[number];
  category: (typeof CATEGORIES)[number];
  tags: string[];
  hero_color: string;
  featured: boolean;
};

type DiffChange = {
  articleKey: string;
  kind: "insert" | "update" | "delete";
  title: string;
  changed: string[];
  conflicts: string[];
};

const SEED_ARTICLES: Array<ArticleFormValues & { articleKey: string }> = [
  {
    articleKey: "welcome",
    title: "Welcome to Editorial",
    slug: "welcome-to-editorial",
    excerpt: "A guided tour of branch-powered publishing.",
    body: "Branches let your team draft, review, and merge changes — across many articles at once — without ever touching the live site.",
    status: "published",
    category: "news",
    tags: ["announcement", "intro"],
    hero_color: "#6366f1",
    featured: true,
  },
  {
    articleKey: "real-time",
    title: "Introducing real-time relations",
    slug: "real-time-relations",
    excerpt: "How Jazz keeps your relational data in sync, instantly.",
    body: "Local-first, server-replicated, conflict-aware. Plus, branches you can merge atomically.",
    status: "published",
    category: "engineering",
    tags: ["sync", "relational"],
    hero_color: "#10b981",
    featured: false,
  },
  {
    articleKey: "offline-design",
    title: "Designing for offline",
    slug: "designing-for-offline",
    excerpt: "Make every interaction feel local — because it is.",
    body: "A design system that treats network latency as a non-event.",
    status: "review",
    category: "design",
    tags: ["ux", "offline"],
    hero_color: "#ec4899",
    featured: false,
  },
];

function emptyFormValues(): ArticleFormValues {
  return {
    title: "",
    slug: "",
    excerpt: "",
    body: "",
    status: "draft",
    category: "news",
    tags: [],
    hero_color: HERO_COLORS[0],
    featured: false,
  };
}

function valuesFromArticle(article: Article | null): ArticleFormValues {
  if (!article) return emptyFormValues();
  return {
    title: article.title,
    slug: article.slug,
    excerpt: article.excerpt,
    body: article.body,
    status: article.status,
    category: article.category,
    tags: [...article.tags],
    hero_color: article.hero_color,
    featured: article.featured,
  };
}

function slugify(input: string): string {
  return input
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .slice(0, 60);
}

function generateArticleKey(): string {
  return `art-${Math.random().toString(36).slice(2, 10)}`;
}

function describeDiffField(field: string): string {
  if (field === "hero_color") return "hero color";
  if (field === "articleKey" || field === "revision" || field === "owner_id") return "";
  return field;
}

/**
 * Collapse all article revisions into a per-articleKey "head" — the highest-numbered
 * non-deleted revision visible in this view. This is how the UI treats a logical article.
 */
function collapseRevisions(rows: Article[]): Article[] {
  const headByKey = new Map<string, Article>();
  for (const row of rows) {
    const existing = headByKey.get(row.articleKey);
    if (!existing || row.revision > existing.revision) {
      headByKey.set(row.articleKey, row);
    }
  }
  const result: Article[] = [];
  for (const head of headByKey.values()) {
    if (!head.deleted) result.push(head);
  }
  return result;
}

/**
 * Convert branch-side diff rows (per-revision) into per-article changes. We compare
 * each branch-side head against the main-side head for the same articleKey to decide
 * whether the article was inserted, updated, or deleted from the branch's perspective.
 */
function summarizeDiff(diffRows: DiffArticle[], mainArticles: Article[]): Map<string, DiffChange> {
  const mainHeads = new Map<string, Article>();
  for (const row of mainArticles) {
    const head = mainHeads.get(row.articleKey);
    if (!head || row.revision > head.revision) mainHeads.set(row.articleKey, row);
  }
  const branchHeads = new Map<string, DiffArticle>();
  for (const row of diffRows) {
    const head = branchHeads.get(row.articleKey);
    if (!head || row.revision > head.revision) branchHeads.set(row.articleKey, row);
  }

  const changes = new Map<string, DiffChange>();
  for (const [articleKey, head] of branchHeads.entries()) {
    if (!head.$diff || head.$diff.kind === "unchanged" || head.$diff.kind === "error") continue;
    const mainHead = mainHeads.get(articleKey);
    const mainDeleted = mainHead?.deleted ?? true;
    const branchDeleted = head.deleted;

    let kind: "insert" | "update" | "delete";
    if (branchDeleted && !mainDeleted) {
      kind = "delete";
    } else if (!branchDeleted && mainDeleted) {
      kind = "insert";
    } else {
      kind = "update";
    }

    const changed: string[] = [];
    if (mainHead) {
      for (const key of [
        "title",
        "slug",
        "excerpt",
        "body",
        "status",
        "category",
        "hero_color",
        "featured",
        "tags",
      ] as const) {
        const a = mainHead[key];
        const b = head[key];
        if (Array.isArray(a) && Array.isArray(b)) {
          if (a.join(",") !== b.join(",")) changed.push(key);
        } else if (a !== b) {
          changed.push(key);
        }
      }
    }

    changes.set(articleKey, {
      articleKey,
      kind,
      title: head.title || mainHead?.title || "Untitled",
      changed,
      conflicts: head.$diff.conflicts,
    });
  }
  return changes;
}

function ArticleListItem({
  article,
  selected,
  diff,
  onClick,
}: {
  article: Article;
  selected: boolean;
  diff?: DiffChange;
  onClick: () => void;
}) {
  const conflicted = (diff?.conflicts.length ?? 0) > 0;
  return (
    <li
      data-testid="article-row"
      data-article-key={article.articleKey}
      data-diff-kind={diff?.kind ?? "unchanged"}
      data-conflicted={conflicted ? "true" : "false"}
      className={selected ? "selected" : ""}
      onClick={onClick}
    >
      <div className="hero-stripe" style={{ background: article.hero_color }} />
      <div className="item-content">
        <p className="item-title">
          {article.featured ? <span title="Featured">★ </span> : null}
          {article.title || "Untitled"}
        </p>
        <div className="item-meta">
          <span className={`status-pill status-${article.status}`}>{article.status}</span>
          <span>{article.category}</span>
        </div>
      </div>
      {diff && (
        <span
          className={`item-badge diff-${conflicted ? "conflict" : diff.kind}`}
          data-testid="article-diff-badge"
        >
          {conflicted ? "conflict" : diff.kind}
        </span>
      )}
    </li>
  );
}

function TagEditor({ tags, onChange }: { tags: string[]; onChange: (tags: string[]) => void }) {
  const [input, setInput] = useState("");

  const addTag = (value: string) => {
    const trimmed = value.trim().toLowerCase();
    if (!trimmed || tags.includes(trimmed)) return;
    onChange([...tags, trimmed]);
    setInput("");
  };

  const removeTag = (tag: string) => {
    onChange(tags.filter((t) => t !== tag));
  };

  return (
    <div className="tag-input">
      {tags.map((tag) => (
        <span key={tag} className="tag-chip">
          {tag}
          <button type="button" aria-label={`Remove ${tag}`} onClick={() => removeTag(tag)}>
            ×
          </button>
        </span>
      ))}
      <input
        type="text"
        placeholder={tags.length === 0 ? "Add tags…" : ""}
        value={input}
        data-testid="tag-input"
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === ",") {
            e.preventDefault();
            addTag(input);
          } else if (e.key === "Backspace" && !input && tags.length > 0) {
            removeTag(tags[tags.length - 1]!);
          }
        }}
        onBlur={() => input && addTag(input)}
      />
    </div>
  );
}

function CreateBranchDialog({
  onClose,
  onCreate,
}: {
  onClose: () => void;
  onCreate: (name: string, description: string) => void;
}) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>New branch</h2>
        <label>
          Branch name
          <input
            type="text"
            value={name}
            data-testid="branch-name-input"
            autoFocus
            placeholder="Spring relaunch"
            onChange={(e) => setName(e.target.value)}
          />
        </label>
        <label>
          Description <span className="helper-text">optional</span>
          <input
            type="text"
            value={description}
            data-testid="branch-description-input"
            placeholder="What is this branch for?"
            onChange={(e) => setDescription(e.target.value)}
          />
        </label>
        <div className="modal-actions">
          <button type="button" className="ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            className="primary"
            data-testid="create-branch-confirm"
            disabled={!name.trim()}
            onClick={() => onCreate(name.trim(), description.trim())}
          >
            Create branch
          </button>
        </div>
      </div>
    </div>
  );
}

function MergeBranchDialog({
  branch,
  changes,
  onClose,
  onMerge,
}: {
  branch: Branch;
  changes: DiffChange[];
  onClose: () => void;
  onMerge: () => void;
}) {
  const counts = changes.reduce(
    (acc, change) => {
      acc[change.kind]++;
      if (change.conflicts.length) acc.conflict++;
      return acc;
    },
    { insert: 0, update: 0, delete: 0, conflict: 0 },
  );

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>Merge “{branch.name}”?</h2>
        <p className="helper-text" style={{ marginBottom: "1rem" }}>
          This replays every change in this branch onto the published timeline.
        </p>
        <div className="diff-summary">
          {counts.insert > 0 && <span className="diff-stat insert">+ {counts.insert} new</span>}
          {counts.update > 0 && <span className="diff-stat update">~ {counts.update} edited</span>}
          {counts.delete > 0 && <span className="diff-stat delete">– {counts.delete} removed</span>}
          {counts.conflict > 0 && (
            <span className="diff-stat conflict">⚠ {counts.conflict} with conflicts</span>
          )}
          {changes.length === 0 && (
            <span style={{ color: "var(--text-muted)" }}>No changes to merge.</span>
          )}
        </div>
        <div className="modal-actions">
          <button type="button" className="ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            className="primary"
            data-testid="merge-branch-confirm"
            onClick={onMerge}
          >
            Merge to Published
          </button>
        </div>
      </div>
    </div>
  );
}

export function EditorialWorkspace() {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;

  const [activeBranchId, setActiveBranchId] = useState<string | null>(null);
  const [selectedArticleKey, setSelectedArticleKey] = useState<string | null>(null);
  const [showCreateBranch, setShowCreateBranch] = useState(false);
  const [showMergeDialog, setShowMergeDialog] = useState(false);
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | (typeof STATUSES)[number]>("all");
  const [formValues, setFormValues] = useState<ArticleFormValues>(emptyFormValues());

  const branches =
    useAll(sessionUserId ? app.branches.where({ owner_id: sessionUserId }) : undefined) ?? [];

  const articlesQuery = activeBranchId ? app.articles.branch(activeBranchId) : app.articles;
  const allRevisions = useAll(articlesQuery) ?? [];
  const articles = useMemo(() => collapseRevisions(allRevisions), [allRevisions]);

  const mainRevisions = useAll(activeBranchId ? app.articles : undefined) ?? [];
  const diffRows = (useAll(
    activeBranchId ? app.articles.branch(activeBranchId).diff() : undefined,
  ) ?? []) as DiffArticle[];

  const diffByArticleKey = useMemo<Map<string, DiffChange>>(
    () => (activeBranchId ? summarizeDiff(diffRows, mainRevisions) : new Map()),
    [diffRows, mainRevisions, activeBranchId],
  );
  const changes = useMemo(() => [...diffByArticleKey.values()], [diffByArticleKey]);

  const diffCounts = useMemo(() => {
    const counts = { insert: 0, update: 0, delete: 0, conflict: 0 };
    for (const change of changes) {
      counts[change.kind]++;
      if (change.conflicts.length > 0) counts.conflict++;
    }
    return counts;
  }, [changes]);

  const filteredArticles = useMemo(() => {
    const term = search.trim().toLowerCase();
    return articles
      .filter((a) => statusFilter === "all" || a.status === statusFilter)
      .filter(
        (a) =>
          !term ||
          a.title.toLowerCase().includes(term) ||
          a.excerpt.toLowerCase().includes(term) ||
          a.tags.some((tag) => tag.toLowerCase().includes(term)),
      )
      .toSorted((a, b) => {
        const featuredDelta = Number(b.featured) - Number(a.featured);
        if (featuredDelta !== 0) return featuredDelta;
        return a.title.localeCompare(b.title);
      });
  }, [articles, search, statusFilter]);

  const selectedArticle =
    articles.find((a) => a.articleKey === selectedArticleKey) ?? filteredArticles[0] ?? null;
  const activeBranch = branches.find((b) => b.id === activeBranchId) ?? null;
  const totalDiffCount = changes.length;

  useEffect(() => {
    setFormValues(valuesFromArticle(selectedArticle));
  }, [selectedArticle?.articleKey, selectedArticle?.revision, activeBranchId]);

  const updateField = <K extends keyof ArticleFormValues>(
    field: K,
    value: ArticleFormValues[K],
  ) => {
    setFormValues((prev) => ({ ...prev, [field]: value }));
  };

  const writeDb = activeBranchId ? db.branch(activeBranchId) : db;

  const handleSeedArticles = () => {
    if (!sessionUserId) return;
    for (const seed of SEED_ARTICLES) {
      db.insert(app.articles, {
        ...seed,
        revision: 1,
        owner_id: sessionUserId,
      });
    }
  };

  const handleCreateBranch = (name: string, description: string) => {
    if (!sessionUserId) return;
    const { value: branch } = db.insert(app.branches, {
      name,
      description,
      owner_id: sessionUserId,
    });
    setActiveBranchId(branch.id);
    setShowCreateBranch(false);
    toast.success(`Switched to "${name}"`);
  };

  const handleDiscardBranch = () => {
    if (!activeBranchId || !activeBranch) return;
    try {
      db.delete(app.branches, activeBranchId);
      setActiveBranchId(null);
      toast.success(`Discarded "${activeBranch.name}"`);
    } catch {
      toast.error("Could not discard branch");
    }
  };

  const handleCreateArticle = () => {
    if (!sessionUserId) return;
    const articleKey = generateArticleKey();
    const slug = `untitled-${Math.random().toString(36).slice(2, 6)}`;
    try {
      writeDb.insert(app.articles, {
        articleKey,
        revision: 1,
        title: "Untitled draft",
        slug,
        owner_id: sessionUserId,
      });
      setSelectedArticleKey(articleKey);
    } catch (err) {
      console.error("[handleCreateArticle]", err);
      toast.error("Could not create article");
    }
  };

  const handleSave = () => {
    if (!selectedArticle || !sessionUserId) return;
    try {
      writeDb.insert(app.articles, {
        articleKey: selectedArticle.articleKey,
        revision: selectedArticle.revision + 1,
        owner_id: sessionUserId,
        title: formValues.title,
        slug: formValues.slug,
        excerpt: formValues.excerpt,
        body: formValues.body,
        status: formValues.status,
        category: formValues.category,
        tags: formValues.tags,
        hero_color: formValues.hero_color,
        featured: formValues.featured,
      });
      toast.success(activeBranchId ? "Saved to branch" : "Published");
    } catch (err) {
      console.error("[handleSave]", err);
      toast.error("You don't have permission to edit this article");
    }
  };

  const handleDeleteArticle = () => {
    if (!selectedArticle || !sessionUserId) return;
    try {
      writeDb.insert(app.articles, {
        articleKey: selectedArticle.articleKey,
        revision: selectedArticle.revision + 1,
        owner_id: sessionUserId,
        title: selectedArticle.title,
        slug: selectedArticle.slug,
        excerpt: selectedArticle.excerpt,
        body: selectedArticle.body,
        status: selectedArticle.status,
        category: selectedArticle.category,
        tags: selectedArticle.tags,
        hero_color: selectedArticle.hero_color,
        featured: selectedArticle.featured,
        deleted: true,
      });
      setSelectedArticleKey(null);
    } catch (err) {
      console.error("[handleDeleteArticle]", err);
      toast.error("Could not delete article");
    }
  };

  const handleMerge = () => {
    if (!activeBranchId || !activeBranch) return;
    try {
      db.branch(activeBranchId).merge();
      db.delete(app.branches, activeBranchId);
      toast.success(`Merged "${activeBranch.name}" into Published`);
      setActiveBranchId(null);
      setShowMergeDialog(false);
    } catch (err) {
      console.error("[handleMerge]", err);
      toast.error("Could not merge branch");
    }
  };

  const isDirty = selectedArticle
    ? JSON.stringify(valuesFromArticle(selectedArticle)) !== JSON.stringify(formValues)
    : false;

  return (
    <div className="app" data-testid="cms-editor">
      <header className="topbar">
        <div className="brand">
          <span className="logo" />
          Editorial
        </div>

        <div className="branch-bar">
          <button
            type="button"
            data-testid="select-published"
            className={`branch-tab published ${!activeBranchId ? "selected" : ""}`}
            onClick={() => setActiveBranchId(null)}
          >
            <span className="dot" />
            Published
          </button>
          {branches.map((branch) => (
            <button
              key={branch.id}
              type="button"
              data-testid="select-branch"
              data-branch-id={branch.id}
              className={`branch-tab ${activeBranchId === branch.id ? "selected" : ""}`}
              onClick={() => setActiveBranchId(branch.id)}
            >
              <span className="dot" />
              {branch.name}
            </button>
          ))}
          <button
            type="button"
            className="ghost"
            data-testid="create-branch"
            disabled={!sessionUserId || articles.length === 0}
            onClick={() => setShowCreateBranch(true)}
          >
            + Branch
          </button>
        </div>
      </header>

      <div className="workspace">
        <aside className="sidebar">
          <div className="sidebar-header">
            <h2>
              Articles{" "}
              <span style={{ color: "var(--text-muted)", fontWeight: 400 }}>
                ({filteredArticles.length})
              </span>
            </h2>
            <button
              type="button"
              data-testid="new-article"
              disabled={!sessionUserId}
              onClick={handleCreateArticle}
              title="New article"
            >
              + New
            </button>
          </div>
          <div className="filter-row">
            <input
              type="text"
              placeholder="Search…"
              value={search}
              data-testid="article-search"
              onChange={(e) => setSearch(e.target.value)}
            />
            <select
              value={statusFilter}
              data-testid="status-filter"
              onChange={(e) => setStatusFilter(e.target.value as "all" | (typeof STATUSES)[number])}
            >
              <option value="all">All</option>
              {STATUSES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>

          {filteredArticles.length === 0 ? (
            <div style={{ padding: "1.5rem", textAlign: "center", color: "var(--text-muted)" }}>
              {articles.length === 0 ? (
                <>
                  <p style={{ margin: "0 0 0.75rem" }}>No articles yet.</p>
                  <button
                    type="button"
                    className="primary"
                    data-testid="seed-articles"
                    disabled={!sessionUserId}
                    onClick={handleSeedArticles}
                  >
                    Seed sample articles
                  </button>
                </>
              ) : (
                <p>No matches.</p>
              )}
            </div>
          ) : (
            <ul className="article-list">
              {filteredArticles.map((article) => (
                <ArticleListItem
                  key={article.articleKey}
                  article={article}
                  selected={article.articleKey === selectedArticle?.articleKey}
                  diff={diffByArticleKey.get(article.articleKey)}
                  onClick={() => setSelectedArticleKey(article.articleKey)}
                />
              ))}
            </ul>
          )}
        </aside>

        <main className="editor-pane">
          {!selectedArticle ? (
            <div className="empty">
              <h3>No article selected</h3>
              <p>Pick an article from the sidebar or create a new one.</p>
            </div>
          ) : (
            <>
              {activeBranch && (
                <div className="branch-banner">
                  <span className="icon">⎇</span>
                  <span>
                    Editing on <span className="name">{activeBranch.name}</span>
                    {activeBranch.description ? ` — ${activeBranch.description}` : ""}
                  </span>
                </div>
              )}

              <div className="editor-header">
                <div>
                  <h1 data-testid="article-title">{formValues.title || "Untitled"}</h1>
                  <div className="editor-header-meta">
                    <span className={`status-pill status-${formValues.status}`}>
                      {formValues.status}
                    </span>
                    <span className="helper-text">/{formValues.slug || "no-slug"}</span>
                  </div>
                </div>
                <div className="editor-actions">
                  <button
                    type="button"
                    className="danger"
                    data-testid="delete-article"
                    onClick={handleDeleteArticle}
                  >
                    Delete
                  </button>
                  <button
                    type="button"
                    className="primary"
                    data-testid="save-article"
                    disabled={!isDirty}
                    onClick={handleSave}
                  >
                    {activeBranchId ? "Save to branch" : "Publish"}
                  </button>
                </div>
              </div>

              <div className="field-grid">
                <label className="full">
                  Title
                  <input
                    type="text"
                    data-testid="title-input"
                    value={formValues.title}
                    onChange={(e) => {
                      const nextTitle = e.target.value;
                      setFormValues((prev) => ({
                        ...prev,
                        title: nextTitle,
                        slug:
                          prev.slug === slugify(prev.title) || prev.slug === ""
                            ? slugify(nextTitle)
                            : prev.slug,
                      }));
                    }}
                  />
                </label>

                <label>
                  Slug
                  <input
                    type="text"
                    data-testid="slug-input"
                    value={formValues.slug}
                    onChange={(e) => updateField("slug", e.target.value)}
                  />
                </label>

                <label>
                  Status
                  <select
                    data-testid="status-input"
                    value={formValues.status}
                    onChange={(e) =>
                      updateField("status", e.target.value as ArticleFormValues["status"])
                    }
                  >
                    {STATUSES.map((s) => (
                      <option key={s} value={s}>
                        {s}
                      </option>
                    ))}
                  </select>
                </label>

                <label>
                  Category
                  <select
                    data-testid="category-input"
                    value={formValues.category}
                    onChange={(e) =>
                      updateField("category", e.target.value as ArticleFormValues["category"])
                    }
                  >
                    {CATEGORIES.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </label>

                <div className="toggle-row">
                  <label>
                    <input
                      type="checkbox"
                      data-testid="featured-input"
                      checked={formValues.featured}
                      onChange={(e) => updateField("featured", e.target.checked)}
                    />
                    Featured
                  </label>
                </div>

                <label className="full">
                  Tags
                  <TagEditor
                    tags={formValues.tags}
                    onChange={(tags) => updateField("tags", tags)}
                  />
                </label>

                <label className="full">
                  Hero color
                  <div className="color-row">
                    {HERO_COLORS.map((color) => (
                      <button
                        key={color}
                        type="button"
                        aria-label={`Hero color ${color}`}
                        data-testid="color-swatch"
                        data-color={color}
                        className={`color-swatch ${formValues.hero_color === color ? "selected" : ""}`}
                        style={{ background: color }}
                        onClick={() => updateField("hero_color", color)}
                      />
                    ))}
                  </div>
                </label>

                <label className="full">
                  Excerpt
                  <input
                    type="text"
                    data-testid="excerpt-input"
                    value={formValues.excerpt}
                    onChange={(e) => updateField("excerpt", e.target.value)}
                  />
                </label>

                <label className="full">
                  Body
                  <textarea
                    data-testid="body-input"
                    value={formValues.body}
                    onChange={(e) => updateField("body", e.target.value)}
                  />
                </label>
              </div>

              {activeBranchId && (
                <section className="diff-panel" data-testid="draft-diff">
                  <div className="diff-header">
                    <h3>Branch changes</h3>
                    <div className="diff-summary">
                      {diffCounts.insert > 0 && (
                        <span className="diff-stat insert">+ {diffCounts.insert}</span>
                      )}
                      {diffCounts.update > 0 && (
                        <span className="diff-stat update">~ {diffCounts.update}</span>
                      )}
                      {diffCounts.delete > 0 && (
                        <span className="diff-stat delete">– {diffCounts.delete}</span>
                      )}
                      {diffCounts.conflict > 0 && (
                        <span className="diff-stat conflict">⚠ {diffCounts.conflict}</span>
                      )}
                      {totalDiffCount === 0 && (
                        <span style={{ color: "var(--text-muted)" }}>No changes yet.</span>
                      )}
                    </div>
                    <div style={{ display: "flex", gap: "0.4rem" }}>
                      <button
                        type="button"
                        className="ghost"
                        data-testid="discard-branch"
                        onClick={handleDiscardBranch}
                      >
                        Discard
                      </button>
                      <button
                        type="button"
                        className="primary"
                        data-testid="merge-branch"
                        disabled={totalDiffCount === 0}
                        onClick={() => setShowMergeDialog(true)}
                      >
                        Merge to Published
                      </button>
                    </div>
                  </div>
                  {totalDiffCount > 0 && (
                    <ul className="diff-list" data-testid="diff-list">
                      {changes.map((change) => (
                        <li key={change.articleKey} data-testid="diff-item">
                          <span className={`kind ${change.kind}`}>{change.kind}</span>
                          <div>
                            <div className="title">{change.title}</div>
                            {change.changed.length > 0 && (
                              <div className="changes">
                                {change.changed.map(describeDiffField).filter(Boolean).join(", ")}
                              </div>
                            )}
                          </div>
                          {change.conflicts.length > 0 && (
                            <span className="conflicts">⚠ conflict</span>
                          )}
                        </li>
                      ))}
                    </ul>
                  )}
                </section>
              )}
            </>
          )}
        </main>
      </div>

      {showCreateBranch && (
        <CreateBranchDialog
          onClose={() => setShowCreateBranch(false)}
          onCreate={handleCreateBranch}
        />
      )}

      {showMergeDialog && activeBranch && (
        <MergeBranchDialog
          branch={activeBranch}
          changes={changes}
          onClose={() => setShowMergeDialog(false)}
          onMerge={handleMerge}
        />
      )}
    </div>
  );
}
