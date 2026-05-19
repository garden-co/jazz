import * as React from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { app, type Article, type Draft } from "../schema.js";
import { CoverImage } from "./CoverImage.js";
import type { Route } from "./router.js";
import { logEvent, shortId, startOperation } from "./telemetry.js";

type Props = {
  navigate: (next: Route) => void;
};

export function FrontPage({ navigate }: Props) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  const [creating, setCreating] = React.useState(false);

  const publishedArticles =
    useAll(app.articles.where({ published: true }).orderBy("createdAt", "desc")) ?? [];
  const myDrafts = useAll(userId ? app.drafts.where({ ownerId: userId }) : undefined) ?? [];

  React.useEffect(() => {
    logEvent("medium.home.visible", {
      "user.id": shortId(userId),
      "articles.published_count": publishedArticles.length,
      "drafts.count": myDrafts.length,
      "drafts.ids": myDrafts.map((draft) => shortId(draft.id)).join(","),
      "articles.ids": publishedArticles.map((article) => shortId(article.id)).join(","),
    });
  }, [userId, publishedArticles, myDrafts]);

  const startNew = async () => {
    if (!userId || creating) return;
    const operation = startOperation("medium.article.create", {
      "user.id": shortId(userId),
    });
    setCreating(true);
    try {
      // 1. Insert a blank, unpublished article on main. It must exist on main
      //    before we capture the branch scope so the scope query picks it up.
      const articleWrite = db.insert(app.articles, {
        title: "",
        subtitle: "",
        content: "",
        labels: [],
        authorId: userId,
        published: false,
        createdAt: new Date(),
      });
      const article = await articleWrite.wait({ tier: "local" });
      operation.step("article_inserted", {
        "article.id": shortId(article.id),
      });

      // 2. Insert a draft metadata row. Its id doubles as the branch id.
      const draftWrite = db.insert(app.drafts, {
        articleId: article.id,
        ownerId: userId,
        createdAt: new Date(),
      });
      const draft = await draftWrite.wait({ tier: "local" });
      operation.step("draft_inserted", {
        "article.id": shortId(article.id),
        "draft.id": shortId(draft.id),
      });

      // 3. Capture a branch scope over the single article the draft points at.
      //    From here on, every read and write through this branch id sees only
      //    that one row, and edits never leak to main until merge.
      await db.createBranch(draft.id, app.articles.where({ id: article.id }));
      operation.done({
        "article.id": shortId(article.id),
        "draft.id": shortId(draft.id),
      });

      navigate({ name: "edit", draftId: draft.id });
    } catch (err) {
      operation.error(err);
      throw err;
    } finally {
      setCreating(false);
    }
  };

  return (
    <>
      <div className="home-hero">
        <div>
          <h2>Latest stories</h2>
          <p>
            {publishedArticles.length} published
            {myDrafts.length > 0 && (
              <>
                {" · "}
                {myDrafts.length} private draft{myDrafts.length === 1 ? "" : "s"}
              </>
            )}
          </p>
        </div>
        <button className="btn btn-primary" onClick={startNew} disabled={!userId || creating}>
          {creating ? "Creating..." : "Write"}
        </button>
      </div>

      {myDrafts.length > 0 && <DraftsPanel drafts={myDrafts} navigate={navigate} />}

      {publishedArticles.length === 0 ? (
        <div className="empty-state">
          <strong>No published stories yet.</strong>
          Click <em>Write</em> to start the first one — your work stays on a private branch until
          you publish.
        </div>
      ) : (
        <section className="article-grid">
          {publishedArticles.map((article) => (
            <ArticleCard
              key={article.id}
              article={article}
              isMine={article.authorId === userId}
              onOpen={() => navigate({ name: "view", articleId: article.id })}
              onEdit={() => editArticle(db, article.id, userId, myDrafts, navigate)}
            />
          ))}
        </section>
      )}
    </>
  );
}

async function editArticle(
  db: ReturnType<typeof useDb>,
  articleId: string,
  userId: string | null,
  myDrafts: Draft[],
  navigate: (next: Route) => void,
) {
  if (!userId) return;
  // Reuse an existing draft for this (article, user) if there is one. Drafts
  // persist across reloads because the branch scope is stored locally and
  // resolved by id.
  const existing = myDrafts.find((d) => d.articleId === articleId);
  if (existing) {
    logEvent("medium.article.edit_existing_draft", {
      "article.id": shortId(articleId),
      "draft.id": shortId(existing.id),
      "user.id": shortId(userId),
    });
    navigate({ name: "edit", draftId: existing.id });
    return;
  }
  const operation = startOperation("medium.article.edit", {
    "article.id": shortId(articleId),
    "user.id": shortId(userId),
  });
  const draftWrite = db.insert(app.drafts, {
    articleId,
    ownerId: userId,
    createdAt: new Date(),
  });
  try {
    const draft = await draftWrite.wait({ tier: "local" });
    operation.step("draft_inserted", { "draft.id": shortId(draft.id) });
    await db.createBranch(draft.id, app.articles.where({ id: articleId }));
    operation.done({ "draft.id": shortId(draft.id) });
    navigate({ name: "edit", draftId: draft.id });
  } catch (err) {
    operation.error(err);
    throw err;
  }
}

function DraftsPanel({ drafts, navigate }: { drafts: Draft[]; navigate: (next: Route) => void }) {
  return (
    <div className="drafts-panel">
      <h3>Your drafts</h3>
      <p className="panel-sub">
        Only visible to you. Edits live on a private Jazz branch until you publish.
      </p>
      <div className="drafts-list">
        {drafts.map((draft) => (
          <DraftRow key={draft.id} draft={draft} navigate={navigate} />
        ))}
      </div>
    </div>
  );
}

function DraftRow({ draft, navigate }: { draft: Draft; navigate: (next: Route) => void }) {
  // We could read the branch-scoped article to show the working title, but
  // that requires the branch scope to be loaded — keep it cheap and just
  // show the main-side title (which may be empty for brand-new drafts).
  const main =
    useAll(app.articles.where({ id: draft.articleId as unknown as string }).limit(1)) ?? [];
  const article = main[0];
  const label = article?.title?.trim() || "Untitled draft";
  const subtitle = article?.subtitle?.trim() || "";

  React.useEffect(() => {
    logEvent(article ? "medium.draft_row.visible" : "medium.draft_row.article_missing", {
      "draft.id": shortId(draft.id),
      "article.id": shortId(draft.articleId as unknown as string),
      "article.found": Boolean(article),
      "article.published": article?.published,
      "article.title_length": article?.title.length,
      "article.content_length": article?.content.length,
    });
  }, [draft.id, draft.articleId, article]);

  return (
    <a
      className="draft-row"
      href="#"
      onClick={(event) => {
        event.preventDefault();
        navigate({ name: "edit", draftId: draft.id });
      }}
    >
      <CoverImage
        fileId={(article?.coverImageId as unknown as string | undefined) ?? null}
        title={label}
      />
      <div>
        <div className="draft-title">{label}</div>
        <div className="draft-meta">
          {article?.published ? "Editing a published story" : "Unpublished — only you can see this"}
          {subtitle && ` · ${subtitle}`}
        </div>
      </div>
    </a>
  );
}

function ArticleCard({
  article,
  isMine,
  onOpen,
  onEdit,
}: {
  article: Article;
  isMine: boolean;
  onOpen: () => void;
  onEdit: () => void;
}) {
  return (
    <article className="article-card" onClick={onOpen}>
      <CoverImage
        fileId={(article.coverImageId as unknown as string | undefined) ?? null}
        title={article.title || "Untitled"}
      />
      <div className="body">
        <h3 className="title">{article.title || "Untitled"}</h3>
        {article.subtitle && <p className="sub">{article.subtitle}</p>}
        <div className="meta">
          <span>by {article.authorId.slice(0, 8)}</span>
          <span>·</span>
          <span>{new Date(article.createdAt).toLocaleDateString()}</span>
          {article.labels.slice(0, 3).map((label) => (
            <span key={label} className="label-pill">
              {label}
            </span>
          ))}
          {isMine && (
            <>
              <span>·</span>
              <a
                className="edit-link"
                href="#"
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  onEdit();
                }}
              >
                edit
              </a>
            </>
          )}
        </div>
      </div>
    </article>
  );
}
