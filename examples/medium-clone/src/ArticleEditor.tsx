import * as React from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { app, type Article } from "../schema.js";
import { withBranchScope } from "./branchScope.js";
import { CoverImage } from "./CoverImage.js";
import { logError, logEvent, shortId, startOperation } from "./telemetry.js";

type Props = {
  draftId: string;
  onDone: () => void;
};

type FormState = {
  title: string;
  subtitle: string;
  content: string;
  labelsText: string;
};

function parseLabels(text: string): string[] {
  return text
    .split(",")
    .map((label) => label.trim())
    .filter((label) => label.length > 0);
}

function articleToForm(article: Article): FormState {
  return {
    title: article.title,
    subtitle: article.subtitle,
    content: article.content,
    labelsText: article.labels.join(", "),
  };
}

export function ArticleEditor({ draftId, onDone }: Props) {
  const drafts = useAll(app.drafts.where({ id: draftId }).limit(1)) ?? [];
  const draft = drafts[0];

  React.useEffect(() => {
    logEvent(draft ? "medium.editor.draft_found" : "medium.editor.draft_missing", {
      "draft.id": shortId(draftId),
      "draft.found": Boolean(draft),
      "article.id": shortId(draft?.articleId as unknown as string | undefined),
    });
  }, [draftId, draft]);

  if (!draft) {
    return <p>Draft not found, or not visible to you.</p>;
  }

  return (
    <EditorInner
      draftId={draftId}
      articleId={draft.articleId as unknown as string}
      onDone={onDone}
    />
  );
}

function EditorInner({
  draftId,
  articleId,
  onDone,
}: {
  draftId: string;
  articleId: string;
  onDone: () => void;
}) {
  const [queryRetry, setQueryRetry] = React.useState(0);
  const retryQuery = React.useCallback(() => {
    setQueryRetry((current) => current + 1);
  }, []);

  return (
    <EditorBody
      key={`${draftId}:${articleId}:${queryRetry}`}
      draftId={draftId}
      articleId={articleId}
      onDone={onDone}
      onRetryQuery={retryQuery}
      repairAttempt={queryRetry}
    />
  );
}

function EditorBody({
  draftId,
  articleId,
  onDone,
  onRetryQuery,
  repairAttempt,
}: {
  draftId: string;
  articleId: string;
  onDone: () => void;
  onRetryQuery: () => void;
  repairAttempt: number;
}) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;

  // Read the article through the branch overlay. Saved branch writes flow
  // back here without affecting main.
  const branchedArticles =
    useAll(withBranchScope(app.articles.where({ id: articleId }).limit(1), draftId)) ?? [];
  const branchedArticle = branchedArticles[0];

  const [form, setForm] = React.useState<FormState | null>(null);
  const [lastSavedAt, setLastSavedAt] = React.useState<Date | null>(null);
  const [busy, setBusy] = React.useState<
    null | "save" | "publish" | "discard" | "publishing-cover"
  >(null);
  const [error, setError] = React.useState<string | null>(null);
  const [scopeRepair, setScopeRepair] = React.useState<"idle" | "waiting" | "repairing" | "failed">(
    "idle",
  );
  const initialized = React.useRef(false);
  const repairedScopeKey = React.useRef<string | null>(null);

  React.useEffect(() => {
    initialized.current = false;
    repairedScopeKey.current = null;
    setForm(null);
    setScopeRepair("idle");
    setError(null);
  }, [draftId, articleId]);

  // Seed the form from the branched article once it's loaded. After that the
  // form is the source of truth; we don't clobber it on every branch change.
  React.useEffect(() => {
    if (!initialized.current && branchedArticle) {
      logEvent("medium.editor.article_loaded", {
        "draft.id": shortId(draftId),
        "article.id": shortId(articleId),
        "article.published": branchedArticle.published,
        "article.title_length": branchedArticle.title.length,
        "article.content_length": branchedArticle.content.length,
        "article.labels_count": branchedArticle.labels.length,
      });
      setForm(articleToForm(branchedArticle));
      initialized.current = true;
      setScopeRepair("idle");
    }
  }, [branchedArticle]);

  React.useEffect(() => {
    if (branchedArticle) return;
    logEvent("medium.editor.article_waiting", {
      "draft.id": shortId(draftId),
      "article.id": shortId(articleId),
    });
  }, [draftId, articleId, branchedArticle]);

  React.useEffect(() => {
    if (branchedArticle) return;
    if (repairAttempt > 0) return;

    const scopeKey = `${draftId}:${articleId}`;
    if (repairedScopeKey.current === scopeKey) return;

    setScopeRepair("waiting");
    const timeoutId = window.setTimeout(() => {
      repairedScopeKey.current = scopeKey;
      setScopeRepair("repairing");
      setError(null);

      const operation = startOperation("medium.branch_scope.recover", {
        "draft.id": shortId(draftId),
        "article.id": shortId(articleId),
      });

      void db.all(app.articles.where({ id: articleId }).limit(1)).then((rows) => {
        logEvent("medium.branch_scope.recover_base_probe", {
          "draft.id": shortId(draftId),
          "article.id": shortId(articleId),
          "probe.rows": rows.length,
          "probe.title_length": rows[0]?.title.length ?? 0,
        });
      });
      void db
        .branch(draftId)
        .all(app.articles.where({ id: articleId }).limit(1))
        .then((rows) => {
          logEvent("medium.branch_scope.recover_branch_probe", {
            "draft.id": shortId(draftId),
            "article.id": shortId(articleId),
            "probe.rows": rows.length,
            "probe.title_length": rows[0]?.title.length ?? 0,
          });
        });

      db.createBranch(draftId, app.articles.where({ id: articleId }))
        .then(() => {
          operation.done();
          setScopeRepair("idle");
          void db
            .all(withBranchScope(app.articles.where({ id: articleId }).limit(1), draftId))
            .then((rows) => {
              logEvent("medium.branch_scope.recover_probe", {
                "draft.id": shortId(draftId),
                "article.id": shortId(articleId),
                "probe.rows": rows.length,
                "probe.title_length": rows[0]?.title.length ?? 0,
              });
            })
            .catch((err) => {
              logError("medium.branch_scope.recover_probe_failed", err, {
                "draft.id": shortId(draftId),
                "article.id": shortId(articleId),
              });
            });
          onRetryQuery();
        })
        .catch((err) => {
          operation.error(err);
          logError("medium.branch_scope.recover_failed", err, {
            "draft.id": shortId(draftId),
            "article.id": shortId(articleId),
          });
          setScopeRepair("failed");
          setError(err instanceof Error ? err.message : String(err));
        });
    }, 500);

    return () => window.clearTimeout(timeoutId);
  }, [db, draftId, articleId, branchedArticle, onRetryQuery, repairAttempt]);

  if (!branchedArticle || !form) {
    return (
      <p>
        {scopeRepair === "repairing"
          ? "Repairing draft..."
          : scopeRepair === "failed"
            ? `Could not load draft: ${error}`
            : "Loading draft..."}
      </p>
    );
  }

  const update = (patch: Partial<FormState>) =>
    setForm((current) => (current ? { ...current, ...patch } : current));

  const saveDraft = async () => {
    if (!form || busy) return;
    const operation = startOperation("medium.draft.save", {
      "draft.id": shortId(draftId),
      "article.id": shortId(articleId),
      "form.title_length": form.title.length,
      "form.content_length": form.content.length,
      "form.labels_count": parseLabels(form.labelsText).length,
    });
    setBusy("save");
    setError(null);
    try {
      // Branch-scoped update: this write goes to the draft branch only. Main
      // readers do not see it.
      await db
        .update(
          app.articles,
          articleId,
          {
            title: form.title,
            subtitle: form.subtitle,
            content: form.content,
            labels: parseLabels(form.labelsText),
          },
          { branch: draftId },
        )
        .wait({ tier: "local" });
      operation.done();
      setLastSavedAt(new Date());
    } catch (err) {
      operation.error(err);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(null);
    }
  };

  const publish = async () => {
    if (!form || busy) return;
    const operation = startOperation("medium.article.publish", {
      "draft.id": shortId(draftId),
      "article.id": shortId(articleId),
      "form.title_length": form.title.length,
      "form.content_length": form.content.length,
      "form.labels_count": parseLabels(form.labelsText).length,
      "article.was_published": branchedArticle.published,
    });
    setBusy("publish");
    setError(null);
    try {
      await db
        .update(
          app.articles,
          articleId,
          {
            title: form.title,
            subtitle: form.subtitle,
            content: form.content,
            labels: parseLabels(form.labelsText),
            published: true,
          },
          { branch: draftId },
        )
        .wait({ tier: "local" });
      operation.step("branch_article_updated");
      // Merge the branch into main. After this, normal readers see the
      // article as it looks in the draft.
      await db.branch(draftId).merge().wait({ tier: "local" });
      operation.step("branch_merged");
      // Clean up the draft metadata; the branch scope itself can stay in
      // storage, but with no row pointing at it there's no way to reach it
      // from the UI.
      await db.delete(app.drafts, draftId).wait({ tier: "local" });
      operation.done();
      onDone();
    } catch (err) {
      operation.error(err);
      setError(err instanceof Error ? err.message : String(err));
      setBusy(null);
    }
  };

  const discardDraft = async () => {
    if (busy) return;
    if (!confirm("Discard this draft? Branch writes are not merged into main.")) return;
    const operation = startOperation("medium.draft.discard", {
      "draft.id": shortId(draftId),
      "article.id": shortId(articleId),
      "article.published": branchedArticle.published,
      "article.title_length": branchedArticle.title.length,
      "article.content_length": branchedArticle.content.length,
    });
    setBusy("discard");
    try {
      // If the article was never published we also clean it up so the front
      // page isn't littered with empty rows. Published articles stay; we
      // just drop the working copy.
      const isUnpublishedFirstDraft =
        !branchedArticle.published &&
        branchedArticle.title === "" &&
        branchedArticle.content === "";
      await db.delete(app.drafts, draftId).wait({ tier: "local" });
      operation.step("draft_deleted", {
        "article.delete_candidate": isUnpublishedFirstDraft,
      });
      if (isUnpublishedFirstDraft) {
        await db.delete(app.articles, articleId).wait({ tier: "local" });
        operation.step("article_deleted");
      }
      operation.done();
      onDone();
    } catch (err) {
      operation.error(err);
      setError(err instanceof Error ? err.message : String(err));
      setBusy(null);
    }
  };

  const uploadCover = async (file: File) => {
    if (busy) return;
    if (!file.type.startsWith("image/")) {
      setError("Cover must be an image.");
      return;
    }
    const operation = startOperation("medium.cover.upload", {
      "draft.id": shortId(draftId),
      "article.id": shortId(articleId),
      "file.size": file.size,
      "file.type": file.type,
    });
    setBusy("publishing-cover");
    setError(null);
    try {
      // Files live on `main`, not on the branch — they're public bytes. The
      // article's `coverImageId` pointer is the branch-scoped change.
      const inserted = await db.createFileFromBlob(app, file, {
        name: file.name,
        mimeType: file.type,
      });
      operation.step("file_uploaded", { "file.id": shortId(inserted.id) });

      if (userId) {
        // Claim ownership so we can delete the file later if the draft is
        // discarded. Failure here isn't fatal — the cover still works.
        await db
          .insert(app.image_uploads, {
            fileId: inserted.id,
            ownerId: userId,
            createdAt: new Date(),
          })
          .wait({ tier: "local" });
        operation.step("upload_claimed");
      }

      await db
        .update(app.articles, articleId, { coverImageId: inserted.id }, { branch: draftId })
        .wait({ tier: "local" });
      operation.done({ "file.id": shortId(inserted.id) });
    } catch (err) {
      operation.error(err);
      logError("medium.cover.upload_failed", err, {
        "draft.id": shortId(draftId),
        "article.id": shortId(articleId),
      });
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(null);
    }
  };

  const clearCover = async () => {
    if (busy) return;
    setError(null);
    try {
      await db
        .update(
          app.articles,
          articleId,
          { coverImageId: null as unknown as undefined },
          { branch: draftId },
        )
        .wait({ tier: "local" });
      logEvent("medium.cover.cleared", {
        "draft.id": shortId(draftId),
        "article.id": shortId(articleId),
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const previewLabels = parseLabels(form.labelsText);
  const coverImageId = (branchedArticle.coverImageId as unknown as string | undefined) ?? null;

  const statusText = error
    ? `Error: ${error}`
    : busy === "save"
      ? "Saving..."
      : busy === "publish"
        ? "Publishing..."
        : busy === "publishing-cover"
          ? "Uploading cover..."
          : lastSavedAt
            ? `Saved ${lastSavedAt.toLocaleTimeString()}`
            : branchedArticle.published
              ? "Editing a published story on a private branch"
              : "Unpublished draft — only you can see it";

  return (
    <>
      <div className="toolbar">
        <button className="btn btn-ghost" onClick={onDone}>
          ← Back
        </button>
        <span className="spacer" />
        <span className={`status${error ? " error" : ""}`}>{statusText}</span>
        <button className="btn btn-danger" onClick={discardDraft} disabled={!!busy}>
          Discard
        </button>
        <button className="btn" onClick={saveDraft} disabled={!!busy}>
          {busy === "save" ? "Saving..." : "Save draft"}
        </button>
        <button
          className="btn btn-primary"
          onClick={publish}
          disabled={!!busy || !form.title.trim()}
        >
          {busy === "publish"
            ? "Publishing..."
            : branchedArticle.published
              ? "Republish"
              : "Publish"}
        </button>
      </div>

      <CoverControl
        title={form.title}
        coverImageId={coverImageId}
        uploading={busy === "publishing-cover"}
        onChange={uploadCover}
        onClear={clearCover}
      />

      <div className="editor-grid">
        <div className="editor-pane">
          <span className="pane-label">Write</span>
          <input
            className="title-input"
            type="text"
            value={form.title}
            onChange={(e) => update({ title: e.target.value })}
            placeholder="Title"
            aria-label="Title"
          />
          <input
            className="subtitle-input"
            type="text"
            value={form.subtitle}
            onChange={(e) => update({ subtitle: e.target.value })}
            placeholder="Subtitle"
            aria-label="Subtitle"
          />
          <textarea
            className="content-input"
            value={form.content}
            onChange={(e) => update({ content: e.target.value })}
            placeholder="Tell your story…"
            aria-label="Content"
          />
          <input
            className="labels-input"
            type="text"
            value={form.labelsText}
            onChange={(e) => update({ labelsText: e.target.value })}
            placeholder="Labels, comma separated"
            aria-label="Labels"
          />
        </div>
        <div className="preview-pane">
          <span className="pane-label">Preview</span>
          <div className="preview-cover">
            <CoverImage fileId={coverImageId} title={form.title} />
          </div>
          <h2 className="preview-title">{form.title || "Untitled"}</h2>
          {form.subtitle && <p className="preview-subtitle">{form.subtitle}</p>}
          {previewLabels.length > 0 && (
            <div className="preview-labels">
              {previewLabels.map((label) => (
                <span key={label} className="label-pill">
                  {label}
                </span>
              ))}
            </div>
          )}
          <p className={`preview-body${form.content ? "" : " empty"}`}>
            {form.content || "Nothing here yet."}
          </p>
        </div>
      </div>
    </>
  );
}

function CoverControl({
  title,
  coverImageId,
  uploading,
  onChange,
  onClear,
}: {
  title: string;
  coverImageId: string | null;
  uploading: boolean;
  onChange: (file: File) => void;
  onClear: () => void;
}) {
  const inputRef = React.useRef<HTMLInputElement>(null);

  return (
    <div className="cover-control">
      <CoverImage fileId={coverImageId} title={title} />
      <div className="cover-actions">
        <button
          type="button"
          className="btn"
          onClick={() => inputRef.current?.click()}
          disabled={uploading}
        >
          {uploading ? "Uploading..." : coverImageId ? "Change cover" : "Add cover image"}
        </button>
        {coverImageId && (
          <button type="button" className="btn btn-ghost" onClick={onClear} disabled={uploading}>
            Remove
          </button>
        )}
        <span className="upload-hint">
          Stored on `main`. Pointer to it is on the draft branch until publish.
        </span>
        <input
          ref={inputRef}
          className="hidden-file-input"
          type="file"
          accept="image/*"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) onChange(file);
            event.target.value = "";
          }}
        />
      </div>
    </div>
  );
}
