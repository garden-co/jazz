import { useAll } from "jazz-tools/react";
import * as React from "react";
import { app } from "../schema.js";
import { CoverImage } from "./CoverImage.js";
import { logEvent, shortId } from "./telemetry.js";

type Props = {
  articleId: string;
  onBack: () => void;
};

export function ArticleView({ articleId, onBack }: Props) {
  const rows = useAll(app.articles.where({ id: articleId }).limit(1)) ?? [];
  const article = rows[0];

  React.useEffect(() => {
    logEvent(article ? "medium.article_view.visible" : "medium.article_view.article_missing", {
      "article.id": shortId(articleId),
      "article.found": Boolean(article),
      "article.published": article?.published,
      "article.title_length": article?.title.length,
      "article.content_length": article?.content.length,
    });
  }, [articleId, article]);

  return (
    <>
      <div className="toolbar">
        <button className="btn btn-ghost" onClick={onBack}>
          ← Back
        </button>
      </div>
      {!article && <p className="muted">Article not found.</p>}
      {article && (
        <article className="reader">
          <div className="hero-cover">
            <CoverImage
              fileId={(article.coverImageId as unknown as string | undefined) ?? null}
              title={article.title || "Untitled"}
              showInitial={false}
            />
          </div>
          <h1>{article.title || "Untitled"}</h1>
          {article.subtitle && <p className="subtitle">{article.subtitle}</p>}
          <div className="meta">
            <span>by {article.authorId.slice(0, 8)}</span>
            <span>·</span>
            <span>{new Date(article.createdAt).toLocaleDateString()}</span>
            {article.labels.map((label) => (
              <span key={label} className="label-pill">
                {label}
              </span>
            ))}
          </div>
          <div className="body">{article.content}</div>
        </article>
      )}
    </>
  );
}
