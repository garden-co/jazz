import { useState, type FormEvent, type ReactNode, type RefObject } from "react";
import { segmentRichText } from "./rich-text.js";
import type {
  DisplayPost,
  TimelineItem,
  TimelinePostNode,
} from "./timeline-model.js";

export function LoadingScreen({ label = "Opening your local timeline…" }: { label?: string }) {
  return (
    <main className="loading-screen" aria-live="polite">
      <div className="brand-mark" aria-hidden="true">J</div>
      <div className="loading-copy">
        <strong>Jazz ❤️ Bluesky</strong>
        <span>{label}</span>
      </div>
      <span className="spinner" aria-hidden="true" />
    </main>
  );
}

function formatPostDate(value: string) {
  return new Date(value).toLocaleString(undefined, {
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function linkMentions(text: string) {
  const parts: ReactNode[] = [];
  const handlePattern = /@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?/g;
  const mentions = text.matchAll(handlePattern);
  let end = 0;
  for (const mention of mentions) {
    const start = mention.index;
    if (start > 0 && /[\w@]/.test(text[start - 1])) continue;
    if (start > end) parts.push(text.slice(end, start));
    parts.push(
      <a
        className="mention-link"
        href={`https://bsky.app/profile/${mention[0].slice(1)}`}
        target="_blank"
        rel="noopener noreferrer"
        key={start}
      >
        {mention[0]}
      </a>,
    );
    end = start + mention[0].length;
  }
  parts.push(text.slice(end));
  return parts;
}

export function PostText({ text, facetsJson }: { text: string; facetsJson?: string | null }) {
  const segments = segmentRichText(text, facetsJson);
  if (!segments.some((segment) => segment.uri)) return <>{linkMentions(text)}</>;

  return (
    <>
      {segments.map((segment, index) => segment.uri ? (
        <a
          className="post-link"
          href={segment.uri}
          target="_blank"
          rel="noopener noreferrer"
          title={segment.uri}
          key={index}
        >
          {segment.text}
        </a>
      ) : (
        <span key={index}>{linkMentions(segment.text)}</span>
      ))}
    </>
  );
}

function PostImages({ post, compact = false }: { post: DisplayPost; compact?: boolean }) {
  if (post.images.length === 0) return null;
  return (
    <div className={`post-images${compact ? " compact" : ""}`} data-count={post.images.length}>
      {post.images.map((image) => (
        <a
          className="post-image"
          href={image.fullsize}
          target="_blank"
          rel="noopener noreferrer"
          aria-label={image.alt || "View full-size image"}
          key={image.id}
        >
          <img
            src={image.thumb}
            alt={image.alt}
            loading="lazy"
            style={{
              aspectRatio: image.aspectWidth && image.aspectHeight
                ? `${image.aspectWidth} / ${image.aspectHeight}`
                : undefined,
            }}
          />
        </a>
      ))}
    </div>
  );
}

function QuotedPost({ post }: { post: DisplayPost }) {
  const profile = post.authorProfile;
  const author = profile?.handle ?? profile?.displayName ?? post.authorDid;
  return (
    <aside className="quoted-post" aria-label={`Quoted post by ${author}`}>
      <header className="quoted-post-header">
        {profile?.avatar ? (
          <img className="avatar quoted-avatar" src={profile.avatar} alt="" loading="lazy" />
        ) : (
          <span className="avatar quoted-avatar" aria-hidden="true">
            {author.charAt(0).toUpperCase()}
          </span>
        )}
        <strong>{author}</strong>
        <time dateTime={post.createdAt}>{formatPostDate(post.createdAt)}</time>
      </header>
      <p><PostText text={post.text} facetsJson={post.facetsJson} /></p>
      <PostImages post={post} compact />
    </aside>
  );
}

export function AppHeader({
  online,
  profile,
  handle,
  onSignOut,
}: {
  online: boolean;
  profile?: DisplayPost["authorProfile"];
  handle: string;
  onSignOut: () => void;
}) {
  return (
    <header className="app-header">
      <div className="brand-lockup">
        <div className="brand-mark" aria-hidden="true">J</div>
        <div>
          <p className="eyebrow">Local-first ATProto</p>
          <h1>Jazz ❤️ Bluesky</h1>
        </div>
      </div>
      <div className="account-actions">
        <span
          className={online ? "status online" : "status"}
          role="status"
          aria-live="polite"
        >
          <span aria-hidden="true" />
          {online ? "Online" : "Offline"}
        </span>
        <span className="account-identity">
          {profile?.avatar ? (
            <img className="avatar account-avatar" src={profile.avatar} alt="" />
          ) : (
            <span className="avatar account-avatar" aria-hidden="true">
              {handle.charAt(0).toUpperCase()}
            </span>
          )}
          <span className="account-handle">{handle}</span>
        </span>
        <button className="link" onClick={onSignOut}>Sign out</button>
      </div>
    </header>
  );
}

export function Intro() {
  return (
    <section className="intro" aria-labelledby="intro-title">
      <p className="eyebrow">Why Jazz?</p>
      <h2 id="intro-title">Your Bluesky timeline, available offline.</h2>
      <p>
        Jazz updates your feed live with posts and reposts from people you follow
        while you’re online, then keeps them available without a connection. You
        can even write offline. Your changes stay safely queued until you’re back
        online.
      </p>
    </section>
  );
}

export function Composer({ text, onChange, onPublish }: {
  text: string;
  onChange: (text: string) => void;
  onPublish: () => void;
}) {
  return (
    <section className="composer" aria-labelledby="composer-title">
      <div className="composer-heading">
        <label id="composer-title" htmlFor="post-text">Write a post</label>
        <span>{text.length} / 300</span>
      </div>
      <textarea
        id="post-text"
        value={text}
        onChange={(event) => onChange(event.target.value)}
        placeholder="What’s happening?"
        maxLength={300}
      />
      <div className="composer-actions">
        <button className="primary" onClick={onPublish} disabled={!text.trim()}>
          Post
        </button>
      </div>
    </section>
  );
}

export function SyncBanner({ count, online, onSync }: {
  count: number;
  online: boolean;
  onSync: () => void;
}) {
  if (!count) return null;
  return (
    <section className="sync-banner" aria-live="polite">
      <div>
        <strong>
          {count} {count === 1 ? "change" : "changes"} waiting to sync
        </strong>
        <span>
          {online
            ? "Sync will keep retrying automatically."
            : "They’re safe in your local Jazz cache."}
        </span>
      </div>
      <button className="secondary" onClick={onSync} disabled={!online}>
        {online ? "Sync now" : "Waiting for network"}
      </button>
    </section>
  );
}

function PostCard({
  post,
  threadRoot,
  pendingLike,
  pendingRepost,
  pendingPost,
  isNew,
  onToggleReaction,
  onReply,
}: {
  post: DisplayPost;
  threadRoot: DisplayPost;
  pendingLike: boolean;
  pendingRepost: boolean;
  pendingPost: boolean;
  isNew?: boolean;
  onToggleReaction: (kind: "like" | "repost", post: DisplayPost) => void;
  onReply: (parent: DisplayPost, root: DisplayPost, text: string) => Promise<void>;
}) {
  const [replying, setReplying] = useState(false);
  const [replyText, setReplyText] = useState("");
  const profile = post.authorProfile;
  const author = profile?.handle ?? profile?.displayName ?? post.authorDid;
  const canReply = Boolean(post.cid && threadRoot.cid);
  async function submitReply(event: FormEvent) {
    event.preventDefault();
    const value = replyText.trim();
    if (!value || !canReply) return;
    await onReply(post, threadRoot, value);
    setReplyText("");
    setReplying(false);
  }
  return (
    <article
      className={`post-card${pendingPost ? " pending" : ""}${isNew ? " new" : ""}`}
      data-post-uri={post.uri}
    >
      <header className="post-header">
        {profile?.avatar ? (
          <img className="avatar" src={profile.avatar} alt="" loading="lazy" />
        ) : (
          <span className="avatar" aria-hidden="true">
            {author.charAt(0).toUpperCase()}
          </span>
        )}
        <div>
          <strong>{author}</strong>
          <time dateTime={post.createdAt}>{formatPostDate(post.createdAt)}</time>
        </div>
        {pendingPost && <span className="pending-label">Pending</span>}
      </header>
      <p><PostText text={post.text} facetsJson={post.facetsJson} /></p>
      <PostImages post={post} />
      {post.quote && <QuotedPost post={post.quote} />}
      <footer className="post-actions">
        <button
          className="reaction-button reply-button"
          type="button"
          aria-label={`Reply to post by ${author}`}
          aria-expanded={replying}
          title={canReply ? undefined : "Replies are available once this thread has synced"}
          disabled={!canReply}
          onClick={() => setReplying((open) => !open)}
        >
          <span aria-hidden="true">↩</span>
          <span>{post.replyCount || ""}</span>
        </button>
        <button
          className={`reaction-button like-button${post.like?.active ? " active" : ""}`}
          type="button"
          aria-pressed={post.like?.active ?? false}
          aria-label={`${post.like?.active ? "Unlike" : "Like"} post by ${author}`}
          title={post.cid ? undefined : "Likes are available once this post has synced"}
          disabled={!post.cid}
          onClick={() => onToggleReaction("like", post)}
        >
          <span aria-hidden="true">{post.like?.active ? "♥" : "♡"}</span>
          <span>{post.likeCount || ""}</span>
        </button>
        <button
          className={`reaction-button repost-button${post.repost?.active ? " active" : ""}`}
          type="button"
          aria-pressed={post.repost?.active ?? false}
          aria-label={`${post.repost?.active ? "Undo repost" : "Repost"} post by ${author}`}
          title={post.cid ? undefined : "Reposts are available once this post has synced"}
          disabled={!post.cid}
          onClick={() => onToggleReaction("repost", post)}
        >
          <span aria-hidden="true">↻</span>
          <span>{post.repostCount || ""}</span>
        </button>
        {(pendingLike || pendingRepost) && (
          <span className="reaction-pending">Pending</span>
        )}
      </footer>
      {replying && (
        <form className="reply-composer" onSubmit={submitReply}>
          <label htmlFor={`reply-${post.id}`}>Reply to {author}</label>
          <textarea
            id={`reply-${post.id}`}
            value={replyText}
            onChange={(event) => setReplyText(event.target.value)}
            placeholder={`Reply to @${author}`}
            maxLength={300}
            autoFocus
          />
          <div>
            <span>{replyText.length} / 300</span>
            <button
              type="button"
              className="link"
              onClick={() => setReplying(false)}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="primary"
              disabled={!replyText.trim() || !canReply}
            >
              Reply
            </button>
          </div>
        </form>
      )}
    </article>
  );
}

type PostState = {
  pendingLikePostIds: Set<string>;
  pendingRepostPostIds: Set<string>;
  pendingPostIds: Set<string>;
  newEntryPostIds: Set<string>;
  loadingThreadUris: Set<string>;
  online: boolean;
};

type ThreadActions = {
  onToggleReaction: (kind: "like" | "repost", post: DisplayPost) => void;
  onReply: (parent: DisplayPost, root: DisplayPost, text: string) => Promise<void>;
  onLoadThread: (post: DisplayPost) => void;
};

type TimelineThreadProps = {
  postState: PostState;
  actions: ThreadActions;
};

function TimelinePostTree({ node, threadRoot, onReroot, postState, actions, depth = 0 }: TimelineThreadProps & {
  node: TimelinePostNode;
  threadRoot: DisplayPost;
  onReroot: (id: string) => void;
  depth?: number;
}) {
  const hasUncachedReplies = node.post.replyCount > node.replies.length;
  const loadingReplies = postState.loadingThreadUris.has(node.post.uri);

  return (
    <div className="thread-node" data-timeline-thread={node.post.uri}>
      <PostCard
        post={node.post}
        threadRoot={threadRoot}
        pendingLike={postState.pendingLikePostIds.has(node.post.id)}
        pendingRepost={postState.pendingRepostPostIds.has(node.post.id)}
        pendingPost={postState.pendingPostIds.has(node.post.id)}
        isNew={postState.newEntryPostIds.has(node.post.id)}
        onToggleReaction={actions.onToggleReaction}
        onReply={actions.onReply}
      />
      {node.replies.length > 0 && (depth < 2 ? (
        <div className="thread-replies">
          {node.replies.map((reply) => (
            <TimelinePostTree
              key={reply.post.id}
              node={reply}
              threadRoot={threadRoot}
              onReroot={onReroot}
              postState={postState}
              actions={actions}
              depth={depth + 1}
            />
          ))}
        </div>
      ) : (
        <button
          className="thread-disclosure"
          onClick={() => onReroot(node.post.id)}
        >
          Show {node.replies.length === 1 ? "reply" : `${node.replies.length} replies`}
        </button>
      ))}
      {hasUncachedReplies && (
        <button
          className="thread-disclosure load-thread"
          disabled={!postState.online || loadingReplies}
          onClick={() => actions.onLoadThread(node.post)}
        >
          {loadingReplies ? (
            <>
              <span className="spinner" aria-hidden="true" />
              Loading replies…
            </>
          ) : postState.online ? (
            `Load ${node.post.replyCount === 1 ? "reply" : `${node.post.replyCount} replies`}`
          ) : "Replies not cached"}
        </button>
      )}
    </div>
  );
}

function TimelineThread({ item, postState, actions }: TimelineThreadProps & { item: TimelineItem }) {
  const [focusedId, setFocusedId] = useState(item.node.post.id);
  const findNode = (node: TimelinePostNode): TimelinePostNode | undefined =>
    node.post.id === focusedId ? node : node.replies.map(findNode).find(Boolean);
  const focusedNode = findNode(item.node) ?? item.node;
  const reposter = item.repost?.actorProfile;
  const reposterName = reposter?.handle ?? reposter?.displayName ?? item.repost?.actorDid;
  return (
    <div className="timeline-thread">
      {item.repost && (
        <div className="repost-reason">
          <span aria-hidden="true">↻</span>
          {reposter?.avatar && (
            <img
              className="repost-avatar"
              src={reposter.avatar}
              alt=""
              loading="lazy"
            />
          )}
          <strong>{reposterName}</strong>
          <span>reposted</span>
        </div>
      )}
      {focusedId !== item.node.post.id && (
        <button
          className="thread-reset"
          onClick={() => setFocusedId(item.node.post.id)}
        >
          Back to top level
        </button>
      )}
      <TimelinePostTree
        node={focusedNode}
        threadRoot={item.threadRoot}
        onReroot={setFocusedId}
        postState={postState}
        actions={actions}
      />
      {item.threadUrl && (
        <a
          className="thread-link"
          href={item.threadUrl}
          target="_blank"
          rel="noopener noreferrer"
        >
          View thread
        </a>
      )}
    </div>
  );
}

export function TimelineFeed({
  items,
  waiting,
  hasMore,
  loadingMore,
  loadMoreRef,
  pendingLikePostIds,
  pendingRepostPostIds,
  pendingPostIds,
  newEntryPostIds,
  loadingThreadUris,
  online,
  onToggleReaction,
  onReply,
  onLoadThread,
}: PostState & ThreadActions & {
  items: TimelineItem[];
  waiting: boolean;
  hasMore: boolean;
  loadingMore: boolean;
  loadMoreRef: RefObject<HTMLDivElement | null>;
}) {
  const postState = {
    pendingLikePostIds,
    pendingRepostPostIds,
    pendingPostIds,
    newEntryPostIds,
    loadingThreadUris,
    online,
  };
  const actions = { onToggleReaction, onReply, onLoadThread };

  return (
    <section className="timeline" aria-labelledby="timeline-title">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Timeline</p>
          <h2 id="timeline-title">Latest posts</h2>
        </div>
        <p>Cached locally by Jazz for offline availability</p>
      </div>
      <div className="feed">
        {items.map((item) => (
          <TimelineThread
            key={item.id}
            item={item}
            postState={postState}
            actions={actions}
          />
        ))}
        {waiting && (
          <div className="empty-state">
            <span className="spinner" aria-hidden="true" />
            <h3>Syncing your timeline</h3>
            <p>New posts will appear as Jazz receives them.</p>
          </div>
        )}
        {!items.length && !waiting && (
          <div className="empty-state">
            <div className="brand-mark" aria-hidden="true">J</div>
            <h3>Your offline timeline is empty</h3>
            <p>Connect once to cache recent posts from people you follow.</p>
          </div>
        )}
      </div>
      <div ref={loadMoreRef} className="feed-sentinel" aria-hidden="true" />
      {loadingMore && (
        <p className="pagination-status">
          <span className="spinner" aria-hidden="true" />
          Loading more posts…
        </p>
      )}
      {!hasMore && items.length > 0 && (
        <p className="pagination-status">
          <span aria-hidden="true">✓</span>
          You’re all caught up
        </p>
      )}
    </section>
  );
}

export function AppFooter({ onSignOut }: { onSignOut: () => void }) {
  return (
    <footer className="app-footer">
      <span>Jazz ❤️ Bluesky is a local-first ATProto proof of concept.</span>
      <button className="link" onClick={onSignOut}>Sign out</button>
    </footer>
  );
}
