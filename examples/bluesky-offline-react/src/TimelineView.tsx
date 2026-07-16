import { useState, type ReactNode, type RefObject } from "react";
import type {
  DisplayPost,
  ProfileView,
  TimelineItem,
  TimelinePostNode,
} from "./timeline-model.js";

export function LoadingScreen({ label = "Opening your local timeline…" }: { label?: string }) {
  return <main className="loading-screen" aria-live="polite"><div className="brand-mark" aria-hidden="true">J</div><div className="loading-copy"><strong>{"Jazz ❤️ Bluesky"}</strong><span>{label}</span></div><span className="spinner" aria-hidden="true" /></main>;
}

function formatPostDate(value: string) {
  return new Date(value).toLocaleString(undefined, { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
}

function linkMentions(text: string) {
  const parts: ReactNode[] = [];
  const mentions = text.matchAll(/@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?/g);
  let end = 0;
  for (const mention of mentions) {
    const start = mention.index;
    if (start > 0 && /[\w@]/.test(text[start - 1])) continue;
    if (start > end) parts.push(text.slice(end, start));
    parts.push(<a className="mention-link" href={`https://bsky.app/profile/${mention[0].slice(1)}`} target="_blank" rel="noopener noreferrer" key={start}>{mention[0]}</a>);
    end = start + mention[0].length;
  }
  parts.push(text.slice(end));
  return parts;
}

export function AppHeader({
  online,
  profile,
  handle,
  onSignOut,
}: {
  online: boolean;
  profile?: ProfileView;
  handle: string;
  onSignOut: () => void;
}) {
  return <header className="app-header">
    <div className="brand-lockup"><div className="brand-mark" aria-hidden="true">J</div><div><p className="eyebrow">Local-first ATProto</p><h1>{"Jazz ❤️ Bluesky"}</h1></div></div>
    <div className="account-actions"><span className={online ? "status online" : "status"} role="status" aria-live="polite"><span aria-hidden="true" />{online ? "Online" : "Offline"}</span><span className="account-identity">{profile?.avatar ? <img className="avatar account-avatar" src={profile.avatar} alt="" /> : <span className="avatar account-avatar" aria-hidden="true">{handle.charAt(0).toUpperCase()}</span>}<span className="account-handle">{handle}</span></span><button className="link" onClick={onSignOut}>Sign out</button></div>
  </header>;
}

export function Intro() {
  return <section className="intro" aria-labelledby="intro-title"><p className="eyebrow">Why Jazz?</p><h2 id="intro-title">Your Bluesky timeline, available offline.</h2><p>Jazz keeps posts and reposts from people you follow updating while you’re online, then keeps them available without a connection. You can even write offline—your changes stay safely queued until you’re back online.</p></section>;
}

export function Composer({ text, onChange, onPublish }: {
  text: string;
  onChange: (text: string) => void;
  onPublish: () => void;
}) {
  return <section className="composer" aria-labelledby="composer-title"><div className="composer-heading"><label id="composer-title" htmlFor="post-text">Write a post</label><span>{text.length} / 300</span></div><textarea id="post-text" value={text} onChange={(event) => onChange(event.target.value)} placeholder="What’s happening?" maxLength={300} /><div className="composer-actions"><button className="primary" onClick={onPublish} disabled={!text.trim()}>Post</button></div></section>;
}

export function SyncBanner({ count, online, onSync }: {
  count: number;
  online: boolean;
  onSync: () => void;
}) {
  if (!count) return null;
  return <section className="sync-banner" aria-live="polite"><div><strong>{count} {count === 1 ? "change" : "changes"} waiting to sync</strong><span>{online ? "Sync will keep retrying automatically." : "They’re safe in your local Jazz cache."}</span></div><button className="secondary" onClick={onSync} disabled={!online}>{online ? "Sync now" : "Waiting for network"}</button></section>;
}

function PostCard({
  post,
  pendingLike,
  pendingRepost,
  pendingPost,
  isNew,
  onToggleReaction,
}: {
  post: DisplayPost;
  pendingLike: boolean;
  pendingRepost: boolean;
  pendingPost: boolean;
  isNew?: boolean;
  onToggleReaction: (kind: "like" | "repost", post: DisplayPost) => void;
}) {
  const profile = post.authorProfile;
  const author = profile?.handle ?? profile?.displayName ?? post.authorDid;
  return <article className={`post-card${pendingPost ? " pending" : ""}${isNew ? " new" : ""}`} data-post-uri={post.uri}>
    <header className="post-header">{profile?.avatar ? <img className="avatar" src={profile.avatar} alt="" loading="lazy" /> : <span className="avatar" aria-hidden="true">{author.charAt(0).toUpperCase()}</span>}<div><strong>{author}</strong><time dateTime={post.createdAt}>{formatPostDate(post.createdAt)}</time></div>{pendingPost && <span className="pending-label">Pending</span>}</header>
    <p>{linkMentions(post.text)}</p>
    {post.images.length > 0 && <div className="post-images" data-count={post.images.length}>{post.images.map((image) => <a className="post-image" href={image.fullsize} target="_blank" rel="noopener noreferrer" aria-label={image.alt || "View full-size image"} key={image.id}><img src={image.thumb} alt={image.alt} loading="lazy" style={{ aspectRatio: image.aspectWidth && image.aspectHeight ? `${image.aspectWidth} / ${image.aspectHeight}` : undefined }} /></a>)}</div>}
    <footer className="post-actions">
      <button className={`reaction-button like-button${post.like?.active ? " active" : ""}`} type="button" aria-pressed={post.like?.active ?? false} aria-label={`${post.like?.active ? "Unlike" : "Like"} post by ${author}`} title={post.cid ? undefined : "Likes are available once this post has synced"} disabled={!post.cid} onClick={() => onToggleReaction("like", post)}><span aria-hidden="true">{post.like?.active ? "♥" : "♡"}</span><span>{post.likeCount || ""}</span></button>
      <button className={`reaction-button repost-button${post.repost?.active ? " active" : ""}`} type="button" aria-pressed={post.repost?.active ?? false} aria-label={`${post.repost?.active ? "Undo repost" : "Repost"} post by ${author}`} title={post.cid ? undefined : "Reposts are available once this post has synced"} disabled={!post.cid} onClick={() => onToggleReaction("repost", post)}><span aria-hidden="true">↻</span><span>{post.repostCount || ""}</span></button>
      {(pendingLike || pendingRepost) && <span className="reaction-pending">Pending</span>}
    </footer>
  </article>;
}

type ThreadProps = {
  pendingLikePostIds: Set<string>;
  pendingRepostPostIds: Set<string>;
  pendingPostIds: Set<string>;
  newEntryPostIds: Set<string>;
  loadingThreadUris: Set<string>;
  online: boolean;
  onToggleReaction: (kind: "like" | "repost", post: DisplayPost) => void;
  onLoadThread: (post: DisplayPost) => void;
};

function TimelinePostTree({ node, onReroot, depth = 0, ...props }: ThreadProps & {
  node: TimelinePostNode;
  onReroot: (id: string) => void;
  depth?: number;
}) {
  const hasUncachedReplies = node.post.replyCount > node.replies.length;
  return <div className="thread-node" data-timeline-thread={node.post.uri}>
    <PostCard post={node.post} pendingLike={props.pendingLikePostIds.has(node.post.id)} pendingRepost={props.pendingRepostPostIds.has(node.post.id)} pendingPost={props.pendingPostIds.has(node.post.id)} isNew={props.newEntryPostIds.has(node.post.id)} onToggleReaction={props.onToggleReaction} />
    {node.replies.length > 0 && (depth < 2
      ? <div className="thread-replies">{node.replies.map((reply) => <TimelinePostTree key={reply.post.id} node={reply} onReroot={onReroot} depth={depth + 1} {...props} />)}</div>
      : <button className="thread-disclosure" onClick={() => onReroot(node.post.id)}>Show {node.replies.length === 1 ? "reply" : `${node.replies.length} replies`}</button>)}
    {hasUncachedReplies && <button className="thread-disclosure load-thread" disabled={!props.online || props.loadingThreadUris.has(node.post.uri)} onClick={() => props.onLoadThread(node.post)}>{props.loadingThreadUris.has(node.post.uri) ? <><span className="spinner" aria-hidden="true" />Loading replies…</> : props.online ? `Load ${node.post.replyCount === 1 ? "reply" : `${node.post.replyCount} replies`}` : "Replies not cached"}</button>}
  </div>;
}

function TimelineThread({ item, ...props }: ThreadProps & { item: TimelineItem }) {
  const [focusedId, setFocusedId] = useState(item.node.post.id);
  const findNode = (node: TimelinePostNode): TimelinePostNode | undefined =>
    node.post.id === focusedId ? node : node.replies.map(findNode).find(Boolean);
  const focusedNode = findNode(item.node) ?? item.node;
  const reposter = item.repost?.actorProfile;
  const reposterName = reposter?.handle ?? reposter?.displayName ?? item.repost?.actorDid;
  return <div className="timeline-thread">
    {item.repost && <div className="repost-reason"><span aria-hidden="true">↻</span>{reposter?.avatar && <img className="repost-avatar" src={reposter.avatar} alt="" loading="lazy" />}<strong>{reposterName}</strong><span>reposted</span></div>}
    {focusedId !== item.node.post.id && <button className="thread-reset" onClick={() => setFocusedId(item.node.post.id)}>Back to top level</button>}
    <TimelinePostTree node={focusedNode} onReroot={setFocusedId} {...props} />
  </div>;
}

export function TimelineFeed({
  items,
  waiting,
  hasMore,
  loadingMore,
  loadMoreRef,
  ...threadProps
}: ThreadProps & {
  items: TimelineItem[];
  waiting: boolean;
  hasMore: boolean;
  loadingMore: boolean;
  loadMoreRef: RefObject<HTMLDivElement | null>;
}) {
  return <section className="timeline" aria-labelledby="timeline-title">
    <div className="section-heading"><div><p className="eyebrow">Timeline</p><h2 id="timeline-title">Latest posts</h2></div><p>Cached locally by Jazz for offline availability</p></div>
    <div className="feed">{items.map((item) => <TimelineThread key={item.id} item={item} {...threadProps} />)}
      {waiting && <div className="empty-state"><span className="spinner" aria-hidden="true" /><h3>Syncing your timeline</h3><p>New posts will appear as Jazz receives them.</p></div>}
      {!items.length && !waiting && <div className="empty-state"><div className="brand-mark" aria-hidden="true">J</div><h3>Your offline timeline is empty</h3><p>Connect once to cache recent posts from people you follow.</p></div>}
    </div>
    <div ref={loadMoreRef} className="feed-sentinel" aria-hidden="true" />
    {loadingMore && <p className="pagination-status"><span className="spinner" aria-hidden="true" />Loading more posts…</p>}
    {!hasMore && items.length > 0 && <p className="pagination-status"><span aria-hidden="true">✓</span>You’re all caught up</p>}
  </section>;
}

export function AppFooter({ onSignOut }: { onSignOut: () => void }) {
  return <footer className="app-footer"><span>{"Jazz ❤️ Bluesky is a local-first ATProto proof of concept."}</span><button className="link" onClick={onSignOut}>Sign out</button></footer>;
}
