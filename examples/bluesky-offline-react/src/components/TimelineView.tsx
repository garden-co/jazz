import { Avatar, Badge, Button, Card, Spinner, Text, TextArea } from "@radix-ui/themes";
import {
  Content as AccordionContent,
  Header as AccordionHeader,
  Item as AccordionItem,
  Root as AccordionRoot,
  Trigger as AccordionTrigger,
} from "@radix-ui/react-accordion";
import { useState, type FormEvent, type ReactNode } from "react";
import { flushSync } from "react-dom";
import {
  BackIcon,
  DisclosureIcon,
  LikeIcon,
  ReplyIcon,
  RepostIcon,
  SignOutIcon,
  StatusIcon,
  SuccessIcon,
  ThreadLinkIcon,
} from "./Icons.js";
import { ProfileName, profileNameParts } from "./ProfileName.js";
import { segmentRichText } from "../model/rich-text.js";
import {
  hydrateTimelineThread,
  type DisplayPost,
  type TimelineItem,
  type TimelinePostNode,
  type TimelineRelations,
} from "../model/timeline-data.js";
import type { ConnectivityStatus } from "../hooks/use-connectivity.js";

export function LoadingScreen({ label = "Opening your local timeline…" }: { label?: string }) {
  return (
    <main className="loading-screen" aria-live="polite">
      <div className="brand-mark" aria-hidden="true">
        J
      </div>
      <div className="loading-copy">
        <strong>Jazz ❤️ Bluesky</strong>
        <span>{label}</span>
      </div>
      <Spinner aria-hidden="true" />
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
  const handlePattern =
    /@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?/g;
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
      {segments.map((segment, index) =>
        segment.uri ? (
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
        ),
      )}
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
              aspectRatio:
                image.aspectWidth && image.aspectHeight
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
  const author = profileNameParts(profile, post.authorDid).name;
  return (
    <Card asChild size="1">
      <aside className="quoted-post" aria-label={`Quoted post by ${author}`}>
        <header className="quoted-post-header">
          <Avatar
            src={profile?.avatar ?? undefined}
            fallback={author.charAt(0).toUpperCase()}
            size="1"
            radius="medium"
          />
          <ProfileName profile={profile} fallback={post.authorDid} />
          <time dateTime={post.createdAt}>{formatPostDate(post.createdAt)}</time>
        </header>
        <p>
          <PostText text={post.text} facetsJson={post.facetsJson} />
        </p>
        <PostImages post={post} compact />
      </aside>
    </Card>
  );
}

export function AppHeader({
  profile,
  handle,
  onSignOut,
}: {
  profile?: DisplayPost["authorProfile"];
  handle: string;
  onSignOut: () => void;
}) {
  const identity = profileNameParts(profile, handle);
  return (
    <header className="app-header">
      <div className="brand-lockup">
        <div className="brand-mark" aria-hidden="true">
          J
        </div>
        <div>
          <p className="eyebrow">Local-first ATProto</p>
          <h1>Jazz ❤️ Bluesky</h1>
        </div>
      </div>
      <Card asChild size="1">
        <div className="account-card">
          <Avatar
            src={profile?.avatar ?? undefined}
            fallback={handle.charAt(0).toUpperCase()}
            size="2"
            radius="medium"
          />
          <div className="account-identity">
            <strong>{identity.name}</strong>
            {identity.handle && <span className="account-handle">{identity.handle}</span>}
          </div>
          <Button size="1" variant="ghost" color="gray" onClick={onSignOut}>
            <SignOutIcon />
            Sign out
          </Button>
        </div>
      </Card>
    </header>
  );
}

export function Intro() {
  return (
    <Card asChild size="3">
      <AccordionRoot className="intro" type="single" collapsible defaultValue="why-jazz">
        <AccordionItem value="why-jazz">
          <AccordionHeader className="intro-heading">
            <AccordionTrigger className="intro-summary">
              <span className="eyebrow">Why Jazz?</span>
              <DisclosureIcon />
            </AccordionTrigger>
          </AccordionHeader>
          <AccordionContent className="intro-content">
            <h2 id="intro-title">Your Bluesky timeline, available offline.</h2>
            <p className="intro-body">
              Jazz updates your feed live with posts and reposts from people you follow while you’re
              online, then keeps them available without a connection. You can even write offline.
              Your changes stay safely queued until you’re back online.
            </p>
          </AccordionContent>
        </AccordionItem>
      </AccordionRoot>
    </Card>
  );
}

export function Composer({
  text,
  onChange,
  onPublish,
}: {
  text: string;
  onChange: (text: string) => void;
  onPublish: () => void;
}) {
  return (
    <Card asChild size="2">
      <section className="composer" aria-labelledby="composer-title">
        <div className="composer-heading">
          <label id="composer-title" htmlFor="post-text">
            Write a post
          </label>
          <span>{text.length} / 300</span>
        </div>
        <TextArea
          id="post-text"
          rows={4}
          value={text}
          onChange={(event) => onChange(event.target.value)}
          placeholder="What’s happening?"
          maxLength={300}
        />
        <div className="composer-actions">
          <Button onClick={onPublish} disabled={!text.trim()}>
            Post
          </Button>
        </div>
      </section>
    </Card>
  );
}

export function SyncBanner({
  count,
  online,
  onSync,
}: {
  count: number;
  online: boolean;
  onSync: () => void;
}) {
  if (!count) return null;
  return (
    <Card asChild size="2">
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
        <Button highContrast onClick={onSync} disabled={!online}>
          {online ? "Sync now" : "Waiting for network"}
        </Button>
      </section>
    </Card>
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
  const author = profileNameParts(profile, post.authorDid).name;
  const replyTarget = profile?.handle ? `@${profile.handle.replace(/^@/, "")}` : author;
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
    <Card asChild size="1">
      <article
        className={`post-card${pendingPost ? " pending" : ""}${isNew ? " new" : ""}`}
        data-post-uri={post.uri}
      >
        <header className="post-header">
          <Avatar
            src={profile?.avatar ?? undefined}
            fallback={author.charAt(0).toUpperCase()}
            size="2"
            radius="medium"
          />
          <div>
            <ProfileName profile={profile} fallback={post.authorDid} />
            <time dateTime={post.createdAt}>{formatPostDate(post.createdAt)}</time>
          </div>
          {pendingPost && <Badge className="pending-label">Pending</Badge>}
        </header>
        <p>
          <PostText text={post.text} facetsJson={post.facetsJson} />
        </p>
        <PostImages post={post} />
        {post.quote && <QuotedPost post={post.quote} />}
        <footer className="post-actions">
          <Button
            variant="ghost"
            color="gray"
            size="1"
            type="button"
            aria-label={`Reply to post by ${author}`}
            aria-expanded={replying}
            title={canReply ? undefined : "Replies are available once this thread has synced"}
            disabled={!canReply}
            onClick={() => setReplying((open) => !open)}
          >
            <ReplyIcon />
            <span>{post.replyCount || ""}</span>
          </Button>
          <Button
            variant={post.like?.active ? "soft" : "ghost"}
            color={post.like?.active ? "crimson" : "gray"}
            size="1"
            type="button"
            aria-pressed={post.like?.active ?? false}
            aria-label={`${post.like?.active ? "Unlike" : "Like"} post by ${author}`}
            title={post.cid ? undefined : "Likes are available once this post has synced"}
            disabled={!post.cid}
            onClick={() => onToggleReaction("like", post)}
          >
            <LikeIcon active={post.like?.active ?? false} />
            <span>{post.likeCount || ""}</span>
          </Button>
          <Button
            variant={post.repost?.active ? "soft" : "ghost"}
            color={post.repost?.active ? "jade" : "gray"}
            size="1"
            type="button"
            aria-pressed={post.repost?.active ?? false}
            aria-label={`${post.repost?.active ? "Undo repost" : "Repost"} post by ${author}`}
            title={post.cid ? undefined : "Reposts are available once this post has synced"}
            disabled={!post.cid}
            onClick={() => onToggleReaction("repost", post)}
          >
            <RepostIcon />
            <span>{post.repostCount || ""}</span>
          </Button>
          {(pendingLike || pendingRepost) && <Badge variant="soft">Pending</Badge>}
        </footer>
        {replying && (
          <form className="reply-composer" onSubmit={submitReply}>
            <label htmlFor={`reply-${post.id}`}>Reply to {author}</label>
            <TextArea
              id={`reply-${post.id}`}
              rows={3}
              value={replyText}
              onChange={(event) => setReplyText(event.target.value)}
              placeholder={`Reply to ${replyTarget}`}
              maxLength={300}
              autoFocus
            />
            <div>
              <span>{replyText.length} / 300</span>
              <Button type="button" variant="ghost" color="gray" onClick={() => setReplying(false)}>
                Cancel
              </Button>
              <Button type="submit" size="1" disabled={!replyText.trim() || !canReply}>
                Reply
              </Button>
            </div>
          </form>
        )}
      </article>
    </Card>
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

type SubscribedTimelineThreadProps = TimelineThreadProps & { relations: TimelineRelations };

function TimelinePostTree({
  node,
  threadRoot,
  onReroot,
  navigationTarget,
  postState,
  actions,
  depth = 0,
}: TimelineThreadProps & {
  node: TimelinePostNode;
  threadRoot: DisplayPost;
  onReroot: (id: string, control: HTMLButtonElement) => void;
  navigationTarget?: "back" | string;
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
      {node.replies.length > 0 &&
        (depth < 2 ? (
          <div className="thread-replies">
            {node.replies.map((reply) => (
              <TimelinePostTree
                key={reply.post.id}
                node={reply}
                threadRoot={threadRoot}
                onReroot={onReroot}
                navigationTarget={navigationTarget}
                postState={postState}
                actions={actions}
                depth={depth + 1}
              />
            ))}
          </div>
        ) : (
          <Button
            className="thread-control"
            variant="ghost"
            size="1"
            style={
              navigationTarget === node.post.id
                ? { viewTransitionName: "thread-navigation" }
                : undefined
            }
            onClick={(event) => onReroot(node.post.id, event.currentTarget)}
          >
            <DisclosureIcon />
            Show {node.replies.length === 1 ? "reply" : `${node.replies.length} replies`}
          </Button>
        ))}
      {hasUncachedReplies && (
        <Button
          className="thread-control"
          variant="ghost"
          size="1"
          disabled={!postState.online || loadingReplies}
          onClick={() => actions.onLoadThread(node.post)}
        >
          {loadingReplies ? (
            <>
              <Spinner aria-hidden="true" />
              Loading replies…
            </>
          ) : postState.online ? (
            <>
              <DisclosureIcon />
              Load {node.post.replyCount === 1 ? "reply" : `${node.post.replyCount} replies`}
            </>
          ) : (
            "Replies not cached"
          )}
        </Button>
      )}
    </div>
  );
}

function TimelineThread(props: SubscribedTimelineThreadProps & { item: TimelineItem }) {
  const [expanded, setExpanded] = useState(false);
  if (props.item.threadUrl) {
    return <TimelineThreadContent {...props} />;
  }
  const actions = {
    ...props.actions,
    onLoadThread(post: DisplayPost) {
      setExpanded(true);
      props.actions.onLoadThread(post);
    },
  };
  return expanded ? (
    <TimelineThreadContent
      {...props}
      actions={actions}
      item={hydrateTimelineThread(
        props.item,
        props.relations.posts.filter((post) => post.replyRootId === props.item.threadRoot.id),
        props.relations,
      )}
    />
  ) : (
    <TimelineThreadContent {...props} actions={actions} />
  );
}

function TimelineThreadContent({
  item,
  postState,
  actions,
}: TimelineThreadProps & { item: TimelineItem }) {
  const [focusedId, setFocusedId] = useState(item.node.post.id);
  const [navigationTarget, setNavigationTarget] = useState<"back" | string>();
  const findNode = (node: TimelinePostNode): TimelinePostNode | undefined =>
    node.post.id === focusedId ? node : node.replies.map(findNode).find(Boolean);
  const focusedNode = findNode(item.node) ?? item.node;
  const reroot = (id: string, control: HTMLButtonElement) => {
    const target = id === item.node.post.id ? focusedId : "back";
    const updateFocus = () => {
      flushSync(() => {
        setFocusedId(id);
        setNavigationTarget(target);
      });
    };
    if (
      typeof document.startViewTransition !== "function" ||
      matchMedia("(prefers-reduced-motion: reduce)").matches
    ) {
      setFocusedId(id);
      return;
    }
    control.style.viewTransitionName = "thread-navigation";
    document.startViewTransition(updateFocus).finished.finally(() => {
      control.style.removeProperty("view-transition-name");
      setNavigationTarget(undefined);
    });
  };
  const reposter = item.repost?.actorProfile;
  const reposterFallback = item.repost?.actorDid ?? "Unknown account";
  const reposterName = profileNameParts(reposter, reposterFallback).name;
  return (
    <div className="timeline-thread">
      {item.repost && (
        <div className="repost-reason">
          <RepostIcon />
          {reposter?.avatar && (
            <Avatar
              src={reposter.avatar}
              fallback={reposterName.charAt(0).toUpperCase()}
              size="1"
              radius="medium"
            />
          )}
          <ProfileName profile={reposter} fallback={reposterFallback} />
          <span>reposted</span>
        </div>
      )}
      {focusedId !== item.node.post.id && (
        <Button
          className="thread-control"
          variant="ghost"
          size="1"
          style={
            navigationTarget === "back" ? { viewTransitionName: "thread-navigation" } : undefined
          }
          onClick={(event) => reroot(item.node.post.id, event.currentTarget)}
        >
          <BackIcon />
          Back to top level
        </Button>
      )}
      <TimelinePostTree
        node={focusedNode}
        threadRoot={item.threadRoot}
        onReroot={reroot}
        navigationTarget={navigationTarget}
        postState={postState}
        actions={actions}
      />
      {item.threadUrl && (
        <Button asChild className="thread-control" variant="ghost" size="1">
          <a href={item.threadUrl} target="_blank" rel="noopener noreferrer">
            <ThreadLinkIcon />
            View thread
          </a>
        </Button>
      )}
    </div>
  );
}

export function TimelineFeed({
  items,
  waiting,
  hasMore,
  canLoadMore,
  loadingMore,
  onLoadMore,
  pendingLikePostIds,
  pendingRepostPostIds,
  pendingPostIds,
  newEntryPostIds,
  loadingThreadUris,
  online,
  connectivity,
  relations,
  onToggleReaction,
  onReply,
  onLoadThread,
}: PostState &
  ThreadActions & {
    items: TimelineItem[];
    waiting: boolean;
    hasMore: boolean;
    canLoadMore: boolean;
    loadingMore: boolean;
    onLoadMore: () => Promise<void>;
    connectivity: ConnectivityStatus;
    relations: TimelineRelations;
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
          <div className="timeline-title-row">
            <h2 id="timeline-title">Latest posts</h2>
            <Badge
              color={connectivity === "online" ? "jade" : "gray"}
              variant="soft"
              role="status"
              aria-live="polite"
            >
              <StatusIcon />
              {connectivity === "checking"
                ? "Checking"
                : connectivity === "online"
                  ? "Online"
                  : "Offline"}
            </Badge>
          </div>
        </div>
        <Text as="p" size="1" color="gray">
          Cached locally by Jazz for offline availability
        </Text>
      </div>
      <div className="feed">
        {items.map((item) => (
          <TimelineThread
            key={item.id}
            item={item}
            postState={postState}
            actions={actions}
            relations={relations}
          />
        ))}
        {waiting && (
          <div className="empty-state">
            <Spinner aria-hidden="true" />
            <h3>Syncing your timeline</h3>
            <p>New posts will appear as Jazz receives them.</p>
          </div>
        )}
        {!items.length && !waiting && (
          <div className="empty-state">
            <div className="brand-mark" aria-hidden="true">
              J
            </div>
            <h3>Your offline timeline is empty</h3>
            <p>Connect once to cache recent posts from people you follow.</p>
          </div>
        )}
      </div>
      {items.length > 0 && (
        <div className="pagination-status" aria-live="polite">
          {hasMore ? (
            <Button variant="soft" disabled={!canLoadMore} onClick={onLoadMore}>
              {loadingMore && <Spinner aria-hidden="true" />}
              {loadingMore ? "Fetching more posts…" : "Load more"}
            </Button>
          ) : (
            <span>
              <SuccessIcon />
              You’re all caught up
            </span>
          )}
        </div>
      )}
    </section>
  );
}

export function AppFooter({ onSignOut }: { onSignOut: () => void }) {
  return (
    <footer className="app-footer">
      <span>Jazz ❤️ Bluesky is a local-first ATProto proof of concept.</span>
      <Button variant="ghost" color="gray" onClick={onSignOut}>
        Sign out
      </Button>
    </footer>
  );
}
