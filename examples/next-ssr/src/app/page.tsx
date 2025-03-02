import { RenderPosts, RenderPostsClient } from "./components";
import { getJazzWorker } from "./jazz-worker";

export default async function Home() {
  const worker = await getJazzWorker();
  const account = await worker.ensureLoaded({
    root: { posts: [{}] },
    profile: { posts: [{}] },
  });

  if (!account.profile) {
    return <div>No profile</div>;
  }

  const posts =
    account.profile?.posts?.map((post) => ({
      title: post.title,
      id: post.id,
    })) ?? [];

  return (
    <div className="flex flex-col gap-4">
      <h2 className="text-2xl font-bold">Server State</h2>
      <RenderPosts posts={posts as any[]} type="server" />

      <h2 className="text-2xl font-bold">Client State</h2>
      <RenderPostsClient postsCoId={account.profile?._refs.posts?.id as any} />
    </div>
  );
}
