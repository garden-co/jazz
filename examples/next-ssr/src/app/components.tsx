"use client";

import { useCoState } from "jazz-react";
import { Group, ID } from "jazz-tools";
import { useState } from "react";
import { PostList, Post as PostType } from "./schema";

export function RenderPostsClient({ postsCoId }: { postsCoId: ID<PostList> }) {
  const [value, setValue] = useState<string>("");
  const list = useCoState(PostList, postsCoId, [{}]);

  return (
    <div>
      <RenderPosts posts={list ?? []} type="client" />

      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          const newPost = PostType.create(
            {
              title: value,
            },
            {
              owner: list?._owner.castAs(Group)!,
            },
          );

          list?.push(newPost);
          setValue("");
        }}
      >
        <input
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder="New Post"
        />
        <button type="submit">Add Post</button>
      </form>
    </div>
  );
}

export function RenderPosts({
  posts,
  type,
}: {
  posts: PostType[];
  type: string;
}) {
  return (
    <div>
      <ul>
        {posts.map((post) => (
          <li key={post?.id + type}>{post?.title}</li>
        ))}
      </ul>
    </div>
  );
}
