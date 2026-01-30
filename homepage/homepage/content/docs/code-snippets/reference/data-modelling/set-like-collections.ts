import { co, z } from 'jazz-tools';

const Post = co.map({
  title: z.string(),
  content: co.richText(),
});

// #region SetLikeCollections
const Author = co.map({
  name: z.string(),
  posts: co.record(z.string(), Post)
});

// Assuming 'newPost' is a Post we want to link to from the Author
// [!code hide]
const author = Author.create({ name: '', posts: {} })
// [!code hide]
const newPost = Post.create({ title: '', content: '' })
author.posts.$jazz.set(newPost.$jazz.id, newPost);
// #endregion
