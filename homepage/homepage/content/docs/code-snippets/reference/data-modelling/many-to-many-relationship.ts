import { co, z } from 'jazz-tools';

// #region ManyToManyRelationship
const Author = co.map({
  name: z.string(),
  // A single author has multiple posts
  get posts() {
    return co.list(Post)
  }
});

const Post = co.map({
  title: z.string(),
  content: co.richText(),
  // Multiple authors collaborating on a single post
  authors: co.list(Author)
});
// #endregion
