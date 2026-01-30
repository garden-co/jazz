import { co, z } from 'jazz-tools';

// #region RecursiveReferences
const Author = co.map({
  name: z.string(),
  // This allows us to defer the evaluation
  get posts() {
    return co.list(Post);
  }
});

const Post = co.map({
  title: z.string(),
  content: co.richText(),
  author: Author
});
// #endregion
