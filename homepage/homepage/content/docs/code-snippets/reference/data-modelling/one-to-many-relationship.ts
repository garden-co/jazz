import { co, z } from 'jazz-tools';

const Author = co.map({
  name: z.string(),
});
// #region OneToManyRelationship
const Post = co.map({
  title: z.string(),
  content: co.richText(),
  // Multiple authors collaborating on a single post
  authors: co.list(Author)
});
// #endregion
