import { co, z } from 'jazz-tools';

const Author = co.map({
  name: z.string()
});

// #region OneDirectionalRelationship
const Post = co.map({
  title: z.string(),
  content: co.richText(),
  // A single author
  author: Author
});
// #endregion
