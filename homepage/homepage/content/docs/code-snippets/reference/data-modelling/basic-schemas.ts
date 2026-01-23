import { co, z } from 'jazz-tools';

// #region BasicSchemas
const Author = co.map({
  name: z.string()
});

const Post = co.map({
  title: z.string(),
  content: co.richText(),
});
// #endregion
