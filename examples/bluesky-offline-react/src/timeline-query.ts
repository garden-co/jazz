import { app } from "../schema.js";

export function timelineQuery(ownerDid: string, includeThreadDetails = true) {
  const quotedPost = app.posts.include({
    authorProfile: true,
    postImagesViaPost: true,
  });
  const post = app.posts.include({
    authorProfile: true,
    postImagesViaPost: true,
    quotedPost,
    likesViaSubjectPost: app.likes.where({ actorDid: { eq: ownerDid } }),
    repostsViaSubjectPost: app.reposts.where({ actorDid: { eq: ownerDid } }),
  });
  return app.timelineEntries
    .where({ ownerDid: { eq: ownerDid }, active: { eq: true } })
    .orderBy("sortAt", "desc")
    .include({
      post,
      repost: app.reposts.include({ actorProfile: true }),
      threadRoot: includeThreadDetails
        ? app.posts.include({
          authorProfile: true,
          postImagesViaPost: true,
          quotedPost,
          threadEntriesViaRootPost: app.threadEntries.orderBy("sortOrder", "asc").include({ post }),
        })
        : app.posts.include({
          authorProfile: true,
          postImagesViaPost: true,
          quotedPost,
        }),
    });
}
