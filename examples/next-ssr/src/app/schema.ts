import { Account, CoList, CoMap, Group, Profile, co } from "jazz-tools";

export class Post extends CoMap {
  title = co.string;
}

export class PostList extends CoList.Of(co.ref(Post)) {}

export class Root extends CoMap {
  posts = co.ref(PostList);
}

export class PublicProfile extends Profile {
  posts = co.ref(PostList);
}

export class WorkerAccount extends Account {
  root = co.ref(Root);
  profile = co.ref(PublicProfile);

  async migrate(this: WorkerAccount) {
    if (!this._refs.root) {
      const group = Group.create();
      group.addMember("everyone", "writer");
      const samplePost = Post.create(
        {
          title: "Hello World",
        },
        {
          owner: group,
        },
      );
      if (this.profile) {
        this.profile.posts = PostList.create([samplePost], {
          owner: group,
        });
      }

      return;
    }
  }
}
