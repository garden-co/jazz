import { Account, CoMap, Group, Profile, co } from "jazz-tools";

export class MyProfile extends Profile {
  name = co.string;
}

export class MyAccountRoot extends CoMap {
  count = co.number;
}

export class MyAccount extends Account {
  profile = co.ref(MyProfile);
  root = co.ref(MyAccountRoot);

  migrate(this: MyAccount) {
    if (this.root === undefined) {
      const group = Group.create();
      this.root = MyAccountRoot.create({ count: 0 }, group);
    }
  }
}
