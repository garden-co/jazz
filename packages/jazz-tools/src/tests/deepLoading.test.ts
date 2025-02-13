import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { describe, expect, expectTypeOf, test } from "vitest";
import {
  Account,
  CoFeed,
  CoList,
  CoMap,
  Group,
  ID,
  Profile,
  SessionID,
  co,
  createJazzContextFromExistingCredentials,
  isControlledAccount,
} from "../index.js";
import { randomSessionProvider } from "../internal.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";

const Crypto = await WasmCrypto.create();
const { connectedPeers } = cojsonInternals;

class TestMap extends CoMap {
  list = co.ref(TestList);
  optionalRef = co.ref(InnermostMap, { optional: true });
}

class TestList extends CoList.Of(co.ref(() => InnerMap)) {}

class InnerMap extends CoMap {
  stream = co.ref(TestStream);
}

class TestStream extends CoFeed.Of(co.ref(() => InnermostMap)) {}

class InnermostMap extends CoMap {
  value = co.string;
}

describe("Deep loading with depth arg", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
    peer1role: "server",
    peer2role: "client",
  });

  if (!isControlledAccount(me)) {
    throw "me is not a controlled account";
  }
  me._raw.core.node.syncManager.addPeer(secondPeer);
  const { account: meOnSecondPeer } =
    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.id,
        secret: me._raw.agentSecret,
      },
      sessionProvider: randomSessionProvider,
      peersToLoadFrom: [initialAsPeer],
      crypto: Crypto,
    });

  const ownership = { owner: me };
  const map = TestMap.create(
    {
      list: TestList.create(
        [
          InnerMap.create(
            {
              stream: TestStream.create(
                [InnermostMap.create({ value: "hello" }, ownership)],
                ownership,
              ),
            },
            ownership,
          ),
        ],
        ownership,
      ),
    },
    ownership,
  );

  test("load without resolve", async () => {
    const map1 = await TestMap.load(map.id, { loadAs: meOnSecondPeer });
    expectTypeOf(map1).toEqualTypeOf<TestMap | undefined>();
    if (map1 === undefined) {
      throw new Error("map1 is undefined");
    }
    expect(map1.list).toBe(null);
  });

  test("load with resolve { list: true }", async () => {
    const map2 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: true },
    });
    expectTypeOf(map2).toEqualTypeOf<
      | (TestMap & {
          list: TestList;
        })
      | undefined
    >();
    if (map2 === undefined) {
      throw new Error("map2 is undefined");
    }
    expect(map2.list).toBeTruthy();
    expect(map2.list[0]).toBe(null);
  });

  test("load with resolve { list: { $each: true } }", async () => {
    const map3 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: true } },
    });
    expectTypeOf(map3).toEqualTypeOf<
      | (TestMap & {
          list: TestList & InnerMap[];
        })
      | undefined
    >();
    if (map3 === undefined) {
      throw new Error("map3 is undefined");
    }
    expect(map3.list[0]).toBeTruthy();
    expect(map3.list[0]?.stream).toBe(null);
  });

  test("load with resolve { optionalRef: true }", async () => {
    const map3a = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { optionalRef: true } as const,
    });
    expectTypeOf(map3a).toEqualTypeOf<
      | (TestMap & {
          optionalRef: InnermostMap | undefined;
        })
      | undefined
    >();
    expect(map3a).toBeTruthy();
  });

  test("load with resolve { list: { $each: { stream: true } } }", async () => {
    const map4 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: { stream: true } } },
    });
    expectTypeOf(map4).toEqualTypeOf<
      | (TestMap & {
          list: TestList & (InnerMap & { stream: TestStream })[];
        })
      | undefined
    >();
    if (map4 === undefined) {
      throw new Error("map4 is undefined");
    }
    expect(map4.list[0]?.stream).toBeTruthy();
    expect(map4.list[0]?.stream?.[me.id]).toBeTruthy();
    expect(map4.list[0]?.stream?.byMe?.value).toBe(null);
  });

  test("load with resolve { list: { $each: { stream: { $each: true } } } }", async () => {
    const map5 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: { stream: { $each: true } } } },
    });
    type ExpectedMap5 =
      | (TestMap & {
          list: TestList &
            (InnerMap & {
              stream: TestStream & {
                byMe?: { value: InnermostMap };
                inCurrentSession?: { value: InnermostMap };
                perSession: {
                  [sessionID: SessionID]: {
                    value: InnermostMap;
                  };
                };
              } & {
                [key: ID<Account>]: { value: InnermostMap };
              };
            })[];
        })
      | undefined;
    expectTypeOf(map5).toEqualTypeOf<ExpectedMap5>();
    if (map5 === undefined) {
      throw new Error("map5 is undefined");
    }

    expect(map5.list[0]?.stream?.[me.id]?.value).toBeTruthy();
    expect(map5.list[0]?.stream?.byMe?.value).toBeTruthy();
  });
});

class CustomProfile extends Profile {
  stream = co.ref(TestStream);
}

class CustomAccount extends Account {
  profile = co.ref(CustomProfile);
  root = co.ref(TestMap);

  async migrate(
    this: CustomAccount,
    creationProps?: { name: string } | undefined,
  ) {
    if (creationProps) {
      this.profile = CustomProfile.create(
        {
          name: creationProps.name,
          stream: TestStream.create([], { owner: this }),
        },
        { owner: this },
      );
      this.root = TestMap.create(
        { list: TestList.create([], { owner: this }) },
        { owner: this },
      );
    }

    const thisLoaded = await this.ensureLoaded({
      resolve: {
        profile: { stream: true },
        root: { list: true },
      },
    });
    expectTypeOf(thisLoaded).toEqualTypeOf<
      CustomAccount & {
        profile: CustomProfile & {
          stream: TestStream;
        };
        root: TestMap & {
          list: TestList;
        };
      }
    >();
  }
}

test("Deep loading within account", async () => {
  const me = await CustomAccount.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const meLoaded = await me.ensureLoaded({
    resolve: {
      profile: { stream: true },
      root: { list: true },
    },
  });
  expectTypeOf(meLoaded).toEqualTypeOf<
    CustomAccount & {
      profile: CustomProfile & {
        stream: TestStream;
      };
      root: TestMap & {
        list: TestList;
      };
    }
  >();

  expect(meLoaded.profile.stream).toBeTruthy();
  expect(meLoaded.root.list).toBeTruthy();
});

class RecordLike extends CoMap.Record(co.ref(TestMap)) {}

test("Deep loading a record-like coMap", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
    peer1role: "server",
    peer2role: "client",
  });

  if (!isControlledAccount(me)) {
    throw "me is not a controlled account";
  }

  me._raw.core.node.syncManager.addPeer(secondPeer);
  const { account: meOnSecondPeer } =
    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.id,
        secret: me._raw.agentSecret,
      },
      sessionProvider: randomSessionProvider,
      peersToLoadFrom: [initialAsPeer],
      crypto: Crypto,
    });

  const record = RecordLike.create(
    {
      key1: TestMap.create(
        { list: TestList.create([], { owner: me }) },
        { owner: me },
      ),
      key2: TestMap.create(
        { list: TestList.create([], { owner: me }) },
        { owner: me },
      ),
    },
    { owner: me },
  );

  const recordLoaded = await RecordLike.load(record.id, {
    loadAs: meOnSecondPeer,
    resolve: {
      $each: { list: { $each: true } },
    },
  });
  expectTypeOf(recordLoaded).toEqualTypeOf<
    | (RecordLike & {
        [key: string]: TestMap & {
          list: TestList & InnerMap[];
        };
      })
    | undefined
  >();
  if (recordLoaded === undefined) {
    throw new Error("recordLoaded is undefined");
  }
  expect(recordLoaded.key1?.list).not.toBe(null);
  expect(recordLoaded.key1?.list).toBeTruthy();
  expect(recordLoaded.key2?.list).not.toBe(null);
  expect(recordLoaded.key2?.list).toBeTruthy();
});

test("The resolve type doesn't accept extra keys", async () => {
  const me = await CustomAccount.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const meLoaded = await me.ensureLoaded({
    resolve: {
      // @ts-expect-error
      profile: { stream: true, extraKey: true },
      // @ts-expect-error
      root: { list: true, extraKey: true },
    },
  });

  await me.ensureLoaded({
    resolve: {
      // @ts-expect-error
      root: { list: { $each: true, extraKey: true } },
    },
  });

  await me.ensureLoaded({
    resolve: {
      root: { list: true },
      // @ts-expect-error
      extraKey: true,
    },
  });

  expectTypeOf(meLoaded).toEqualTypeOf<
    CustomAccount & {
      profile: CustomProfile & {
        stream: TestStream;
        extraKey: never;
      };
      root: TestMap & {
        list: TestList;
        extraKey: never;
      };
    }
  >();
});

describe("Deep loading with unauthorized account", async () => {
  const bob = await createJazzTestAccount({
    creationProps: { name: "Bob" },
  });

  const alice = await createJazzTestAccount({
    creationProps: { name: "Alice" },
  });

  linkAccounts(bob, alice);

  await alice.waitForAllCoValuesSync();

  const onlyBob = bob;
  const group = Group.create(bob);

  group.addMember(alice, "reader");

  test("unaccessible root", async () => {
    const map = TestMap.create({ list: TestList.create([], group) }, onlyBob);

    const mapOnAlice = await TestMap.load(map.id, { loadAs: alice });

    expect(mapOnAlice).toBe(undefined);
  });

  test("unaccessible list", async () => {
    const map = TestMap.create({ list: TestList.create([], onlyBob) }, group);

    const mapOnAlice = await TestMap.load(map.id, { loadAs: alice });
    expect(mapOnAlice).toBeTruthy();

    const mapWithListOnAlice = await TestMap.load(map.id, {
      resolve: { list: true },
      loadAs: alice,
    });

    expect(mapWithListOnAlice).toBe(undefined);
  });

  test("unaccessible list element", async () => {
    const map = TestMap.create(
      {
        list: TestList.create(
          [
            InnerMap.create(
              {
                stream: TestStream.create([], group),
              },
              onlyBob,
            ),
          ],
          group,
        ),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.id, {
      resolve: { list: { $each: true } },
      loadAs: alice,
    });

    expect(mapOnAlice).toBe(undefined);
  });

  test("unaccessible optional element", async () => {
    const map = TestMap.create(
      {
        list: TestList.create([], group),
        optionalRef: InnermostMap.create({ value: "hello" }, onlyBob),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.id, {
      loadAs: alice,
      resolve: { optionalRef: true } as const,
    });
    expect(mapOnAlice).not.toBe(undefined);

    // TODO: We should make the whole inaccessble map undefined
    expect(mapOnAlice?.optionalRef).not.toBe(undefined);
    expect(mapOnAlice?.optionalRef?.value).toBe(undefined);
  });

  test("unaccessible stream", async () => {
    const map = TestMap.create(
      {
        list: TestList.create(
          [
            InnerMap.create(
              {
                stream: TestStream.create([], onlyBob),
              },
              group,
            ),
          ],
          group,
        ),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.id, {
      resolve: { list: { $each: { stream: true } } },
      loadAs: alice,
    });

    expect(mapOnAlice).toBe(undefined);
  });

  test("unaccessible stream element", async () => {
    const map = TestMap.create(
      {
        list: TestList.create(
          [
            InnerMap.create(
              {
                stream: TestStream.create(
                  [InnermostMap.create({ value: "hello" }, onlyBob)],
                  group,
                ),
              },
              group,
            ),
          ],
          group,
        ),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.id, {
      resolve: { list: { $each: { stream: { $each: true } } } },
      loadAs: alice,
    });

    expect(mapOnAlice).toBe(undefined);
  });
});
