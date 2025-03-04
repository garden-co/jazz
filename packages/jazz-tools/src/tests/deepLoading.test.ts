import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { describe, expect, expectTypeOf, test, vi } from "vitest";
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
import { waitFor } from "./utils.js";

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

  test("loading a deeply nested object will wait until all required refs are loaded", async () => {
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

    const map1 = await TestMap.load(map.id, meOnSecondPeer, {});
    expectTypeOf(map1).toEqualTypeOf<TestMap | undefined>();
    if (map1 === undefined) {
      throw new Error("map1 is undefined");
    }
    expect(map1.list).toBe(null);

    const map2 = await TestMap.load(map.id, meOnSecondPeer, { list: [] });
    expectTypeOf(map2).toEqualTypeOf<
      | (TestMap & {
          list: TestList;
        })
      | undefined
    >();
    if (map2 === undefined) {
      throw new Error("map2 is undefined");
    }
    expect(map2.list).not.toBe(null);
    expect(map2.list[0]).toBe(null);

    const map3 = await TestMap.load(map.id, meOnSecondPeer, { list: [{}] });
    expectTypeOf(map3).toEqualTypeOf<
      | (TestMap & {
          list: TestList & InnerMap[];
        })
      | undefined
    >();
    if (map3 === undefined) {
      throw new Error("map3 is undefined");
    }
    expect(map3.list[0]).not.toBe(null);
    expect(map3.list[0]?.stream).toBe(null);

    const map3a = await TestMap.load(map.id, meOnSecondPeer, {
      optionalRef: {},
    });
    expectTypeOf(map3a).toEqualTypeOf<
      | (TestMap & {
          optionalRef: InnermostMap | undefined;
        })
      | undefined
    >();

    const map4 = await TestMap.load(map.id, meOnSecondPeer, {
      list: [{ stream: [] }],
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
    expect(map4.list[0]?.stream).not.toBe(null);
    expect(map4.list[0]?.stream?.[me.id]).not.toBe(null);
    expect(map4.list[0]?.stream?.byMe?.value).toBe(null);

    const map5 = await TestMap.load(map.id, meOnSecondPeer, {
      list: [{ stream: [{}] }],
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
    expect(map5.list[0]?.stream?.[me.id]?.value).not.toBe(null);
    expect(map5.list[0]?.stream?.byMe?.value).not.toBe(null);
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
      const profileGroup = Group.create(this);
      this.profile = CustomProfile.create(
        {
          name: creationProps.name,
          stream: TestStream.create([], this),
        },
        profileGroup,
      );
      this.root = TestMap.create({ list: TestList.create([], this) }, this);
    }

    const thisLoaded = await this.ensureLoaded({
      profile: { stream: [] },
      root: { list: [] },
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
    profile: { stream: [] },
    root: { list: [] },
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

  expect(meLoaded.profile.stream).not.toBe(null);
  expect(meLoaded.root.list).not.toBe(null);
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

  const recordLoaded = await RecordLike.load(record.id, meOnSecondPeer, [
    { list: [{}] },
  ]);
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
  expect(recordLoaded.key1?.list).not.toBe(undefined);
  expect(recordLoaded.key2?.list).not.toBe(null);
  expect(recordLoaded.key2?.list).not.toBe(undefined);
});

test("doesn't break on Map.Record key deletion when the key is referenced in the depth", async () => {
  class JazzProfile extends CoMap {
    firstName = co.string;
  }

  class JazzySnapStore extends CoMap.Record(co.ref(JazzProfile)) {}

  const snapStore = JazzySnapStore.create({
    profile1: JazzProfile.create({ firstName: "John" }),
    profile2: JazzProfile.create({ firstName: "John" }),
  });

  const spy = vi.fn();
  const unsub = snapStore.subscribe({ profile1: {}, profile2: {} }, spy);

  await waitFor(() => expect(spy).toHaveBeenCalled());

  spy.mockClear();
  delete snapStore.profile1;

  expect(Object.keys(snapStore)).toEqual(["profile2"]);

  unsub();

  await expect(
    snapStore.ensureLoaded({
      profile1: {},
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + snapStore.id);
});

test("throw when calling ensureLoaded on a ref that's required but missing", async () => {
  class JazzProfile extends CoMap {
    firstName = co.string;
  }

  class JazzRoot extends CoMap {
    profile = co.ref(JazzProfile);
  }

  const me = await Account.create({
    creationProps: { name: "Tester McTesterson" },
    crypto: Crypto,
  });

  const root = JazzRoot.create(
    // @ts-expect-error missing required ref
    {},
    { owner: me },
  );

  await expect(
    root.ensureLoaded({
      profile: {},
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + root.id);
});

test("throw when calling ensureLoaded on a ref that is not defined in the schema", async () => {
  class JazzRoot extends CoMap {}

  const me = await Account.create({
    creationProps: { name: "Tester McTesterson" },
    crypto: Crypto,
  });

  const root = JazzRoot.create({}, { owner: me });

  await expect(
    root.ensureLoaded({
      profile: {},
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + root.id);
});

test("should not throw when calling ensureLoaded a record with a deleted ref", async () => {
  class JazzProfile extends CoMap {
    firstName = co.string;
  }

  class JazzySnapStore extends CoMap.Record(co.ref(JazzProfile)) {}

  const me = await Account.create({
    creationProps: { name: "Tester McTesterson" },
    crypto: Crypto,
  });

  const root = JazzySnapStore.create(
    {
      profile: JazzProfile.create({ firstName: "John" }, me),
    },
    me,
  );

  let value: any;
  let unsub = root.subscribe([{}], (v) => {
    value = v;
  });

  await waitFor(() => expect(value.profile).toBeDefined());

  delete root.profile;

  await waitFor(() => expect(value.profile).toBeUndefined());

  unsub();

  value = undefined;
  unsub = root.subscribe([{}], (v) => {
    value = v;
  });

  await waitFor(() => expect(value).toBeDefined());

  expect(value.profile).toBeUndefined();

  unsub();
});
