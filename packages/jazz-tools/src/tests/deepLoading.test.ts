import { Profile, cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, describe, expect, expectTypeOf, test, vi } from "vitest";
import {
  Group,
  ID,
  SessionID,
  createJazzContextFromExistingCredentials,
  isControlledAccount,
  z,
} from "../index.js";
import {
  Account,
  CoListSchema,
  Loaded,
  co,
  randomSessionProvider,
} from "../internal.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";
import { waitFor } from "./utils.js";

const Crypto = await WasmCrypto.create();
const { connectedPeers } = cojsonInternals;

const InnermostMap = co.map({
  value: z.string(),
});

const TestFeed = co.feed(InnermostMap);

const InnerMap = co.map({
  stream: TestFeed,
});

const TestList = co.list(InnerMap);

const TestMap = co.map({
  list: TestList,
  optionalRef: z.optional(InnermostMap),
});

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
        secret: me._raw.core.node.getCurrentAgent().agentSecret,
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
              stream: TestFeed.create(
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
    expectTypeOf(map1).branded.toEqualTypeOf<Loaded<typeof TestMap> | null>();

    assert(map1, "map1 is null");

    expect(map1.list).toBe(null);
  });

  test("load with resolve { list: true }", async () => {
    const map2 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: true },
    });
    expectTypeOf(map2).branded.toEqualTypeOf<
      | (Loaded<typeof TestMap> & {
          list: Loaded<typeof TestList>;
        })
      | null
    >();
    assert(map2, "map2 is null");
    expect(map2.list).toBeTruthy();
    expect(map2.list[0]).toBe(null);
  });

  test("load with resolve { list: { $each: true } }", async () => {
    const map3 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: true } },
    });
    expectTypeOf(map3).branded.toEqualTypeOf<
      | (Loaded<typeof TestMap> & {
          list: Loaded<typeof TestList> & Loaded<typeof InnerMap>[];
        })
      | null
    >();
    assert(map3, "map3 is null");
    expect(map3.list[0]).toBeTruthy();
    expect(map3.list[0]?.stream).toBe(null);
  });

  test("load with resolve { optionalRef: true }", async () => {
    const map3a = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { optionalRef: true } as const,
    });
    expectTypeOf(map3a).branded.toEqualTypeOf<
      | (Loaded<typeof TestMap> & {
          optionalRef: Loaded<typeof InnermostMap> | undefined;
        })
      | null
    >();
    assert(map3a, "map3a is null");
    expect(map3a).toBeTruthy();
  });

  test("load with resolve { list: { $each: { stream: true } } }", async () => {
    const map4 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: { stream: true } } },
    });
    expectTypeOf(map4).branded.toEqualTypeOf<
      | (Loaded<typeof TestMap> & {
          list: Loaded<typeof TestList> &
            (Loaded<typeof InnerMap> & { stream: Loaded<typeof TestFeed> })[];
        })
      | null
    >();
    assert(map4, "map4 is null");
    expect(map4.list[0]?.stream).toBeTruthy();
    expect(map4.list[0]?.stream?.perAccount[me.id]).toBeTruthy();
    expect(map4.list[0]?.stream?.byMe?.value).toBe(null);
  });

  test("load with resolve { list: { $each: { stream: { $each: true } } } }", async () => {
    const map5 = await TestMap.load(map.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: { stream: { $each: true } } } },
    });
    type ExpectedMap5 =
      | (Loaded<typeof TestMap> & {
          list: Loaded<typeof TestList> &
            (Loaded<typeof InnerMap> & {
              stream: Loaded<typeof TestFeed> & {
                byMe?: { value: Loaded<typeof InnermostMap> };
                inCurrentSession?: { value: Loaded<typeof InnermostMap> };
                perSession: {
                  [sessionID: SessionID]: {
                    value: Loaded<typeof InnermostMap>;
                  };
                };
              } & {
                [key: ID<Account>]: { value: Loaded<typeof InnermostMap> };
              };
            })[];
        })
      | null;
    expectTypeOf(map5).branded.toEqualTypeOf<ExpectedMap5>();
    assert(map5, "map5 is null");

    expect(map5.list[0]?.stream?.perAccount[me.id]?.value).toBeTruthy();
    expect(map5.list[0]?.stream?.byMe?.value).toBeTruthy();
  });
});

const CustomProfile = co.profile({
  name: z.string(),
  stream: TestFeed,
});

const CustomAccount = co
  .account({
    profile: CustomProfile,
    root: TestMap,
  })
  .withMigration(async (account, creationProps) => {
    if (creationProps) {
      const profileGroup = Group.create(account);
      account.profile = CustomProfile.create(
        {
          name: creationProps.name,
          stream: TestFeed.create([], account),
        },
        profileGroup,
      );
      account.root = TestMap.create(
        { list: TestList.create([], account) },
        account,
      );
    }

    const accountLoaded = await account.ensureLoaded({
      resolve: {
        profile: { stream: true },
        root: { list: true },
      },
    });

    // using assignment to check type compatibility
    const _T:
      | (Loaded<typeof CustomAccount> & {
          profile: Loaded<typeof CustomProfile> & {
            stream: Loaded<typeof TestFeed>;
          };
          root: Loaded<typeof TestMap> & {
            list: Loaded<typeof TestList>;
          };
        })
      | null = accountLoaded;
  });

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

  // using assignment to check type compatibility
  const _T:
    | (Loaded<typeof CustomAccount> & {
        profile: Loaded<typeof CustomProfile> & {
          stream: Loaded<typeof TestFeed>;
        };
        root: Loaded<typeof TestMap> & {
          list: Loaded<typeof TestList>;
        };
      })
    | null = meLoaded;

  expect(meLoaded.profile.stream).toBeTruthy();
  expect(meLoaded.root.list).toBeTruthy();
});

const RecordLike = co.record(z.string(), TestMap);

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
        secret: me._raw.core.node.getCurrentAgent().agentSecret,
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
    | (Loaded<typeof RecordLike> & {
        [key: string]: Loaded<typeof TestMap> & {
          list: Loaded<typeof TestList> & Loaded<typeof InnerMap>[];
        };
      })
    | null
  >();
  assert(recordLoaded, "recordLoaded is null");
  expect(recordLoaded.key1?.list).not.toBe(null);
  expect(recordLoaded.key1?.list).toBeTruthy();
  expect(recordLoaded.key2?.list).not.toBe(null);
  expect(recordLoaded.key2?.list).toBeTruthy();
});

test("The resolve type doesn't accept extra keys", async () => {
  expect.assertions(1);

  const me = await CustomAccount.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  try {
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

    // using assignment to check type compatibility
    const _T:
      | (Loaded<typeof CustomAccount> & {
          profile: Loaded<typeof CustomProfile> & {
            stream: Loaded<typeof TestFeed>;
            extraKey: never;
          };
          root: Loaded<typeof TestMap> & {
            list: Loaded<typeof TestList>;
            extraKey: never;
          };
        })
      | null = meLoaded;
  } catch (e) {
    expect(e).toBeInstanceOf(Error);
  }
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
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const map = TestMap.create({ list: TestList.create([], group) }, onlyBob);

    const mapOnAlice = await TestMap.load(map.id, { loadAs: alice });

    expect(mapOnAlice).toBe(null);

    expect(errorSpy).toHaveBeenCalledWith(
      `The current user is not authorized to access this value from ${map.id}`,
    );

    errorSpy.mockReset();
  });

  test("unaccessible list", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const map = TestMap.create({ list: TestList.create([], onlyBob) }, group);

    const mapOnAlice = await TestMap.load(map.id, { loadAs: alice });
    expect(mapOnAlice).toBeTruthy();

    const mapWithListOnAlice = await TestMap.load(map.id, {
      resolve: { list: true },
      loadAs: alice,
    });

    expect(mapWithListOnAlice).toBe(null);

    expect(errorSpy).toHaveBeenCalledWith(
      `The current user is not authorized to access this value from ${map.id} on path list`,
    );

    errorSpy.mockReset();
  });

  test("unaccessible list element", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const map = TestMap.create(
      {
        list: TestList.create(
          [
            InnerMap.create(
              {
                stream: TestFeed.create([], group),
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

    expect(mapOnAlice).toBe(null);

    expect(errorSpy).toHaveBeenCalledWith(
      `The current user is not authorized to access this value from ${map.id} on path list.0`,
    );

    errorSpy.mockReset();
  });

  test("unaccessible optional element", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

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
    expect(mapOnAlice).toBe(null);

    expect(mapOnAlice?.optionalRef).toBe(undefined);
    expect(mapOnAlice?.optionalRef?.value).toBe(undefined);

    expect(errorSpy).toHaveBeenCalledWith(
      `The current user is not authorized to access this value from ${map.id} on path optionalRef`,
    );

    errorSpy.mockReset();
  });

  test("unaccessible optional element via autoload", async () => {
    const map = TestMap.create(
      {
        list: TestList.create([], group),
        optionalRef: InnermostMap.create({ value: "hello" }, onlyBob),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.id, {
      loadAs: alice,
      resolve: { list: true } as const,
    });

    assert(mapOnAlice, "Alice isn't able to load the map");

    const result = await new Promise((resolve) => {
      const unsub = mapOnAlice.subscribe((value) => {
        resolve(value.optionalRef);
        unsub();
      });
    });

    expect(result).toBe(null);
  });

  test("unaccessible stream", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const map = TestMap.create(
      {
        list: TestList.create(
          [
            InnerMap.create(
              {
                stream: TestFeed.create([], onlyBob),
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

    expect(mapOnAlice).toBe(null);

    expect(errorSpy).toHaveBeenCalledWith(
      `The current user is not authorized to access this value from ${map.id} on path list.0.stream`,
    );

    errorSpy.mockReset();
  });

  test("unaccessible stream element", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const value = InnermostMap.create({ value: "hello" }, onlyBob);

    const map = TestMap.create(
      {
        list: TestList.create(
          [
            InnerMap.create(
              {
                stream: TestFeed.create([value], group),
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

    expect(mapOnAlice).toBe(null);

    expect(errorSpy).toHaveBeenCalledWith(
      `The current user is not authorized to access this value from ${map.id} on path list.0.stream.${value.id}`,
    );

    errorSpy.mockReset();
  });

  test("setting undefined via proxy", async () => {
    const Lv3 = co.map({
      string: z.string(),
    });

    const Lv2 = co.map({
      lv3: z.optional(Lv3),
    });

    const Lv1 = co.map({
      lv2: Lv2,
    });

    const map = Lv1.create(
      { lv2: Lv2.create({ lv3: Lv3.create({ string: "hello" }, bob) }, bob) },
      bob,
    );

    map.lv2!.lv3 = undefined;

    const loadedMap = await Lv1.load(map.id, {
      resolve: { lv2: { lv3: true } },
      loadAs: bob,
    });

    expect(loadedMap?.id).toBe(map.id);
  });

  test("unaccessible record element with $onError", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Friends = co.record(z.string(), Person);

    const map = Friends.create(
      {
        jane: Person.create({ name: "Jane" }, onlyBob),
        alice: Person.create({ name: "Alice" }, group),
      },
      group,
    );

    const friendsOnAlice = await Friends.load(map.id, {
      resolve: { $each: { $onError: null } },
      loadAs: alice,
    });

    assert(friendsOnAlice, "friendsOnAlice is null");

    expect(friendsOnAlice.jane).toBeNull();
    expect(friendsOnAlice.alice).not.toBeNull();
  });

  test("unaccessible nested record element with $onError", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Friends = co.record(z.string(), Person);

    const User = co.map({
      name: z.string(),
      friends: Friends,
    });

    const map = User.create(
      {
        name: "John",
        friends: Friends.create(
          {
            jane: Person.create({ name: "Jane" }, onlyBob),
            alice: Person.create({ name: "Alice" }, group),
          },
          group,
        ),
      },
      group,
    );

    const user = await User.load(map.id, {
      resolve: { friends: { $each: { $onError: null } } },
      loadAs: alice,
    });

    assert(user, "user is null");

    expect(user.friends.jane).toBeNull();
    expect(user.friends.alice).not.toBeNull();
  });

  test("unaccessible element down the chain with $onError on a record", async () => {
    const Dog = co.map({
      name: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    const Friends = co.record(z.string(), Person);

    const User = co.map({
      name: z.string(),
      friends: Friends,
    });

    const map = User.create(
      {
        name: "John",
        friends: Friends.create(
          {
            jane: Person.create(
              {
                name: "Jane",
                dog: Dog.create({ name: "Rex" }, onlyBob), // Jane dog is inaccessible
              },
              group,
            ),
            alice: Person.create(
              { name: "Alice", dog: Dog.create({ name: "Giggino" }, group) },
              group,
            ),
          },
          group,
        ),
      },
      group,
    );

    const user = await User.load(map.id, {
      resolve: { friends: { $each: { dog: true, $onError: null } } },
      loadAs: alice,
    });

    assert(user);

    expect(user.friends.jane).toBeNull(); // jane is null because her dog is inaccessible
    expect(user.friends.alice?.dog).not.toBeNull(); // alice is not null because we have read access to her and her dog
  });

  test("unaccessible list element with $onError and $each with depth", async () => {
    const Person = co.map({
      name: z.string(),
      get friends(): z.ZodOptional<typeof Friends> {
        return z.optional(Friends);
      },
    });
    const Friends: CoListSchema<typeof Person> = co.list(Person); // TODO: annoying that we have to annotate

    const list = Friends.create(
      [
        Person.create(
          {
            name: "Jane",
            friends: Friends.create(
              [Person.create({ name: "Bob" }, onlyBob)],
              group,
            ),
          },
          group,
        ),
        Person.create(
          {
            name: "Alice",
            friends: Friends.create(
              [Person.create({ name: "Bob" }, group)],
              group,
            ),
          },
          group,
        ),
      ],
      group,
    );

    // The error List -> Jane -> Bob should be propagated to the list element Jane
    // and we should have [null, Alice]
    const listOnAlice = await Friends.load(list.id, {
      resolve: { $each: { friends: { $each: true }, $onError: null } },
      loadAs: alice,
    });

    assert(listOnAlice, "listOnAlice is null");

    expect(listOnAlice[0]).toBeNull();
    expect(listOnAlice[1]).not.toBeNull();
    expect(listOnAlice[1]?.name).toBe("Alice");
    expect(listOnAlice[1]?.friends).not.toBeNull();
    expect(listOnAlice[1]?.friends?.[0]?.name).toBe("Bob");
    expect(listOnAlice).toHaveLength(2);
  });

  test("unaccessible record element with $onError", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const Friend = co.record(z.string(), Person);

    const map = Friend.create(
      {
        jane: Person.create({ name: "Jane" }, onlyBob),
        alice: Person.create({ name: "Alice" }, group),
      },
      group,
    );

    const friendsOnAlice = await Friend.load(map.id, {
      resolve: { $each: { $onError: null } },
      loadAs: alice,
    });

    assert(friendsOnAlice, "friendsOnAlice is null");

    expect(friendsOnAlice.jane).toBeNull();
    expect(friendsOnAlice.alice).not.toBeNull();
  });

  test("unaccessible ref catched with $onError", async () => {
    const Dog = co.map({
      name: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    const Friends = co.record(z.string(), Person);

    const User = co.map({
      name: z.string(),
      friends: Friends,
    });

    const map = User.create(
      {
        name: "John",
        friends: Friends.create(
          {
            jane: Person.create(
              {
                name: "Jane",
                dog: Dog.create({ name: "Rex" }, onlyBob), // Jane dog is inaccessible
              },
              group,
            ),
            alice: Person.create(
              { name: "Alice", dog: Dog.create({ name: "Giggino" }, group) },
              group,
            ),
          },
          group,
        ),
      },
      group,
    );

    const user = await User.load(map.id, {
      resolve: { friends: { $each: { dog: { $onError: null } } } },
      loadAs: alice,
    });

    assert(user);

    expect(user.friends.jane?.dog).toBeNull(); // jane is null because her dog is inaccessible
    expect(user.friends.alice?.dog?.name).toBe("Giggino"); // alice is not null because we have read access to her and her dog
  });

  test("using $onError on the resolve root", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const map = Person.create({ name: "John" }, onlyBob);
    const user = await Person.load(map.id, {
      resolve: { $onError: null },
      loadAs: alice,
    });

    expect(user).toBeNull();
  });
});

test("doesn't break on Map.Record key deletion when the key is referenced in the depth", async () => {
  const JazzProfile = co.map({
    name: z.string(),
    firstName: z.string(),
  });

  const JazzySnapStore = co.record(z.string(), JazzProfile);

  const snapStore = JazzySnapStore.create({
    profile1: JazzProfile.create({ name: "John", firstName: "John" }),
    profile2: JazzProfile.create({ name: "John", firstName: "John" }),
  });

  const spy = vi.fn();
  const unsub = snapStore.subscribe(
    { resolve: { profile1: true, profile2: true } },
    spy,
  );

  await waitFor(() => expect(spy).toHaveBeenCalled());

  spy.mockClear();
  delete snapStore.profile1;

  expect(Object.keys(snapStore)).toEqual(["profile2"]);

  unsub();

  await expect(
    snapStore.ensureLoaded({
      resolve: {
        profile1: true,
      },
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + snapStore.id);
});

test("throw when calling ensureLoaded on a ref that's required but missing", async () => {
  const JazzProfile = co.map({
    name: z.string(),
    firstName: z.string(),
  });

  const JazzRoot = co.map({
    profile: JazzProfile,
  });

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
      resolve: { profile: true },
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + root.id);
});

test("throw when calling ensureLoaded on a ref that is not defined in the schema", async () => {
  const JazzRoot = co.map({});

  const me = await Account.create({
    creationProps: { name: "Tester McTesterson" },
    crypto: Crypto,
  });

  const root = JazzRoot.create({}, { owner: me });

  await expect(
    root.ensureLoaded({
      // @ts-expect-error missing required ref
      resolve: { profile: true },
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + root.id);
});

test("should not throw when calling ensureLoaded a record with a deleted ref", async () => {
  const JazzProfile = co.map({
    name: z.string(),
    firstName: z.string(),
  });

  const JazzySnapStore = co.record(z.string(), JazzProfile);

  const me = await Account.create({
    creationProps: { name: "Tester McTesterson" },
    crypto: Crypto,
  });

  const root = JazzySnapStore.create(
    {
      profile: JazzProfile.create({ name: "John", firstName: "John" }, me),
    },
    me,
  );

  let value: any;
  let unsub = root.subscribe({ resolve: { $each: true } }, (v) => {
    value = v;
  });

  await waitFor(() => expect(value.profile).toBeDefined());

  delete root.profile;

  await waitFor(() => expect(value.profile).toBeUndefined());

  unsub();

  value = undefined;
  unsub = root.subscribe({ resolve: { $each: true } }, (v) => {
    value = v;
  });

  await waitFor(() => expect(value).toBeDefined());

  expect(value.profile).toBeUndefined();

  unsub();
});
