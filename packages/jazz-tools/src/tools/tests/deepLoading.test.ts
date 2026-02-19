import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  assert,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from "vitest";
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
  Loaded,
  MaybeLoaded,
  Settled,
  co,
  MockSessionProvider,
  CoValueLoadingState,
  CoValueErrorState,
} from "../internal.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";
import { assertLoaded, waitFor } from "./utils.js";
import { setCustomErrorReporter } from "../config.js";

const Crypto = await WasmCrypto.create();
const { connectedPeers } = cojsonInternals;
const randomSessionProvider = new MockSessionProvider();
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
  optionalRef: co.optional(InnermostMap),
});

let lastError: Error | undefined;

beforeEach(() => {
  lastError = undefined;
  setCustomErrorReporter((error) => {
    lastError = error;
  });
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
  me.$jazz.localNode.syncManager.addPeer(secondPeer);
  const { account: meOnSecondPeer } =
    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.$jazz.id,
        secret: me.$jazz.localNode.getCurrentAgent().agentSecret,
      },
      sessionProvider: randomSessionProvider,
      peers: [initialAsPeer],
      crypto: Crypto,
      asActiveAccount: true,
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
    const map1 = await TestMap.load(map.$jazz.id, { loadAs: meOnSecondPeer });

    type ExpectedType = MaybeLoaded<Loaded<typeof TestMap>>;
    function matches(value: ExpectedType) {
      return value;
    }
    matches(map1);

    assertLoaded(map1);

    expect(map1.list.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);
  });

  test("load with resolve { list: true }", async () => {
    const map2 = await TestMap.load(map.$jazz.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: true },
    });
    type ExpectedType = MaybeLoaded<
      Loaded<typeof TestMap> & {
        readonly list: Loaded<typeof TestList>;
      }
    >;
    function matches(value: ExpectedType) {
      return value;
    }
    matches(map2);
    assertLoaded(map2);
    assertLoaded(map2.list);
    expect(map2.list[0]?.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);
  });

  test("load with resolve { list: { $each: true } }", async () => {
    const map3 = await TestMap.load(map.$jazz.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: true } },
    });
    type ExpectedType = MaybeLoaded<
      Loaded<typeof TestMap> & {
        readonly list: Loaded<typeof TestList> &
          ReadonlyArray<Loaded<typeof InnerMap>>;
      }
    >;
    function matches(value: ExpectedType) {
      return value;
    }
    matches(map3);
    assertLoaded(map3);
    assert(map3.list[0]);
    expect(map3.list[0].stream.$jazz.loadingState).toBe(
      CoValueLoadingState.LOADING,
    );
  });

  test("load with resolve { optionalRef: true }", async () => {
    const map3a = await TestMap.load(map.$jazz.id, {
      loadAs: meOnSecondPeer,
      resolve: { optionalRef: true } as const,
    });
    type ExpectedType = MaybeLoaded<
      Loaded<typeof TestMap> & {
        readonly optionalRef: Loaded<typeof InnermostMap> | undefined;
      }
    >;
    function matches(value: ExpectedType) {
      return value;
    }
    matches(map3a);
    assertLoaded(map3a);
  });

  test("load with resolve { list: { $each: { stream: true } } }", async () => {
    const map4 = await TestMap.load(map.$jazz.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: { stream: true } } },
    });
    type ExpectedType = MaybeLoaded<
      Loaded<typeof TestMap> & {
        readonly list: Loaded<typeof TestList> &
          ReadonlyArray<
            Loaded<typeof InnerMap> & {
              readonly stream: Loaded<typeof TestFeed>;
            }
          >;
      }
    >;
    function matches(value: ExpectedType) {
      return value;
    }
    matches(map4);
    assertLoaded(map4);
    expect(map4.list[0]?.stream).toBeTruthy();
    expect(map4.list[0]?.stream?.perAccount[me.$jazz.id]).toBeTruthy();
    expect(map4.list[0]?.stream?.byMe?.value.$jazz.loadingState).toBe(
      CoValueLoadingState.LOADING,
    );
  });

  test("load with resolve { list: { $each: { stream: { $each: true } } } }", async () => {
    const map5 = await TestMap.load(map.$jazz.id, {
      loadAs: meOnSecondPeer,
      resolve: { list: { $each: { stream: { $each: true } } } },
    });
    type ExpectedMap5 = MaybeLoaded<
      Loaded<typeof TestMap> & {
        readonly list: Loaded<typeof TestList> &
          ReadonlyArray<
            Loaded<typeof InnerMap> & {
              readonly stream: Loaded<typeof TestFeed> & {
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
            }
          >;
      }
    >;
    function matches(value: ExpectedMap5) {
      return value;
    }
    matches(map5);
    assertLoaded(map5);

    expect(map5.list[0]?.stream?.perAccount[me.$jazz.id]?.value).toBeTruthy();
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
      account.$jazz.set("profile", {
        name: creationProps.name,
        stream: TestFeed.create([], account),
      });
      account.$jazz.set("root", { list: [] });
    }

    const accountLoaded = await account.$jazz.ensureLoaded({
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

  const meLoaded = await me.$jazz.ensureLoaded({
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

  me.$jazz.localNode.syncManager.addPeer(secondPeer);
  const { account: meOnSecondPeer } =
    await createJazzContextFromExistingCredentials({
      credentials: {
        accountID: me.$jazz.id,
        secret: me.$jazz.localNode.getCurrentAgent().agentSecret,
      },
      sessionProvider: randomSessionProvider,
      peers: [initialAsPeer],
      crypto: Crypto,
      asActiveAccount: true,
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

  const recordLoaded = await RecordLike.load(record.$jazz.id, {
    loadAs: meOnSecondPeer,
    resolve: {
      $each: { list: { $each: true } },
    },
  });
  expectTypeOf(recordLoaded).branded.toEqualTypeOf<
    Settled<
      Loaded<typeof RecordLike> & {
        readonly [key: string]: Loaded<typeof TestMap> & {
          readonly list: Loaded<typeof TestList> &
            ReadonlyArray<Loaded<typeof InnerMap>>;
        };
      }
    >
  >();
  assertLoaded(recordLoaded);
  expect(recordLoaded.key1?.list).not.toBe(null);
  expect(recordLoaded.key1?.list).toBeTruthy();
  expect(recordLoaded.key2?.list).not.toBe(null);
  expect(recordLoaded.key2?.list).toBeTruthy();
});

test("The resolve type doesn't accept extra keys, but the load resolves anyway", async () => {
  const me = await CustomAccount.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const meLoaded = await me.$jazz.ensureLoaded({
    resolve: {
      // @ts-expect-error
      profile: { stream: true, extraKey: true },
      // @ts-expect-error
      root: { list: true, extraKey: true },
    },
  });

  await me.$jazz.ensureLoaded({
    resolve: {
      // @ts-expect-error
      root: { list: { $each: true, extraKey: true } },
    },
  });

  await me.$jazz.ensureLoaded({
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
});

test("The resolve type accepts keys from optional fields", async () => {
  const Person = co.map({
    name: z.string(),
  });
  const Dog = co.map({
    type: z.literal("dog"),
    owner: Person.optional(),
  });
  const Pets = co.list(Dog);

  const pets = await Pets.create([
    Dog.create({ type: "dog", owner: Person.create({ name: "Rex" }) }),
  ]);

  await pets.$jazz.ensureLoaded({
    resolve: {
      $each: { owner: true },
    },
  });

  expect(pets[0]?.owner?.name).toEqual("Rex");
});

test("The resolve type accepts keys from discriminated unions", async () => {
  const Person = co.map({
    name: z.string(),
  });
  const Dog = co.map({
    type: z.literal("dog"),
    owner: Person,
  });
  const Cat = co.map({
    type: z.literal("cat"),
  });
  const Pet = co.discriminatedUnion("type", [Dog, Cat]);
  const Pets = co.list(Pet);

  const pets = Pets.create([
    Dog.create({ type: "dog", owner: Person.create({ name: "Rex" }) }),
    Cat.create({ type: "cat" }),
  ]);

  await pets.$jazz.ensureLoaded({
    resolve: { $each: { owner: true } },
  });

  expect(pets).toBeTruthy();

  for (const pet of pets) {
    if (pet.type === "dog") {
      expect(pet.owner?.name).toEqual("Rex");
    } else {
      // `in` returns true because the discriminated union injects a dummy
      // field descriptor for "owner" on Cat. Use value check instead.
      expect("owner" in pet).toEqual(true);
      // @ts-expect-error - this should still not appear in the types
      expect(pet.owner).toBeUndefined();
    }
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

  await alice.$jazz.waitForAllCoValuesSync();

  const onlyBob = bob;
  const group = Group.create(bob);

  group.addMember(alice, "reader");

  test("unaccessible root", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const map = TestMap.create({ list: TestList.create([], group) }, onlyBob);

    const mapOnAlice = await TestMap.load(map.$jazz.id, { loadAs: alice });

    expect(mapOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );

    expect(lastError?.message).toBe(
      `Jazz Authorization Error: The current user (${alice.$jazz.id}) is not authorized to access ${map.$jazz.id}`,
    );
  });

  test("unaccessible list", async () => {
    const innerList = TestList.create([], onlyBob);
    const map = TestMap.create({ list: innerList }, group);

    const mapOnAlice = await TestMap.load(map.$jazz.id, { loadAs: alice });
    expect(mapOnAlice).toBeTruthy();

    const mapWithListOnAlice = await TestMap.load(map.$jazz.id, {
      resolve: { list: true },
      loadAs: alice,
    });

    expect(mapWithListOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );

    expect(lastError?.message).toBe(
      `Jazz Authorization Error: The current user (${alice.$jazz.id}) is not authorized to access ${innerList.$jazz.id}. Subscription starts from ${map.$jazz.id} and the value is on path list`,
    );
  });

  test("unaccessible list element", async () => {
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

    const mapOnAlice = await TestMap.load(map.$jazz.id, {
      resolve: { list: { $each: true } },
      loadAs: alice,
    });

    expect(mapOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );

    expect(lastError?.message).toBe(
      `Jazz Authorization Error: The current user (${alice.$jazz.id}) is not authorized to access ${map.list[0]!.$jazz.id}. Subscription starts from ${map.$jazz.id} and the value is on path list.0`,
    );
  });

  test("unaccessible optional element", async () => {
    const map = TestMap.create(
      {
        list: TestList.create([], group),
        optionalRef: InnermostMap.create({ value: "hello" }, onlyBob),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.$jazz.id, {
      loadAs: alice,
      resolve: { optionalRef: true } as const,
    });
    expect(mapOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
    expect(lastError?.message).toBe(
      `Jazz Authorization Error: The current user (${alice.$jazz.id}) is not authorized to access ${map.optionalRef!.$jazz.id}. Subscription starts from ${map.$jazz.id} and the value is on path optionalRef`,
    );
  });

  test("unaccessible optional element via autoload", async () => {
    const map = TestMap.create(
      {
        list: TestList.create([], group),
        optionalRef: InnermostMap.create({ value: "hello" }, onlyBob),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.$jazz.id, {
      loadAs: alice,
      resolve: { list: true } as const,
    });

    assertLoaded(mapOnAlice);

    const result: MaybeLoaded<Loaded<typeof InnermostMap>> | undefined =
      await new Promise((resolve) => {
        const unsub = mapOnAlice.$jazz.subscribe((value) => {
          resolve(value.optionalRef);
          unsub();
        });
      });

    expect(result?.$jazz.loadingState).toBe(CoValueLoadingState.UNAUTHORIZED);
    expect(lastError?.message).toBe(
      `Jazz Authorization Error: The current user (${alice.$jazz.id}) is not authorized to access ${map.optionalRef!.$jazz.id}`,
    );
  });

  test("unaccessible stream", async () => {
    const stream = TestFeed.create([], onlyBob);
    const map = TestMap.create(
      {
        list: TestList.create(
          [
            InnerMap.create(
              {
                stream,
              },
              group,
            ),
          ],
          group,
        ),
      },
      group,
    );

    const mapOnAlice = await TestMap.load(map.$jazz.id, {
      resolve: { list: { $each: { stream: true } } },
      loadAs: alice,
    });

    expect(mapOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );

    expect(lastError?.message).toBe(
      `Jazz Authorization Error: The current user (${alice.$jazz.id}) is not authorized to access ${stream.$jazz.id}. Subscription starts from ${map.$jazz.id} and the value is on path list.0.stream`,
    );
  });

  test("unaccessible stream element", async () => {
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

    const mapOnAlice = await TestMap.load(map.$jazz.id, {
      resolve: { list: { $each: { stream: { $each: true } } } },
      loadAs: alice,
    });

    expect(mapOnAlice.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );

    expect(lastError?.message).toBe(
      `Jazz Authorization Error: The current user (${alice.$jazz.id}) is not authorized to access ${value.$jazz.id}. Subscription starts from ${map.$jazz.id} and the value is on path list.0.stream.${value.$jazz.id}`,
    );
  });

  test("setting undefined via proxy", async () => {
    const Lv3 = co.map({
      string: z.string(),
    });

    const Lv2 = co.map({
      lv3: co.optional(Lv3),
    });

    const Lv1 = co.map({
      lv2: Lv2,
    });

    const map = Lv1.create(
      { lv2: Lv2.create({ lv3: Lv3.create({ string: "hello" }, bob) }, bob) },
      bob,
    );

    map.lv2!.$jazz.set("lv3", undefined);

    const loadedMap = await Lv1.load(map.$jazz.id, {
      resolve: { lv2: { lv3: true } },
      loadAs: bob,
    });

    expect(loadedMap?.$jazz.id).toBe(map.$jazz.id);
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

    const friendsOnAlice = await Friends.load(map.$jazz.id, {
      resolve: { $each: { $onError: "catch" } },
      loadAs: alice,
    });

    assertLoaded(friendsOnAlice);

    expect(friendsOnAlice.jane?.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
    assert(friendsOnAlice.alice);
    assertLoaded(friendsOnAlice.alice);
    expect(friendsOnAlice.alice.name).toBe("Alice");
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

    const user = await User.load(map.$jazz.id, {
      resolve: { friends: { $each: { $onError: "catch" } } },
      loadAs: alice,
    });

    assertLoaded(user);

    expect(user.friends.jane?.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
    assert(user.friends.alice);
    assertLoaded(user.friends.alice);
    expect(user.friends.alice.name).toBe("Alice");
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

    const user = await User.load(map.$jazz.id, {
      resolve: { friends: { $each: { dog: true, $onError: "catch" } } },
      loadAs: alice,
    });

    assertLoaded(user);

    // jane is not loaded because her dog is inaccessible
    expect(user.friends.jane?.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
    // alice is loaded because we have read access to her and her dog
    assert(user.friends.alice);
    assertLoaded(user.friends.alice);
    expect(user.friends.alice.dog.name).toBe("Giggino");
  });

  test("unaccessible list element with $onError and $each with depth", async () => {
    const Person = co.map({
      name: z.string(),
      get friends(): co.Optional<typeof Friends> {
        return co.optional(Friends);
      },
    });
    const Friends = co.list(Person);

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
    const listOnAlice = await Friends.load(list.$jazz.id, {
      resolve: { $each: { friends: { $each: true }, $onError: "catch" } },
      loadAs: alice,
    });

    assertLoaded(listOnAlice);

    expect(listOnAlice[0]?.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
    assert(listOnAlice[1]);
    assertLoaded(listOnAlice[1]);
    expect(listOnAlice[1].name).toBe("Alice");
    expect(listOnAlice[1].friends?.[0]?.name).toBe("Bob");
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

    const friendsOnAlice = await Friend.load(map.$jazz.id, {
      resolve: { $each: { $onError: "catch" } },
      loadAs: alice,
    });

    assertLoaded(friendsOnAlice);

    expect(friendsOnAlice.jane?.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
    assert(friendsOnAlice.alice);
    assertLoaded(friendsOnAlice.alice);
    expect(friendsOnAlice.alice.name).toBe("Alice");
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

    const user = await User.load(map.$jazz.id, {
      resolve: { friends: { $each: { dog: { $onError: "catch" } } } },
      loadAs: alice,
    });

    assertLoaded(user);

    // jane's dog is not loaded because it is inaccessible
    expect(user.friends.jane?.dog.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
    // we have read access to alice and her dog
    const aliceDog = user.friends.alice?.dog;
    assert(aliceDog);
    assertLoaded(aliceDog);
    expect(aliceDog.name).toBe("Giggino");
  });

  test("using $onError on the resolve root", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const map = Person.create({ name: "John" }, onlyBob);
    const user = await Person.load(map.$jazz.id, {
      resolve: { $onError: "catch" },
      loadAs: alice,
    });

    expect(user.$jazz.loadingState).toBe(CoValueLoadingState.UNAUTHORIZED);
  });

  test("using $onError on a plain text value", async () => {
    const Person = co.map({
      name: co.plainText(),
    });

    const person = Person.create(
      { name: Person.shape.name.create("John", onlyBob) },
      group,
    );

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: { name: { $onError: "catch" } },
      loadAs: alice,
    });

    expect(loadedPerson.$isLoaded).toBe(true);
    assertLoaded(loadedPerson);
    expect(loadedPerson.name.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });

  test("using $onError on a file stream", async () => {
    const Person = co.map({
      avatar: co.fileStream(),
    });

    const person = Person.create(
      { avatar: Person.shape.avatar.create(onlyBob) },
      group,
    );

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: { avatar: { $onError: "catch" } },
      loadAs: alice,
    });

    expect(loadedPerson.$isLoaded).toBe(true);
    assertLoaded(loadedPerson);
    expect(loadedPerson.avatar.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
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
  const unsub = snapStore.$jazz.subscribe(
    { resolve: { profile1: true, profile2: true } },
    spy,
  );

  await waitFor(() => expect(spy).toHaveBeenCalled());

  spy.mockClear();
  snapStore.$jazz.delete("profile1");

  expect(Object.keys(snapStore)).toEqual(["profile2"]);

  unsub();

  await expect(
    snapStore.$jazz.ensureLoaded({
      resolve: {
        profile1: true,
      },
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + snapStore.$jazz.id);
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
    { owner: me, validation: "loose" },
  );

  await expect(
    root.$jazz.ensureLoaded({
      resolve: { profile: true },
    }),
  ).rejects.toThrow("Failed to deeply load CoValue " + root.$jazz.id);
});

test("returns the value when calling ensureLoaded on a ref that is not defined in the schema", async () => {
  const JazzRoot = co.map({});

  const me = await Account.create({
    creationProps: { name: "Tester McTesterson" },
    crypto: Crypto,
  });

  const root = JazzRoot.create({}, { owner: me });

  const loadedRoot = await JazzRoot.load(root.$jazz.id, {
    // @ts-expect-error missing required ref
    resolve: { profile: true },
    loadAs: me,
  });

  expect(loadedRoot.$jazz.loadingState).toBe(CoValueLoadingState.LOADED);
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
  let unsub = root.$jazz.subscribe({ resolve: { $each: true } }, (v) => {
    value = v;
  });

  await waitFor(() => expect(value.profile).toBeDefined());

  root.$jazz.delete("profile");

  await waitFor(() => expect(value.profile).toBeUndefined());

  unsub();

  value = undefined;
  unsub = root.$jazz.subscribe({ resolve: { $each: true } }, (v) => {
    value = v;
  });

  await waitFor(() => expect(value).toBeDefined());

  expect(value.profile).toBeUndefined();

  unsub();
});

test("should not throw when calling ensureLoaded a record with a non-existent key if there's a catch block", async () => {
  const Person = co.record(
    z.string(),
    co.map({
      name: z.string(),
      breed: z.string(),
    }),
  );

  const person = Person.create({});

  const loadedPerson = await person.$jazz.ensureLoaded({
    resolve: {
      ["pet1"]: {
        $onError: "catch",
      },
    },
  });

  expect(loadedPerson.pet1).toBeUndefined();
});

test("should load a record with a non-existent key if there's a catch block", async () => {
  const Person = co.record(
    z.string(),
    co.map({
      name: z.string(),
      breed: z.string(),
    }),
  );

  const person = Person.create({});

  const loadedPerson = await Person.load(person.$jazz.id, {
    resolve: {
      pet1: {
        $onError: "catch",
      },
    },
  });

  assertLoaded(loadedPerson);
  expect(loadedPerson.pet1).toBeUndefined();
});

// This was a regression that ocurred when we migrated `DeeplyLoaded` to use explicit loading states.
// Keeping this test to prevent it from happening again.
test("deep loaded CoList nested inside another CoValue can be iterated over", async () => {
  const TestMap = co.map({ list: co.list(z.number()) });

  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const map = TestMap.create({ list: [1, 2, 3] }, { owner: me });

  const loadedMap = await TestMap.load(map.$jazz.id, {
    resolve: {
      list: true,
    },
    loadAs: me,
  });
  assertLoaded(loadedMap);

  const list = loadedMap.list;

  let expectedValue = 1;
  // @ts-expect-error - https://github.com/microsoft/TypeScript/issues/62621
  for (const item of list) {
    expect(item).toEqual(expectedValue);
    expectedValue++;
  }
});

describe("$isLoaded", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const map = TestMap.create({ list: [] }, { owner: me });

  test("$isLoaded narrows a maybe-loaded CoValue to a loaded CoValue", async () => {
    const maybeLoadedMap = await TestMap.load(map.$jazz.id, {
      loadAs: me,
    });

    expect(maybeLoadedMap.$isLoaded).toBe(true);
    if (maybeLoadedMap.$isLoaded) {
      expect(maybeLoadedMap.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADED,
      );
      expect(maybeLoadedMap.$jazz.id).toBe(map.$jazz.id);
      expect(maybeLoadedMap.list).toEqual([]);
    } else {
      expectTypeOf(
        maybeLoadedMap.$jazz.loadingState,
      ).toEqualTypeOf<CoValueErrorState>();
    }
  });

  test("$isLoaded narrows a maybe-loaded CoValue to a not loaded CoValue", async () => {
    const otherAccount = await Account.create({
      creationProps: { name: "Other Account" },
      crypto: Crypto,
    });
    const unloadedMap = await TestMap.load(map.$jazz.id, {
      loadAs: otherAccount,
    });

    expect(unloadedMap.$isLoaded).toBe(false);
    if (!unloadedMap.$isLoaded) {
      expect(unloadedMap.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
      expect(unloadedMap.$jazz.id).toBe(map.$jazz.id);
      // @ts-expect-error - list should not be accessible on NotLoaded
      unloadedMap.list;
    } else {
      expectTypeOf(unloadedMap.$jazz.loadingState).toEqualTypeOf<
        typeof CoValueLoadingState.LOADED
      >();
    }
  });
});
