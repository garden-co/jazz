type FixtureNativeRelay = {
  getAbiVersion(): number;
  execute(commandBase64: string): Promise<string>;
  installForegroundRuntime?(): void;
};

const foregroundRuntimeGlobal = '__jazzNativeForegroundRuntimeV1';

type NativeForegroundCommand =
  | 'probe'
  | 'tick'
  | 'close'
  | { type: 'prepareQuery'; query: Uint8Array }
  | { type: 'all'; query: number }
  | { type: 'poll'; operation: number }
  | { type: 'cancel'; operation: number }
  | { type: 'beginTransaction'; kind: 'mergeable' | 'exclusive' }
  | {
      type: 'insert';
      transaction: number;
      table: string;
      cells: Uint8Array;
      rowId?: Uint8Array;
    }
  | {
      type: 'update';
      transaction: number;
      table: string;
      rowId: Uint8Array;
      patch: Uint8Array;
    }
  | {
      type: 'upsert';
      transaction: number;
      table: string;
      rowId: Uint8Array;
      cells: Uint8Array;
    }
  | { type: 'delete'; transaction: number; table: string; rowId: Uint8Array }
  | { type: 'commitTransaction'; transaction: number }
  | { type: 'rollbackTransaction'; transaction: number };

type RelayExports = {
  executeNativeRelayCommand(command: string): Promise<string>;
  installNativeForegroundRuntime(): {
    abiVersion: number;
    openAttached(capability: Uint8Array): {
      execute(command: Uint8Array): Uint8Array;
      tick(): void;
      close(): boolean;
    };
  };
  encodeNativeForegroundCommand(command: NativeForegroundCommand): Uint8Array;
  decodeNativeForegroundResponse(bytes: Uint8Array): unknown;
};

function foregroundFixture() {
  return {
    execute: jest.fn(() => Uint8Array.of(1)),
    tick: jest.fn(),
    close: jest.fn(() => true),
  };
}

function loadRelay(nativeRelay: FixtureNativeRelay | null) {
  jest.resetModules();
  jest.doMock('../NativeJazzRelay', () => ({
    __esModule: true,
    default: nativeRelay,
  }));
  // Each case supplies the native boundary before importing the wrapper. That
  // is the same one-time lookup Metro performs for an installed native build.
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  return require('../relay') as RelayExports;
}

afterEach(() => {
  delete (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal];
  jest.resetModules();
  jest.dontMock('../NativeJazzRelay');
});

it('tells Expo Go and old development builds that a native artifact is required', async () => {
  const relay = loadRelay(null);

  await expect(relay.executeNativeRelayCommand('AA==')).rejects.toThrow(
    'install a matching native development or release build containing the Jazz relay artifact. Expo Go never includes it.'
  );
});

it('rejects an installed native build with an incompatible ABI before executing a command', async () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 2,
    execute: jest.fn(),
  };
  const relay = loadRelay(nativeRelay);

  await expect(relay.executeNativeRelayCommand('AA==')).rejects.toThrow(
    'Jazz native relay ABI 2 is incompatible with JavaScript ABI 7..=7; install a matching native development or release build.'
  );
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it('rejects the source-only ABI fallback before executing a command', async () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 0,
    execute: jest.fn(),
  };
  const relay = loadRelay(nativeRelay);

  await expect(relay.executeNativeRelayCommand('AA==')).rejects.toThrow(
    'Jazz native relay is unavailable: this native build contains only the source fallback (ABI 0), not the Jazz relay artifact. Install a matching native development or release build.'
  );
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it('forwards opaque commands only after the embedded relay ABI matches', async () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 7,
    execute: jest.fn().mockResolvedValue('AQ=='),
  };
  const relay = loadRelay(nativeRelay);

  await expect(relay.executeNativeRelayCommand('AA==')).resolves.toBe('AQ==');
  expect(nativeRelay.execute).toHaveBeenCalledWith('AA==');
});

it('requires a matching JSI foreground installer instead of attempting browser WASM', () => {
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 7,
    execute: jest.fn(),
  };
  const relay = loadRelay(nativeRelay);

  expect(() => relay.installNativeForegroundRuntime()).toThrow(
    'Jazz native foreground runtime is unavailable: install a matching native development or release build containing the JSI foreground engine. Expo Go never includes it.'
  );
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it('accepts only the matching capability-only JSI foreground factory', () => {
  const foreground = foregroundFixture();
  const openAttached = jest.fn(() => foreground);
  const installForegroundRuntime = jest.fn(() => {
    (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = {
      abiVersion: 7,
      openAttached,
    };
  });
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 7,
    execute: jest.fn(),
    installForegroundRuntime,
  };
  const relay = loadRelay(nativeRelay);

  const factory = relay.installNativeForegroundRuntime();

  expect(installForegroundRuntime).toHaveBeenCalledTimes(1);
  expect(factory.abiVersion).toBe(7);
  const capability = new Uint8Array(32);
  expect(factory.openAttached(capability)).toMatchObject({
    execute: expect.any(Function),
    tick: expect.any(Function),
    close: expect.any(Function),
  });
  expect(openAttached).toHaveBeenCalledWith(capability);
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it('rejects a missing, malformed, ABI-incompatible, or stale JSI foreground factory after installation', () => {
  for (const factory of [
    undefined,
    {},
    { abiVersion: 2, openAttached: () => foregroundFixture() },
  ]) {
    const nativeRelay: FixtureNativeRelay = {
      getAbiVersion: () => 7,
      execute: jest.fn(),
      installForegroundRuntime: () => {
        if (factory !== undefined)
          (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] =
            factory;
      },
    };
    const relay = loadRelay(nativeRelay);

    expect(() => relay.installNativeForegroundRuntime()).toThrow(
      'Jazz native foreground runtime installation failed: the native build did not install a compatible JSI foreground engine. Install a matching native development or release build.'
    );
    delete (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal];
  }

  // Plant a same-ABI HostObject left by a preceding bridge. A native installer
  // that does not replace it must not make that object callable in this
  // runtime. This would have passed before the global was cleared first.
  const staleOpenAttached = jest.fn(() => foregroundFixture());
  (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = {
    abiVersion: 7,
    openAttached: staleOpenAttached,
  };
  const staleNativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 7,
    execute: jest.fn(),
    installForegroundRuntime: jest.fn(),
  };
  const staleRelay = loadRelay(staleNativeRelay);
  expect(() => staleRelay.installNativeForegroundRuntime()).toThrow(
    'Jazz native foreground runtime installation failed: the native build did not install a compatible JSI foreground engine. Install a matching native development or release build.'
  );
  expect(staleNativeRelay.installForegroundRuntime).toHaveBeenCalledTimes(1);
  expect(staleOpenAttached).not.toHaveBeenCalled();
});

it('keeps malformed capability input out of the JSI foreground factory', () => {
  const openAttached = jest.fn(() => foregroundFixture());
  const nativeRelay: FixtureNativeRelay = {
    getAbiVersion: () => 7,
    execute: jest.fn(),
    installForegroundRuntime: () => {
      (globalThis as Record<string, unknown>)[foregroundRuntimeGlobal] = {
        abiVersion: 7,
        openAttached,
      };
    },
  };
  const relay = loadRelay(nativeRelay);
  const factory = relay.installNativeForegroundRuntime();

  for (const malformed of [
    new Uint8Array(31),
    new Uint8Array(33),
    [] as unknown as Uint8Array,
  ]) {
    expect(() => factory.openAttached(malformed)).toThrow(
      'Jazz native foreground runtime requires a 32-byte admitted capability'
    );
  }
  expect(openAttached).not.toHaveBeenCalled();

  const admitted = new Uint8Array(32);
  factory.openAttached(admitted);
  expect(openAttached).toHaveBeenCalledWith(admitted);
});

it('uses a compact versioned byte vocabulary for the initial foreground NativeDb slice', () => {
  const relay = loadRelay({ getAbiVersion: () => 7, execute: jest.fn() });

  expect(relay.encodeNativeForegroundCommand('probe')).toEqual(
    Uint8Array.of(0)
  );
  expect(relay.encodeNativeForegroundCommand('tick')).toEqual(Uint8Array.of(1));
  expect(relay.encodeNativeForegroundCommand('close')).toEqual(
    Uint8Array.of(7)
  );
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'prepareQuery',
      query: Uint8Array.of(1, 2),
    })
  ).toEqual(Uint8Array.of(2, 2, 1, 2));
  expect(
    relay.encodeNativeForegroundCommand({ type: 'all', query: 129 })
  ).toEqual(Uint8Array.of(3, 129, 1));
  expect(
    relay.encodeNativeForegroundCommand({ type: 'poll', operation: 129 })
  ).toEqual(Uint8Array.of(8, 129, 1));
  expect(
    relay.encodeNativeForegroundCommand({ type: 'cancel', operation: 129 })
  ).toEqual(Uint8Array.of(9, 129, 1));
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'beginTransaction',
      kind: 'mergeable',
    })
  ).toEqual(Uint8Array.of(10, 0));
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'beginTransaction',
      kind: 'exclusive',
    })
  ).toEqual(Uint8Array.of(10, 1));
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'insert',
      transaction: 3,
      table: 'todos',
      cells: Uint8Array.of(1, 2),
      rowId: undefined,
    })
  ).toEqual(Uint8Array.of(11, 3, 5, 116, 111, 100, 111, 115, 2, 1, 2, 0));
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'update',
      transaction: 3,
      table: 'todos',
      rowId: new Uint8Array(16).fill(7),
      patch: Uint8Array.of(9),
    })
  ).toEqual(
    Uint8Array.from([
      12,
      3,
      5,
      116,
      111,
      100,
      111,
      115,
      ...new Uint8Array(16).fill(7),
      1,
      9,
    ])
  );
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'upsert',
      transaction: 3,
      table: 'todos',
      rowId: new Uint8Array(16).fill(8),
      cells: Uint8Array.of(9),
    })
  ).toEqual(
    Uint8Array.from([
      13,
      3,
      5,
      116,
      111,
      100,
      111,
      115,
      ...new Uint8Array(16).fill(8),
      1,
      9,
    ])
  );
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'delete',
      transaction: 3,
      table: 'todos',
      rowId: new Uint8Array(16).fill(9),
    })
  ).toEqual(
    Uint8Array.from([
      14,
      3,
      5,
      116,
      111,
      100,
      111,
      115,
      ...new Uint8Array(16).fill(9),
    ])
  );
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'commitTransaction',
      transaction: 129,
    })
  ).toEqual(Uint8Array.of(15, 129, 1));
  expect(
    relay.encodeNativeForegroundCommand({
      type: 'rollbackTransaction',
      transaction: 129,
    })
  ).toEqual(Uint8Array.of(16, 129, 1));
  expect(() =>
    relay.encodeNativeForegroundCommand({
      type: 'beginTransaction',
      kind: 'neither',
    } as unknown as NativeForegroundCommand)
  ).toThrow(
    'Jazz native foreground transaction kind must be mergeable or exclusive'
  );
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(0, 7))).toEqual({
    type: 'probe',
    abiVersion: 7,
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(1))).toEqual({
    type: 'ticked',
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(7, 1))).toEqual({
    type: 'closed',
    closed: true,
  });
  expect(
    relay.decodeNativeForegroundResponse(Uint8Array.of(8, 129, 1))
  ).toEqual({
    type: 'pending',
    operation: 129,
  });
  expect(
    relay.decodeNativeForegroundResponse(
      Uint8Array.of(9, 4, 111, 111, 112, 115)
    )
  ).toEqual({
    type: 'operationError',
    reason: 'oops',
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(10, 1))).toEqual({
    type: 'cancelled',
    cancelled: true,
  });
  expect(
    relay.decodeNativeForegroundResponse(Uint8Array.of(11, 129, 1))
  ).toEqual({
    type: 'transactionOpened',
    transaction: 129,
  });
  expect(
    relay.decodeNativeForegroundResponse(
      Uint8Array.from([12, ...new Uint8Array(16).fill(3)])
    )
  ).toEqual({
    type: 'inserted',
    rowId: new Uint8Array(16).fill(3),
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(13))).toEqual({
    type: 'mutationStaged',
  });
  expect(
    relay.decodeNativeForegroundResponse(
      Uint8Array.from([14, ...new Uint8Array(16).fill(4)])
    )
  ).toEqual({
    type: 'transactionCommitted',
    txId: new Uint8Array(16).fill(4),
  });
  expect(relay.decodeNativeForegroundResponse(Uint8Array.of(15, 1))).toEqual({
    type: 'transactionRolledBack',
    rolledBack: true,
  });
  expect(() =>
    relay.decodeNativeForegroundResponse(Uint8Array.of(1, 0))
  ).toThrow('unknown or malformed command response');
});
