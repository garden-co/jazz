type NativeRelay = {
  getAbiVersion(): number;
  execute(commandBase64: string): Promise<string>;
};

function loadRelay(nativeRelay: NativeRelay | null) {
  jest.resetModules();
  jest.doMock('../NativeJazzRelay', () => ({
    __esModule: true,
    default: nativeRelay,
  }));
  // Each case supplies the native boundary before importing the wrapper. That
  // is the same one-time lookup Metro performs for an installed native build.
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  return require('../relay') as typeof import('../relay');
}

afterEach(() => {
  jest.resetModules();
  jest.dontMock('../NativeJazzRelay');
});

it('tells Expo Go and old development builds that a native artifact is required', async () => {
  const relay = loadRelay(null);

  await expect(relay.executeNativeRelayCommand('AA==')).rejects.toThrow(
    'install a matching native development or release build containing the Jazz relay artifact. Expo Go never includes it.',
  );
});

it('rejects an installed native build with an incompatible ABI before executing a command', async () => {
  const nativeRelay: NativeRelay = {
    getAbiVersion: () => 2,
    execute: jest.fn(),
  };
  const relay = loadRelay(nativeRelay);

  await expect(relay.executeNativeRelayCommand('AA==')).rejects.toThrow(
    'Jazz native relay ABI 2 is incompatible with JavaScript ABI 3..=3; install a matching native development or release build.',
  );
  expect(nativeRelay.execute).not.toHaveBeenCalled();
});

it('forwards opaque commands only after the embedded relay ABI matches', async () => {
  const nativeRelay: NativeRelay = {
    getAbiVersion: () => 3,
    execute: jest.fn().mockResolvedValue('AQ=='),
  };
  const relay = loadRelay(nativeRelay);

  await expect(relay.executeNativeRelayCommand('AA==')).resolves.toBe('AQ==');
  expect(nativeRelay.execute).toHaveBeenCalledWith('AA==');
});
