import { render, waitFor } from '@testing-library/svelte';
import {
  Account,
  CoMap,
  coField,
  cojsonInternals,
  type CoValue,
  type CoValueClass,
  type RefsToResolve,
} from 'jazz-tools';
import { describe, expect, it, expectTypeOf, beforeEach } from 'vitest';
import { createJazzTestAccount, createJazzTestContext, setupJazzTestSync } from '../testing.js';
import UseCoState from './components/useCoState.svelte';

beforeEach(async () => {
  await setupJazzTestSync();
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

function setup<T extends CoValue>(options: { account: Account; map: T; resolve?: RefsToResolve<T> }) {
  const result = { current: undefined } as { current: T | undefined };

  render(UseCoState, {
    context: createJazzTestContext({ account: options.account }),
    props: {
      Schema: options.map.constructor as CoValueClass<T>,
      id: options.map.id,
      resolve: options.resolve ?? true,
      setResult: (value) => {
        result.current = value as T;
      }
    }
  });

  return result;
}

describe('useCoState', () => {
  it('should return the correct value', async () => {
    class TestMap extends CoMap {
      value = coField.string;
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: '123'
      },
      { owner: account }
    );

    const result = setup({
      account,
      map
    });

    expect(result.current?.value).toBe('123');
  });

  it('should update the value when the coValue changes', async () => {
    class TestMap extends CoMap {
      value = coField.string;
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: '123'
      },
      { owner: account }
    );

    const result = setup({
      account,
      map
    });

    expect(result.current?.value).toBe('123');

    map.value = '456';

    expect(result.current?.value).toBe('456');
  });

  it('should load nested values if requested', async () => {
    class TestNestedMap extends CoMap {
      value = coField.string;
    }

    class TestMap extends CoMap {
      value = coField.string;
      nested = coField.ref(TestNestedMap);
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: '123',
        nested: TestNestedMap.create(
          {
            value: '456'
          },
          { owner: account }
        )
      },
      { owner: account }
    );

    const result = setup({
      account,
      map,
      resolve: {
        nested: {}
      }
    });

    expect(result.current?.value).toBe('123');
    expect(result.current?.nested?.value).toBe('456');
  });

  it('should load nested values on access even if not requested', async () => {
    class TestNestedMap extends CoMap {
      value = coField.string;
    }

    class TestMap extends CoMap {
      value = coField.string;
      nested = coField.ref(TestNestedMap);
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: '123',
        nested: TestNestedMap.create(
          {
            value: '456'
          },
          { owner: account }
        )
      },
      { owner: account }
    );

    const result = setup({
      account,
      map,
      resolve: {
        nested: {}
      }
    });

    expect(result.current?.value).toBe('123');
    expect(result.current?.nested?.value).toBe('456');
  });

  it('should return null if the coValue is not found', async () => {
    class TestMap extends CoMap {
      value = coField.string;
    }

    const unreachableAccount = await createJazzTestAccount({});

    const map = TestMap.create(
      {
        value: '123'
      },
      unreachableAccount
    );

    unreachableAccount._raw.core.node.gracefulShutdown();

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true
    });

    const result = setup({
      account,
      map
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });

  it('should return the same type as Schema', async () => {
    class TestMap extends CoMap {
      value = coField.string;
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: '123'
      },
      { owner: account }
    );

    const result = setup({
      account,
      map
    });

    expectTypeOf(result).toEqualTypeOf<{
      current: TestMap | undefined;
    }>();
  });
});
