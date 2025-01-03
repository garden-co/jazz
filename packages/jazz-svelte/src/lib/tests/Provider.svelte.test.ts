import { describe, it, expect, vi, beforeEach } from 'vitest';
import { getContext } from 'svelte';
import { Account, type AuthMethod } from 'jazz-tools';
import { getJazzContext, JAZZ_CTX } from '../jazz.svelte.js';
import { render, screen } from '@testing-library/svelte';
import ProviderTestComponent from './components/ProviderTestComponent.svelte'

vi.mock('svelte', async (importOriginal) => {
  return {
    ...await importOriginal<typeof import('svelte')>(),
    getContext: vi.fn(),
    untrack: vi.fn((fn) => fn())
  };
});

// Mock jazz-browser as the browser context is not always available
vi.mock('jazz-browser', () => ({
  createJazzBrowserContext: vi.fn(() =>
    Promise.resolve({
      me: undefined,
      logOut: vi.fn(),
      done: vi.fn()
    })
  )
}));

// Mocks for coValue observables
const mockUnsubscribe = vi.fn();
const mockSubscribe = vi.fn((Schema, id, me, depth, callback) => {
  callback();
  return {
    unsubscribe: mockUnsubscribe
  };
});

// Mock of a coValue observable
const mockObservable = {
  subscribe: mockSubscribe,
  getCurrentValue: vi.fn(() => ({ value: 'test value' }))
};

// Mock jazz-tools as we don't want to actually subscribe to any coValues
vi.mock('jazz-tools', async (importOriginal) => ({
  ...await importOriginal<typeof import('jazz-tools')>(),
  createCoValueObservable: vi.fn(() => mockObservable)
}));

describe('jazz.svelte', () => {
  let mockAccount: Account;

  beforeEach(() => {
    vi.clearAllMocks();

    mockAccount = {
      id: 'test-account-id',
      name: 'Test Account'
    } as unknown as Account;

    mockSubscribe.mockClear();
    mockUnsubscribe.mockClear();
    mockObservable.getCurrentValue.mockClear();
  });

  describe('getJazzContext', () => {
    it('should return the jazz context from svelte context', () => {
      const mockContext = { current: { me: mockAccount } };
      (getContext as jest.Mock).mockReturnValue(mockContext);

      const result = getJazzContext();
      expect(result).toBe(mockContext);
      expect(getContext).toHaveBeenCalledWith(JAZZ_CTX);
    });

    it('should return undefined if no context is set', () => {
      (getContext as jest.Mock).mockReturnValue(undefined);

      const result = getJazzContext();
      expect(result).toBeUndefined();
    });
  });

  describe('Provider Component', () => {
    it('should provide jazz context to children', async () => {
      const mockAuthState = {
        type: 'account',
        account: mockAccount,
        signOut: vi.fn()
      } as unknown as AuthMethod;

      render(ProviderTestComponent, {
        props: {
          auth: mockAuthState
        }
      });

      const accountName = await screen.findByTestId('provider-auth-test');
      expect(JSON.parse(accountName.textContent || '').account).toEqual(mockAccount);
    });

    it('should handle guest mode correctly', async () => {
      render(ProviderTestComponent, {
        props: {
          auth: 'guest'
        }
      });

      const errorMessage = await screen.findByTestId('provider-auth-test');
      expect(JSON.parse(errorMessage.textContent || '')).toBe('guest');
    });
  });
});
