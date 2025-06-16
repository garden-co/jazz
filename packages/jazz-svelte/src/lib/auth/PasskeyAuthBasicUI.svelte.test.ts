import { render, screen } from '@testing-library/svelte';
import { userEvent } from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import PasskeyAuthBasicUI from './PasskeyAuthBasicUI.svelte';
import { createJazzTestAccount, createJazzTestContext } from '../testing.js';
import { BrowserPasskeyAuth } from 'jazz-browser';

vi.mock('jazz-browser', () => ({
  BrowserPasskeyAuth: class {
    logIn() {
      return Promise.resolve();
    }
    signUp() {
      return Promise.resolve();
    }
  }
}));

const logIn = vi.spyOn(BrowserPasskeyAuth.prototype, 'logIn');
const signUp = vi.spyOn(BrowserPasskeyAuth.prototype, 'signUp');

const defaultProps = {
  appName: 'Test App',
};

beforeEach(async () => {
  vi.clearAllMocks();
  logIn.mockResolvedValue(undefined);
  signUp.mockResolvedValue(undefined);

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe('PasskeyAuthBasicUI', () => {
  it('should show ready state with login and signup options', () => {
    render(PasskeyAuthBasicUI, {
      context: createJazzTestContext(),
      props: defaultProps
    });

    expect(screen.getByText('Log in with existing account')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Display name')).toBeInTheDocument();
    expect(screen.getByText('Sign up')).toBeInTheDocument();
  });

  it('should call logIn when login button is clicked', async () => {
    render(PasskeyAuthBasicUI, {
      context: createJazzTestContext(),
      props: defaultProps
    });

    await userEvent.click(screen.getByText('Log in with existing account'));
    expect(logIn).toHaveBeenCalled();
  });

  it.skip('should show login errors', async () => {
    logIn.mockRejectedValue(new Error('Test error message'));
    render(PasskeyAuthBasicUI, {
      context: createJazzTestContext(),
      props: defaultProps
    });

    await userEvent.click(screen.getByText('Log in with existing account'));
    expect(logIn).toHaveBeenCalled();
    expect(screen.getByText('Test error message')).toBeInTheDocument();
  });

  it('should call signUp with name when form is submitted', async () => {
    render(PasskeyAuthBasicUI, {
      context: createJazzTestContext(),
      props: defaultProps
    });

    const input = screen.getByPlaceholderText('Display name');
    await userEvent.type(input, 'Test User');
    await userEvent.click(screen.getByText('Sign up'));

    expect(signUp).toHaveBeenCalledWith('Test User');
  });

  it.skip('should show signup errors', async () => {
    signUp.mockRejectedValue(new Error('Test error message'));
    render(PasskeyAuthBasicUI, {
      context: createJazzTestContext(),
      props: defaultProps
    });

    const input = screen.getByPlaceholderText('Display name');
    await userEvent.type(input, 'Test User');
    await userEvent.click(screen.getByText('Sign up'));

    expect(signUp).toHaveBeenCalledWith('Test User');
    expect(screen.getByText('Test error message')).toBeInTheDocument();
  });

  it('should not call signUp when name is empty', async () => {
    render(PasskeyAuthBasicUI, {
      context: createJazzTestContext(),
      props: defaultProps
    });

    await userEvent.click(screen.getByText('Sign up'));
    expect(signUp).not.toHaveBeenCalled();
  });
});