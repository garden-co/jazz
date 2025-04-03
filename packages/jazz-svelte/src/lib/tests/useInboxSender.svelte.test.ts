import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, cleanup } from '@testing-library/svelte';
import { InboxSender } from 'jazz-tools';
import { getJazzContext, type RegisteredAccount } from '../jazz.svelte';
import type { ID } from 'jazz-tools';
import { default as TestComponent } from './components/useInboxSender.svelte';

// Mock dependencies
vi.mock('./jazz.svelte', () => ({
  getJazzContext: vi.fn()
}));

vi.mock('./inbox', () => ({
  InboxSender: {
    load: vi.fn()
  }
}));

describe('experimental_useInboxSender', () => {
  const mockMe = { id: 'user123' as ID<RegisteredAccount> };
  const mockInboxOwnerID = 'recipient456' as ID<RegisteredAccount>;
  const mockSendMessage = vi.fn();
  const mockInboxSender = {
    owner: { id: mockInboxOwnerID },
    sendMessage: mockSendMessage
  };

  beforeEach(() => {
    // Mock the context
    vi.mocked(getJazzContext).mockReturnValue({
      me: mockMe
    } as any);

    // Mock InboxSender.load to return a promise that resolves to the mock inbox sender
    vi.mocked(InboxSender.load).mockResolvedValue(mockInboxSender as any);
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it('should throw an error when inboxOwnerID is undefined and message is sent', async () => {
    // Create a test component that uses the hook and displays UI to trigger it
    const TestWrapper = {
      render: () => {
        render(TestComponent, {
          props: {
            inboxOwnerID: undefined
          }
        });
      }
    };

    TestWrapper.render();

    // Try to send message with undefined inboxOwnerID
    await expect(screen.getByTestId('send-button').click()).rejects.toThrow(
      'Inbox owner ID is required'
    );
  });

  it('should load and use the inbox sender when inboxOwnerID is provided', async () => {
    // Create a test component that uses the hook and displays UI to trigger it
    render(TestComponent, {
      props: {
        inboxOwnerID: mockInboxOwnerID
      }
    });

    const message = { text: 'Hello' };

    // Set message content
    await fireEvent.input(screen.getByTestId('message-input'), {
      target: { value: JSON.stringify(message) }
    });

    // Send message
    await fireEvent.click(screen.getByTestId('send-button'));

    // Check if InboxSender.load was called with the correct arguments
    expect(InboxSender.load).toHaveBeenCalledWith(mockInboxOwnerID, mockMe);

    // Check if sendMessage was called with the correct message
    expect(mockSendMessage).toHaveBeenCalledWith(message);
  });

  it('should reload the inbox sender if the inboxOwnerID changes', async () => {
    const newInboxOwnerID = 'new789' as ID<RegisteredAccount>;
    const newMockInboxSender = {
      owner: { id: newInboxOwnerID },
      sendMessage: mockSendMessage
    };

    // Set up two different inbox senders based on ID
    vi.mocked(InboxSender.load).mockImplementation((id) => {
      if (id === mockInboxOwnerID) {
        return Promise.resolve(mockInboxSender as any);
      } else {
        return Promise.resolve(newMockInboxSender as any);
      }
    });

    const { component } = render(TestComponent, {
      props: {
        inboxOwnerID: mockInboxOwnerID
      }
    });

    const message = { text: 'Initial message' };

    // Set message content and send
    await fireEvent.input(screen.getByTestId('message-input'), {
      target: { value: JSON.stringify(message) }
    });
    await fireEvent.click(screen.getByTestId('send-button'));

    // Update inboxOwnerID prop
    await component.$set({ inboxOwnerID: newInboxOwnerID });

    const newMessage = { text: 'New message' };

    // Set new message content and send
    await fireEvent.input(screen.getByTestId('message-input'), {
      target: { value: JSON.stringify(newMessage) }
    });
    await fireEvent.click(screen.getByTestId('send-button'));

    // Verify InboxSender.load was called with both IDs
    expect(InboxSender.load).toHaveBeenCalledWith(mockInboxOwnerID, mockMe);
    expect(InboxSender.load).toHaveBeenCalledWith(newInboxOwnerID, mockMe);

    // Verify messages were sent
    expect(mockSendMessage).toHaveBeenCalledWith(message);
    expect(mockSendMessage).toHaveBeenCalledWith(newMessage);
  });
});
