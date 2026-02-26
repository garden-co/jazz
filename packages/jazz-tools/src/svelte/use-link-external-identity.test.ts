import { describe, it, expect, vi, beforeEach } from 'vitest';

const mockLinkExternalIdentity = vi.fn().mockResolvedValue({
	principal_id: 'local:abc123',
	issuer: 'https://auth.example.com',
	subject: 'user-42',
	created: true
});

const mockGetActiveSyntheticAuth = vi.fn().mockReturnValue({
	localAuthMode: 'anonymous' as const,
	localAuthToken: 'fallback-token',
	profile: { id: '1', name: 'User 1', mode: 'anonymous' as const, token: 'fallback-token' }
});

vi.mock('../runtime/sync-transport.js', () => ({
	linkExternalIdentity: mockLinkExternalIdentity
}));

vi.mock('../synthetic-users.js', () => ({
	getActiveSyntheticAuth: mockGetActiveSyntheticAuth
}));

const { useLinkExternalIdentity } = await import('./use-link-external-identity.js');

describe('useLinkExternalIdentity', () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('calls linkExternalIdentity with provided auth fields', async () => {
		const link = useLinkExternalIdentity({
			appId: 'test-app',
			serverUrl: 'https://jazz.example.com',
			localAuthMode: 'demo',
			localAuthToken: 'explicit-token'
		});

		const result = await link({ jwtToken: 'jwt-abc' });

		expect(mockLinkExternalIdentity).toHaveBeenCalledWith(
			'https://jazz.example.com',
			{
				jwtToken: 'jwt-abc',
				localAuthMode: 'demo',
				localAuthToken: 'explicit-token',
				pathPrefix: undefined
			},
			undefined
		);
		expect(result.principal_id).toBe('local:abc123');
	});

	it('falls back to synthetic user auth when local auth not provided', async () => {
		const link = useLinkExternalIdentity({
			appId: 'test-app',
			serverUrl: 'https://jazz.example.com'
		});

		await link({ jwtToken: 'jwt-xyz' });

		expect(mockGetActiveSyntheticAuth).toHaveBeenCalledWith('test-app', {
			storage: undefined,
			storageKey: undefined,
			defaultMode: 'anonymous'
		});
		expect(mockLinkExternalIdentity).toHaveBeenCalledWith(
			'https://jazz.example.com',
			expect.objectContaining({
				localAuthMode: 'anonymous',
				localAuthToken: 'fallback-token'
			}),
			undefined
		);
	});

	it('input fields override option fields', async () => {
		const link = useLinkExternalIdentity({
			appId: 'test-app',
			serverUrl: 'https://jazz.example.com',
			localAuthMode: 'anonymous',
			localAuthToken: 'options-token'
		});

		await link({
			jwtToken: 'jwt-123',
			localAuthMode: 'demo',
			localAuthToken: 'input-token'
		});

		expect(mockLinkExternalIdentity).toHaveBeenCalledWith(
			'https://jazz.example.com',
			expect.objectContaining({
				localAuthMode: 'demo',
				localAuthToken: 'input-token'
			}),
			undefined
		);
	});

	it('passes serverPathPrefix and logPrefix through', async () => {
		const link = useLinkExternalIdentity({
			appId: 'test-app',
			serverUrl: 'https://jazz.example.com',
			serverPathPrefix: '/apps/test',
			localAuthMode: 'demo',
			localAuthToken: 'tok',
			logPrefix: '[test]'
		});

		await link({ jwtToken: 'jwt' });

		expect(mockLinkExternalIdentity).toHaveBeenCalledWith(
			'https://jazz.example.com',
			expect.objectContaining({ pathPrefix: '/apps/test' }),
			'[test]'
		);
	});

	it('throws when no local auth can be resolved', async () => {
		mockGetActiveSyntheticAuth.mockReturnValueOnce({
			localAuthMode: undefined,
			localAuthToken: undefined,
			profile: { id: '1', name: 'User 1', mode: undefined, token: undefined }
		});

		const link = useLinkExternalIdentity({
			appId: 'test-app',
			serverUrl: 'https://jazz.example.com'
		});

		await expect(link({ jwtToken: 'jwt' })).rejects.toThrow(
			'Local auth mode and token are required'
		);
	});
});
