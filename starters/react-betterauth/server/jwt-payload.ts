export function jwtPayload({ user }: { user: { id: string } }): Record<string, unknown> {
  return {
    jazz_principal_id: user.id,
  };
}
