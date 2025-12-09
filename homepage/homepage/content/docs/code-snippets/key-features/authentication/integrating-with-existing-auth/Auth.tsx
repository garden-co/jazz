import { useJazzContext } from "jazz-tools/react";

export function Auth() {
  const context = useJazzContext();

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();

    const formData = new FormData(e.currentTarget);
    const username = formData.get("username");
    const password = formData.get("password");

    // Whatever your existing auth system is
    // @ts-expect-error Virtual implementation
    const myOldAppUser = await myApp.logIn(username, password);
    const accountID = myOldAppUser.jazzAccountID;
    // If you've stored this in an encrypted form, make sure to decrypt it first
    const accountSecret = myOldAppUser.jazzAccountSecret;

    await context.authenticate({
      accountID,
      accountSecret,
      provider: "my-old-app-auth", // Use any string here to identify your authentication provider. This avoids Jazz considering your users unauthenticated.
    });

    // The Jazz session is now authenticated!
  }

  return (
    <form onSubmit={handleSubmit}>
      <input name="username" type="text" placeholder="Username" required />
      <input name="password" type="password" placeholder="Password" required />
      <button type="submit">Log In</button>
    </form>
  );
}
