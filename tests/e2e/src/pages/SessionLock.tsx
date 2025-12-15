export function SessionLockTest() {
  const url = new URL(window.location.href);

  if (url.searchParams.has("sessionId")) {
    const sessionId = url.searchParams.get("sessionId");

    return <h1>Session {sessionId}</h1>;
  }

  const concurrentSessions = Array.from({ length: 9 }, (_, i) => {
    const url = new URL(window.location.href);
    url.searchParams.set("sessionId", i.toString());
    return url.toString();
  });

  return (
    <div>
      <h1>Session Lock Test</h1>
      <p>
        {concurrentSessions.map((url) => (
          <iframe key={url} src={url}></iframe>
        ))}
      </p>
    </div>
  );
}
