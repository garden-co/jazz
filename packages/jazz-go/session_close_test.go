package jazz

import (
	"context"
	"sync"
	"testing"
	"time"
)

// openStalledRefresh is a session with a Refresh waiting in its Token
// provider, which runs inCallback and then waits for its ctx.
func openStalledRefresh(t *testing.T, inCallback func(*Session)) *Session {
	t.Helper()
	cfg := offlineConfig(t)
	var session *Session
	asked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { close(release) })
	cfg.Auth = Token(func(ctx context.Context, _ TokenReason) (string, error) {
		once.Do(func() {
			close(asked)
			if inCallback != nil {
				inCallback(session)
			}
		})
		select {
		case <-ctx.Done():
		case <-release:
		}
		return "", context.Canceled
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var err error
	if session, err = OpenSession(ctx, cfg, SessionOptions{Start: StartSignedOut}); err != nil {
		t.Fatalf("opening a signed-out session: %v", err)
	}
	go func() { _ = session.Refresh(context.Background()) }()
	select {
	case <-asked:
	case <-time.After(10 * time.Second):
		t.Fatal("Refresh never asked the provider")
	}
	return session
}

// Close cancels the provider's ctx, so a provider that honours it doesn't
// hold Close up.
func TestSessionCloseWithAStalledRefresh(t *testing.T) {
	t.Parallel()
	session := openStalledRefresh(t, nil)
	_ = returnsWithin(t, "Session.Close", 5*time.Second, session.Close)
}

// A Close from inside the provider can't wait for the call that waits on
// that provider.
func TestSessionCloseFromItsTokenProvider(t *testing.T) {
	t.Parallel()
	closed := make(chan error, 1)
	openStalledRefresh(t, func(s *Session) { closed <- s.Close() })
	_ = returnsWithin(t, "Session.Close in the provider", 5*time.Second, func() error { return <-closed })
}
