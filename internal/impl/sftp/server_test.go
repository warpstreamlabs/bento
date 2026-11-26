package sftp

import (
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// testSFTPServer is an in-process SSH server that serves SFTP from a
// temporary directory and can drop every open connection on demand. Paths
// used by tests must be relative, so that they resolve inside dir.
type testSFTPServer struct {
	address string
	dir     string

	mu    sync.Mutex
	conns []net.Conn
	live  int // SSH connections that are up
}

func startTestSFTPServer(t *testing.T) *testSFTPServer {
	t.Helper()

	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	require.NoError(t, err)

	conf := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
			return nil, nil
		},
	}
	conf.AddHostKey(signer)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &testSFTPServer{
		address: l.Addr().String(),
		dir:     t.TempDir(),
	}
	t.Cleanup(func() {
		_ = l.Close()
		srv.dropConnections()
	})

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			srv.mu.Lock()
			srv.conns = append(srv.conns, conn)
			srv.mu.Unlock()
			go srv.serve(conn, conf)
		}
	}()
	return srv
}

func (s *testSFTPServer) serve(conn net.Conn, conf *ssh.ServerConfig) {
	sconn, chans, reqs, err := ssh.NewServerConn(conn, conf)
	if err != nil {
		_ = conn.Close()
		return
	}
	s.mu.Lock()
	s.live++
	s.mu.Unlock()
	go func() {
		_ = sconn.Wait()
		s.mu.Lock()
		s.live--
		s.mu.Unlock()
	}()
	go ssh.DiscardRequests(reqs)

	for newCh := range chans {
		if newCh.ChannelType() != "session" {
			_ = newCh.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}
		ch, chReqs, err := newCh.Accept()
		if err != nil {
			continue
		}
		go func() {
			for req := range chReqs {
				ok := req.Type == "subsystem" && len(req.Payload) > 4 && string(req.Payload[4:]) == "sftp"
				_ = req.Reply(ok, nil)
				if ok {
					go func() {
						srv, err := sftp.NewServer(ch, sftp.WithServerWorkingDirectory(s.dir))
						if err != nil {
							_ = ch.Close()
							return
						}
						_ = srv.Serve()
						_ = srv.Close()
					}()
				}
			}
		}()
	}
}

// dropConnections closes every connection the server has accepted, as an
// idle timeout or a network failure would.
func (s *testSFTPServer) dropConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
}

// liveConns returns how many SSH connections to the server are still up.
func (s *testSFTPServer) liveConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live
}

func (s *testSFTPServer) readFile(t *testing.T, path string) string {
	t.Helper()

	b, err := os.ReadFile(filepath.Join(s.dir, path))
	require.NoError(t, err)
	return string(b)
}

func (s *testSFTPServer) writeFile(t *testing.T, path, data string) {
	t.Helper()

	require.NoError(t, os.WriteFile(filepath.Join(s.dir, path), []byte(data), 0o644))
}
