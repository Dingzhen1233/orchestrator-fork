package builder

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// fakeStepEnvd answers the builder's envd process.Start surface over a UDS:
// it records each call's Basic-auth user and command, scripting the `id -u`
// probes (account existence) and `useradd` outcomes the USER step performs.
type fakeStepEnvd struct {
	addr     string
	existing map[string]bool
	failCmd  string // command prefix that exits non-zero
	mu       sync.Mutex
	calls    []fakeStepEnvdCall
}

type fakeStepEnvdCall struct {
	user string
	cmd  string
}

func (f *fakeStepEnvd) recorded() []fakeStepEnvdCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeStepEnvdCall(nil), f.calls...)
}

func startFakeStepEnvd(t *testing.T, existing []string, failCmd string) *fakeStepEnvd {
	t.Helper()
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "envd.sock"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeStepEnvd{addr: ln.Addr().String(), existing: map[string]bool{}, failCmd: failCmd}
	for _, u := range existing {
		f.existing[u] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /process.Process/Start", func(w http.ResponseWriter, r *http.Request) {
		var hdr [5]byte
		if _, err := io.ReadFull(r.Body, hdr[:]); err != nil {
			http.Error(w, "short frame", http.StatusBadRequest)
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr[1:5]))
		if _, err := io.ReadFull(r.Body, body); err != nil {
			http.Error(w, "short frame", http.StatusBadRequest)
			return
		}
		var req struct {
			Process struct {
				Args []string `json:"args"`
			} `json:"process"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		user, _, _ := r.BasicAuth()
		cmd := ""
		if len(req.Process.Args) > 0 {
			cmd = req.Process.Args[len(req.Process.Args)-1]
		}
		f.mu.Lock()
		f.calls = append(f.calls, fakeStepEnvdCall{user: user, cmd: cmd})
		f.mu.Unlock()

		exit := 0
		switch {
		case strings.HasPrefix(cmd, "id -u "):
			if !f.existing[cmd[len("id -u "):]] {
				exit = 1
			}
		case strings.HasPrefix(cmd, "useradd "):
			if failCmd != "" && strings.HasPrefix(cmd, failCmd) {
				exit = 1
			} else if fields := strings.Fields(cmd); len(fields) > 0 {
				f.mu.Lock()
				f.existing[fields[len(fields)-1]] = true
				f.mu.Unlock()
			}
		}
		writeStepEnvdFrame(w, 0, []byte(`{"event":{"start":{"pid":42}}}`))
		if exit != 0 {
			writeStepEnvdFrame(w, 0, []byte(`{"event":{"end":{"exitCode":1,"exited":true,"status":"exited"}}}`))
		} else {
			writeStepEnvdFrame(w, 0, []byte(`{"event":{"end":{"exited":true,"status":"exited"}}}`))
		}
		writeStepEnvdFrame(w, 0x02, []byte(`{}`))
	})
	go http.Serve(ln, mux)
	t.Cleanup(func() { ln.Close() })
	return f
}

func writeStepEnvdFrame(w io.Writer, flags byte, payload []byte) {
	hdr := [5]byte{flags}
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	_, _ = w.Write(hdr[:])
	_, _ = w.Write(payload)
}

func TestUserStepResolvesAndCreatesAccount(t *testing.T) {
	for _, test := range []struct {
		name        string
		user        string
		existing    []string
		failCmd     string
		wantErr     string
		wantCalls   []string
		wantContext string
	}{
		{
			name: "existing account is only probed", user: "app", existing: []string{"app"},
			wantCalls: []string{"id -u app"}, wantContext: "app",
		},
		{
			name:      "missing account is created",
			user:      "app",
			wantCalls: []string{"id -u app", "useradd --create-home --shell /bin/bash app"}, wantContext: "app",
		},
		{
			name: "creation failure fails the step", user: "app", failCmd: "useradd",
			wantErr:   "create missing user",
			wantCalls: []string{"id -u app", "useradd --create-home --shell /bin/bash app"},
		},
		{name: "uid:gid stays a pure identity switch", user: "1000:1000", wantContext: "1000:1000"},
		{name: "bare uid stays a pure identity switch", user: "0", wantContext: "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := startFakeStepEnvd(t, test.existing, test.failCmd)
			p := &buildPipeline{
				ctx:  context.Background(),
				spec: &configsock.BuildSpec{Timeouts: configsock.BuildTimeouts{StepSec: 5}},
			}
			c := &stepCtx{env: map[string]string{}}
			err := p.applyStep(nil, &envdExec{uds: f.addr}, c, 0,
				configsock.BuildStep{Type: "USER", Args: []string{test.user}})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("USER step error = %v, want containing %q", err, test.wantErr)
				}
			} else if err != nil {
				t.Fatalf("USER step: %v", err)
			} else if c.user != test.wantContext {
				t.Fatalf("step context user = %q, want %q", c.user, test.wantContext)
			}
			var cmds []string
			for _, call := range f.recorded() {
				if call.user != "root" {
					t.Fatalf("USER step probe ran as %q, want root", call.user)
				}
				cmds = append(cmds, call.cmd)
			}
			if !reflect.DeepEqual(cmds, test.wantCalls) {
				t.Fatalf("guest commands = %#v, want %#v", cmds, test.wantCalls)
			}
		})
	}
}
