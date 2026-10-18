package proxyapp

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCandidateControlLossReportsDrainResult(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		result string
	}{
		{"finished", nil, "completed"},
		{"deadline", ErrForcedClose, "completed_with_forced_close"},
		{"callback", errors.New("extension shutdown"), "failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := NewService()
			s.local = NewInstance("candidate", "candidate-boot", "restart", 1)
			s.parent = &serviceWire{boot: "candidate-boot", operation: "restart"}
			s.timeout = time.Hour
			deadline := time.Now().Add(time.Second)
			s.local.requestStop(deadline)
			done := make(chan error, 1)
			done <- test.err
			s.localDone = done
			var log bytes.Buffer
			s.logger = slog.New(slog.NewJSONHandler(&log, nil))
			err := s.finishCandidateAfterControlLoss(io.EOF)
			if !errors.Is(err, io.EOF) || errors.Is(err, ErrForcedClose) != errors.Is(test.err, ErrForcedClose) {
				t.Fatalf("control loss changed actual drain result: %v", err)
			}
			if !s.stopping() || s.local.deadline != deadline {
				t.Fatal("control loss reset stop deadline or permitted another start")
			}
			if !strings.Contains(log.String(), `"result":"`+test.result+`"`) {
				t.Fatalf("missing drain outcome: %s", &log)
			}
		})
	}
}

func TestCandidateReleasesOnlyOwnedPaths(t *testing.T) {
	for _, state := range []string{"parent-alive", "parent-exited", "path-replaced"} {
		t.Run(state, func(t *testing.T) {
			makePaths := func(prefix string) *retainedResources {
				r := &retainedResources{StatsPath: filepath.Join(t.TempDir(), prefix+".sock"), ShmPath: filepath.Join(t.TempDir(), prefix+".shm")}
				for _, path := range []string{r.StatsPath, r.ShmPath} {
					f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
					if err != nil {
						t.Fatal(err)
					}
					// Keep the inode allocated when replacing a pathname.
					t.Cleanup(func() { f.Close() })
				}
				var err error
				r.StatsIdentity, err = identifyPath(r.StatsPath)
				if err != nil {
					t.Fatal(err)
				}
				r.ShmIdentity, err = identifyPath(r.ShmPath)
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			s := NewService()
			s.candidate = true
			s.coordinatorPID = os.Getppid()
			s.resources, s.coordinatorPaths = makePaths("b"), makePaths("a")
			if state != "parent-alive" {
				s.coordinatorPID = 1 << 30
			}
			if state == "path-replaced" {
				if err := os.Remove(s.coordinatorPaths.StatsPath); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(s.coordinatorPaths.StatsPath, []byte("replacement"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := s.releaseService()
			if (err != nil) != (state == "path-replaced") {
				t.Fatalf("cleanup result: %v", err)
			}
			for _, path := range []string{s.resources.StatsPath, s.resources.ShmPath} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("B resource remains: %s %v", path, err)
				}
			}
			_, err = os.Stat(s.coordinatorPaths.StatsPath)
			if os.IsNotExist(err) != (state == "parent-exited") {
				t.Fatalf("coordinator path ownership violated: %v", err)
			}
			if err := s.resources.removePaths(); err != nil {
				t.Fatalf("parent repeated cleanup: %v", err)
			}
		})
	}
}

func TestCandidateControlLossRetainsForwardedStopDeadline(t *testing.T) {
	s := NewService()
	s.local = NewInstance("candidate", "boot", "restart", 1)
	s.timeout = time.Hour
	s.stopRequested = true
	s.stopDeadline = time.Now().Add(time.Second)
	done := make(chan error, 1)
	done <- nil
	s.localDone = done
	if err := s.finishCandidateAfterControlLoss(io.EOF); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if s.local.deadline != s.stopDeadline {
		t.Fatal("control EOF extended already forwarded shutdown")
	}
}
