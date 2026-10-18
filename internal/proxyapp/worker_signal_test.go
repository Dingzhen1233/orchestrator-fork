package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
)

func TestWorkerEntryCancellationUsesDrain(t *testing.T) {
	for _, test := range []struct {
		name, timeout       string
		masterFirst, forced bool
	}{
		{"finite", "2s", false, false},
		{"zero", "0s", false, true},
		{"deadline", "100ms", false, true},
		{"master-deadline-first", "2s", true, true},
		{"master-unbounded-first", "0s", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if runWorkerLifetimeSubprocess(t) {
				return
			}
			h := newPreparedWorkerHarness(t)
			cfg := h.worker.effective.Config()
			cfg.ShutdownTimeout = test.timeout
			var err error
			h.worker.effective, err = FreezeConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			h.table.BeginSync()
			h.table.Bookmark()
			release := make(chan struct{})
			var once sync.Once
			finishRequest := func() { once.Do(func() { close(release) }) }
			defer finishRequest()
			extension := &wrappingWorkerExtension{}
			var lifetime context.Context
			extension.start = func(ctx context.Context, _ proxyextension.WorkerHost) error { lifetime = ctx; return nil }
			extension.wrap = func(http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Length", "15")
					fmt.Fprint(w, "begin\n")
					w.(http.Flusher).Flush()
					select {
					case <-release:
						fmt.Fprint(w, "complete\n")
					case <-r.Context().Done():
					}
				})
			}
			entry, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- h.worker.Run(entry, &Runtime{Logger: testMasterLogger(), WorkerExtension: extension}) }()
			message, err := h.control.receive()
			if err != nil || message.Kind != "prepared" {
				t.Fatalf("prepared: %+v %v", message, err)
			}
			if err := h.control.send(controlMessage{Kind: "start"}); err != nil {
				t.Fatal(err)
			}
			message, err = h.control.receive()
			if err != nil || message.Kind != "serving" {
				t.Fatalf("serving: %+v %v", message, err)
			}
			messages := make(chan controlMessage, 16)
			go func() {
				defer close(messages)
				for {
					message, err := h.control.receive()
					if err != nil {
						return
					}
					messages <- message
					if message.Kind == "stopped" {
						return
					}
				}
			}()
			waitMessage := func(kind string) controlMessage {
				t.Helper()
				for {
					select {
					case message, ok := <-messages:
						if !ok {
							t.Fatalf("control ended before %s", kind)
						}
						if message.Kind == kind {
							return message
						}
					case <-time.After(3 * time.Second):
						t.Fatalf("missing %s", kind)
					}
				}
			}
			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 3 * time.Second}
			response, err := client.Get("http://" + h.data.Addr().String() + "/held")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			prefix := make([]byte, 6)
			if _, err := io.ReadFull(response.Body, prefix); err != nil || string(prefix) != "begin\n" {
				t.Fatalf("prefix: %q %v", prefix, err)
			}
			started := time.Now()
			if test.masterFirst {
				var deadline time.Time
				if test.forced {
					deadline = started.Add(100 * time.Millisecond)
				}
				if err := h.control.send(controlMessage{Kind: "drain", Deadline: deadline}); err != nil {
					t.Fatal(err)
				}
				waitMessage("data_draining")
			}
			cancel()
			waitMessage("stop_requested")
			if !test.forced {
				if lifetime.Err() != nil {
					t.Fatal("entry cancellation ended dependency before accepted response")
				}
				finishRequest()
			}
			body, readErr := io.ReadAll(response.Body)
			if !test.forced && (readErr != nil || string(body) != "complete\n") {
				t.Fatalf("finite response: %q %v", body, readErr)
			}
			if test.forced && readErr == nil {
				t.Fatalf("held response falsely completed: %q", body)
			}
			stopped := waitMessage("stopped")
			if stopped.Forced != test.forced {
				t.Fatalf("stop result: %+v", stopped)
			}
			select {
			case err := <-done:
				if errors.Is(err, ErrForcedClose) != test.forced || (!test.forced && err != nil) {
					t.Fatalf("Run result: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not finish")
			}
			if lifetime.Err() == nil {
				t.Fatal("dependency lifetime did not end after drain")
			}
			if test.forced && time.Since(started) >= time.Second {
				t.Fatal("automatic stop reset the original deadline")
			}
		})
	}
}

func TestWorkerOwnStopRetainsEpochAndOrderingChecks(t *testing.T) {
	i := NewInstance("proxy", "boot", "", 1)
	i.workers[0] = workerState{alive: true, prepared: true, ready: true, epoch: 2}
	if err := i.workerEvent(0, 1, controlMessage{Kind: "stop_requested"}); err != nil || i.workers[0].stopping {
		t.Fatal("stale stop changed worker")
	}
	if err := i.workerEvent(0, 2, controlMessage{Kind: "data_drained"}); err == nil {
		t.Fatal("unrequested drain accepted")
	}
	if err := i.workerEvent(0, 2, controlMessage{Kind: "stop_requested"}); err != nil {
		t.Fatal(err)
	}
	if i.stopping || i.workers[0].ready {
		t.Fatal("worker stop affected instance stop or retained readiness")
	}
	if err := i.workerEvent(0, 2, controlMessage{Kind: "stopped"}); err == nil {
		t.Fatal("stop before drain accepted")
	}
	for _, kind := range []string{"data_draining", "data_drained", "mmds_drained", "stopped"} {
		if err := i.workerEvent(0, 2, controlMessage{Kind: kind}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWorkerOwnForcedExitDoesNotPoisonLaterServiceShutdown(t *testing.T) {
	i := NewInstance("proxy", "boot", "", 1)
	i.workers[0] = workerState{alive: true, prepared: true, ready: true, epoch: 2}
	for _, kind := range []string{"stop_requested", "data_drained", "mmds_drained", "stopped"} {
		message := controlMessage{Kind: kind}
		if kind == "stopped" {
			message.Forced, message.Error = true, ErrForcedClose.Error()
		}
		if err := i.workerEvent(0, 2, message); err != nil {
			t.Fatal(err)
		}
	}
	i.workerExited(0, 2, ErrForcedClose)
	// This service stop begins after the separately signaled worker has exited.
	// No remaining work is forced closed by this later action.
	i.requestStop(time.Now().Add(time.Second))
	if err := i.drainWorkers(); err != nil {
		t.Fatalf("previous worker stop changed later shutdown result: %v", err)
	}
}
