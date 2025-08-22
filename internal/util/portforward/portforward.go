package portforward

// Source: https://github.com/gianarb/kube-port-forward

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/klog"
)

// Request is the structure used as argument for ToPod
type Request struct {
	// RestConfig is the kubernetes config
	RestConfig *rest.Config
	// Pod is the selected pod for this port forwarding
	Pod v1.Pod
	// LocalPort is the local port that will be selected to expose the PodPort
	LocalPort int
	// PodPort is the target port for the pod
	PodPort int
	// StopCh is the channel used to manage the port forward lifecycle
	StopCh <-chan struct{}
	// ReadyCh communicates when the tunnel is ready to receive traffic
	ReadyCh chan struct{}
}

// ToPod establishes a port-forward to the given pod and supervises it.
//   - Establishment uses a retry budget until the Ready signal is observed.
//   - Once Ready, the forward runs indefinitely until req.StopCh closes.
//   - A TCP-hold watchdog probes the local forwarded port; on repeated failures
//     it force-closes the forwarder to trigger re-establishment.
func ToPod(req Request) error {
	// Tunables (wire to flags/envs if you prefer)
	pfTimeoutDial := 1 * time.Minute  // TCP/TLS handshake when dialing SPDY
	readyDeadline := 30 * time.Second // per-attempt time-to-ready
	maxEstablish := 5                 // attempts per (re)establish phase
	restartBackoffBase := 2 * time.Second
	restartBackoffMax := 30 * time.Second

	klog.V(5).Infof("PF(supervise): dial=%s ready=%s attempts=%d", pfTimeoutDial, readyDeadline, maxEstablish)

	// Transport with extended dial/TLS (do NOT set http.Client.Timeout).
	cfg := rest.CopyConfig(req.RestConfig)
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if t, ok := rt.(*http.Transport); ok {
			ct := t.Clone()
			ct.DialContext = (&net.Dialer{Timeout: pfTimeoutDial, KeepAlive: 30 * time.Second}).DialContext
			ct.TLSHandshakeTimeout = pfTimeoutDial
			return ct
		}
		return rt
	}

	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return err
	}

	logr := NewLogger()
	img := findImageForPort(req.Pod, req.PodPort)
	klog.Infof("start port-forward %s/%s [%s] %d->%d", req.Pod.Namespace, req.Pod.Name, img, req.LocalPort, req.PodPort)

	url, err := getURLScheme(req)
	if err != nil {
		return err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	ports := []string{fmt.Sprintf("%d:%d", req.LocalPort, req.PodPort)}
	backoff := restartBackoffBase

supervise:
	for {
		// Phase A: (re)establish with retry budget
		for attempt := 1; attempt <= maxEstablish; attempt++ {
			select {
			case <-req.StopCh:
				return nil
			default:
			}

			attemptStop := make(chan struct{})
			attemptReady := make(chan struct{}, 1)
			stopCh := mergeStops(req.StopCh, attemptStop)

			pf, err := portforward.New(dialer, ports, stopCh, attemptReady, logr, logr)
			if err != nil {
				klog.Warningf("port-forward create failed (attempt %d/%d): %v", attempt, maxEstablish, err)
				if attempt == maxEstablish {
					return fmt.Errorf("port-forward create failed after %d attempts: %w", maxEstablish, err)
				}
				time.Sleep(backoff)
				continue
			}

			// Run this attempt
			done := make(chan struct{})
			var runErr error
			go func() {
				defer close(done)
				runErr = pf.ForwardPorts() // blocks until stopCh closes or streams end
			}()

			// Wait for readiness or timeout/stop
			select {
			case <-attemptReady:
				klog.Infof("port-forward ready after attempt %d/%d", attempt, maxEstablish)

				// Phase B: supervise running forwarder + watchdog
				localAddr := fmt.Sprintf("127.0.0.1:%d", req.LocalPort)
				quitWatch := make(chan struct{})
				go watchdogTCPHold(localAddr, quitWatch, func() {
					pf.Close() // force ForwardPorts() to exit so your supervisor can recreate it
				})

				select {
				case <-req.StopCh:
					closeSafe(quitWatch)
					closeSafe(attemptStop)
					// bounded wait, then force-close
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						pf.Close()
						<-done
					}
					return nil

				case <-done:
					// stream died or we forced close
					closeSafe(quitWatch)
					if runErr != nil {
						klog.Warningf("port-forward ended: %v", runErr)
					} else {
						klog.Warning("port-forward ended without error")
					}
					// Re-establish after backoff
					time.Sleep(backoff)
					backoff *= 2
					if backoff > restartBackoffMax {
						backoff = restartBackoffMax
					}
					continue supervise
				}

			case <-time.After(readyDeadline):
				// Not ready yet: abort this attempt and retry
				klog.Warningf("port-forward not ready within %s (attempt %d/%d) — retrying",
					readyDeadline, attempt, maxEstablish)
				closeSafe(attemptStop)
				// bounded wait, then force-close
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					pf.Close()
					<-done
				}
				if attempt == maxEstablish {
					if runErr != nil {
						return fmt.Errorf("port-forward failed to become ready after %d attempts: %w", maxEstablish, runErr)
					}
					return fmt.Errorf("port-forward failed to become ready after %d attempts (timed out waiting %s each attempt)",
						maxEstablish, readyDeadline)
				}
				time.Sleep(backoff)

			case <-req.StopCh:
				closeSafe(attemptStop)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					pf.Close()
					<-done
				}
				return nil
			}
		}
	}
}

// watchdogTCPHold probes the local forwarded port by connecting and attempting a
// 1-byte read with a short deadline. If the PF/backend is dead, the connection
// is usually closed promptly and the read returns early with EOF/error.
// After N consecutive failures, forceRestart() is called.
func watchdogTCPHold(localAddr string, quit <-chan struct{}, forceRestart func()) {
	interval := 5 * time.Second     // how often to probe
	hold := 1500 * time.Millisecond // how long we expect the socket to stay usable
	maxConsecFailures := 3          // restart after this many consecutive failures

	consec := 0
	t := time.NewTicker(interval)
	defer t.Stop()

	klog.Infof("watchdog: starting TCP-hold probe on %s (interval=%s, hold=%s, threshold=%d)",
		localAddr, interval, hold, maxConsecFailures)

	for {
		select {
		case <-quit:
			klog.Infof("watchdog: stopping TCP-hold probe on %s", localAddr)
			return
		case <-t.C:
			d := net.Dialer{Timeout: 2 * time.Second, KeepAlive: 0}
			conn, err := d.Dial("tcp", localAddr)
			if err != nil {
				consec++
				klog.Warningf("watchdog: dial %s failed (%d/%d): %v", localAddr, consec, maxConsecFailures, err)
			} else {
				// Try to detect early close by attempting a tiny read with a deadline.
				// If the PF closed right after accept, this will return EOF/error quickly.
				_ = conn.SetReadDeadline(time.Now().Add(hold))
				var buf [1]byte
				_, rerr := conn.Read(buf[:])
				// Interpret result:
				// - Timeout -> connection stayed open for 'hold' -> success (reset).
				// - Any other error (including EOF) -> failure (increment).
				if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
					if consec > 0 {
						klog.V(4).Infof("watchdog: %s healthy; reset failures (was %d)", localAddr, consec)
					}
					consec = 0
					klog.V(4).Infof("watchdog: %s alive (timeout after %s)", localAddr, hold)
				} else if rerr != nil {
					consec++
					klog.Warningf("watchdog: early close on %s (%d/%d): %v", localAddr, consec, maxConsecFailures, rerr)
				} else {
					// Got data (unlikely for plain TCP services without banner) -> success.
					consec = 0
				}
				_ = conn.Close()
			}

			if consec >= maxConsecFailures {
				klog.Warningf("watchdog: forcing port-forward restart after %d consecutive failures on %s", consec, localAddr)
				forceRestart()
				consec = 0
			}
		}
	}
}

// findImageForPort tries to locate the image of the container that exposes the given port.
// If none match, falls back to the first container (if any).
func findImageForPort(pod v1.Pod, port int) string {
	for _, c := range pod.Spec.Containers {
		for _, p := range c.Ports {
			if int(p.ContainerPort) == port {
				return c.Image
			}
		}
	}
	if len(pod.Spec.Containers) > 0 {
		return pod.Spec.Containers[0].Image
	}
	return "<unknown>"
}

// mergeStops returns a channel that closes when either input channel closes.
// Useful for combining multiple stop signals into a single one.
func mergeStops(a, b <-chan struct{}) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		select {
		case <-a:
		case <-b:
		}
		close(out)
	}()
	return out
}

// closeSafe closes the given channel, recovering from a panic if it is already closed.
// This makes repeated or concurrent close attempts safe.
func closeSafe(ch chan struct{}) { defer func() { _ = recover() }(); close(ch) }

// getURLScheme will take given request and create a valid url scheme for use
// by the portforward api.
func getURLScheme(req Request) (*url.URL, error) {
	portfw := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", req.Pod.Namespace, req.Pod.Name)

	base, err := url.Parse(req.RestConfig.Host)
	if err != nil {
		return nil, fmt.Errorf("error parsing base URL: %w", err)
	}
	if base.Scheme == "" {
		base.Scheme = "https"
	}

	return &url.URL{Scheme: base.Scheme, Host: base.Host, Path: path.Join(base.Path, portfw)}, nil
}
