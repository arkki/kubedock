package portforward

// Source: https://github.com/gianarb/kube-port-forward

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sync"
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

// ToPod will portforward to given pod.
func ToPod(req Request) error {
	const pfLifetime = 10 * time.Minute

	// Build SPDY round tripper/dialer from RestConfig
	transport, upgrader, err := spdy.RoundTripperFor(req.RestConfig)
	if err != nil {
		return err
	}

	img := findImageForPort(req.Pod, req.PodPort)
	klog.Infof("start port-forward %s/%s [%s] %d->%d", req.Pod.Namespace, req.Pod.Name, img, req.LocalPort, req.PodPort)

	url, err := getURLScheme(req)
	if err != nil {
		return err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)

	// Enforce a hard lifetime while still honoring external StopCh
	localStop := make(chan struct{})
	var once sync.Once
	closeLocal := func() { once.Do(func() { close(localStop) }) }

	go func() {
		select {
		case <-req.StopCh:
			closeLocal()
		case <-time.After(pfLifetime):
			klog.Warningf("port-forward: killed after lifetime limit [%d->%d]", req.LocalPort, req.PodPort)
			closeLocal()
		}
	}()

	ports := []string{fmt.Sprintf("%d:%d", req.LocalPort, req.PodPort)}
	logr := NewLogger()
	fw, err := portforward.New(dialer, ports, localStop, req.ReadyCh, logr, logr)
	if err != nil {
		return err
	}

	return fw.ForwardPorts()
}

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
