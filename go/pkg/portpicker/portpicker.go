// Package portpicker allows Go programs and tests to receive the best guess of
// an unused port that may be used for ad hoc purposes.
package portpicker

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var (
	// A private random number generator to avoid messing with any program determinism.
	rng = struct {
		sync.Mutex
		*rand.Rand
	}{Rand: rand.New(rand.NewSource(time.Now().UnixNano() + int64(syscall.Getpid())))}

	// errNoUnusedPort is the error returned when an unused port is not found.
	errNoUnusedPort = errors.New("portpicker: no unused port")

	// Ports that have previously been assigned by portserver to this process.
	served struct {
		sync.Mutex
		m map[int]bool // port => {true if in use, false if recycled}
	}
)

const (
	// Range of port numbers allocated by portpicker
	minPort = 32768
	maxPort = 60000
)

func isPortFree(port int) bool {
	return isPortTypeFree(port, syscall.SOCK_STREAM) &&
		isPortTypeFree(port, syscall.SOCK_DGRAM)
}

func isPortTypeFree(port, typ int) bool {
	// For the port to be considered available, the kernel must support at
	// least one of (IPv6, IPv4), and the port must be available on each
	// supported family.
	var probes = []struct {
		family int
		addr   syscall.Sockaddr
	}{
		{syscall.AF_INET6, &syscall.SockaddrInet6{Port: port}},
		{syscall.AF_INET, &syscall.SockaddrInet4{Port: port}},
	}
	gotSocket := false
	var socketErrs []error
	for _, probe := range probes {
		// We assume that Socket will succeed iff the kernel supports this
		// address family.
		fd, err := syscall.Socket(probe.family, typ, 0)
		if err != nil {
			socketErrs = append(socketErrs, err)
			continue
		}
		// Now that we have a socket, any subsequent error means the port is unavailable.
		gotSocket = true
		err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		if err != nil {
			log.Printf("portpicker: failed to set SO_REUSEADDR: %v", err)
			syscall.Close(fd)
			return false
		}
		err = syscall.Bind(fd, probe.addr)
		syscall.Close(fd)
		if err != nil {
			return false
		}
	}
	// At this point, all supported families have bound successfully,
	// but we still need to check that at least one family was supported.
	if !gotSocket {
		log.Printf("portpicker: failed to create sockets: %q", socketErrs)
	}
	return gotSocket
}

func queryPortServer(addr string) int {
	if addr == "" {
		addr = "@google-unittest-portserver"
	}
	// TODO(dsymonds): NUL-byte shenanigans?
	c, err := net.Dial("unix", addr)
	if err != nil {
		// failed connection to portserver; this is normal in many circumstances.
		return -1
	}
	defer c.Close()
	if _, err := fmt.Fprintf(c, "%d\n", os.Getpid()); err != nil {
		log.Printf("portpicker: failed writing request to portserver: %v", err)
		return -1
	}
	buf, err := io.ReadAll(c)
	if err != nil || len(buf) == 0 {
		log.Printf("portpicker: failed reading response from portserver: %v", err)
		return -1
	}
	buf = buf[:len(buf)-1] // remove newline char
	port, err := strconv.Atoi(string(buf))
	if err != nil {
		log.Printf("portpicker: bad response from portserver: %v", err)
		return -1
	}

	served.Lock()
	if served.m == nil {
		served.m = make(map[int]bool)
	}
	served.m[port] = true
	served.Unlock()

	return port
}

// RecycleUnusedPort recycles a port obtained by PickUnusedPort after it is no
// longer needed.
func RecycleUnusedPort(port int) error {
	served.Lock()
	defer served.Unlock()

	// We only recycle ports from the port server since we expect to own those
	// ports until the current process dies.
	if _, ok := served.m[port]; !ok {
		return nil
	}
	if !isPortFree(port) {
		return fmt.Errorf("portpicker: recycling port still in use: %d", port)
	}
	if !served.m[port] {
		return fmt.Errorf("portpicker: double recycle for port: %d", port)
	}
	served.m[port] = false
	return nil
}

func recycledPort() int {
	served.Lock()
	defer served.Unlock()

	for port, inuse := range served.m {
		if !inuse {
			served.m[port] = true
			return port
		}
	}
	return 0
}

// PickUnusedPort returns a port number that is not currently bound.
// There is an inherent race condition between this function being called and
// any other process on the same computer, so the caller should bind to the port as soon as possible.
func PickUnusedPort() (port int, err error) {
	// Try PortServer first.
	portserver := os.Getenv("PORTSERVER_ADDRESS")
	if portserver != "" {
		if port = recycledPort(); port > 0 {
			return port, nil
		}
		if port = queryPortServer(portserver); port > 0 {
			return port, nil
		}
		log.Printf("portpicker: portserver configured, but couldn't get a port from it. Already got %d ports. Perhaps the pool is exhausted.",
			len(served.m))
		return 0, errNoUnusedPort
	}

	// Start with random port in range [32768, 60000]
	rng.Lock()
	port = minPort + rng.Intn(maxPort-minPort+1)
	rng.Unlock()

	// Test entire range of ports
	stop := port
	for {
		if isPortFree(port) {
			return port, nil
		}
		port++
		if port > maxPort {
			port = minPort
		}
		if port == stop {
			break
		}
	}

	return 0, errNoUnusedPort
}

// TB is the minimal viable interface that captures testing.TB.  It accepts
// implementations of
//
//	*testing.B
//	*testing.T
//
// This interface is introduced for the sole reason that portpicker is not a
// testonly library, and we want to avoid having non-testing code suddenly
// depend on package testing.  In short, per https://go-proverbs.github.io
// "A little bit of copying is better than a little dependency."
type TB interface {
	// Helper corresponds to (testing.TB).Helper.
	Helper()
	// Helper corresponds to (testing.TB).Cleanup.
	Cleanup(func())
	// Helper corresponds to (testing.TB).Fatalf.
	Fatalf(string, ...interface{})
	// Helper corresponds to (testing.TB).Logf.
	Logf(string, ...interface{})
}

// PickUnusedPortTB is a package testing-aware variant of PickUnusedPort that
// treats provisioning errors as fatal and handles port releasing for the user.
//
//	func TestSmoke(t *testing.T) {
//	  backend := startTestBackendOnPort(portpicker.PickUnusedPortTB(t))
//
//	  // test backend
//	}
//
// The returned port should NOT be manually returned with RecycleUnusedPort.
//
// # Caution
//
// Port release failures do not result in test failures but leave error message
// in test logs. Investigate logs to ensure that your servers under test (SUT)
// terminate when the test is done.
func PickUnusedPortTB(tb TB) int {
	tb.Helper()
	port, err := PickUnusedPort()
	if err != nil {
		tb.Fatalf("could not pick a port: %v", err)
	}
	tb.Cleanup(func() {
		if err := RecycleUnusedPort(port); err != nil {
			tb.Logf("could not return port %v: %v", port, err)
		}
	})
	return port
}
