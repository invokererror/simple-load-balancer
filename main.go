package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

type Backend struct {
	URL				*url.URL
	Alive			bool
	mux				sync.RWMutex
	ReverseProxy	*httputil.ReverseProxy
}

type ServerPool struct {
	backends	[]*Backend
	current		uint64
	url2Backend	map[string]*Backend
}

// u,_ := url.Parse("http://localhost:8080")
// rp := httputil.NewSingleHostReverseProxy(u)

// // init rp
// http.HandlerFunc(rp.ServeHTTP)

func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

func (s *ServerPool) GetNextPeer() *Backend {
	numOfServers := len(s.backends)
	idx := s.NextIndex()
	for it := 0; it < numOfServers; it++ {
		if s.backends[idx].IsAlive() {
			/* set current pointer */
			if uint64(idx) != s.current {
				atomic.StoreUint64(&s.current, uint64(idx))
			}
			return s.backends[idx]
		}

		idx++;
		if idx >= numOfServers {
			idx %= numOfServers
		}
	}
	return nil
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mux.RLock()
	ans := b.Alive
	b.mux.RUnlock()
	return ans
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}

func (s *ServerPool) MarkBackendStatus(backendUrl *url.URL, alive bool) {
	bkd, in := s.url2Backend[backendUrl.String()]
	if !in {
		log.Fatal("cannot find, among backends, url ", backendUrl)
	}
	bkd.SetAlive(alive)
}

/**
 * passive health checking
 */
func testBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	if err != nil {
		log.Println("Site unreachable, error: ", err)
		return false
	}
	defer conn.Close()
	return true
}

/* passive */
func (s *ServerPool) HealthCheck() {
	for _, bkd := range s.backends {
		hasHeartbeat := testBackendAlive(bkd.URL)
		bkd.SetAlive(hasHeartbeat)
		statusLog := "up"
		if !hasHeartbeat {
			statusLog = "down"
		}
		log.Printf("%s [%s]\n", bkd.URL, statusLog)
	}
}

func getAttemptsFromContext(req *http.Request) int {
	ans, in := req.Context().Value(Attempts).(int)
	if !in {
		return 0
	}
	return ans
}

func getRetryFromContext(req *http.Request) int {
	ans, in := req.Context().Value(Retry).(int)
	if !in {
		return 0
	}
	return ans
}

/**
 * load balancing
 */
func lb(rw http.ResponseWriter, req *http.Request) {
	attempts := getAttemptsFromContext(req)
	if attempts > 3 {
		log.Printf("%s(%s) exceeding max attempts, terminating\n", req.RemoteAddr, req.URL.Path)
		http.Error(rw, "service not available", http.StatusServiceUnavailable)
		return
	}
	log.Printf("load balancer at %s—%s(%s) attempting attempt #%d\n", req.Host, req.RemoteAddr, req.URL.Path, attempts)
	maybeAvailablePeer := theServerPool.GetNextPeer()
	if maybeAvailablePeer != nil {
		maybeAvailablePeer.ReverseProxy.ServeHTTP(rw, req)
		return
	}
	http.Error(rw, "Service Not Available", http.StatusServiceUnavailable)
}


var theServerPool ServerPool

/* goroutine */
func healthCheck() {
	ticker := time.NewTicker(time.Second * 30)
	for {
		select {
		case <-ticker.C:
			log.Println("Starting health check...")
			theServerPool.HealthCheck()
			log.Println("Health check completed")
		}
	}
}

func main() {
	var serversArg string
	var port int

	theServerPool.url2Backend = make(map[string]*Backend)
	flag.StringVar(&serversArg, "backends", "", "multiple comma-separated backend URLs")
	flag.IntVar(&port, "port", 3030, "port to which load balancer listens")
	flag.Parse()

	serverURLs := strings.Split(serversArg, ",")
	for _, serverURLStr := range serverURLs {
		serverURL, err := url.Parse(serverURLStr)
		if err != nil {
			log.Fatal("illegal url parsed. error: ", err)
		}

		rp := httputil.NewSingleHostReverseProxy(serverURL)
		rp.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
			// q: maybe set rp.ErrorLog?
			log.Printf("%s [%s]\n", serverURL.Host, err.Error())
			retriedTimes := getRetryFromContext(req)
			if retriedTimes < 3 {
				select {
				case <-time.After(time.Millisecond * 10):
					ctx := req.Context()
					rp.ServeHTTP(rw, req.WithContext(context.WithValue(ctx, Retry, retriedTimes+1)))
				}
				return
			}
			theServerPool.MarkBackendStatus(serverURL, false)
			log.Printf("%s exceeding retry attempts\n", serverURL.Host)
			newCtx := req.Context()
			newCtx = context.WithValue(newCtx, Attempts, getAttemptsFromContext(req)+1)
			newCtx = context.WithValue(newCtx, Retry, 0)
			lb(rw, req.WithContext(newCtx))
		}

		bkd := &Backend{
			URL: serverURL,
			Alive: true,
			ReverseProxy: rp,
		}
		theServerPool.AddBackend(bkd)
		theServerPool.url2Backend[serverURL.String()] = bkd
		log.Printf("Configured server %s\n", serverURL.Host)
	}

	loadBalancer := http.Server{
		Addr: fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	go healthCheck()

	/* testing messages, always assume passed backends are :60000 and :60001 */
	http.HandleFunc("/", func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintf(rw, "%s received request from %s!\n", req.Host, req.RemoteAddr)
	})
	go http.ListenAndServe(":60000", nil)
	go http.ListenAndServe(":60001", nil)

	log.Printf("load balancer started at port %d\n", port)
	if err := loadBalancer.ListenAndServe(); err != nil {
		log.Fatal("load balancer error: ", err)
	}
}
