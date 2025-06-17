package metrics

import (
	"fmt"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"net/http"
	"os"
	"time"
)

var (
	RootScope       tally.Scope
	ThroughputScope tally.Scope
	ResptimeScope   tally.Scope
	PromReporter    promreporter.Reporter
)

func InitializeMetrics() {
	// TODO handle the closer
	benchmarkName := os.Getenv("BENCHMARK_NAME")
	if benchmarkName == "" {
		benchmarkName = "UnnamedBenchmark"
	}
	PromReporter = promreporter.NewReporter(promreporter.Options{})
	RootScope, _ = tally.NewRootScope(tally.ScopeOptions{
		Tags: map[string]string{
			"benchmark_name": benchmarkName,
		},
		Prefix:         "ycsb",
		CachedReporter: PromReporter,
		Separator:      promreporter.DefaultSeparator,
	}, 1*time.Second)

	ThroughputScope = RootScope.SubScope("throughput")
	ResptimeScope = RootScope.SubScope("response")
}

func ServeHTTP() {
	err := http.ListenAndServe(":8080", PromReporter.HTTPHandler())
	if err != nil {
		fmt.Println("Error initializing HTTP server for metrics")
		os.Exit(1)
	} else {
		fmt.Println("Successfully initialized HTTP server for metrics")
	}
}
