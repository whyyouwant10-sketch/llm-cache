package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	llmcache "github.com/whyyouwant10-sketch/llm-cache"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "proxy":
		proxy(os.Args[2:])
	case "stats":
		stats(os.Args[2:])
	case "inspect":
		inspect(os.Args[2:])
	case "clear":
		clear(os.Args[2:])
	case "prune":
		prune(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}
func usage() { fmt.Fprintln(os.Stderr, "usage: llm-cache {proxy|stats|inspect|clear|prune} [flags]") }
func defaultDB() string {
	if home, err := os.UserHomeDir(); err == nil {
		return home + "/.cache/llm-cache/cache.sqlite"
	}
	return "cache.sqlite"
}
func proxy(args []string) {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8787", "listen address")
	upstream := fs.String("upstream", "", "upstream base URL")
	dbPath := fs.String("db", defaultDB(), "SQLite database path")
	ttl := fs.Duration("ttl", llmcache.DefaultTTL, "cache TTL")
	pricingPath := fs.String("pricing", "", "JSON model pricing file")
	fs.Parse(args)
	if *upstream == "" {
		log.Fatal("--upstream is required")
	}
	base, err := url.Parse(*upstream)
	if err != nil {
		log.Fatal(err)
	}
	pricing, err := readPricing(*pricingPath)
	if err != nil {
		log.Fatal(err)
	}
	store, err := llmcache.Open(*dbPath, llmcache.StoreConfig{Pricing: pricing})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	if err = llmcache.ValidateTTL(*ttl); err != nil {
		log.Fatal(err)
	}
	handler := proxyHandler(base, store, *ttl)
	log.Printf("listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, handler))
}

func proxyHandler(base *url.URL, store llmcache.Store, ttl time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := *base
		target.Path = strings.TrimRight(base.Path, "/") + r.URL.Path
		target.RawQuery = r.URL.RawQuery
		out := r.Clone(r.Context())
		out.URL = &target
		out.Host = target.Host
		resp, err := (&llmcache.Transport{Next: http.DefaultTransport, Store: store, TTL: ttl}).RoundTrip(out)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		for k, values := range resp.Header {
			for _, v := range values {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = ioCopy(w, resp.Body)
	})
}
func ioCopy(dst http.ResponseWriter, src io.Reader) (int64, error) { return io.Copy(dst, src) }
func stats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "SQLite database path")
	jsonOut := fs.Bool("json", false, "JSON output")
	pricingPath := fs.String("pricing", "", "JSON model pricing file")
	since := fs.String("since", "", "inclusive UTC date or timestamp")
	until := fs.String("until", "", "inclusive UTC date or timestamp")
	fs.Parse(args)
	pricing, err := readPricing(*pricingPath)
	if err != nil {
		log.Fatal(err)
	}
	store, err := llmcache.Open(*dbPath, llmcache.StoreConfig{Pricing: pricing})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	filter, err := statsFilter(*since, *until)
	if err != nil {
		log.Fatal(err)
	}
	value, err := store.Stats(context.Background(), filter)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(value)
		return
	}
	fmt.Printf("lookups: %d\nhits: %d\nmisses: %d\nbypasses: %d\nupstream errors: %d\nhit rate: %.2f%%\ntokens saved: %d\ncost saved (micro-USD): %d\npricing known: %t\n", value.Lookups, value.Hits, value.Misses, value.Bypasses, value.UpstreamErrors, value.HitRate*100, value.TokensSaved, value.CostSavedMicroUSD, value.CostPricingKnown)
}

func inspect(args []string) {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "SQLite database path")
	model := fs.String("model", "", "model filter")
	endpoint := fs.String("endpoint", "", "endpoint filter")
	prefix := fs.String("key-prefix", "", "key prefix filter")
	jsonOut := fs.Bool("json", false, "JSON output")
	fs.Parse(args)
	store, err := llmcache.Open(*dbPath, llmcache.StoreConfig{})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	items, err := store.Inspect(context.Background(), llmcache.Filter{Model: *model, Endpoint: *endpoint, KeyPrefix: *prefix})
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		if err := json.NewEncoder(os.Stdout).Encode(items); err != nil {
			log.Fatal(err)
		}
		return
	}
	for _, item := range items {
		fmt.Printf("%s %s %s %d bytes=%d hits=%d expires=%s\n", item.CacheKey, item.Method, item.Path, item.StatusCode, item.ResponseBytes, item.HitCount, item.ExpiresAt.Format(time.RFC3339))
	}
}

func readPricing(path string) (map[string]llmcache.Pricing, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pricing map[string]llmcache.Pricing
	if err := json.Unmarshal(data, &pricing); err != nil {
		return nil, fmt.Errorf("pricing: %w", err)
	}
	return pricing, nil
}

func statsFilter(since, until string) (llmcache.StatsFilter, error) {
	parse := func(value string) (*time.Time, error) {
		if value == "" {
			return nil, nil
		}
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			parsed, err = time.Parse("2006-01-02", value)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid stats time %q", value)
		}
		return &parsed, nil
	}
	a, err := parse(since)
	if err != nil {
		return llmcache.StatsFilter{}, err
	}
	b, err := parse(until)
	if err != nil {
		return llmcache.StatsFilter{}, err
	}
	return llmcache.StatsFilter{Since: a, Until: b}, nil
}
func clear(args []string) {
	fs := flag.NewFlagSet("clear", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "SQLite database path")
	model := fs.String("model", "", "model filter")
	endpoint := fs.String("endpoint", "", "endpoint filter")
	prefix := fs.String("key-prefix", "", "key prefix filter")
	apply := fs.Bool("apply", false, "apply deletion")
	dry := fs.Bool("dry-run", false, "preview only")
	include := fs.Bool("include-stats", false, "also clear stats")
	fs.Parse(args)
	if *apply && *dry {
		log.Fatal("--apply and --dry-run cannot be combined")
	}
	if *include && !*apply {
		log.Fatal("--include-stats requires --apply")
	}
	store, err := llmcache.Open(*dbPath, llmcache.StoreConfig{})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	plan, err := store.PlanClear(context.Background(), llmcache.Filter{Model: *model, Endpoint: *endpoint, KeyPrefix: *prefix, IncludeStats: *include})
	if err != nil {
		log.Fatal(err)
	}
	printPlan(plan)
	if *apply {
		if err = store.ApplyClear(context.Background(), plan); err != nil {
			log.Fatal(err)
		}
		fmt.Println("applied")
	}
}
func prune(args []string) {
	fs := flag.NewFlagSet("prune", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "SQLite database path")
	expired := fs.Bool("expired", true, "remove expired entries")
	older := fs.Duration("older-than", 0, "remove entries older than duration")
	maxBytes := fs.Int64("max-bytes", 0, "retain at most this many bytes")
	apply := fs.Bool("apply", false, "apply deletion")
	dry := fs.Bool("dry-run", false, "preview only")
	fs.Parse(args)
	if *apply && *dry {
		log.Fatal("--apply and --dry-run cannot be combined")
	}
	store, err := llmcache.Open(*dbPath, llmcache.StoreConfig{})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	plan, err := store.PlanPrune(context.Background(), llmcache.PrunePolicy{Expired: *expired, OlderThan: *older, MaxBytes: *maxBytes})
	if err != nil {
		log.Fatal(err)
	}
	printPlan(plan)
	if *apply {
		if err = store.ApplyPrune(context.Background(), plan); err != nil {
			log.Fatal(err)
		}
		fmt.Println("applied")
	}
}
func printPlan(plan llmcache.Plan) {
	fmt.Printf("%s: %d entries, %d bytes\n", plan.Operation, plan.Count, plan.Bytes)
}
