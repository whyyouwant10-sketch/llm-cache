package llmcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	DefaultTTL         = 24 * time.Hour
	MaxTTL             = 30 * 24 * time.Hour
	DefaultMaxRequest  = 1 << 20
	DefaultMaxResponse = 10 << 20
	DefaultMaxDatabase = 1 << 30
)

type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

type Entry struct {
	Key              Key
	StatusCode       int
	Header           http.Header
	Body             []byte
	ResponseBytes    int64
	StoredAt         time.Time
	ExpiresAt        time.Time
	LastHitAt        *time.Time
	HitCount         int64
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	EstimatedCost    int64
}

type Pricing struct {
	InputMicroUSD  int64 `json:"input_micro_usd_per_token"`
	OutputMicroUSD int64 `json:"output_micro_usd_per_token"`
}

type StoreConfig struct {
	Path             string
	Clock            func() time.Time
	Pricing          map[string]Pricing
	MaxDatabaseBytes int64
	MaxResponseBytes int64
}

type Filter struct {
	Model        string
	Endpoint     string
	KeyPrefix    string
	IncludeStats bool
}

type PrunePolicy struct {
	Expired         bool
	OlderThan       time.Duration
	MaxBytes        int64
	EventsOlderThan time.Duration
}

type Plan struct {
	Operation    string      `json:"operation"`
	Count        int64       `json:"count"`
	Bytes        int64       `json:"bytes"`
	Keys         []string    `json:"-"`
	Filter       Filter      `json:"filter,omitempty"`
	Policy       PrunePolicy `json:"policy,omitempty"`
	IncludeStats bool        `json:"include_stats,omitempty"`
}

type StatsFilter struct {
	Since *time.Time
	Until *time.Time
}

type Stats struct {
	Lookups           int64   `json:"lookups"`
	Hits              int64   `json:"hits"`
	Misses            int64   `json:"misses"`
	Bypasses          int64   `json:"bypasses"`
	UpstreamErrors    int64   `json:"upstream_errors"`
	TokensSaved       int64   `json:"tokens_saved"`
	CostSavedMicroUSD int64   `json:"cost_saved_microusd"`
	CostPricingKnown  bool    `json:"cost_pricing_known"`
	HitRate           float64 `json:"hit_rate"`
}

type Inspection struct {
	CacheKey      string     `json:"cache_key"`
	KeyVersion    int        `json:"key_version"`
	Method        string     `json:"method"`
	Path          string     `json:"path"`
	Model         string     `json:"model,omitempty"`
	StatusCode    int        `json:"status_code"`
	ResponseBytes int64      `json:"response_bytes"`
	StoredAt      time.Time  `json:"stored_at"`
	ExpiresAt     time.Time  `json:"expires_at"`
	LastHitAt     *time.Time `json:"last_hit_at,omitempty"`
	HitCount      int64      `json:"hit_count"`
}

type Store interface {
	Lookup(context.Context, Key) (Entry, bool, error)
	Put(context.Context, Key, Response, time.Duration) error
	PlanClear(context.Context, Filter) (Plan, error)
	ApplyClear(context.Context, Plan) error
	PlanPrune(context.Context, PrunePolicy) (Plan, error)
	ApplyPrune(context.Context, Plan) error
	Stats(context.Context, StatsFilter) (Stats, error)
}

type eventRecorder interface {
	Record(context.Context, string) error
}

type SQLiteStore struct {
	db          *sql.DB
	path        string
	clock       func() time.Time
	pricing     map[string]Pricing
	maxDB       int64
	maxResponse int64
	mu          sync.Mutex
}

func Open(path string, config StoreConfig) (*SQLiteStore, error) {
	if path == "" {
		path = config.Path
	}
	if path == "" {
		return nil, errors.New("database path is required")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if dir != "." {
		_ = os.Chmod(dir, 0700)
	}
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000&_foreign_keys=on&_journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	store := &SQLiteStore{db: db, path: path, clock: config.Clock, pricing: config.Pricing, maxDB: config.MaxDatabaseBytes, maxResponse: config.MaxResponseBytes}
	if store.clock == nil {
		store.clock = time.Now
	}
	if store.maxDB == 0 {
		store.maxDB = DefaultMaxDatabase
	}
	if store.maxResponse == 0 {
		store.maxResponse = DefaultMaxResponse
	}
	if err := store.init(); err != nil {
		db.Close()
		return nil, err
	}
	_ = os.Chmod(path, 0600)
	return store, nil
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) init() error {
	const schema = `
CREATE TABLE IF NOT EXISTS cache_entries (
 cache_key TEXT PRIMARY KEY, key_version INTEGER NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL,
 model TEXT, request_sha256 TEXT NOT NULL, request_body BLOB, status_code INTEGER NOT NULL,
 response_headers_json TEXT NOT NULL, response_body BLOB NOT NULL, response_bytes INTEGER NOT NULL,
 stored_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, last_hit_at INTEGER, hit_count INTEGER NOT NULL DEFAULT 0,
 prompt_tokens INTEGER, completion_tokens INTEGER, total_tokens INTEGER, estimated_cost_microusd INTEGER NOT NULL DEFAULT 0,
 CHECK(length(cache_key)=64), CHECK(response_bytes=length(response_body)), CHECK(expires_at>=stored_at), CHECK(hit_count>=0));
CREATE INDEX IF NOT EXISTS cache_entries_expiry ON cache_entries(expires_at);
CREATE INDEX IF NOT EXISTS cache_entries_lru ON cache_entries(last_hit_at,stored_at);
CREATE INDEX IF NOT EXISTS cache_entries_model ON cache_entries(model);
CREATE TABLE IF NOT EXISTS stats_daily (
 day_utc TEXT PRIMARY KEY, lookups INTEGER NOT NULL DEFAULT 0, hits INTEGER NOT NULL DEFAULT 0,
 misses INTEGER NOT NULL DEFAULT 0, bypasses INTEGER NOT NULL DEFAULT 0, upstream_errors INTEGER NOT NULL DEFAULT 0,
 tokens_saved INTEGER NOT NULL DEFAULT 0, cost_saved_microusd INTEGER NOT NULL DEFAULT 0,
 CHECK(lookups>=0 AND hits>=0 AND misses>=0 AND bypasses>=0 AND upstream_errors>=0 AND tokens_saved>=0 AND cost_saved_microusd>=0));
CREATE TABLE IF NOT EXISTS meta(name TEXT PRIMARY KEY,value TEXT NOT NULL);`
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err = tx.Exec(schema); err != nil {
		tx.Rollback()
		return err
	}
	var version int
	if err = tx.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		tx.Rollback()
		return err
	}
	if version == 0 {
		if _, err = tx.Exec("PRAGMA user_version = 1"); err != nil {
			tx.Rollback()
			return err
		}
	} else if version != 1 {
		tx.Rollback()
		return fmt.Errorf("unsupported schema version %d", version)
	}
	return tx.Commit()
}

func (s *SQLiteStore) Lookup(ctx context.Context, key Key) (Entry, bool, error) {
	now := s.clock().UTC()
	row := s.db.QueryRowContext(ctx, `SELECT key_version,method,path,model,request_sha256,status_code,response_headers_json,response_body,response_bytes,stored_at,expires_at,last_hit_at,hit_count,prompt_tokens,completion_tokens,total_tokens,estimated_cost_microusd FROM cache_entries WHERE cache_key=?`, key.CacheKey)
	var e Entry
	var headers string
	var model, requestSHA string
	var last sql.NullInt64
	var prompt, completion, total sql.NullInt64
	var storedAt, expiresAt int64
	if err := row.Scan(&e.Key.KeyVersion, &e.Key.Method, &e.Key.Path, &model, &requestSHA, &e.StatusCode, &headers, &e.Body, &e.ResponseBytes, &storedAt, &expiresAt, &last, &e.HitCount, &prompt, &completion, &total, &e.EstimatedCost); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Entry{}, false, nil
		}
		return Entry{}, false, err
	}
	e.Key.CacheKey = key.CacheKey
	e.Key.Model = model
	e.Key.RequestSHA256 = requestSHA
	e.StoredAt = time.Unix(storedAt, 0).UTC()
	e.ExpiresAt = time.Unix(expiresAt, 0).UTC()
	if last.Valid {
		t := time.Unix(last.Int64, 0).UTC()
		e.LastHitAt = &t
	}
	if prompt.Valid {
		e.PromptTokens = prompt.Int64
	}
	if completion.Valid {
		e.CompletionTokens = completion.Int64
	}
	if total.Valid {
		e.TotalTokens = total.Int64
	}
	if err := json.Unmarshal([]byte(headers), &e.Header); err != nil {
		return Entry{}, false, err
	}
	if !now.Before(e.ExpiresAt) {
		return e, false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Entry{}, false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE cache_entries SET hit_count=hit_count+1,last_hit_at=? WHERE cache_key=? AND expires_at>?", now.Unix(), key.CacheKey, now.Unix()); err != nil {
		return Entry{}, false, err
	}
	cost := e.EstimatedCost
	if _, err = s.updateStats(ctx, tx, now, "hit", e.TotalTokens, cost); err != nil {
		return Entry{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Entry{}, false, err
	}
	e.HitCount++
	e.LastHitAt = &now
	return e, true, nil
}

func (s *SQLiteStore) Put(ctx context.Context, key Key, response Response, ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}
	if ttl < time.Second || ttl > MaxTTL {
		return fmt.Errorf("ttl must be 0 or between 1s and 720h")
	}
	if int64(len(response.Body)) > s.maxResponse {
		return errors.New("response exceeds cache limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("only successful responses may be cached")
	}
	if _, err := CanonicalizeJSON(response.Body); err != nil {
		return fmt.Errorf("response is not valid JSON: %w", err)
	}
	if s.databaseBytes() >= s.maxDB {
		return errors.New("database size limit reached")
	}
	now := s.clock().UTC().Truncate(time.Second)
	expires := now.Add(ttl)
	headers, err := json.Marshal(safeHeaders(response.Header))
	if err != nil {
		return err
	}
	prompt, completion, total := responseUsage(response.Body)
	cost := s.estimate(key.Model, prompt, completion)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO cache_entries(cache_key,key_version,method,path,model,request_sha256,request_body,status_code,response_headers_json,response_body,response_bytes,stored_at,expires_at,last_hit_at,hit_count,prompt_tokens,completion_tokens,total_tokens,estimated_cost_microusd) VALUES(?,?,?,?,?,?,NULL,?,?,?,?,?,?,NULL,0,?,?,?,?) ON CONFLICT(cache_key) DO UPDATE SET key_version=excluded.key_version,method=excluded.method,path=excluded.path,model=excluded.model,request_sha256=excluded.request_sha256,status_code=excluded.status_code,response_headers_json=excluded.response_headers_json,response_body=excluded.response_body,response_bytes=excluded.response_bytes,stored_at=excluded.stored_at,expires_at=excluded.expires_at,last_hit_at=NULL,hit_count=0,prompt_tokens=excluded.prompt_tokens,completion_tokens=excluded.completion_tokens,total_tokens=excluded.total_tokens,estimated_cost_microusd=excluded.estimated_cost_microusd`, key.CacheKey, key.KeyVersion, key.Method, key.Path, key.Model, key.RequestSHA256, response.StatusCode, string(headers), response.Body, len(response.Body), now.Unix(), expires.Unix(), nullableInt(prompt), nullableInt(completion), nullableInt(total), cost)
	if err != nil {
		return err
	}
	if _, err = s.updateStats(ctx, tx, now, "miss", 0, 0); err != nil {
		return err
	}
	return tx.Commit()
}

func nullableInt(v int64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

func safeHeaders(h http.Header) http.Header {
	out := make(http.Header)
	for _, name := range []string{"Content-Type", "Cache-Control", "ETag", "Content-Encoding"} {
		if values := h.Values(name); len(values) > 0 {
			out[name] = append([]string(nil), values...)
		}
	}
	return out
}

func responseUsage(body []byte) (int64, int64, int64) {
	var payload struct {
		Usage struct {
			PromptTokens     json.Number `json:"prompt_tokens"`
			CompletionTokens json.Number `json:"completion_tokens"`
			TotalTokens      json.Number `json:"total_tokens"`
		} `json:"usage"`
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	if dec.Decode(&payload) != nil {
		return 0, 0, 0
	}
	parse := func(n json.Number) int64 {
		v, err := n.Int64()
		if err != nil || v < 0 {
			return 0
		}
		return v
	}
	return parse(payload.Usage.PromptTokens), parse(payload.Usage.CompletionTokens), parse(payload.Usage.TotalTokens)
}

func (s *SQLiteStore) estimate(model string, prompt, completion int64) int64 {
	p, ok := s.pricing[model]
	if !ok {
		return 0
	}
	return prompt*p.InputMicroUSD + completion*p.OutputMicroUSD
}

func (s *SQLiteStore) updateStats(ctx context.Context, tx *sql.Tx, now time.Time, event string, tokens, cost int64) (sql.Result, error) {
	day := now.UTC().Format("2006-01-02")
	if _, err := tx.ExecContext(ctx, "INSERT INTO stats_daily(day_utc) VALUES(?) ON CONFLICT(day_utc) DO NOTHING", day); err != nil {
		return nil, err
	}
	switch event {
	case "hit":
		return tx.ExecContext(ctx, "UPDATE stats_daily SET lookups=lookups+1,hits=hits+1,tokens_saved=tokens_saved+?,cost_saved_microusd=cost_saved_microusd+? WHERE day_utc=?", tokens, cost, day)
	case "miss":
		return tx.ExecContext(ctx, "UPDATE stats_daily SET lookups=lookups+1,misses=misses+1 WHERE day_utc=?", day)
	case "error":
		return tx.ExecContext(ctx, "UPDATE stats_daily SET lookups=lookups+1,misses=misses+1,upstream_errors=upstream_errors+1 WHERE day_utc=?", day)
	case "bypass":
		return tx.ExecContext(ctx, "UPDATE stats_daily SET bypasses=bypasses+1 WHERE day_utc=?", day)
	}
	return nil, nil
}

func (s *SQLiteStore) Record(ctx context.Context, event string) error {
	now := s.clock().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = s.updateStats(ctx, tx, now, event, 0, 0); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) Stats(ctx context.Context, filter StatsFilter) (Stats, error) {
	query := `SELECT COALESCE(SUM(lookups),0),COALESCE(SUM(hits),0),COALESCE(SUM(misses),0),COALESCE(SUM(bypasses),0),COALESCE(SUM(upstream_errors),0),COALESCE(SUM(tokens_saved),0),COALESCE(SUM(cost_saved_microusd),0) FROM stats_daily WHERE 1=1`
	args := []interface{}{}
	if filter.Since != nil {
		query += " AND day_utc>=?"
		args = append(args, filter.Since.UTC().Format("2006-01-02"))
	}
	if filter.Until != nil {
		query += " AND day_utc<=?"
		args = append(args, filter.Until.UTC().Format("2006-01-02"))
	}
	var out Stats
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&out.Lookups, &out.Hits, &out.Misses, &out.Bypasses, &out.UpstreamErrors, &out.TokensSaved, &out.CostSavedMicroUSD)
	if err != nil {
		return out, err
	}
	if out.Lookups > 0 {
		out.HitRate = float64(out.Hits) / float64(out.Lookups)
	}
	out.CostPricingKnown = len(s.pricing) > 0
	return out, nil
}

func (s *SQLiteStore) Inspect(ctx context.Context, filter Filter) ([]Inspection, error) {
	where, args := filterWhere(filter)
	rows, err := s.db.QueryContext(ctx, `SELECT cache_key,key_version,method,path,model,status_code,response_bytes,stored_at,expires_at,last_hit_at,hit_count FROM cache_entries WHERE `+where+` ORDER BY stored_at DESC,cache_key`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Inspection
	for rows.Next() {
		var item Inspection
		var model string
		var stored, expires int64
		var last sql.NullInt64
		if err := rows.Scan(&item.CacheKey, &item.KeyVersion, &item.Method, &item.Path, &model, &item.StatusCode, &item.ResponseBytes, &stored, &expires, &last, &item.HitCount); err != nil {
			return nil, err
		}
		item.Model = model
		item.StoredAt = time.Unix(stored, 0).UTC()
		item.ExpiresAt = time.Unix(expires, 0).UTC()
		if last.Valid {
			t := time.Unix(last.Int64, 0).UTC()
			item.LastHitAt = &t
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) PlanClear(ctx context.Context, filter Filter) (Plan, error) {
	where, args := filterWhere(filter)
	plan, err := s.planKeys(ctx, "clear", where, args, filter, nil)
	plan.IncludeStats = filter.IncludeStats
	return plan, err
}
func (s *SQLiteStore) planKeys(ctx context.Context, operation, where string, args []interface{}, filter Filter, policy *PrunePolicy) (Plan, error) {
	q := `SELECT cache_key,response_bytes FROM cache_entries WHERE ` + where + ` ORDER BY cache_key`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return Plan{}, err
	}
	defer rows.Close()
	p := Plan{Operation: operation, Filter: filter}
	if policy != nil {
		p.Policy = *policy
	}
	for rows.Next() {
		var key string
		var bytes int64
		if err := rows.Scan(&key, &bytes); err != nil {
			return p, err
		}
		p.Keys = append(p.Keys, key)
		p.Count++
		p.Bytes += bytes
	}
	return p, rows.Err()
}
func filterWhere(f Filter) (string, []interface{}) {
	clauses := []string{"1=1"}
	args := []interface{}{}
	if f.Model != "" {
		clauses = append(clauses, "model=?")
		args = append(args, f.Model)
	}
	if f.Endpoint != "" {
		clauses = append(clauses, "path=?")
		args = append(args, f.Endpoint)
	}
	if f.KeyPrefix != "" {
		clauses = append(clauses, "cache_key LIKE ?")
		args = append(args, f.KeyPrefix+"%")
	}
	return strings.Join(clauses, " AND "), args
}
func (s *SQLiteStore) ApplyClear(ctx context.Context, plan Plan) error {
	return s.applyKeys(ctx, plan, plan.IncludeStats)
}
func (s *SQLiteStore) PlanPrune(ctx context.Context, policy PrunePolicy) (Plan, error) {
	if !policy.Expired && policy.OlderThan == 0 && policy.MaxBytes == 0 {
		policy.Expired = true
	}
	where := "1=1"
	args := []interface{}{}
	if policy.Expired || policy.OlderThan > 0 {
		clauses := []string{}
		if policy.Expired {
			clauses = append(clauses, "expires_at<=?")
			args = append(args, s.clock().UTC().Unix())
		}
		if policy.OlderThan > 0 {
			clauses = append(clauses, "stored_at<=?")
			args = append(args, s.clock().Add(-policy.OlderThan).UTC().Unix())
		}
		where = "(" + strings.Join(clauses, " OR ") + ")"
	}
	p, err := s.planKeys(ctx, "prune", where, args, Filter{}, &policy)
	if err != nil {
		return p, err
	}
	if policy.MaxBytes <= 0 {
		return p, nil
	}
	// Max-bytes is an LRU cap over all entries. Entries selected by the
	// expiry/age predicate are deleted first, then the oldest survivors.
	rows, err := s.db.QueryContext(ctx, `SELECT cache_key,response_bytes FROM cache_entries ORDER BY COALESCE(last_hit_at,0),stored_at,cache_key`)
	if err != nil {
		return Plan{}, err
	}
	defer rows.Close()
	selected := make(map[string]bool, len(p.Keys))
	for _, key := range p.Keys {
		selected[key] = true
	}
	var total, selectedBytes int64
	var all []struct {
		key   string
		bytes int64
	}
	for rows.Next() {
		var item struct {
			key   string
			bytes int64
		}
		if err := rows.Scan(&item.key, &item.bytes); err != nil {
			return p, err
		}
		all = append(all, item)
		total += item.bytes
		if selected[item.key] {
			selectedBytes += item.bytes
		}
	}
	if err := rows.Err(); err != nil {
		return p, err
	}
	for _, item := range all {
		if total-selectedBytes <= policy.MaxBytes {
			break
		}
		if !selected[item.key] {
			selected[item.key] = true
			p.Keys = append(p.Keys, item.key)
			p.Count++
			selectedBytes += item.bytes
		}
	}
	p.Bytes = selectedBytes
	return p, nil
}
func (s *SQLiteStore) ApplyPrune(ctx context.Context, plan Plan) error {
	return s.applyKeys(ctx, plan, false)
}
func (s *SQLiteStore) applyKeys(ctx context.Context, plan Plan, includeStats bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, key := range plan.Keys {
		if _, err = tx.ExecContext(ctx, "DELETE FROM cache_entries WHERE cache_key=?", key); err != nil {
			return err
		}
	}
	if includeStats {
		if _, err = tx.ExecContext(ctx, "DELETE FROM stats_daily"); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *SQLiteStore) databaseBytes() int64 {
	var n int64
	for _, suffix := range []string{"", "-wal"} {
		if st, err := os.Stat(s.path + suffix); err == nil {
			n += st.Size()
		}
	}
	return n
}

func ValidateTTL(ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}
	if ttl < time.Second || ttl > MaxTTL {
		return fmt.Errorf("ttl must be 0 or between 1s and 720h")
	}
	return nil
}
