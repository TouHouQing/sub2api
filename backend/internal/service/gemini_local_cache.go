package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	defaultGeminiLocalCacheTTL       = time.Hour
	defaultGeminiLocalCacheMinBytes  = 2048
	defaultGeminiLocalCacheTailChars = 4096
	geminiLocalCacheKeyPrefix        = "gemini-local-cache:v1"
)

const (
	GeminiLocalCacheEventSkipped = "skipped"
	GeminiLocalCacheEventCreated = "created"
	GeminiLocalCacheEventHit     = "hit"
	GeminiLocalCacheEventError   = "error"
)

var geminiLocalCacheStableFields = []string{
	"systemInstruction",
	"tools",
	"toolConfig",
}

type GeminiLocalCache interface {
	GetGeminiLocalCache(ctx context.Context, key string) (*GeminiLocalCacheEntry, error)
	SetGeminiLocalCache(ctx context.Context, entry *GeminiLocalCacheEntry, ttl time.Duration) error
	RecordGeminiLocalCacheEvent(ctx context.Context, event GeminiLocalCacheEvent) error
	GetGeminiLocalCacheStats(ctx context.Context, filter GeminiLocalCacheStatsFilter) (*GeminiLocalCacheStats, error)
}

type GeminiLocalCacheEntry struct {
	CacheKey        string    `json:"cache_key"`
	AccountID       int64     `json:"account_id"`
	Model           string    `json:"model"`
	PayloadHash     string    `json:"payload_hash"`
	PayloadBytes    int       `json:"payload_bytes"`
	EstimatedTokens int       `json:"estimated_tokens"`
	CreatedAt       time.Time `json:"created_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

type GeminiLocalCacheResult struct {
	Enabled         bool
	Created         bool
	Hit             bool
	CacheKey        string
	PayloadHash     string
	PayloadBytes    int
	EstimatedTokens int
}

type GeminiLocalCacheEvent struct {
	AccountID       int64
	Model           string
	Event           string
	EstimatedTokens int
}

type GeminiLocalCacheStatsFilter struct {
	AccountID int64
	Model     string
}

type GeminiLocalCacheStats struct {
	Scope                  string  `json:"scope"`
	AccountID              int64   `json:"account_id,omitempty"`
	Model                  string  `json:"model,omitempty"`
	Requests               int64   `json:"requests"`
	CandidateRequests      int64   `json:"candidate_requests"`
	CacheCreationRequests  int64   `json:"cache_creation_requests"`
	CacheReadRequests      int64   `json:"cache_read_requests"`
	SkippedRequests        int64   `json:"skipped_requests"`
	ErrorRequests          int64   `json:"error_requests"`
	EstimatedCreatedTokens int64   `json:"estimated_created_tokens"`
	EstimatedReadTokens    int64   `json:"estimated_read_tokens"`
	HitRate                float64 `json:"hit_rate"`
}

type GeminiLocalCacheTracker struct {
	store GeminiLocalCache
}

func NewGeminiLocalCacheTracker(store GeminiLocalCache) *GeminiLocalCacheTracker {
	return &GeminiLocalCacheTracker{store: store}
}

func (t *GeminiLocalCacheTracker) Track(ctx context.Context, account *Account, model string, body []byte, promptTokens int) GeminiLocalCacheResult {
	if t == nil || t.store == nil || account == nil || !geminiLocalCacheEnabled(account) {
		return GeminiLocalCacheResult{}
	}
	candidate, ok := buildGeminiLocalCacheCandidate(account, model, body, promptTokens)
	if !ok {
		t.record(ctx, account, model, GeminiLocalCacheEventSkipped, 0)
		return GeminiLocalCacheResult{Enabled: true}
	}

	now := time.Now()
	if entry, err := t.store.GetGeminiLocalCache(ctx, candidate.CacheKey); err != nil {
		t.record(ctx, account, model, GeminiLocalCacheEventError, candidate.EstimatedTokens)
		return GeminiLocalCacheResult{
			Enabled:         true,
			CacheKey:        candidate.CacheKey,
			PayloadHash:     candidate.PayloadHash,
			PayloadBytes:    candidate.PayloadBytes,
			EstimatedTokens: candidate.EstimatedTokens,
		}
	} else if usableGeminiLocalCacheEntry(entry, account.ID, model, now) {
		t.record(ctx, account, model, GeminiLocalCacheEventHit, entry.EstimatedTokens)
		return GeminiLocalCacheResult{
			Enabled:         true,
			Hit:             true,
			CacheKey:        entry.CacheKey,
			PayloadHash:     entry.PayloadHash,
			PayloadBytes:    entry.PayloadBytes,
			EstimatedTokens: entry.EstimatedTokens,
		}
	}

	entry := &GeminiLocalCacheEntry{
		CacheKey:        candidate.CacheKey,
		AccountID:       account.ID,
		Model:           model,
		PayloadHash:     candidate.PayloadHash,
		PayloadBytes:    candidate.PayloadBytes,
		EstimatedTokens: candidate.EstimatedTokens,
		CreatedAt:       now,
		ExpiresAt:       now.Add(geminiLocalCacheTTL(account)),
	}
	if err := t.store.SetGeminiLocalCache(ctx, entry, time.Until(entry.ExpiresAt)); err != nil {
		t.record(ctx, account, model, GeminiLocalCacheEventError, candidate.EstimatedTokens)
		return GeminiLocalCacheResult{
			Enabled:         true,
			CacheKey:        entry.CacheKey,
			PayloadHash:     entry.PayloadHash,
			PayloadBytes:    entry.PayloadBytes,
			EstimatedTokens: entry.EstimatedTokens,
		}
	}
	t.record(ctx, account, model, GeminiLocalCacheEventCreated, entry.EstimatedTokens)
	return GeminiLocalCacheResult{
		Enabled:         true,
		Created:         true,
		CacheKey:        entry.CacheKey,
		PayloadHash:     entry.PayloadHash,
		PayloadBytes:    entry.PayloadBytes,
		EstimatedTokens: entry.EstimatedTokens,
	}
}

func (t *GeminiLocalCacheTracker) record(ctx context.Context, account *Account, model string, event string, estimatedTokens int) {
	if t == nil || t.store == nil || account == nil {
		return
	}
	_ = t.store.RecordGeminiLocalCacheEvent(ctx, GeminiLocalCacheEvent{
		AccountID:       account.ID,
		Model:           model,
		Event:           event,
		EstimatedTokens: estimatedTokens,
	})
}

func geminiLocalCacheEnabled(account *Account) bool {
	if account == nil {
		return false
	}
	if account.Extra == nil {
		return true
	}
	if raw, ok := account.Extra["gemini_local_cache_enabled"]; ok {
		enabled, ok := raw.(bool)
		return ok && enabled
	}
	return true
}

type geminiLocalCacheCandidate struct {
	CacheKey        string
	PayloadHash     string
	PayloadBytes    int
	EstimatedTokens int
}

func buildGeminiLocalCacheCandidate(account *Account, model string, body []byte, promptTokens int) (*geminiLocalCacheCandidate, bool) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, false
	}

	cachePayload := make(map[string]any)
	for _, field := range geminiLocalCacheStableFields {
		if value, ok := req[field]; ok {
			cachePayload[field] = value
		}
	}

	if rawContents, ok := req["contents"].([]any); ok && len(rawContents) > 0 {
		if contents, ok := geminiLocalCacheStableContents(rawContents, geminiLocalCacheTailChars(account)); ok {
			cachePayload["contents"] = contents
		}
	}
	if _, hasContents := cachePayload["contents"]; !hasContents && cachePayload["systemInstruction"] == nil {
		return nil, false
	}

	payloadBytes, err := json.Marshal(cachePayload)
	if err != nil || len(payloadBytes) < geminiLocalCacheMinBytes(account) {
		return nil, false
	}
	sum := sha256.Sum256(payloadBytes)
	payloadHash := hex.EncodeToString(sum[:])
	cacheKey := fmt.Sprintf("%s:%d:%s:%s", geminiLocalCacheKeyPrefix, account.ID, model, payloadHash)
	return &geminiLocalCacheCandidate{
		CacheKey:        cacheKey,
		PayloadHash:     payloadHash,
		PayloadBytes:    len(payloadBytes),
		EstimatedTokens: estimateGeminiLocalCacheTokens(promptTokens, len(body), len(payloadBytes)),
	}, true
}

func geminiLocalCacheStableContents(rawContents []any, tailChars int) ([]any, bool) {
	if len(rawContents) == 0 {
		return nil, false
	}
	if prefix, ok := splitGeminiLocalCacheFirstContent(rawContents[0], tailChars, len(rawContents) == 1); ok {
		return []any{prefix}, true
	}
	if len(rawContents) > 1 && geminiLocalCacheContentHasParts(rawContents[0]) {
		return []any{cloneGeminiLocalCacheJSONValue(rawContents[0])}, true
	}
	return nil, false
}

func splitGeminiLocalCacheFirstContent(content any, tailChars int, allowFallback bool) (map[string]any, bool) {
	contentMap, ok := content.(map[string]any)
	if !ok {
		return nil, false
	}
	rawParts, ok := contentMap["parts"].([]any)
	if !ok || len(rawParts) == 0 {
		return nil, false
	}
	if len(rawParts) > 1 {
		if !allowFallback {
			return nil, false
		}
		prefix := cloneGeminiLocalCacheJSONMap(contentMap)
		prefix["parts"] = cloneGeminiLocalCacheJSONSlice(rawParts[:len(rawParts)-1])
		return prefix, true
	}

	partMap, ok := rawParts[0].(map[string]any)
	if !ok {
		return nil, false
	}
	text, ok := partMap["text"].(string)
	if !ok || strings.TrimSpace(text) == "" {
		return nil, false
	}
	splitIndex := chooseGeminiLocalCacheTextSplitIndex(text, tailChars, allowFallback)
	if splitIndex <= 0 || splitIndex >= len([]rune(text)) {
		return nil, false
	}
	runes := []rune(text)
	prefixText := strings.TrimRight(string(runes[:splitIndex]), "\r\n")
	if strings.TrimSpace(prefixText) == "" {
		return nil, false
	}
	prefixPart := cloneGeminiLocalCacheJSONMap(partMap)
	prefixPart["text"] = prefixText
	prefix := cloneGeminiLocalCacheJSONMap(contentMap)
	prefix["parts"] = []any{prefixPart}
	return prefix, true
}

func chooseGeminiLocalCacheTextSplitIndex(text string, tailChars int, allowFallback bool) int {
	runes := []rune(text)
	if tailChars <= 0 {
		tailChars = defaultGeminiLocalCacheTailChars
	}
	if len(runes) <= tailChars {
		return -1
	}

	windowStart := len(runes) - tailChars
	window := string(runes[windowStart:])
	for _, marker := range []string{
		"\n\n问题", "\n\n提问", "\n\nQuestion", "\n\nQ:", "\n\nUser:", "\n\n用户",
		"\n问题", "\n提问", "\nQuestion", "\nQ:", "\nUser:", "\n用户",
		"\n\n",
	} {
		if idx := strings.LastIndex(window, marker); idx >= 0 {
			return windowStart + len([]rune(window[:idx]))
		}
	}

	if !allowFallback {
		return -1
	}
	return len(runes) - tailChars
}

func geminiLocalCacheContentHasParts(content any) bool {
	contentMap, ok := content.(map[string]any)
	if !ok {
		return false
	}
	rawParts, ok := contentMap["parts"].([]any)
	return ok && len(rawParts) > 0
}

func estimateGeminiLocalCacheTokens(promptTokens int, requestBytes int, payloadBytes int) int {
	if promptTokens <= 0 || requestBytes <= 0 || payloadBytes <= 0 {
		return 0
	}
	estimated := int(float64(promptTokens) * float64(payloadBytes) / float64(requestBytes))
	if estimated < 0 {
		return 0
	}
	return estimated
}

func geminiLocalCacheMinBytes(account *Account) int {
	if account != nil {
		if configured := account.getExtraInt("gemini_local_cache_min_bytes"); configured > 0 {
			return configured
		}
	}
	return defaultGeminiLocalCacheMinBytes
}

func geminiLocalCacheTTL(account *Account) time.Duration {
	if account != nil {
		if seconds := account.getExtraInt("gemini_local_cache_ttl_seconds"); seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultGeminiLocalCacheTTL
}

func geminiLocalCacheTailChars(account *Account) int {
	if account != nil {
		if configured := account.getExtraInt("gemini_local_cache_tail_chars"); configured > 0 {
			return configured
		}
	}
	return defaultGeminiLocalCacheTailChars
}

func usableGeminiLocalCacheEntry(entry *GeminiLocalCacheEntry, accountID int64, model string, now time.Time) bool {
	if entry == nil || entry.CacheKey == "" || entry.AccountID != accountID || entry.Model != model {
		return false
	}
	return entry.ExpiresAt.IsZero() || entry.ExpiresAt.After(now.Add(5*time.Second))
}

func shortGeminiLocalCacheHash(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}

func cloneGeminiLocalCacheJSONMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneGeminiLocalCacheJSONValue(v)
	}
	return out
}

func cloneGeminiLocalCacheJSONSlice(in []any) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = cloneGeminiLocalCacheJSONValue(v)
	}
	return out
}

func cloneGeminiLocalCacheJSONValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		return cloneGeminiLocalCacheJSONMap(typed)
	case []any:
		return cloneGeminiLocalCacheJSONSlice(typed)
	default:
		return typed
	}
}
