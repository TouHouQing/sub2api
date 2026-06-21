package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

const (
	defaultGeminiContextCacheTTL       = time.Hour
	defaultGeminiContextCacheMinBytes  = 2048
	defaultGeminiContextCacheTailChars = 4096
	geminiContextCacheKeyPrefix        = "gemini-context-cache:v1"
)

var geminiContextCacheStableFields = []string{
	"systemInstruction",
	"tools",
	"toolConfig",
}

// GeminiContextCache stores Gemini explicit context cache metadata.
type GeminiContextCache interface {
	GetGeminiContextCache(ctx context.Context, key string) (*GeminiContextCacheEntry, error)
	SetGeminiContextCache(ctx context.Context, entry *GeminiContextCacheEntry, ttl time.Duration) error
}

type GeminiContextCacheEntry struct {
	CacheKey   string    `json:"cache_key"`
	Name       string    `json:"name"`
	AccountID  int64     `json:"account_id"`
	Model      string    `json:"model"`
	TokenCount int       `json:"token_count"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type geminiContextCacheApplyResult struct {
	cacheCreationTokens int
	cacheCreationTTL    time.Duration
	cacheName           string
}

type geminiContextCacheCandidate struct {
	cacheKey    string
	createBody  []byte
	requestBody []byte
	ttl         time.Duration
}

func (s *GeminiMessagesCompatService) maybeApplyGeminiContextCache(
	ctx context.Context,
	account *Account,
	model string,
	body []byte,
	apiKey string,
	baseURL string,
	proxyURL string,
) ([]byte, geminiContextCacheApplyResult) {
	if !shouldUseGeminiContextCache(account, apiKey, s.geminiContextCache, s.httpUpstream) {
		return body, geminiContextCacheApplyResult{}
	}

	normalizedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return body, geminiContextCacheApplyResult{}
	}

	candidate, ok := buildGeminiContextCacheCandidate(account, model, body)
	if !ok {
		return body, geminiContextCacheApplyResult{}
	}

	now := time.Now()
	if entry, err := s.geminiContextCache.GetGeminiContextCache(ctx, candidate.cacheKey); err == nil && usableGeminiContextCacheEntry(entry, account.ID, model, now) {
		rewritten, ok := injectGeminiCachedContent(candidate.requestBody, entry.Name)
		if ok {
			return rewritten, geminiContextCacheApplyResult{cacheName: entry.Name}
		}
	} else if err != nil {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache lookup failed: %v", err)
		return body, geminiContextCacheApplyResult{}
	}

	entry, createTokens, err := s.createGeminiContextCache(ctx, account, apiKey, normalizedBaseURL, proxyURL, model, candidate)
	if err != nil {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache create skipped: %v", err)
		return body, geminiContextCacheApplyResult{}
	}

	if err := s.geminiContextCache.SetGeminiContextCache(ctx, entry, time.Until(entry.ExpiresAt)); err != nil {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache store failed: %v", err)
	}
	rewritten, ok := injectGeminiCachedContent(candidate.requestBody, entry.Name)
	if !ok {
		return body, geminiContextCacheApplyResult{}
	}
	return rewritten, geminiContextCacheApplyResult{
		cacheCreationTokens: createTokens,
		cacheCreationTTL:    candidate.ttl,
		cacheName:           entry.Name,
	}
}

func shouldUseGeminiContextCache(account *Account, apiKey string, cache GeminiContextCache, upstream HTTPUpstream) bool {
	if account == nil || account.Type != AccountTypeAPIKey || strings.TrimSpace(apiKey) == "" || cache == nil || upstream == nil {
		return false
	}
	if account.Extra == nil {
		return true
	}
	if raw, ok := account.Extra["gemini_context_cache_enabled"]; ok {
		enabled, ok := raw.(bool)
		return ok && enabled
	}
	return true
}

func buildGeminiContextCacheCandidate(account *Account, model string, body []byte) (*geminiContextCacheCandidate, bool) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, false
	}

	cachePayload := make(map[string]any)
	requestPayload := cloneJSONMap(req)
	for _, field := range geminiContextCacheStableFields {
		if value, ok := req[field]; ok {
			cachePayload[field] = value
			delete(requestPayload, field)
		}
	}

	if rawContents, ok := req["contents"].([]any); ok && len(rawContents) > 1 {
		prefixContents := cloneJSONSlice(rawContents[:len(rawContents)-1])
		tailContents := cloneJSONSlice(rawContents[len(rawContents)-1:])
		cachePayload["contents"] = prefixContents
		requestPayload["contents"] = tailContents
	} else if rawContents, ok := req["contents"].([]any); ok && len(rawContents) == 1 {
		prefixContent, tailContent, ok := splitSingleGeminiContentParts(rawContents[0], geminiContextCacheTailChars(account))
		if ok {
			cachePayload["contents"] = []any{prefixContent}
			requestPayload["contents"] = []any{tailContent}
		}
	}
	if _, hasContents := cachePayload["contents"]; !hasContents && cachePayload["systemInstruction"] == nil {
		return nil, false
	}

	minBytes := defaultGeminiContextCacheMinBytes
	if account != nil {
		if configured := account.getExtraInt("gemini_context_cache_min_bytes"); configured > 0 {
			minBytes = configured
		}
	}
	createPayload := cloneJSONMap(cachePayload)
	createPayload["model"] = geminiCachedContentModelName(model)
	ttl := geminiContextCacheTTL(account)
	createPayload["ttl"] = fmt.Sprintf("%ds", int(ttl.Seconds()))

	cacheBytes, err := json.Marshal(cachePayload)
	if err != nil || len(cacheBytes) < minBytes {
		return nil, false
	}
	createBytes, err := json.Marshal(createPayload)
	if err != nil {
		return nil, false
	}
	requestBytes, err := json.Marshal(requestPayload)
	if err != nil {
		return nil, false
	}

	sum := sha256.Sum256(cacheBytes)
	cacheKey := fmt.Sprintf("%s:%d:%s:%s", geminiContextCacheKeyPrefix, account.ID, model, hex.EncodeToString(sum[:]))
	return &geminiContextCacheCandidate{
		cacheKey:    cacheKey,
		createBody:  createBytes,
		requestBody: requestBytes,
		ttl:         ttl,
	}, true
}

func geminiContextCacheTTL(account *Account) time.Duration {
	if account != nil {
		if seconds := account.getExtraInt("gemini_context_cache_ttl_seconds"); seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultGeminiContextCacheTTL
}

func geminiContextCacheTailChars(account *Account) int {
	if account != nil {
		if configured := account.getExtraInt("gemini_context_cache_tail_chars"); configured > 0 {
			return configured
		}
	}
	return defaultGeminiContextCacheTailChars
}

func applyGeminiContextCacheCreationUsage(usage *ClaudeUsage, applied geminiContextCacheApplyResult) {
	if usage == nil || applied.cacheCreationTokens <= 0 {
		return
	}
	if usage.CacheCreationInputTokens == 0 {
		usage.CacheCreationInputTokens = applied.cacheCreationTokens
	}
	if usage.CacheCreation5mTokens != 0 || usage.CacheCreation1hTokens != 0 {
		return
	}
	if applied.cacheCreationTTL > 5*time.Minute {
		usage.CacheCreation1hTokens = applied.cacheCreationTokens
		return
	}
	usage.CacheCreation5mTokens = applied.cacheCreationTokens
}

func geminiCachedContentModelName(model string) string {
	model = strings.TrimSpace(model)
	if model == "" || strings.HasPrefix(model, "models/") {
		return model
	}
	return "models/" + model
}

func usableGeminiContextCacheEntry(entry *GeminiContextCacheEntry, accountID int64, model string, now time.Time) bool {
	if entry == nil || strings.TrimSpace(entry.Name) == "" || entry.AccountID != accountID || entry.Model != model {
		return false
	}
	return entry.ExpiresAt.IsZero() || entry.ExpiresAt.After(now.Add(5*time.Second))
}

func (s *GeminiMessagesCompatService) createGeminiContextCache(
	ctx context.Context,
	account *Account,
	apiKey string,
	baseURL string,
	proxyURL string,
	model string,
	candidate *geminiContextCacheCandidate,
) (*GeminiContextCacheEntry, int, error) {
	fullURL := strings.TrimRight(baseURL, "/") + "/v1beta/cachedContents"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(candidate.createBody))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", apiKey)

	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, 0, fmt.Errorf("upstream cachedContents returned %d: %s", resp.StatusCode, sanitizeUpstreamErrorMessage(strings.TrimSpace(string(respBody))))
	}

	var parsed struct {
		Name          string `json:"name"`
		UsageMetadata struct {
			TotalTokenCount int `json:"totalTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, 0, err
	}
	if strings.TrimSpace(parsed.Name) == "" {
		return nil, 0, fmt.Errorf("cachedContents response missing name")
	}

	now := time.Now()
	entry := &GeminiContextCacheEntry{
		CacheKey:   candidate.cacheKey,
		Name:       parsed.Name,
		AccountID:  account.ID,
		Model:      model,
		TokenCount: parsed.UsageMetadata.TotalTokenCount,
		CreatedAt:  now,
		ExpiresAt:  now.Add(candidate.ttl),
	}
	return entry, parsed.UsageMetadata.TotalTokenCount, nil
}

func injectGeminiCachedContent(body []byte, name string) ([]byte, bool) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return body, false
	}
	req["cachedContent"] = name
	out, err := json.Marshal(req)
	if err != nil {
		return body, false
	}
	return out, true
}

func splitSingleGeminiContentParts(content any, tailChars int) (map[string]any, map[string]any, bool) {
	contentMap, ok := content.(map[string]any)
	if !ok {
		return nil, nil, false
	}
	rawParts, ok := contentMap["parts"].([]any)
	if !ok || len(rawParts) == 0 {
		return nil, nil, false
	}
	if len(rawParts) == 1 {
		return splitSingleGeminiTextPart(contentMap, rawParts[0], tailChars)
	}

	prefix := cloneJSONMap(contentMap)
	tail := cloneJSONMap(contentMap)
	prefix["parts"] = cloneJSONSlice(rawParts[:len(rawParts)-1])
	tail["parts"] = cloneJSONSlice(rawParts[len(rawParts)-1:])
	return prefix, tail, true
}

func splitSingleGeminiTextPart(contentMap map[string]any, rawPart any, tailChars int) (map[string]any, map[string]any, bool) {
	partMap, ok := rawPart.(map[string]any)
	if !ok {
		return nil, nil, false
	}
	text, ok := partMap["text"].(string)
	if !ok || strings.TrimSpace(text) == "" {
		return nil, nil, false
	}

	splitIndex := chooseGeminiTextCacheSplitIndex(text, tailChars)
	if splitIndex <= 0 || splitIndex >= len([]rune(text)) {
		return nil, nil, false
	}

	runes := []rune(text)
	prefixText := strings.TrimRight(string(runes[:splitIndex]), "\r\n")
	tailText := strings.TrimLeft(string(runes[splitIndex:]), "\r\n")
	if strings.TrimSpace(prefixText) == "" || strings.TrimSpace(tailText) == "" {
		return nil, nil, false
	}

	prefixPart := cloneJSONMap(partMap)
	tailPart := cloneJSONMap(partMap)
	prefixPart["text"] = prefixText
	tailPart["text"] = tailText

	prefix := cloneJSONMap(contentMap)
	tail := cloneJSONMap(contentMap)
	prefix["parts"] = []any{prefixPart}
	tail["parts"] = []any{tailPart}
	return prefix, tail, true
}

func chooseGeminiTextCacheSplitIndex(text string, tailChars int) int {
	runes := []rune(text)
	if tailChars <= 0 {
		tailChars = defaultGeminiContextCacheTailChars
	}
	if len(runes) <= tailChars {
		return -1
	}

	windowStart := len(runes) - tailChars
	if windowStart < 0 {
		windowStart = 0
	}
	windowEnd := len(runes)
	if windowEnd <= windowStart {
		return len(runes) - tailChars
	}

	window := string(runes[windowStart:windowEnd])
	for _, marker := range []string{
		"\n\n问题", "\n\n提问", "\n\nQuestion", "\n\nQ:", "\n\nUser:", "\n\n用户",
		"\n问题", "\n提问", "\nQuestion", "\nQ:", "\nUser:", "\n用户",
		"\n\n",
	} {
		if idx := strings.LastIndex(window, marker); idx >= 0 {
			return windowStart + len([]rune(window[:idx]))
		}
	}

	return len(runes) - tailChars
}

func cloneJSONMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneJSONValue(v)
	}
	return out
}

func cloneJSONSlice(in []any) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = cloneJSONValue(v)
	}
	return out
}

func cloneJSONValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		return cloneJSONMap(typed)
	case []any:
		return cloneJSONSlice(typed)
	default:
		return typed
	}
}
