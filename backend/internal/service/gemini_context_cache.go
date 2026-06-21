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
	defaultGeminiContextCacheTTL           = time.Hour
	defaultGeminiContextCacheMinBytes      = 2048
	defaultGeminiContextCacheTailChars     = 4096
	defaultGeminiContextCacheCreateTimeout = 15 * time.Second
	geminiContextCacheKeyPrefix            = "gemini-context-cache:v1"
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
	cacheKey          string
	createBody        []byte
	requestBody       []byte
	ttl               time.Duration
	cachePayloadBytes int
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
		if len(body) >= geminiContextCacheMinBytes(account) {
			logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache candidate skipped account=%d model=%s request_bytes=%d", account.ID, model, len(body))
		}
		return body, geminiContextCacheApplyResult{}
	}
	logger.LegacyPrintf(
		"service.gemini_context_cache",
		"Gemini context cache candidate account=%d model=%s key=%s cache_bytes=%d create_bytes=%d request_bytes=%d original_bytes=%d ttl=%s",
		account.ID,
		model,
		shortGeminiContextCacheKey(candidate.cacheKey),
		candidate.cachePayloadBytes,
		len(candidate.createBody),
		len(candidate.requestBody),
		len(body),
		candidate.ttl,
	)

	now := time.Now()
	if entry, err := s.geminiContextCache.GetGeminiContextCache(ctx, candidate.cacheKey); err == nil && usableGeminiContextCacheEntry(entry, account.ID, model, now) {
		rewritten, ok := injectGeminiCachedContent(candidate.requestBody, entry.Name)
		if ok {
			logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache hit account=%d model=%s key=%s name=%s", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey), entry.Name)
			return rewritten, geminiContextCacheApplyResult{cacheName: entry.Name}
		}
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache hit could not be injected account=%d model=%s key=%s name=%s", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey), entry.Name)
	} else if err != nil {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache lookup failed account=%d model=%s key=%s: %v", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey), err)
		return body, geminiContextCacheApplyResult{}
	} else {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache miss account=%d model=%s key=%s", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey))
	}

	createStart := time.Now()
	entry, createTokens, err := s.createGeminiContextCache(ctx, account, apiKey, normalizedBaseURL, proxyURL, model, candidate)
	if err != nil {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache create skipped account=%d model=%s key=%s elapsed=%s: %v", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey), time.Since(createStart).Truncate(time.Millisecond), err)
		return body, geminiContextCacheApplyResult{}
	}
	logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache created account=%d model=%s key=%s name=%s tokens=%d elapsed=%s", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey), entry.Name, createTokens, time.Since(createStart).Truncate(time.Millisecond))

	if err := s.geminiContextCache.SetGeminiContextCache(ctx, entry, time.Until(entry.ExpiresAt)); err != nil {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache store failed account=%d model=%s key=%s: %v", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey), err)
	}
	rewritten, ok := injectGeminiCachedContent(candidate.requestBody, entry.Name)
	if !ok {
		logger.LegacyPrintf("service.gemini_context_cache", "Gemini context cache create result could not be injected account=%d model=%s key=%s name=%s", account.ID, model, shortGeminiContextCacheKey(candidate.cacheKey), entry.Name)
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

	stablePayload := make(map[string]any)
	requestPayload := cloneJSONMap(req)
	for _, field := range geminiContextCacheStableFields {
		if value, ok := req[field]; ok {
			stablePayload[field] = value
			delete(requestPayload, field)
		}
	}

	cachePayload := cloneJSONMap(stablePayload)
	if rawContents, ok := req["contents"].([]any); ok && len(rawContents) > 0 {
		prefixContents, requestContents, ok := splitGeminiContentsForStableCache(rawContents, geminiContextCacheTailChars(account))
		if ok {
			cachePayload["contents"] = prefixContents
			requestPayload["contents"] = requestContents
		}
	}
	if _, hasContents := cachePayload["contents"]; !hasContents && cachePayload["systemInstruction"] == nil {
		return nil, false
	}

	return makeGeminiContextCacheCandidate(account, model, cachePayload, requestPayload)
}

func makeGeminiContextCacheCandidate(account *Account, model string, cachePayload map[string]any, requestPayload map[string]any) (*geminiContextCacheCandidate, bool) {
	minBytes := geminiContextCacheMinBytes(account)
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
		cacheKey:          cacheKey,
		createBody:        createBytes,
		requestBody:       requestBytes,
		ttl:               ttl,
		cachePayloadBytes: len(cacheBytes),
	}, true
}

func geminiContextCacheMinBytes(account *Account) int {
	if account != nil {
		if configured := account.getExtraInt("gemini_context_cache_min_bytes"); configured > 0 {
			return configured
		}
	}
	return defaultGeminiContextCacheMinBytes
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

func geminiContextCacheCreateTimeout(account *Account) time.Duration {
	if account != nil {
		if seconds := account.getExtraInt("gemini_context_cache_create_timeout_seconds"); seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultGeminiContextCacheCreateTimeout
}

func shortGeminiContextCacheKey(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) == 0 {
		return ""
	}
	hash := parts[len(parts)-1]
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
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
	if timeout := geminiContextCacheCreateTimeout(account); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

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
		upstreamMsg := sanitizeUpstreamErrorMessage(strings.TrimSpace(extractUpstreamErrorMessage(respBody)))
		if upstreamMsg == "" {
			upstreamMsg = sanitizeUpstreamErrorMessage(truncateForLog(respBody, 512))
		}
		return nil, 0, fmt.Errorf("upstream cachedContents returned %d: %s", resp.StatusCode, upstreamMsg)
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

func splitGeminiContentsForStableCache(rawContents []any, tailChars int) ([]any, []any, bool) {
	if len(rawContents) == 0 {
		return nil, nil, false
	}

	allowTailFallback := len(rawContents) == 1
	prefixContent, tailContent, ok := splitSingleGeminiContentParts(rawContents[0], tailChars, allowTailFallback)
	if ok {
		requestContents := make([]any, 0, len(rawContents))
		requestContents = append(requestContents, tailContent)
		requestContents = append(requestContents, cloneJSONSlice(rawContents[1:])...)
		return []any{prefixContent}, requestContents, true
	}

	if len(rawContents) > 1 && geminiContentHasParts(rawContents[0]) {
		return []any{cloneJSONValue(rawContents[0])}, cloneJSONSlice(rawContents[1:]), true
	}

	return nil, nil, false
}

func geminiContentHasParts(content any) bool {
	contentMap, ok := content.(map[string]any)
	if !ok {
		return false
	}
	rawParts, ok := contentMap["parts"].([]any)
	return ok && len(rawParts) > 0
}

func splitSingleGeminiContentParts(content any, tailChars int, allowFallback bool) (map[string]any, map[string]any, bool) {
	contentMap, ok := content.(map[string]any)
	if !ok {
		return nil, nil, false
	}
	rawParts, ok := contentMap["parts"].([]any)
	if !ok || len(rawParts) == 0 {
		return nil, nil, false
	}
	if len(rawParts) == 1 {
		return splitSingleGeminiTextPart(contentMap, rawParts[0], tailChars, allowFallback)
	}
	if !allowFallback {
		return nil, nil, false
	}

	prefix := cloneJSONMap(contentMap)
	tail := cloneJSONMap(contentMap)
	prefix["parts"] = cloneJSONSlice(rawParts[:len(rawParts)-1])
	tail["parts"] = cloneJSONSlice(rawParts[len(rawParts)-1:])
	return prefix, tail, true
}

func splitSingleGeminiTextPart(contentMap map[string]any, rawPart any, tailChars int, allowFallback bool) (map[string]any, map[string]any, bool) {
	partMap, ok := rawPart.(map[string]any)
	if !ok {
		return nil, nil, false
	}
	text, ok := partMap["text"].(string)
	if !ok || strings.TrimSpace(text) == "" {
		return nil, nil, false
	}

	splitIndex := chooseGeminiTextCacheSplitIndex(text, tailChars, allowFallback)
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

func chooseGeminiTextCacheSplitIndex(text string, tailChars int, allowFallback bool) int {
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

	if !allowFallback {
		return -1
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
