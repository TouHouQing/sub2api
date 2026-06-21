package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type geminiLocalCacheStoreStub struct {
	entries map[string]*GeminiLocalCacheEntry
	stats   map[string]*GeminiLocalCacheStats
}

func (s *geminiLocalCacheStoreStub) GetGeminiLocalCache(ctx context.Context, key string) (*GeminiLocalCacheEntry, error) {
	if s.entries == nil {
		return nil, nil
	}
	return s.entries[key], nil
}

func (s *geminiLocalCacheStoreStub) SetGeminiLocalCache(ctx context.Context, entry *GeminiLocalCacheEntry, ttl time.Duration) error {
	if s.entries == nil {
		s.entries = make(map[string]*GeminiLocalCacheEntry)
	}
	cp := *entry
	s.entries[entry.CacheKey] = &cp
	return nil
}

func (s *geminiLocalCacheStoreStub) RecordGeminiLocalCacheEvent(ctx context.Context, event GeminiLocalCacheEvent) error {
	if s.stats == nil {
		s.stats = make(map[string]*GeminiLocalCacheStats)
	}
	for _, key := range geminiLocalCacheStatsStubKeys(event.AccountID, event.Model) {
		stats := s.stats[key]
		if stats == nil {
			stats = &GeminiLocalCacheStats{}
			s.stats[key] = stats
		}
		stats.Requests++
		switch event.Event {
		case GeminiLocalCacheEventCreated:
			stats.CandidateRequests++
			stats.CacheCreationRequests++
			stats.EstimatedCreatedTokens += int64(event.EstimatedTokens)
		case GeminiLocalCacheEventHit:
			stats.CandidateRequests++
			stats.CacheReadRequests++
			stats.EstimatedReadTokens += int64(event.EstimatedTokens)
		case GeminiLocalCacheEventSkipped:
			stats.SkippedRequests++
		case GeminiLocalCacheEventError:
			stats.CandidateRequests++
			stats.ErrorRequests++
		}
		if stats.CandidateRequests > 0 {
			stats.HitRate = float64(stats.CacheReadRequests) / float64(stats.CandidateRequests)
		}
	}
	return nil
}

func (s *geminiLocalCacheStoreStub) GetGeminiLocalCacheStats(ctx context.Context, filter GeminiLocalCacheStatsFilter) (*GeminiLocalCacheStats, error) {
	key := "global"
	scope := "global"
	if filter.AccountID > 0 && strings.TrimSpace(filter.Model) != "" {
		key = fmt.Sprintf("account:%d:model:%s", filter.AccountID, strings.TrimSpace(filter.Model))
		scope = "account_model"
	} else if filter.AccountID > 0 {
		key = fmt.Sprintf("account:%d", filter.AccountID)
		scope = "account"
	}
	if s.stats == nil || s.stats[key] == nil {
		return &GeminiLocalCacheStats{Scope: scope, AccountID: filter.AccountID, Model: strings.TrimSpace(filter.Model)}, nil
	}
	cp := *s.stats[key]
	cp.Scope = scope
	cp.AccountID = filter.AccountID
	cp.Model = strings.TrimSpace(filter.Model)
	return &cp, nil
}

func geminiLocalCacheStatsStubKeys(accountID int64, model string) []string {
	keys := []string{
		"global",
		fmt.Sprintf("account:%d", accountID),
	}
	if strings.TrimSpace(model) != "" {
		keys = append(keys, fmt.Sprintf("account:%d:model:%s", accountID, strings.TrimSpace(model)))
	}
	return keys
}

func TestGeminiLocalCacheTrackerCreatesThenHitsStablePayload(t *testing.T) {
	store := &geminiLocalCacheStoreStub{}
	tracker := NewGeminiLocalCacheTracker(store)
	account := &Account{ID: 42, Type: AccountTypeAPIKey}
	stableDoc := strings.Repeat("stable document text ", 300)

	body := []byte(fmt.Sprintf(`{
		"systemInstruction":{"parts":[{"text":"system"}]},
		"contents":[
			{"role":"user","parts":[{"text":%q}]},
			{"role":"user","parts":[{"text":"question"}]}
		]
	}`, stableDoc))

	first := tracker.Track(context.Background(), account, "gemini-3.5-flash", body, 1000)
	require.True(t, first.Enabled)
	require.True(t, first.Created)
	require.False(t, first.Hit)
	require.Greater(t, first.EstimatedTokens, 0)
	require.LessOrEqual(t, first.EstimatedTokens, 1000)
	require.NotEmpty(t, first.CacheKey)

	second := tracker.Track(context.Background(), account, "gemini-3.5-flash", body, 1000)
	require.True(t, second.Enabled)
	require.False(t, second.Created)
	require.True(t, second.Hit)
	require.Equal(t, first.CacheKey, second.CacheKey)
	require.Equal(t, first.EstimatedTokens, second.EstimatedTokens)

	stats, err := store.GetGeminiLocalCacheStats(context.Background(), GeminiLocalCacheStatsFilter{AccountID: account.ID, Model: "gemini-3.5-flash"})
	require.NoError(t, err)
	require.Equal(t, int64(2), stats.Requests)
	require.Equal(t, int64(2), stats.CandidateRequests)
	require.Equal(t, int64(1), stats.CacheCreationRequests)
	require.Equal(t, int64(1), stats.CacheReadRequests)
	require.Equal(t, 0.5, stats.HitRate)
}
