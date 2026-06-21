package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

type geminiLocalCache struct {
	rdb *redis.Client
}

func NewGeminiLocalCache(rdb *redis.Client) service.GeminiLocalCache {
	return &geminiLocalCache{rdb: rdb}
}

func (c *geminiLocalCache) GetGeminiLocalCache(ctx context.Context, key string) (*service.GeminiLocalCacheEntry, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var entry service.GeminiLocalCacheEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (c *geminiLocalCache) SetGeminiLocalCache(ctx context.Context, entry *service.GeminiLocalCacheEntry, ttl time.Duration) error {
	if entry == nil || entry.CacheKey == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = time.Until(entry.ExpiresAt)
	}
	if ttl <= 0 {
		return nil
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, entry.CacheKey, raw, ttl).Err()
}

func (c *geminiLocalCache) RecordGeminiLocalCacheEvent(ctx context.Context, event service.GeminiLocalCacheEvent) error {
	if c == nil || c.rdb == nil || event.AccountID <= 0 {
		return nil
	}
	eventName := strings.TrimSpace(event.Event)
	if eventName == "" {
		return nil
	}
	for _, key := range geminiLocalCacheStatsKeys(event.AccountID, event.Model) {
		pipe := c.rdb.TxPipeline()
		pipe.HIncrBy(ctx, key, "requests", 1)
		switch eventName {
		case service.GeminiLocalCacheEventCreated:
			pipe.HIncrBy(ctx, key, "candidate_requests", 1)
			pipe.HIncrBy(ctx, key, "cache_creation_requests", 1)
			if event.EstimatedTokens > 0 {
				pipe.HIncrBy(ctx, key, "estimated_created_tokens", int64(event.EstimatedTokens))
			}
		case service.GeminiLocalCacheEventHit:
			pipe.HIncrBy(ctx, key, "candidate_requests", 1)
			pipe.HIncrBy(ctx, key, "cache_read_requests", 1)
			if event.EstimatedTokens > 0 {
				pipe.HIncrBy(ctx, key, "estimated_read_tokens", int64(event.EstimatedTokens))
			}
		case service.GeminiLocalCacheEventSkipped:
			pipe.HIncrBy(ctx, key, "skipped_requests", 1)
		case service.GeminiLocalCacheEventError:
			pipe.HIncrBy(ctx, key, "candidate_requests", 1)
			pipe.HIncrBy(ctx, key, "error_requests", 1)
		default:
			continue
		}
		pipe.Expire(ctx, key, 30*24*time.Hour)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *geminiLocalCache) GetGeminiLocalCacheStats(ctx context.Context, filter service.GeminiLocalCacheStatsFilter) (*service.GeminiLocalCacheStats, error) {
	if c == nil || c.rdb == nil {
		return &service.GeminiLocalCacheStats{}, nil
	}
	key, scope := geminiLocalCacheStatsKeyForFilter(filter)
	values, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	stats := &service.GeminiLocalCacheStats{
		Scope:     scope,
		AccountID: filter.AccountID,
		Model:     strings.TrimSpace(filter.Model),
	}
	stats.Requests = parseGeminiLocalCacheStatInt(values["requests"])
	stats.CandidateRequests = parseGeminiLocalCacheStatInt(values["candidate_requests"])
	stats.CacheCreationRequests = parseGeminiLocalCacheStatInt(values["cache_creation_requests"])
	stats.CacheReadRequests = parseGeminiLocalCacheStatInt(values["cache_read_requests"])
	stats.SkippedRequests = parseGeminiLocalCacheStatInt(values["skipped_requests"])
	stats.ErrorRequests = parseGeminiLocalCacheStatInt(values["error_requests"])
	stats.EstimatedCreatedTokens = parseGeminiLocalCacheStatInt(values["estimated_created_tokens"])
	stats.EstimatedReadTokens = parseGeminiLocalCacheStatInt(values["estimated_read_tokens"])
	if stats.CandidateRequests > 0 {
		stats.HitRate = float64(stats.CacheReadRequests) / float64(stats.CandidateRequests)
	}
	return stats, nil
}

func geminiLocalCacheStatsKeys(accountID int64, model string) []string {
	model = strings.TrimSpace(model)
	keys := []string{
		"gemini-local-cache:stats:global",
		fmt.Sprintf("gemini-local-cache:stats:account:%d", accountID),
	}
	if model != "" {
		keys = append(keys, fmt.Sprintf("gemini-local-cache:stats:account:%d:model:%s", accountID, model))
	}
	return keys
}

func geminiLocalCacheStatsKeyForFilter(filter service.GeminiLocalCacheStatsFilter) (string, string) {
	model := strings.TrimSpace(filter.Model)
	if filter.AccountID > 0 && model != "" {
		return fmt.Sprintf("gemini-local-cache:stats:account:%d:model:%s", filter.AccountID, model), "account_model"
	}
	if filter.AccountID > 0 {
		return fmt.Sprintf("gemini-local-cache:stats:account:%d", filter.AccountID), "account"
	}
	return "gemini-local-cache:stats:global", "global"
}

func parseGeminiLocalCacheStatInt(raw string) int64 {
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
