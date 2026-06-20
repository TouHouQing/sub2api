package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

type geminiContextCache struct {
	rdb *redis.Client
}

func NewGeminiContextCache(rdb *redis.Client) service.GeminiContextCache {
	return &geminiContextCache{rdb: rdb}
}

func (c *geminiContextCache) GetGeminiContextCache(ctx context.Context, key string) (*service.GeminiContextCacheEntry, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var entry service.GeminiContextCacheEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (c *geminiContextCache) SetGeminiContextCache(ctx context.Context, entry *service.GeminiContextCacheEntry, ttl time.Duration) error {
	if entry == nil || entry.CacheKey == "" || entry.Name == "" {
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
