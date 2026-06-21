package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type geminiLocalCacheHandlerStoreStub struct {
	stats *service.GeminiLocalCacheStats
}

func (s *geminiLocalCacheHandlerStoreStub) GetGeminiLocalCache(ctx context.Context, key string) (*service.GeminiLocalCacheEntry, error) {
	return nil, nil
}

func (s *geminiLocalCacheHandlerStoreStub) SetGeminiLocalCache(ctx context.Context, entry *service.GeminiLocalCacheEntry, ttl time.Duration) error {
	return nil
}

func (s *geminiLocalCacheHandlerStoreStub) RecordGeminiLocalCacheEvent(ctx context.Context, event service.GeminiLocalCacheEvent) error {
	return nil
}

func (s *geminiLocalCacheHandlerStoreStub) GetGeminiLocalCacheStats(ctx context.Context, filter service.GeminiLocalCacheStatsFilter) (*service.GeminiLocalCacheStats, error) {
	if s.stats == nil {
		return &service.GeminiLocalCacheStats{}, nil
	}
	cp := *s.stats
	cp.AccountID = filter.AccountID
	cp.Model = filter.Model
	return &cp, nil
}

func TestGeminiLocalCacheHandlerGetStats(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewGeminiLocalCacheHandler(&geminiLocalCacheHandlerStoreStub{
		stats: &service.GeminiLocalCacheStats{
			Scope:                 "account_model",
			Requests:              2,
			CandidateRequests:     2,
			CacheCreationRequests: 1,
			CacheReadRequests:     1,
			HitRate:               0.5,
		},
	})
	router := gin.New()
	router.GET("/admin/gemini/local-cache/stats", handler.GetStats)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/gemini/local-cache/stats?account_id=86&model=gemini-3.5-flash", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body struct {
		Data service.GeminiLocalCacheStats `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, int64(86), body.Data.AccountID)
	require.Equal(t, "gemini-3.5-flash", body.Data.Model)
	require.Equal(t, int64(1), body.Data.CacheCreationRequests)
	require.Equal(t, int64(1), body.Data.CacheReadRequests)
	require.Equal(t, 0.5, body.Data.HitRate)
}

func TestGeminiLocalCacheHandlerGetStatsRequiresAccountForModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewGeminiLocalCacheHandler(&geminiLocalCacheHandlerStoreStub{})
	router := gin.New()
	router.GET("/admin/gemini/local-cache/stats", handler.GetStats)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/gemini/local-cache/stats?model=gemini-3.5-flash", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}
