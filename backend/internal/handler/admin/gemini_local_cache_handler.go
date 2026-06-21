package admin

import (
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

type GeminiLocalCacheHandler struct {
	localCache service.GeminiLocalCache
}

func NewGeminiLocalCacheHandler(localCache service.GeminiLocalCache) *GeminiLocalCacheHandler {
	return &GeminiLocalCacheHandler{localCache: localCache}
}

// GetStats handles local Gemini cache observability counters.
// GET /api/v1/admin/gemini/local-cache/stats?account_id=1&model=gemini-2.5-flash
func (h *GeminiLocalCacheHandler) GetStats(c *gin.Context) {
	if h == nil || h.localCache == nil {
		response.InternalError(c, "Gemini local cache stats unavailable")
		return
	}

	var accountID int64
	if raw := strings.TrimSpace(c.Query("account_id")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			response.BadRequest(c, "Invalid account_id")
			return
		}
		accountID = parsed
	}
	model := strings.TrimSpace(c.Query("model"))
	if model != "" && accountID == 0 {
		response.BadRequest(c, "model requires account_id")
		return
	}

	stats, err := h.localCache.GetGeminiLocalCacheStats(c.Request.Context(), service.GeminiLocalCacheStatsFilter{
		AccountID: accountID,
		Model:     model,
	})
	if err != nil {
		response.InternalError(c, "Failed to get Gemini local cache stats")
		return
	}
	response.Success(c, stats)
}
