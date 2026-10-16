package storage

import (
	"strings"
	"time"

	"gorm.io/gorm"
)

// UnpricedModelStat 计费时找不到价格的模型（费用按 0 记）在一段时间内的用量汇总。
type UnpricedModelStat struct {
	Model               string    `json:"model"`
	Requests            int64     `json:"requests"`
	SuccessRequests     int64     `json:"success_requests"`
	InputTokens         int64     `json:"input_tokens"`
	OutputTokens        int64     `json:"output_tokens"`
	CacheReadTokens     int64     `json:"cache_read_tokens"`
	CacheCreationTokens int64     `json:"cache_creation_tokens"`
	LastSeenAt          time.Time `json:"last_seen_at"`
}

// ListUnpricedModels 按计费用的模型名（上游模型，没有则请求模型）汇总 since 之后标记为未定价的用量，按请求数降序。
func (r *GatewayUsageLogs) ListUnpricedModels(since time.Time, limit int) ([]UnpricedModelStat, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	type row struct {
		Model               string
		Requests            int64
		SuccessRequests     int64
		InputTokens         int64
		OutputTokens        int64
		CacheReadTokens     int64
		CacheCreationTokens int64
		LastID              uint
	}
	var rows []row
	err := r.db.Model(&GatewayUsageLog{}).
		Select(`COALESCE(NULLIF(upstream_model, ''), requested_model) AS model,
			COUNT(*) AS requests,
			COALESCE(SUM(CASE WHEN success THEN 1 ELSE 0 END), 0) AS success_requests,
			COALESCE(SUM(input_tokens), 0) AS input_tokens,
			COALESCE(SUM(output_tokens), 0) AS output_tokens,
			COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
			COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
			MAX(id) AS last_id`).
		Where("unpriced = ? AND created_at >= ?", true, since).
		Group("COALESCE(NULLIF(upstream_model, ''), requested_model)").
		Order("requests DESC").
		Limit(limit).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	// 最近一次出现的时间：按每组最大 id 取 created_at（聚合出来的时间在 SQLite 里是文本，不直接扫）
	ids := make([]uint, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.LastID)
	}
	lastSeen := map[uint]time.Time{}
	if len(ids) > 0 {
		var logs []GatewayUsageLog
		if err := r.db.Session(&gorm.Session{}).Select("id", "created_at").Where("id IN ?", ids).Find(&logs).Error; err != nil {
			return nil, err
		}
		for _, l := range logs {
			lastSeen[l.ID] = l.CreatedAt
		}
	}
	out := make([]UnpricedModelStat, 0, len(rows))
	for _, row := range rows {
		model := strings.TrimSpace(row.Model)
		if model == "" {
			continue
		}
		out = append(out, UnpricedModelStat{
			Model:               model,
			Requests:            row.Requests,
			SuccessRequests:     row.SuccessRequests,
			InputTokens:         row.InputTokens,
			OutputTokens:        row.OutputTokens,
			CacheReadTokens:     row.CacheReadTokens,
			CacheCreationTokens: row.CacheCreationTokens,
			LastSeenAt:          lastSeen[row.LastID],
		})
	}
	return out, nil
}
