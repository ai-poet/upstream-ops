// 数据面：解析后的配置缓存。
//
// 分组 / 路由 / 密钥上的配置是 JSON 文本（模型映射、模型清单、系统提示词规则、IP 名单），
// 以前每个请求都重新解析一遍：按模型优先时每条路由的映射都要解析，模型清单大时尤其明显。
// 现在按文本内容缓存解析结果：同一份文本只解析一次，配置一改文本就变，旧条目自然不再命中。
// 结果在请求之间共享，只读，调用方不得修改。管理面仍用原来的解析函数（它们会改结果）。
package gateway

import (
	"strings"
	"sync"
)

// textCacheMax 每种缓存最多保留的不同文本数；满了整体清空（配置种类有限，正常到不了）。
const textCacheMax = 1024

type textCache[T any] struct {
	mu sync.RWMutex
	m  map[string]T
}

func (c *textCache[T]) get(raw string, parse func(string) T) T {
	c.mu.RLock()
	v, ok := c.m[raw]
	c.mu.RUnlock()
	if ok {
		return v
	}
	v = parse(raw)
	c.mu.Lock()
	if c.m == nil || len(c.m) >= textCacheMax {
		c.m = map[string]T{}
	}
	c.m[raw] = v
	c.mu.Unlock()
	return v
}

var (
	modelMappingCache      textCache[ModelMap]
	modelsJSONCache        textCache[[]ModelListItem]
	systemPromptRulesCache textCache[[]SystemPromptRule]
	stringListCache        textCache[[]string]
)

// cachedModelMapping 运行时用的模型映射（只读）。
func cachedModelMapping(raw string) ModelMap {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return modelMappingCache.get(raw, ParseModelMapping)
}

// cachedModelsJSON 运行时用的组模型清单（只读）。
func (svc *Service) cachedModelsJSON(raw string) []ModelListItem {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return modelsJSONCache.get(raw, svc.ParseModelsJSON)
}

// cachedSystemPromptRules 运行时用的系统提示词规则（只读）。
func cachedSystemPromptRules(raw string) []SystemPromptRule {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return systemPromptRulesCache.get(raw, parseSystemPromptRules)
}

// cachedStringList 运行时用的字符串名单（IP 黑白名单，只读）。
func (svc *Service) cachedStringList(raw string) []string {
	if t := strings.TrimSpace(raw); t == "" || t == "[]" || t == "null" {
		return nil
	}
	return stringListCache.get(raw, svc.parseStringListJSON)
}
