# Model Pricing Data

与参考项目 sub2api 对齐的模型单价数据。

## 来源

- 运行时主表：`pricing.remoteURL`，默认
  `https://raw.githubusercontent.com/Wei-Shaw/model-price-repo/main/model_prices_and_context_window.json`
- 本地缓存：`pricing.dataDir/model_prices_and_context_window.json`
- 内置回退：本目录 `default_prices.json`
- 硬编码回退：`catalog.go` 中 `seedFallbackPrices` / `fallbackForModel`，对齐
  `tmp/sub2api/backend/internal/service/billing_service.go`（如 `grok-4.5` 不在 LiteLLM 表中）

## 解析顺序

1. DB 价格覆盖（`model_price_overrides`）
2. 本目录 JSON 精确/模糊匹配
3. 硬编码家族回退（Grok 等）

## DeepSeek

官方 `litellm_provider=deepseek` 条目（含 `deepseek/…` 前缀）与常见裸名别名
（`deepseek-v3`、`deepseek-r1`、`deepseek-v4-flash` 等）已写入本表。
无精确表项时，`catalog.go` 的 DeepSeek 家族前缀回退会落到对应主型号。

## 更新

服务启动时读取本地缓存并校验远程哈希；之后每 `pricing.hashCheckIntervalMinutes`
分钟检查一次，哈希变化即下载、写缓存并热更新内存价目，无需重启。
远程同步关闭或失败时继续使用缓存和内置表。

可在配置文件中调整：

```yaml
pricing:
  enabled: true
  remoteURL: https://raw.githubusercontent.com/Wei-Shaw/model-price-repo/main/model_prices_and_context_window.json
  hashURL: https://raw.githubusercontent.com/Wei-Shaw/model-price-repo/main/model_prices_and_context_window.sha256
  dataDir: ./data
  hashCheckIntervalMinutes: 10
```

手工更新内置回退表时：

```bash
# 从参考项目同步（精选子集，DeepSeek 较少）
cp tmp/sub2api/backend/resources/model-pricing/model_prices_and_context_window.json \
  backend/gateway/pricing/default_prices.json

# 再从 LiteLLM 主表补全 litellm_provider=deepseek 条目与常见别名
# （见仓库内补价脚本或手工合并 /tmp litellm json）
```
