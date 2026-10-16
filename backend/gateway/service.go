// Package gateway 实现 OpenAI / Anthropic / Responses 兼容的请求转发网关。
//
// 分层：
//   - core：两面共用的依赖与状态（仓储、运行时配置、配置缓存、源分组缓存、上游 HTTP 客户端、上游目标解析），见 core_*.go；
//   - Runtime：数据面（鉴权 / 转发 / 流式 / 公开端点 / 用量落库），见 runtime_*.go；
//   - AdminService：管理面（分组 / 密钥 / 路由 / 直连渠道 / 模型同步），见 admin_*.go；
//     要用数据面能力（探测转发、拉上游模型）时显式经 rt 字段，数据面不依赖管理面；
//   - Service：组装根，只负责构造与生命周期（启动 / 停止后台任务、下发配置）。调用方用 svc.Admin / svc.Runtime。
//
// 协议互转见子包 protocol；运行时默认值见 config.DefaultGateway*，可由设置页 gateway 段覆盖。
package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

// 使用记录 attempt_kind
const (
	attemptKindPrimary  = "primary"
	attemptKindRetry    = "retry"
	attemptKindFailover = "failover"
)

// ChannelAPI 上游密钥管理能力（由 channel.Service 实现）。
type ChannelAPI interface {
	ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error)
	ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error)
	CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error)
	UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error)
	RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error)
}

// core 管理面与数据面共用的依赖与状态。Runtime 与 AdminService 都持有同一个 *core。
type core struct {
	Groups     *storage.GatewayGroups
	Keys       *storage.GatewayKeys
	Routes     *storage.GatewayRoutes
	Providers  *storage.GatewayProviders
	Usage      *storage.GatewayUsageLogs
	Prices     *storage.ModelPriceOverrides
	Channels   *storage.Channels
	ChannelAPI ChannelAPI
	Cipher     *crypto.Cipher
	Pricing    *pricing.Catalog
	Log        *slog.Logger

	mu          sync.RWMutex
	proxyConfig config.ProxyConfig
	upstream    config.UpstreamConfig
	gatewayCfg  config.GatewayConfig
	// gatewayCfgReady gatewayCfg 已经过 WithDefaults（热路径上每个请求要读几十次，避免重复规范化）
	gatewayCfgReady bool

	modelsCacheMu sync.Mutex
	modelsCache   map[uint]modelsCacheEntry // keyed by group id

	// 源分组列表缓存（ListAPIKeyGroups 远程调用昂贵；列表接口不再实时拉，运行时/保存仍可复用缓存）
	channelGroupsCacheMu sync.Mutex
	channelGroupsCache   map[uint]channelGroupsCacheEntry // keyed by channel id
	// 进行中的源分组拉取（同一渠道并发只打一次上游），由 channelGroupsCacheMu 保护
	channelGroupsInflight map[uint]*channelGroupsCall

	// 异步任务（图片 / 视频 / Seedance / 语音 / cachedContents）创建时的路由，供后续查询回到同一上游
	tasks *taskAffinity

	autoRecoverMu   sync.Mutex
	autoRecoverStop chan struct{}

	// 热路径配置缓存（密钥 / 分组 / 路由 / 渠道，见 core_config_cache.go）
	configCacheOnce sync.Once
	cfgCache        *runtimeConfigCache

	// 后台批量写入（用量日志、密钥额度 / 最后使用时间），见 write_behind.go；nil 时同步写
	writer atomic.Pointer[writeBehind]

	// 转发用的 HTTP 客户端，按「代理 + 转发超时」复用连接（见 httpClientForTarget）
	upstreamClientsMu sync.Mutex
	upstreamClients   map[string]*http.Client

	// 找不到价格的模型告警限频（见 warnUnpricedModel）
	unpricedWarn unpricedWarner
}

// Service 网关组装根：构造 core / Admin / Runtime，管理后台任务与配置下发。
// 管理面操作走 Admin，数据面转发走 Runtime；仓储与配置字段可经 core 直接读写。
type Service struct {
	core
	// Admin 管理面。
	Admin *AdminService
	// Runtime 数据面。
	Runtime *Runtime
}

type modelsCacheEntry struct {
	at   time.Time
	body []byte
}

type channelGroupsCacheEntry struct {
	at     time.Time
	groups []connector.APIKeyGroup
}

// NewService 构造网关服务并初始化 Admin / Runtime。
func NewService(
	groups *storage.GatewayGroups,
	keys *storage.GatewayKeys,
	routes *storage.GatewayRoutes,
	usage *storage.GatewayUsageLogs,
	prices *storage.ModelPriceOverrides,
	channels *storage.Channels,
	channelAPI ChannelAPI,
	cipher *crypto.Cipher,
	log *slog.Logger,
) *Service {
	s := &Service{core: core{
		Groups:             groups,
		Keys:               keys,
		Routes:             routes,
		Usage:              usage,
		Prices:             prices,
		Channels:           channels,
		ChannelAPI:         channelAPI,
		Cipher:             cipher,
		Pricing:            pricing.NewCatalog(prices),
		Log:                log,
		gatewayCfg:         config.GatewayConfig{}.WithDefaults(),
		gatewayCfgReady:    true,
		modelsCache:        map[uint]modelsCacheEntry{},
		channelGroupsCache: map[uint]channelGroupsCacheEntry{},
		tasks:              newTaskAffinity(),
	}}
	s.Runtime = &Runtime{core: &s.core}
	s.Admin = &AdminService{core: &s.core, rt: s.Runtime}
	return s
}

// SetProviders 注入直连渠道仓储（main 组装时调用，保持 NewService 签名兼容）。
func (s *Service) SetProviders(p *storage.GatewayProviders) {
	s.Providers = p
}

// ListDefaultPrices 返回内置模型价目（管理端只读）。
func (s *Service) ListDefaultPrices(query string) []pricing.DefaultPriceItem {
	if s.Pricing == nil {
		return nil
	}
	return s.Pricing.ListDefaults(query)
}

func (s *Service) UpdateProxyConfig(cfg config.ProxyConfig) {
	s.mu.Lock()
	s.proxyConfig = cfg
	s.mu.Unlock()
	s.resetUpstreamClients()
}

func (s *Service) UpdateUpstreamConfig(cfg config.UpstreamConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upstream = cfg.WithDefaults()
}

func (s *Service) UpdateGatewayConfig(cfg config.GatewayConfig) {
	s.mu.Lock()
	s.gatewayCfg = cfg.WithDefaults()
	s.gatewayCfgReady = true
	protocol.SetEngine(s.gatewayCfg.ProtocolEngine)
	s.mu.Unlock()
	s.resetUpstreamClients()
}

// StartPricing 启动模型价格远程同步。内置价目始终可用，远程失败不会中断网关。
func (s *Service) StartPricing(cfg config.PricingConfig) {
	if s != nil && s.Pricing != nil {
		s.Pricing.Start(cfg, s.Log)
	}
}

// UpdatePricingConfig 应用新配置并重启价格同步任务。
func (s *Service) UpdatePricingConfig(cfg config.PricingConfig) {
	s.StartPricing(cfg)
}

// Stop 停止网关后台任务。
func (s *Service) Stop() {
	if s == nil {
		return
	}
	if s.Pricing != nil {
		s.Pricing.Stop()
	}
	s.stopAutoRecover()
	s.stopWriteBehind()
}
