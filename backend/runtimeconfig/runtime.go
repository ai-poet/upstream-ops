// Package runtimeconfig 管理可热更新的运行时配置。
package runtimeconfig

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/auth"
	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/scheduler"
	"github.com/gin-gonic/gin"
)

type SchedulerFactory func(config.SchedulerConfig, config.ProxyConfig) *scheduler.Scheduler

type Manager struct {
	mu               sync.RWMutex
	configPath       string
	securitySecret   string
	log              *slog.Logger
	dispatcher       *notify.Dispatcher
	channelSvc       *channel.Service
	gatewaySvc       *gateway.Service
	schedulerFactory SchedulerFactory
	auth             *auth.Service
	scheduler        *scheduler.Scheduler
	proxyConfig      config.ProxyConfig
	upstreamConfig   config.UpstreamConfig
	gatewayConfig    config.GatewayConfig
}

type ApplyResult struct {
	AppliedSections []string `json:"applied_sections"`
	Message         string   `json:"message"`
}

func New(
	configPath string,
	securitySecret string,
	log *slog.Logger,
	dispatcher *notify.Dispatcher,
	channelSvc *channel.Service,
	gatewaySvc *gateway.Service,
	authSvc *auth.Service,
	schedulerSvc *scheduler.Scheduler,
	proxyConfig config.ProxyConfig,
	upstreamConfig config.UpstreamConfig,
	gatewayConfig config.GatewayConfig,
	schedulerFactory SchedulerFactory,
) *Manager {
	return &Manager{
		configPath:       configPath,
		securitySecret:   securitySecret,
		log:              log,
		dispatcher:       dispatcher,
		channelSvc:       channelSvc,
		gatewaySvc:       gatewaySvc,
		schedulerFactory: schedulerFactory,
		auth:             authSvc,
		scheduler:        schedulerSvc,
		proxyConfig:      proxyConfig,
		upstreamConfig:   upstreamConfig.WithDefaults(),
		gatewayConfig:    gatewayConfig.WithDefaults(),
	}
}

func (m *Manager) ConfigPath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.configPath
}

func (m *Manager) CurrentAuth() *auth.Service {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.auth
}

func (m *Manager) CurrentProxy() config.ProxyConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.proxyConfig
}

func (m *Manager) CurrentUpstream() config.UpstreamConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.upstreamConfig
}

func (m *Manager) CurrentGateway() config.GatewayConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gatewayConfig
}

func (m *Manager) AuthMiddleware() gin.HandlerFunc {
	whitelist := map[string]struct{}{
		"/healthz":        {},
		"/api/version":    {},
		"/api/auth/login": {},
	}
	return func(c *gin.Context) {
		if _, ok := whitelist[c.FullPath()]; ok {
			c.Next()
			return
		}
		if _, ok := whitelist[c.Request.URL.Path]; ok {
			c.Next()
			return
		}
		svc := m.CurrentAuth()
		if svc == nil {
			c.Next()
			return
		}
		svc.Middleware()(c)
	}
}

func (m *Manager) ApplyFromFile() (*ApplyResult, error) {
	m.mu.RLock()
	path := m.configPath
	secret := m.securitySecret
	dispatcher := m.dispatcher
	channelSvc := m.channelSvc
	gatewaySvc := m.gatewaySvc
	factory := m.schedulerFactory
	oldScheduler := m.scheduler
	m.mu.RUnlock()

	cfg, err := config.LoadFile(path)
	if err != nil {
		return nil, err
	}

	authSvc, err := buildAuth(cfg.Auth, secret)
	if err != nil {
		return nil, err
	}

	if dispatcher != nil {
		dispatcher.UpdatePolicy(notify.Policy{
			NotificationPrefix:                       cfg.App.NotificationPrefix,
			MinChangePct:                             cfg.Notifications.MinChangePct,
			BalanceLowCooldown:                       time.Duration(cfg.Notifications.BalanceLowCooldownMinutes) * time.Minute,
			BalanceDepletionLead:                     time.Duration(cfg.Notifications.BalanceDepletionLeadMinutes) * time.Minute,
			SubscriptionDailyRemainingThresholdPct:   cfg.Notifications.SubscriptionDailyRemainingThresholdPct,
			SubscriptionWeeklyRemainingThresholdPct:  cfg.Notifications.SubscriptionWeeklyRemainingThresholdPct,
			SubscriptionMonthlyRemainingThresholdPct: cfg.Notifications.SubscriptionMonthlyRemainingThresholdPct,
			SubscriptionExpiryThreshold:              time.Duration(cfg.Notifications.SubscriptionExpiryThresholdHours) * time.Hour,
			SubscriptionAlertCooldown:                time.Duration(cfg.Notifications.SubscriptionAlertCooldownMinutes) * time.Minute,
			SendMaxAttempts:                          cfg.Notifications.SendMaxAttempts,
		})
		dispatcher.UpdateProxyConfig(cfg.Proxy)
	}
	if channelSvc != nil {
		channelSvc.UpdateProxyConfig(cfg.Proxy)
		channelSvc.UpdateUpstreamConfig(cfg.Upstream)
	}
	gwCfg := cfg.Gateway.WithDefaults()
	if gatewaySvc != nil {
		gatewaySvc.UpdateProxyConfig(cfg.Proxy)
		gatewaySvc.UpdateUpstreamConfig(cfg.Upstream)
		gatewaySvc.UpdateGatewayConfig(gwCfg)
		gatewaySvc.UpdatePricingConfig(cfg.Pricing)
	}

	newScheduler := factory(cfg.Scheduler, cfg.Proxy)
	if err := newScheduler.Start(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.auth = authSvc
	m.scheduler = newScheduler
	m.proxyConfig = cfg.Proxy
	m.upstreamConfig = cfg.Upstream.WithDefaults()
	m.gatewayConfig = gwCfg
	m.mu.Unlock()

	if oldScheduler != nil {
		oldScheduler.Stop()
	}

	sections := []string{"app", "auth", "scheduler", "notifications", "retention", "proxy", "upstream", "gateway", "pricing"}
	if m.log != nil {
		m.log.Info("runtime config applied",
			"sections", sections,
			"config_path", path,
		)
	}

	return &ApplyResult{
		AppliedSections: sections,
		Message:         "app、auth、scheduler、notifications、retention、proxy、upstream、gateway 已立即生效",
	}, nil
}

func buildAuth(cfg config.AuthConfig, securitySecret string) (*auth.Service, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	tokenSecret := cfg.TokenSecret
	if tokenSecret == "" {
		tokenSecret = securitySecret
	}
	svc, err := auth.New(
		cfg.Username,
		cfg.Password,
		tokenSecret,
		time.Duration(cfg.SessionTTLHours)*time.Hour,
	)
	if err != nil {
		return nil, fmt.Errorf("init auth: %w", err)
	}
	return svc, nil
}
