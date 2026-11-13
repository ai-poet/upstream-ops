package api

import (
	"fmt"
	"net/http"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/gin-gonic/gin"
)

type settingsConfigView struct {
	App           config.AppConfig           `json:"app"`
	Auth          config.AuthConfig          `json:"auth"`
	Scheduler     config.SchedulerConfig     `json:"scheduler"`
	Notifications config.NotificationsConfig `json:"notifications"`
	Proxy         config.ProxyConfig         `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
	Gateway       config.GatewayConfig       `json:"gateway"`
}

type settingsConfigInput struct {
	App           config.AppConfig           `json:"app" binding:"required"`
	Auth          settingsAuthInput          `json:"auth" binding:"required"`
	Scheduler     config.SchedulerConfig     `json:"scheduler" binding:"required"`
	Notifications config.NotificationsConfig `json:"notifications" binding:"required"`
	Proxy         config.ProxyConfig         `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
	Gateway       config.GatewayConfig       `json:"gateway"`
}

// settingsAuthInput 使用指针区分字段缺失与零值，避免部分保存时意外覆盖现有配置。
type settingsAuthInput struct {
	Enabled         *bool   `json:"enabled"`
	CaptchaEnabled  *bool   `json:"captchaEnabled"`
	Username        *string `json:"username"`
	Password        *string `json:"password"`
	TokenSecret     *string `json:"tokenSecret"`
	SessionTTLHours *int    `json:"sessionTTLHours"`
}

func (in settingsAuthInput) apply(cfg *config.AuthConfig) {
	if in.Enabled != nil {
		cfg.Enabled = *in.Enabled
	}
	if in.CaptchaEnabled != nil {
		cfg.CaptchaEnabled = *in.CaptchaEnabled
	}
	if in.Username != nil {
		cfg.Username = *in.Username
	}
	// 密码留空表示不修改，避免设置页回传空密码时清空已有凭据。
	if in.Password != nil && *in.Password != "" {
		cfg.Password = *in.Password
	}
	if in.TokenSecret != nil {
		cfg.TokenSecret = *in.TokenSecret
	}
	if in.SessionTTLHours != nil {
		cfg.SessionTTLHours = *in.SessionTTLHours
	}
}

func registerSettings(g *gin.RouterGroup, d *Deps) {
	gs := g.Group("/settings")
	gs.GET("/config", func(c *gin.Context) { getSettingsConfig(c, d) })
	gs.PUT("/config", func(c *gin.Context) { saveSettingsConfig(c, d) })
	gs.POST("/apply", func(c *gin.Context) { applySettingsConfig(c, d) })
	gs.POST("/proxy/test", func(c *gin.Context) { testProxy(c) })
}

func getSettingsConfig(c *gin.Context, d *Deps) {
	cfg, err := config.LoadFile(d.Runtime.ConfigPath())
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path": d.Runtime.ConfigPath(),
			"config": settingsConfigView{
				App:           cfg.App,
				Auth:          cfg.Auth,
				Scheduler:     cfg.Scheduler,
				Notifications: cfg.Notifications,
				Proxy:         cfg.Proxy,
				Upstream:      cfg.Upstream.WithDefaults(),
				Gateway:       cfg.Gateway.WithDefaults(),
			},
		},
	})
}

func saveSettingsConfig(c *gin.Context, d *Deps) {
	var in settingsConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	// 状态码写错会让运行时悄悄跳过整条规则，保存时就拦下
	codes, err := gateway.NormalizeAutoDisableStatusCodes(in.Gateway.AutoDisableStatusCodes)
	if err != nil {
		fail(c, http.StatusBadRequest, fmt.Errorf("gateway.autoDisableStatusCodes: %w", err))
		return
	}
	in.Gateway.AutoDisableStatusCodes = codes

	path := d.Runtime.ConfigPath()
	cfg, err := config.LoadFile(path)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	cfg.App.Title = in.App.Title
	cfg.App.NotificationPrefix = in.App.NotificationPrefix
	in.Auth.apply(&cfg.Auth)
	cfg.Scheduler = in.Scheduler
	cfg.Notifications = in.Notifications
	cfg.Proxy = in.Proxy
	cfg.Upstream = in.Upstream.WithDefaults()
	// 与内置默认一致的自动禁用规则不落文件，以后版本更新默认关键词时自动生效
	cfg.Gateway = in.Gateway.WithDefaults().ForSave()

	if err := config.Save(path, cfg); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path": path,
			"message":     "已写入配置文件",
		},
	})
}

func applySettingsConfig(c *gin.Context, d *Deps) {
	result, err := d.Runtime.ApplyFromFile()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}
