package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	captchaErrorRequired    = "captcha_required"
	captchaErrorInvalid     = "captcha_invalid"
	captchaErrorUnavailable = "captcha_unavailable"
)

func registerAuth(g *gin.RouterGroup, d *Deps) {
	g.GET("/auth/captcha", func(c *gin.Context) { getLoginCaptcha(c, d) })
	g.POST("/auth/login", func(c *gin.Context) { login(c, d) })
	g.GET("/auth/me", func(c *gin.Context) { whoami(c, d) })
	g.POST("/auth/logout", func(c *gin.Context) {
		// 无状态 token，客户端丢弃即可；这个接口仅作语义存在。
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
}

type loginInput struct {
	Username    string `json:"username" binding:"required"`
	Password    string `json:"password" binding:"required"`
	CaptchaID   string `json:"captcha_id"`
	CaptchaCode string `json:"captcha_code"`
}

func getLoginCaptcha(c *gin.Context, d *Deps) {
	c.Header("Cache-Control", "no-store")
	if d.Runtime == nil || !d.Runtime.LoginCaptchaEnabled() {
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"enabled":      false,
			"captcha_id":   "",
			"image_base64": "",
		}})
		return
	}
	if d.LoginCaptcha == nil {
		captchaFail(c, http.StatusInternalServerError, captchaErrorUnavailable, "captcha service is unavailable")
		return
	}
	id, image, err := d.LoginCaptcha.Generate()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"enabled":      true,
		"captcha_id":   id,
		"image_base64": image,
	}})
}

func login(c *gin.Context, d *Deps) {
	// 鉴权关闭：任何登录请求都直接成功，也不要求验证码字段；前端在 /auth/me 已经知道无需登录。
	authSvc := d.Runtime.CurrentAuth()
	if authSvc == nil {
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{
				"auth_disabled": true,
				"username":      "anonymous",
			},
		})
		return
	}
	var in loginInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if d.Runtime.LoginCaptchaEnabled() {
		if strings.TrimSpace(in.CaptchaID) == "" || strings.TrimSpace(in.CaptchaCode) == "" {
			captchaFail(c, http.StatusBadRequest, captchaErrorRequired, "captcha is required")
			return
		}
		if d.LoginCaptcha == nil {
			captchaFail(c, http.StatusInternalServerError, captchaErrorUnavailable, "captcha service is unavailable")
			return
		}
		if !d.LoginCaptcha.Verify(in.CaptchaID, in.CaptchaCode) {
			captchaFail(c, http.StatusBadRequest, captchaErrorInvalid, "captcha is invalid or expired")
			return
		}
	}
	token, exp, err := authSvc.Login(in.Username, in.Password)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"token":      token,
			"expires_at": exp.Unix(),
			"username":   authSvc.Username(),
		},
	})
}

func captchaFail(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": message, "code": code})
}

// whoami 既是"前端启动探测"接口也是"已登录信息"接口。
//
//   - 鉴权关闭 → 返回 {auth_disabled: true}，前端据此跳过登录页
//   - 鉴权开启但未带 token → 中间件已经在前面 401 拦截，根本走不到这里
//   - 鉴权开启 + 有效 token → 返回 username
func whoami(c *gin.Context, d *Deps) {
	if d.Runtime.CurrentAuth() == nil {
		c.JSON(http.StatusOK, gin.H{
			"data": gin.H{
				"auth_disabled": true,
				"username":      "anonymous",
			},
		})
		return
	}
	sub, _ := c.Get("authSubject")
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"username": sub}})
}
