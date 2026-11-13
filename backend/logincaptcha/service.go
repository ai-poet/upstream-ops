package logincaptcha

import (
	"strings"

	base64Captcha "github.com/mojocn/base64Captcha"
)

const captchaSource = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz"

// Service 生成并校验登录图形验证码。它拥有独立 Store，不随鉴权配置热更新重建。
type Service struct {
	captcha *base64Captcha.Captcha
	store   *Store
}

// NewService 创建 4 位数字/字母图形验证码服务。
func NewService() *Service {
	store := NewStore(DefaultTTL)
	driver := base64Captcha.NewDriverString(
		80,
		240,
		2,
		base64Captcha.OptionShowSlimeLine,
		4,
		captchaSource,
		nil,
		nil,
		nil,
	)
	return &Service{
		captcha: base64Captcha.NewCaptcha(driver, store),
		store:   store,
	}
}

// Generate 返回验证码 ID 和可直接用于 img.src 的 data URI。
func (s *Service) Generate() (id, imageBase64 string, err error) {
	id, imageBase64, _, err = s.captcha.Generate()
	return id, imageBase64, err
}

// Verify 一次性消费验证码；过期、错误或不存在的验证码都返回 false。
func (s *Service) Verify(id, answer string) bool {
	return s.store.Verify(id, strings.TrimSpace(answer), true)
}

// Close 停止验证码 Store 的后台清理协程。
func (s *Service) Close() {
	s.store.Close()
}
