// 数据面入口：Runtime 持有共用的 *core；包文档见 service.go。
package gateway

// Runtime 网关数据面运行时（鉴权、转发、流式、公开端点、用量落库）。
type Runtime struct {
	*core
}

// runtime 返回数据面入口；兼容测试里没走 NewService 的 &Service{} 字面量。
func (s *Service) runtime() *Runtime {
	if s == nil {
		return nil
	}
	if s.Runtime == nil {
		s.Runtime = &Runtime{core: &s.core}
	}
	return s.Runtime
}
