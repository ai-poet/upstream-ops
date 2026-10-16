// 管理面入口：AdminService 持有共用的 *core；包文档见 service.go。
package gateway

// AdminService 网关管理面（分组 / 密钥 / 路由 / 直连渠道 / 模型同步）。
type AdminService struct {
	*core
	// rt 管理面要用数据面能力时（模型测试的探测转发）显式经它调用；数据面不反向依赖管理面
	rt *Runtime
}
