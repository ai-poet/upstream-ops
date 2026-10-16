package storage

import "sync/atomic"

// gatewayConfigVersion 网关配置版本：密钥 / 分组 / 路由 / 直连渠道 / 价格覆盖任一写入成功后加一。
// 网关热路径的内存缓存读库前记下版本，版本变了就重新加载（写后加一，读前取版本，不会把旧数据当新的缓存）。
var gatewayConfigVersion atomic.Uint64

// GatewayConfigVersion 当前网关配置版本。
func GatewayConfigVersion() uint64 { return gatewayConfigVersion.Load() }

// bumpGatewayConfig 写入成功（*err == nil）时让网关配置版本加一；用法：defer bumpGatewayConfig(&err)。
func bumpGatewayConfig(err *error) {
	if err == nil || *err == nil {
		gatewayConfigVersion.Add(1)
	}
}
