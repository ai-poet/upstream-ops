package gateway

import "testing"

func TestStableUpstreamKeyName(t *testing.T) {
	sg := int64(14)
	// 同渠道 + 同源分组 ID → 同名（跨网关组复用）
	a := (&AdminService{core: &core{}}).stableUpstreamKeyName(1, &sg, "grok")
	b := (&AdminService{core: &core{}}).stableUpstreamKeyName(1, &sg, "other-name-ignored")
	if a != b || a != "uops-ch1-sg14" {
		t.Fatalf("got %q %q", a, b)
	}
	// 不同渠道 → 不同名
	if (&AdminService{core: &core{}}).stableUpstreamKeyName(2, &sg, "") == a {
		t.Fatal("channel should differ")
	}
	// 仅有名
	n := (&AdminService{core: &core{}}).stableUpstreamKeyName(3, nil, "grok")
	if n != "uops-ch3-sgn-grok" {
		t.Fatalf("name-only: %q", n)
	}
	// 未绑定分组
	if (&AdminService{core: &core{}}).stableUpstreamKeyName(3, nil, "") != "uops-ch3-default" {
		t.Fatal("default")
	}
}

func TestSlugForKeyName_Chinese(t *testing.T) {
	s1 := (&AdminService{core: &core{}}).slugForKeyName("高倍率")
	s2 := (&AdminService{core: &core{}}).slugForKeyName("高倍率")
	if s1 != s2 || s1 == "" {
		t.Fatalf("stable chinese slug: %q", s1)
	}
	if (&AdminService{core: &core{}}).slugForKeyName("低倍率") == s1 {
		t.Fatal("different names should differ")
	}
}
