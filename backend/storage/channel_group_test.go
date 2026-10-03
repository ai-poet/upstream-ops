package storage

import (
	"fmt"
	"testing"
)

// seedGroupedChannels 在 seedChannelListFixtures 的基础上设置分组：
//
//	Alpha → 主力   bravo → 备用   delta → 主力   Foxtrot → Claude   golf → claude
//	Charlie、echo 未分组
//
// 分组之间按名称规则排序：中文按拼音（备用 < 主力）排在字母前；Claude 与 claude
// 排序规则相同，再按原始字符串比较（大写在前）；未分组排在最后。
func seedGroupedChannels(t *testing.T) *Channels {
	t.Helper()
	channels := seedChannelListFixtures(t)
	groups := map[string]string{
		"Alpha":   "主力",
		"bravo":   "备用",
		"delta":   "主力",
		"Foxtrot": "Claude",
		"golf":    "claude",
	}
	all, err := channels.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for i := range all {
		group, ok := groups[all[i].Name]
		if !ok {
			continue
		}
		all[i].GroupName = group
		if err := channels.Update(&all[i]); err != nil {
			t.Fatalf("set group of %s: %v", all[i].Name, err)
		}
	}
	return channels
}

func TestChannelListPageGroupsFirst(t *testing.T) {
	channels := seedGroupedChannels(t)

	cases := []struct {
		label  string
		filter ChannelListFilter
		want   []string
	}{
		// 组内保持默认顺序（sort_order DESC, id ASC）：Alpha 排序 5，排在 delta 前面。
		{"default", ChannelListFilter{}, []string{"bravo", "Alpha", "delta", "Foxtrot", "golf", "Charlie", "echo"}},
		// 内存排序（名称）同样先分组，组内按名称降序。
		{"name desc", ChannelListFilter{Sort: ChannelSortName, Order: ChannelOrderDesc},
			[]string{"bravo", "delta", "Alpha", "Foxtrot", "golf", "echo", "Charlie"}},
		// SQL 排序（余额）同样先分组；未采集余额的 delta 在组内仍排最后。
		{"balance asc", ChannelListFilter{Sort: ChannelSortBalance, Order: ChannelOrderAsc},
			[]string{"bravo", "Alpha", "delta", "Foxtrot", "golf", "echo", "Charlie"}},
		// 搜索命中分组名；Alpha 同时由备注"主力渠道"命中。
		{"query group", ChannelListFilter{Query: "主力"}, []string{"Alpha", "delta"}},
		{"query group ascii case", ChannelListFilter{Query: "CLAUDE"}, []string{"Foxtrot", "golf"}},
		// 筛选后只剩一个分组和未分组的渠道。
		{"status failed", ChannelListFilter{Status: ChannelStatusFailed}, []string{"golf", "Charlie"}},
	}
	for _, tc := range cases {
		got, total := listChannelNames(t, channels, 1, -1, tc.filter)
		assertChannelNames(t, tc.label, got, tc.want...)
		if total != int64(len(tc.want)) {
			t.Fatalf("%s total = %d, want %d", tc.label, total, len(tc.want))
		}
	}

	// 先分组再分页，翻页不重不漏。
	for i, want := range [][]string{
		{"bravo", "Alpha", "delta"},
		{"Foxtrot", "golf", "Charlie"},
		{"echo"},
		nil,
	} {
		got, total := listChannelNames(t, channels, i+1, 3, ChannelListFilter{})
		assertChannelNames(t, fmt.Sprintf("page %d", i+1), got, want...)
		if total != 7 {
			t.Fatalf("page %d total = %d, want 7", i+1, total)
		}
	}

	// 全量列表 List() 不分组，保持默认顺序。
	all, err := channels.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	assertChannelNames(t, "List()", channelNames(all), defaultChannelListOrder...)
}
