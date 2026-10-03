package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

type channelGroupOut struct {
	ID        uint    `json:"id"`
	Name      string  `json:"name"`
	GroupName *string `json:"group_name"` // 指针用来区分 "" 与缺失
}

func decodeChannelGroup(t *testing.T, label string, rec *httptest.ResponseRecorder) channelGroupOut {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status = %d, body = %s", label, rec.Code, rec.Body.String())
	}
	var resp struct {
		Data channelGroupOut `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: decode: %v body=%s", label, err, rec.Body.String())
	}
	if resp.Data.GroupName == nil {
		t.Fatalf("%s: group_name missing, body=%s", label, rec.Body.String())
	}
	return resp.Data
}

func TestChannelGroupNameCreateUpdate(t *testing.T) {
	r, channels := newChannelMetaRouter(t)

	assertGroup := func(label string, id uint, method, path string, body any, want string) {
		t.Helper()
		got := decodeChannelGroup(t, label, doChannelJSON(t, r, method, path, body))
		if *got.GroupName != want {
			t.Fatalf("%s: group_name = %q, want %q", label, *got.GroupName, want)
		}
		if id == 0 {
			id = got.ID
		}
		ch, err := channels.FindByID(id)
		if err != nil {
			t.Fatalf("%s: find: %v", label, err)
		}
		if ch.GroupName != want {
			t.Fatalf("%s: stored group_name = %q, want %q", label, ch.GroupName, want)
		}
	}

	body := baseChannelCreateBody("demo")
	body["group_name"] = "  Claude 渠道 "
	assertGroup("create", 0, http.MethodPost, "/api/channels", body, "Claude 渠道")

	list, err := channels.List()
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %#v err = %v", list, err)
	}
	id := list[0].ID
	path := fmt.Sprintf("/api/channels/%d", id)

	assertGroup("get", id, http.MethodGet, path, nil, "Claude 渠道")
	// 省略 / null 不修改
	assertGroup("omitted", id, http.MethodPut, path, `{"name":"demo2"}`, "Claude 渠道")
	assertGroup("null", id, http.MethodPut, path, `{"group_name":null}`, "Claude 渠道")
	assertGroup("rename", id, http.MethodPut, path, `{"group_name":"GPT"}`, "GPT")
	// 空白等同移出分组
	assertGroup("clear", id, http.MethodPut, path, `{"group_name":"  "}`, "")

	// 未提供分组的新建渠道输出 ""
	assertGroup("create plain", 0, http.MethodPost, "/api/channels", baseChannelCreateBody("plain"), "")

	okName := strings.Repeat("组", storage.MaxChannelGroupRunes)
	longName := okName + "组"
	edge := baseChannelCreateBody("edge")
	edge["group_name"] = " " + okName + " "
	assertGroup("edge create", 0, http.MethodPost, "/api/channels", edge, okName)

	bad := baseChannelCreateBody("bad")
	bad["group_name"] = longName
	if rec := doChannelJSON(t, r, http.MethodPost, "/api/channels", bad); rec.Code != http.StatusBadRequest ||
		!strings.Contains(rec.Body.String(), "分组名最多 32 个字符") {
		t.Fatalf("create too long: status = %d body = %s", rec.Code, rec.Body.String())
	}
	if rec := doChannelJSON(t, r, http.MethodPut, path, map[string]any{"name": "renamed", "group_name": longName}); rec.Code != http.StatusBadRequest ||
		!strings.Contains(rec.Body.String(), "分组名最多 32 个字符") {
		t.Fatalf("update too long: status = %d body = %s", rec.Code, rec.Body.String())
	}
	// 校验失败的更新不落库任何字段
	if ch, err := channels.FindByID(id); err != nil || ch.Name != "demo2" || ch.GroupName != "" {
		t.Fatalf("rejected update changed channel: %#v err=%v", ch, err)
	}
}
