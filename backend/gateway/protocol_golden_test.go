package gateway

// 协议互转 golden 测试：每对（客户端协议, 上游协议）的请求转换、非流式响应转换与流式转换，
// 完整输出与 testdata/golden 下的样例逐字比对。输入样例在 testdata/golden/input。
// 改了转换逻辑（或升级 relaykit）后重新生成，再逐个审 diff：
//
//	go test ./backend/gateway/ -run TestProtocolGolden -update

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/tidwall/sjson"
)

var updateGolden = flag.Bool("update", false, "重新生成 testdata/golden 下的协议转换样例")

const goldenDir = "testdata/golden"

var goldenKinds = []protocolKind{protocol.KindOpenAIChat, protocol.KindOpenAIResponses, protocol.KindAnthropic, protocol.KindGemini}

func TestProtocolGolden(t *testing.T) {
	rt := &Runtime{core: &core{}}
	for _, client := range goldenKinds {
		for _, upstream := range goldenKinds {
			if client == upstream {
				continue
			}
			name := string(client) + "_via_" + string(upstream)
			t.Run(name, func(t *testing.T) {
				checkGolden(t, "request/"+name+".txt", goldenRequest(t, client, upstream))
				checkGolden(t, "response/"+name+".txt", goldenResponse(t, rt, client, upstream))
				checkGolden(t, "stream/"+name+".txt", goldenStream(t, rt, client, upstream))
			})
		}
	}
}

// goldenInbound 客户端请求：Gemini 的模型与流式在 URL 上，其余协议写在 body 里。
func goldenInbound(t *testing.T, client protocolKind, stream bool) (body []byte, path string) {
	body = readGoldenInput(t, "request/"+string(client)+".json")
	if client == protocol.KindGemini {
		action := ":generateContent"
		if stream {
			action = ":streamGenerateContent"
		}
		return body, "/v1beta/models/m" + action
	}
	if stream {
		body, _ = sjson.SetBytes(body, "stream", true)
	}
	return body, protocol.PathFor(client, "")
}

// goldenPlan 与线上一致：请求模型 m 映射成上游模型 m-up。
func goldenPlan(t *testing.T, client, upstream protocolKind, stream bool) ([]byte, string, *conversionPlan, error) {
	body, path := goldenInbound(t, client, stream)
	return planUpstreamRequest(context.Background(), body, client, upstream, "m", "m-up", stream, path)
}

func goldenRequest(t *testing.T, client, upstream protocolKind) string {
	var b strings.Builder
	for _, stream := range []bool{false, true} {
		fmt.Fprintf(&b, "== stream=%v\n", stream)
		fwd, upPath, plan, err := goldenPlan(t, client, upstream, stream)
		if err != nil {
			fmt.Fprintf(&b, "error: %v\n", err)
			continue
		}
		fmt.Fprintf(&b, "path: %s\nengine: %s\n%s\n", upPath, plan.Engine, prettyJSON(fwd))
	}
	return b.String()
}

func goldenResponse(t *testing.T, rt *Runtime, client, upstream protocolKind) string {
	_, _, plan, err := goldenPlan(t, client, upstream, false)
	if err != nil {
		return fmt.Sprintf("request error: %v\n", err)
	}
	out := rt.convertResponseWithPlan(context.Background(), readGoldenInput(t, "response/"+string(upstream)+".json"), plan)
	return prettyJSON(out) + "\n"
}

func goldenStream(t *testing.T, rt *Runtime, client, upstream protocolKind) string {
	_, _, plan, err := goldenPlan(t, client, upstream, true)
	if err != nil {
		return fmt.Sprintf("request error: %v\n", err)
	}
	conv := rt.newStreamConverter(context.Background(), plan)
	if conv == nil {
		return "no converter\n"
	}
	var b strings.Builder
	for _, lines := range splitSSEEvents(readGoldenInput(t, "stream/"+string(upstream)+".sse")) {
		for _, f := range conv.Feed(parseSSEEventLines(lines)) {
			b.Write(f)
		}
	}
	for _, f := range conv.Close() {
		b.Write(f)
	}
	if ce, ok := conv.(interface{ Err() error }); ok && ce.Err() != nil {
		fmt.Fprintf(&b, "convert error: %v\n", ce.Err())
	}
	return b.String()
}

// splitSSEEvents 按空行切成一个个 event（与 streamRelay 的切法一致）。
func splitSSEEvents(raw []byte) [][]string {
	var events [][]string
	var cur []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			if len(cur) > 0 {
				events = append(events, cur)
				cur = nil
			}
			continue
		}
		cur = append(cur, line)
	}
	if len(cur) > 0 {
		events = append(events, cur)
	}
	return events
}

func prettyJSON(raw []byte) string {
	var buf bytes.Buffer
	if json.Indent(&buf, raw, "", "  ") != nil {
		return string(raw)
	}
	return buf.String()
}

func readGoldenInput(t *testing.T, rel string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(goldenDir, "input", rel))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// 转换时现生成的 id 与时间戳（输入样例里的 id 都很短，不会被替换）。
var (
	goldenGeneratedID = regexp.MustCompile(`\b(chatcmpl-|msg_|resp_|call_|toolu_|fc_|rs_|item_)[A-Za-z0-9_-]{8,}`)
	goldenTimestamp   = regexp.MustCompile(`"(created|created_at)":\s*1[0-9]{9}\b`)
)

func normalizeGolden(s string) string {
	s = goldenGeneratedID.ReplaceAllString(s, "${1}<generated>")
	return goldenTimestamp.ReplaceAllStringFunc(s, func(m string) string {
		if strings.HasSuffix(m, "1700000000") {
			return m // 样例里写死的时间戳
		}
		return regexp.MustCompile(`1[0-9]{9}$`).ReplaceAllString(m, "<now>")
	})
}

func checkGolden(t *testing.T, rel, got string) {
	t.Helper()
	got = normalizeGolden(got)
	path := filepath.Join(goldenDir, rel)
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("缺少 golden 文件 %s（用 -update 生成）：%v", path, err)
	}
	if string(want) == got {
		return
	}
	wantLines, gotLines := strings.Split(string(want), "\n"), strings.Split(got, "\n")
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			t.Fatalf("%s 第 %d 行不同（确认是预期变化后用 -update 重新生成）：\nwant: %s\ngot:  %s", path, i+1, w, g)
		}
	}
}
