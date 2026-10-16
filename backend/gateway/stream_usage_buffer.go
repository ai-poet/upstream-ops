package gateway

import "bytes"

// streamUsageHeadBytes / streamUsageTailBytes 旁路记用量只保留流头和流尾的整行：
// 用量帧在流尾（OpenAI usage chunk、response.completed、message_delta），Anthropic 的 message_start 在流头，
// 中间的正文增量对补算用量没有用。以前整条流都留在内存里，内存随响应大小乘并发数增长。
const (
	streamUsageHeadBytes = 64 << 10
	streamUsageTailBytes = 256 << 10
)

// boundedLineBuffer 按整行保留头部和尾部的有界缓冲。最后一行无论多长都保留（终态帧可能很大）。
type boundedLineBuffer struct {
	head, tail       []byte
	headMax, tailMax int
	dropped          int64 // 被丢弃的中间行数
}

func newBoundedLineBuffer(headMax, tailMax int) *boundedLineBuffer {
	return &boundedLineBuffer{headMax: headMax, tailMax: tailMax}
}

// WriteLine 追加一行（不含换行）。零值缓冲用默认上限。
func (b *boundedLineBuffer) WriteLine(line string) {
	if b.headMax == 0 && b.tailMax == 0 {
		b.headMax, b.tailMax = streamUsageHeadBytes, streamUsageTailBytes
	}
	if len(b.head)+len(line)+1 <= b.headMax {
		b.head = append(b.head, line...)
		b.head = append(b.head, '\n')
		return
	}
	b.tail = append(b.tail, line...)
	b.tail = append(b.tail, '\n')
	// 攒到两倍再整理一次，避免每来一行就搬一次内存
	if len(b.tail) <= 2*b.tailMax {
		return
	}
	cut := 0
	for len(b.tail)-cut > b.tailMax {
		i := bytes.IndexByte(b.tail[cut:], '\n')
		if i < 0 || cut+i+1 >= len(b.tail) {
			break // 只剩最后一行：再长也留着
		}
		cut += i + 1
		b.dropped++
	}
	if cut > 0 {
		b.tail = append([]byte(nil), b.tail[cut:]...)
	}
}

// Bytes 头部 + 尾部（各行仍以换行结尾，可直接交给 SSE 用量解析）。
func (b *boundedLineBuffer) Bytes() []byte {
	if len(b.tail) == 0 {
		return b.head
	}
	out := make([]byte, 0, len(b.head)+len(b.tail))
	out = append(out, b.head...)
	return append(out, b.tail...)
}

// Len 当前占用的字节数。
func (b *boundedLineBuffer) Len() int { return len(b.head) + len(b.tail) }
