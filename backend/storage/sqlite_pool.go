// SQLite 读写分离连接池。
//
// SQLite 同一时刻只能有一个写事务，所以写仍然只用一个连接（避免 SQLITE_BUSY）；
// 但 WAL 模式下读不阻塞写、写也不阻塞读，以前读也挤在这唯一的连接上：
// 管理端的用量统计、缓存重载这类读要排在用量批量写入后面，反过来也一样。
// 现在 SELECT 走多连接的只读池（query_only 防误写），其余语句与事务走写连接。
package storage

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

const (
	sqliteReadConns       = 8
	sqliteReadIdleTimeout = time.Minute
)

// sqliteDSN 给 SQLite 路径加连接级 pragma（每个新连接都会执行）。
// synchronous=NORMAL：WAL 下提交不再逐次 fsync（只在检查点同步），进程崩溃不丢数据，断电可能丢最后几个事务。
func sqliteDSN(path string, readOnly bool) string {
	params := []string{"_pragma=busy_timeout(5000)"}
	if readOnly {
		params = append(params, "_pragma=query_only(1)")
	} else {
		params = append(params, "_pragma=synchronous(NORMAL)")
	}
	return path + "?" + strings.Join(params, "&")
}

// splitConnPool 实现 gorm.ConnPool：只读查询走 reader，其余（写、DDL、PRAGMA、事务）走 writer。
type splitConnPool struct {
	writer *sql.DB
	reader *sql.DB
}

func openSQLiteSplitPool(path string) (*splitConnPool, error) {
	writer, err := sql.Open("sqlite", sqliteDSN(path, false))
	if err != nil {
		return nil, err
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)

	reader, err := sql.Open("sqlite", sqliteDSN(path, true))
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	reader.SetMaxOpenConns(sqliteReadConns)
	reader.SetMaxIdleConns(sqliteReadConns)
	// 空闲的读连接一分钟后关掉（不再使用的库不会一直占着文件句柄）
	reader.SetConnMaxIdleTime(sqliteReadIdleTimeout)
	return &splitConnPool{writer: writer, reader: reader}, nil
}

// isReadQuery 只有以 SELECT 开头的语句走只读池。
// INSERT ... RETURNING 也走查询接口，但不以 SELECT 开头，会落到写连接；WITH 可能带写，一律走写连接。
func isReadQuery(query string) bool {
	q := strings.TrimLeft(query, " \t\r\n(")
	return len(q) >= 6 && strings.EqualFold(q[:6], "select")
}

func (p *splitConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return p.writer.PrepareContext(ctx, query)
}

func (p *splitConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return p.writer.ExecContext(ctx, query, args...)
}

func (p *splitConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if isReadQuery(query) {
		return p.reader.QueryContext(ctx, query, args...)
	}
	return p.writer.QueryContext(ctx, query, args...)
}

func (p *splitConnPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if isReadQuery(query) {
		return p.reader.QueryRowContext(ctx, query, args...)
	}
	return p.writer.QueryRowContext(ctx, query, args...)
}

// BeginTx 事务一律在写连接上（事务内的读也在同一个连接，读到自己的写）。
func (p *splitConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return p.writer.BeginTx(ctx, opts)
}

// GetDBConn db.DB() 返回写连接（连接池设置、Close 等沿用以前的单连接语义）。
func (p *splitConnPool) GetDBConn() (*sql.DB, error) {
	return p.writer, nil
}
