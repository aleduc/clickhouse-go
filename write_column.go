package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/data"
)

// Interface for Clickhouse driver
type Clickhouse interface {
	Block() (*data.Block, error)
	Prepare(query string) (driver.Stmt, error)
	Begin() (driver.Tx, error)
	Commit() error
	Rollback() error
	Close() error
	WriteBlock(block *data.Block) error
}

// Interface for Block allowing writes to individual columns
type ColumnWriter interface {
	WriteDate(c int, v time.Time) error
	WriteDateNullable(c int, v *time.Time) error
	WriteDateTime(c int, v time.Time) error
	WriteDateTimeNullable(c int, v *time.Time) error
	WriteUInt8(c int, v uint8) error
	WriteUInt8Nullable(c int, v *uint8) error
	WriteUInt16(c int, v uint16) error
	WriteUInt16Nullable(c int, v *uint16) error
	WriteUInt32(c int, v uint32) error
	WriteUInt32Nullable(c int, v *uint32) error
	WriteUInt64(c int, v uint64) error
	WriteUInt64Nullable(c int, v *uint64) error
	WriteFloat32(c int, v float32) error
	WriteFloat32Nullable(c int, v *float32) error
	WriteFloat64(c int, v float64) error
	WriteFloat64Nullable(c int, v *float64) error
	WriteBytes(c int, v []byte) error
	WriteArray(c int, v interface{}) error
	WriteBytesNullable(c int, v *[]byte) error
	WriteArrayNullable(c int, v *interface{}) error
	WriteString(c int, v string) error
	WriteStringNullable(c int, v *string) error
	WriteFixedString(c int, v []byte) error
	WriteFixedStringNullable(c int, v *[]byte) error
	WriteInt256(c int, v []byte) error
	WriteUInt256(c int, v []byte) error
}

func OpenDirect(dsn string) (Clickhouse, error) {
	return open(dsn)
}

func (ch *clickhouse) Block() (*data.Block, error) {
	if ch.block == nil {
		return nil, sql.ErrTxDone
	}
	return ch.block, nil
}

func (ch *clickhouse) WriteBlock(block *data.Block) error {
	if block == nil {
		return sql.ErrTxDone
	}
	return ch.writeBlock(block, "")
}
