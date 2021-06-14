package column

import (
	"bytes"
	"math/big"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type UInt256 struct{ base }

func ubigIntToLEBytes(val *big.Int) []byte {
	r := val.Bytes()

	res := bytes.NewBuffer([]byte{})
	for i := 0; i < 32-len(r); i++ {
		res.WriteByte(0)
	}
	res.Write(r)
	return reverse(res.Bytes())
}

func ubytesToBigInt(v []byte) *big.Int {
	r := reverse(v)
	n := new(big.Int).SetBytes(trimLeftZeroes(r))
	return n
}

func (UInt256) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	v, err := decoder.Fixed(32)
	if err != nil {
		return []byte{}, err
	}
	return ubytesToBigInt(v), nil
}

func (i *UInt256) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case *big.Int:
		b := ubigIntToLEBytes(v)
		if _, err := encoder.Write(b); err != nil {
			return err
		}
		return nil
	}

	return &ErrUnexpectedType{
		T:      v,
		Column: i,
	}
}
