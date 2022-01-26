package column

import (
	"bytes"
	"math/big"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type Int256 struct{ base }

func reverse(s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

// TrimLeftZeroes returns a subslice of s without leading zeroes
func trimLeftZeroes(s []byte) []byte {
	idx := 0
	for ; idx < len(s); idx++ {
		if s[idx] != 0 {
			break
		}
	}
	return s[idx:]
}

func bigIntToLEBytes(val *big.Int) []byte {
	n := val //big.NewInt(val)

	var r []byte
	if n.Cmp(big.NewInt(0)) != -1 {
		r = n.Bytes()
	} else {
		mask := big.NewInt(1)
		mask.Lsh(mask, 256)

		r = n.Add(n, mask).Bytes()
	}

	res := bytes.NewBuffer([]byte{})
	for i := 0; i < 32-len(r); i++ {
		res.WriteByte(0)
	}
	res.Write(r)
	return reverse(res.Bytes())
}

func bytesToBigInt(v []byte) *big.Int {
	r := reverse(v)
	n := new(big.Int).SetBytes(trimLeftZeroes(r))
	if r[0] == 1 {
		mask := big.NewInt(1)
		mask.Lsh(mask, 256)

		n = n.Sub(n, mask)
	}

	return n
}

func (Int256) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	v, err := decoder.Fixed(32)
	if err != nil {
		return []byte{}, err
	}
	return bytesToBigInt(v), nil
}

func (i *Int256) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case *big.Int:
		b := bigIntToLEBytes(v)
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
