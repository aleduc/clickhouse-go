package column

import "github.com/ClickHouse/clickhouse-go/lib/binary"

type Int256 struct{ base }

func (Int256) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	v, err := decoder.Fixed(32)
	if err != nil {
		return []byte{}, err
	}
	return v, nil
}

func (i *Int256) Write(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case []byte:
		if _, err := encoder.Write(v); err != nil {
			return err
		}
		return nil
	case string:
		if err := encoder.RawString([]byte(v)); err != nil {
			return err
		}
		return nil
	}

	return &ErrUnexpectedType{
		T:      v,
		Column: i,
	}
}
