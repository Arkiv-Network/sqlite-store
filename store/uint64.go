package store

import (
	"database/sql/driver"
	"fmt"
	"math/big"
)

// Uint64 is a custom type that wraps uint64 for SQLite INTEGER column mapping.
// It implements sql.Scanner and driver.Valuer for database operations.
type Uint64 uint64

// Scan implements the sql.Scanner interface for reading INTEGER values from SQLite.
func (u *Uint64) Scan(src any) error {
	if src == nil {
		*u = 0
		return nil
	}

	switch v := src.(type) {
	case int64:
		if v < 0 {
			return fmt.Errorf("cannot scan negative value %d into Uint64", v)
		}
		*u = Uint64(v)
	case float64:
		if v < 0 || v > float64(^uint64(0)) {
			return fmt.Errorf("value %f out of range for Uint64", v)
		}
		*u = Uint64(v)
	case string:
		bi := new(big.Int)
		if _, ok := bi.SetString(v, 10); !ok {
			return fmt.Errorf("cannot parse %q as Uint64", v)
		}
		if bi.Sign() < 0 || bi.Cmp(new(big.Int).SetUint64(^uint64(0))) > 0 {
			return fmt.Errorf("value %s out of range for Uint64", v)
		}
		*u = Uint64(bi.Uint64())
	case []byte:
		bi := new(big.Int)
		if _, ok := bi.SetString(string(v), 10); !ok {
			return fmt.Errorf("cannot parse %q as Uint64", string(v))
		}
		if bi.Sign() < 0 || bi.Cmp(new(big.Int).SetUint64(^uint64(0))) > 0 {
			return fmt.Errorf("value %s out of range for Uint64", string(v))
		}
		*u = Uint64(bi.Uint64())
	default:
		return fmt.Errorf("cannot scan type %T into Uint64", src)
	}
	return nil
}

// Value implements the driver.Valuer interface for writing uint64 values to SQLite INTEGER.
func (u Uint64) Value() (driver.Value, error) {
	return int64(u), nil
}
