package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

const TsMax uint64 = ^uint64(0)

// 锁以 key 为单位存储在 CfLock。
type Lock struct {
	Primary []byte
	Ts      uint64
	Ttl     uint64
	Kind    WriteKind
}

type KlPair struct {
	Key  []byte
	Lock *Lock
}

// Info creates a LockInfo object from a Lock object for key.
func (lock *Lock) Info(key []byte) *kvrpcpb.LockInfo {
	info := kvrpcpb.LockInfo{}
	info.Key = key
	info.LockVersion = lock.Ts
	info.PrimaryLock = lock.Primary
	info.LockTtl = lock.Ttl
	return &info
}

func (lock *Lock) ToBytes() []byte {
	buf := append(lock.Primary, byte(lock.Kind))
	buf = append(buf, make([]byte, 16)...)
	binary.BigEndian.PutUint64(buf[len(lock.Primary)+1:], lock.Ts)
	binary.BigEndian.PutUint64(buf[len(lock.Primary)+9:], lock.Ttl)
	return buf
}

// ParseLock attempts to parse a byte string into a Lock object.
func ParseLock(input []byte) (*Lock, error) {
	if len(input) <= 16 {
		return nil, fmt.Errorf("mvcc: error parsing lock, not enough input, found %d bytes", len(input))
	}

	primaryLen := len(input) - 17
	primary := input[:primaryLen]
	kind := WriteKind(input[primaryLen])
	ts := binary.BigEndian.Uint64(input[primaryLen+1:])
	ttl := binary.BigEndian.Uint64(input[primaryLen+9:])

	return &Lock{Primary: primary, Ts: ts, Ttl: ttl, Kind: kind}, nil
}

// IsLockedFor checks if lock locks key at txnStartTs.
// 判断一个锁是否会影响当前事务的执行
func (lock *Lock) IsLockedFor(key []byte, txnStartTs uint64, resp interface{}) bool {
	if lock == nil {
		return false
	}
	// If the point get read is from a single statement auto commit transaction, the version
	// will be set to `TsMax` which may save one round trip to fetch version from the the
	// placement driver tso service.
	if txnStartTs == TsMax && bytes.Compare(key, lock.Primary) != 0 {
		return false
	}
	// 当前事务的时间范围与该锁有冲突
	if lock.Ts <= txnStartTs {
		err := &kvrpcpb.KeyError{Locked: lock.Info(key)}
		respValue := reflect.ValueOf(resp)
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
		return true
	}
	return false
}

// AllLocksForTxn returns all locks for the current transaction.
func AllLocksForTxn(txn *RoTxn) ([]KlPair, error) {
	var result []KlPair
	iter := txn.Reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	// Rewind to the first valid key.
	iter.Seek([]byte(""))

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		lock, err := ParseLock(val)
		if err != nil {
			return nil, err
		}
		if lock.Ts == txn.StartTS {
			result = append(result, KlPair{item.Key(), lock})
		}
	}
	return result, nil
}
