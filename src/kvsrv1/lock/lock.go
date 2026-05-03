package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const LockExpirationTime = time.Second * 10

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, name: lockname}
	return lk
}

func (lk *Lock) Acquire() {
	var version rpc.Tversion
	for {
		for {
			lockTime, oldVersion, err := lk.ck.Get(lk.name)
			if err == rpc.ErrNoKey {
				version = 0
				break
			} else if lockTime, err := time.Parse(time.RFC3339, lockTime); err == nil && time.Since(lockTime) > LockExpirationTime {
				version = oldVersion
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		err := lk.ck.Put(lk.name, time.Now().Format(time.RFC3339), version)
		if err == rpc.OK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	var version rpc.Tversion
	for {
		_, oldVersion, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			version = 0
		} else {
			version = oldVersion
		}
		err = lk.ck.Put(lk.name, time.Unix(0, 0).Format(time.RFC3339), version)
		if err == rpc.OK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}
