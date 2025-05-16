package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/util"

	// "github.com/Overclock-Validator/sniper"
	"github.com/dgraph-io/badger/v4"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
)

type AccountsDb struct {
	IndexDb         *badger.DB
	StopGc          chan struct{}
	AcctsDir        string
	IndexDir        string
	LargestFileId   atomic.Uint64
	BankHashBytes   [32]byte
	VoteAcctCache   otter.Cache[solana.PublicKey, *accounts.Account]
	CommonAcctCache otter.Cache[solana.PublicKey, *accounts.Account]
	ProgramCache    otter.Cache[solana.PublicKey, *sbpf.Program]
}

var (
	ErrNoAccount = errors.New("ErrNoAccount")
)

func OpenBadgerForSnapshot(dbDir string) (*badger.DB, error) {
	dbOpts := badger.DefaultOptions(dbDir)
	dbOpts.BlockCacheSize = 8 * 1024 * 1024 * 1024
	dbOpts.NumCompactors = 12
	dbOpts.NumGoroutines = 24
	dbOpts.NumMemtables = 24
	dbOpts.MemTableSize = 1024 * 1024 * 1024
	dbOpts.CompactL0OnClose = true

	db, err := badger.OpenManaged(dbOpts)
	if err != nil {
		mlog.Log.Infof("failed to open database: %s\n", err)
		return nil, err
	}

	return db, nil
}

func OpenBadgerWithRecommendedOptions(dbDir string) (*badger.DB, error) {
	dbOpts := badger.DefaultOptions(dbDir)
	dbOpts.IndexCacheSize = 10 * 1024 * 1024 * 1024
	dbOpts.BlockCacheSize = 1024 * 1024 * 1024
	dbOpts.NumCompactors = 8
	dbOpts.NumGoroutines = 16
	dbOpts.NumMemtables = 16
	dbOpts.MemTableSize = 256 * 1024 * 1024
	db, err := badger.Open(dbOpts)
	if err != nil {
		mlog.Log.Infof("failed to open database: %s\n", err)
		return nil, err
	}

	return db, nil
}

// start a goroutine to clean up badger db
// returns a channel to stop the gc thread
func BadgerGcThread(db *badger.DB) chan struct{} {
	stopGc := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-stopGc:
				return
			case <-ticker.C:
			again:
				err := db.RunValueLogGC(0.7)
				if err == nil {
					goto again
				}
			}
		}
	}()
	return stopGc
}

func OpenDb(accountsDbDir string) (*AccountsDb, error) {
	// check for existence of the 'accounts' directory, which holds the appendvecs
	appendVecsDir := fmt.Sprintf("%s/accounts", accountsDbDir)
	_, err := os.Stat(appendVecsDir)
	if err != nil {
		return nil, err
	}

	// attempt to open largest_file_id file
	largestFileIdFn := fmt.Sprintf("%s/largest_file_id", accountsDbDir)
	lfi, err := os.Open(largestFileIdFn)
	if err != nil {
		mlog.Log.Infof("failed to open %s\n", largestFileIdFn)
		return nil, err
	}

	largestFileIdBytes := make([]byte, 8)
	bytesRead, err := lfi.Read(largestFileIdBytes)
	if err != nil {
		mlog.Log.Infof("error reading %s: %s\n", largestFileIdFn, err)
		return nil, err
	} else if bytesRead != 8 {
		mlog.Log.Infof("error reading %s: expected 8 bytes, got %d\n", largestFileIdFn, bytesRead)
		return nil, fmt.Errorf("only got %d bytes", bytesRead)
	}

	largestFileId := binary.LittleEndian.Uint64(largestFileIdBytes)
	mlog.Log.Infof("accountsdb.OpenDb: largestFileId=%d", largestFileId)

	bankHashFn := fmt.Sprintf("%s/bank_hash", accountsDbDir)
	bhf, err := os.Open(bankHashFn)
	if err != nil {
		mlog.Log.Infof("failed to open %s\n", bankHashFn)
		return nil, err
	}

	bankHashBytes := make([]byte, 32)
	bytesRead, err = bhf.Read(bankHashBytes)
	if err != nil {
		mlog.Log.Infof("error reading %s: %s\n", bankHashFn, err)
		return nil, err
	} else if bytesRead != 32 {
		mlog.Log.Infof("error reading %s: expected 8 bytes, got %d\n", bankHashFn, bytesRead)
		return nil, fmt.Errorf("only got %d bytes", bytesRead)
	}
	mlog.Log.Infof("accountsdb.OpenDb: bankHashBytes=%x", bankHashBytes)

	// attempt to open the index kv store
	indexDir := fmt.Sprintf("%s/index", accountsDbDir)
	db, err := OpenBadgerWithRecommendedOptions(indexDir)
	if err != nil {
		mlog.Log.Infof("failed to open database: %s\n", err)
		return nil, err
	}
	mlog.Log.Infof("accountsdb.OpenDb: done opening indexDir=%s", indexDir)

	stopGc := BadgerGcThread(db)

	accountsDb := &AccountsDb{IndexDb: db, StopGc: stopGc, AcctsDir: appendVecsDir, IndexDir: indexDir}
	accountsDb.LargestFileId.Store(largestFileId)
	copy(accountsDb.BankHashBytes[:], bankHashBytes)

	return accountsDb, nil
}

func (accountsDb *AccountsDb) CloseDb() {
	accountsDb.StopGc <- struct{}{}
	accountsDb.IndexDb.Close()
}

func (accountsDb *AccountsDb) InitCaches() {
	var err error
	accountsDb.VoteAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](10_000).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	// TODO: review size of program cache
	accountsDb.ProgramCache, err = otter.MustBuilder[solana.PublicKey, *sbpf.Program](10_000).
		Cost(func(key solana.PublicKey, prog *sbpf.Program) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	// TODO: review size of common accounts cache
	accountsDb.CommonAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](250_000).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}
}

func (accountsDb *AccountsDb) MaybeGetProgramFromCache(pubkey solana.PublicKey) (*sbpf.Program, bool) {
	return accountsDb.ProgramCache.Get(pubkey)
}

func (accountsDb *AccountsDb) AddProgramToCache(pubkey solana.PublicKey, program *sbpf.Program) {
	accountsDb.ProgramCache.Set(pubkey, program)
}

func (accountsDb *AccountsDb) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	cachedAcct, hasAcct := accountsDb.VoteAcctCache.Get(pubkey)
	if hasAcct {
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.CommonAcctCache.Get(pubkey)
	if hasAcct {
		return cachedAcct, nil
	}

	var acctIdxEntryBytes []byte
	err := accountsDb.IndexDb.View(func(txn *badger.Txn) error {
		prefixedKey := pubKeyWithPrefix(pubkey[:])
		item, err := txn.Get(prefixedKey)
		if err != nil {
			return err
		}
		acctIdxEntryBytes, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		mlog.Log.Debugf("no account found in accountsdb for pubkey %s: %s", pubkey, err)
		return nil, ErrNoAccount
	}

	acctIdxEntry, err := unmarshalAcctIdxEntry(acctIdxEntryBytes)
	if err != nil {
		panic("failed to unmarshal AccountIndexEntry from index kv database")
	}

	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, acctIdxEntry.Slot, acctIdxEntry.FileId)
	appendVecFile, err := os.Open(appendVecFileName)
	if err != nil {
		mlog.Log.Debugf("failed to open appendvec file %s")
		return nil, err
	}

	offset, err := appendVecFile.Seek(int64(acctIdxEntry.Offset), 0)
	if err != nil {
		panic(fmt.Sprintf("file seek failed: %s\n", err))
	}
	if offset != int64(acctIdxEntry.Offset) {
		panic(fmt.Sprintf("file seek gave wrong idx (%d)\n", offset))
	}

	acct, err := unmarshalAcctFromAppendVecAcctHeader(appendVecFile)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal account from appendvec file %s: %s", appendVecFileName, err))
	}

	if acct.Key != pubkey {
		panic(fmt.Sprintf("account unmarshaled from appendvec file %s has the wrong pubkey, expect: %s found: %s", appendVecFileName, pubkey, acct.Key))
	}

	acct.Slot = acctIdxEntry.Slot

	msg := util.PrettyPrintAcct(acct)
	mlog.Log.Debugf("SLOT %d - accountsdb.Get() found acct in %s for %s: %s", slot, appendVecFileName, pubkey, msg)

	appendVecFile.Close()

	return acct, err
}

var voteAcct = solana.MustPublicKeyFromBase58("Vote111111111111111111111111111111111111111")

func pubKeyWithPrefix(pkRaw []byte) []byte {
	prefixedKey := new(bytes.Buffer)
	prefixedKey.WriteString("pubkey:")
	prefixedKey.Write(pkRaw)

	return prefixedKey.Bytes()
}

func SetAccountsForSlotSnapshot(indexDb *badger.DB, slot uint64, k, v [][]byte) {
	writeBatch := indexDb.NewWriteBatchAt(slot)
	defer writeBatch.Cancel()

	for idx, k := range k {
		prefixedKey := pubKeyWithPrefix(k)
		accSlot := binary.LittleEndian.Uint64(v[idx])
		entry := badger.NewEntry(prefixedKey, v[idx])
		err := writeBatch.SetEntryAt(entry, accSlot)
		if err != nil {
			panic(fmt.Sprintf("error setting key %s: %s", prefixedKey, err))
		}
	}
	err := writeBatch.Flush()
	if err != nil {
		panic(fmt.Sprintf("error flushing write batch: %s", err))
	}

	// for {
	// 	err := indexDb.Update(func(txn *badger.Txn) error {
	// 		prefixedKey := pubKeyWithPrefix(k)
	// 		currentValItem, err := txn.Get(prefixedKey)

	// 		if err == nil {
	// 			currentVal, err := currentValItem.ValueCopy(nil)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			newSlot := binary.LittleEndian.Uint64(v)
	// 			existingSlot := binary.LittleEndian.Uint64(currentVal)

	// 			if existingSlot >= newSlot {
	// 				return nil
	// 			}
	// 		}

	// 		err = txn.Set(prefixedKey, v)
	// 		return err
	// 	})

	// 	if err == badger.ErrConflict {
	// 		continue
	// 	} else if err != nil {
	// 		return err
	// 	}

	// 	return nil
	// }
}

func BuildPrefixIndex(indexDb *badger.DB) error {
	iterTxn := indexDb.NewTransactionAt(math.MaxUint64, false)
	defer iterTxn.Discard()

	txn := indexDb.NewTransactionAt(math.MaxUint64, true)

	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte("pubkey:")

	it := iterTxn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		kRaw := item.Key()
		slotBe := make([]byte, 8)
		item.Value(func(v []byte) error {
			slot := binary.LittleEndian.Uint64(v)
			binary.BigEndian.PutUint64(slotBe, slot)
			return nil
		})

		prefixIdxKey := new(bytes.Buffer)
		prefixIdxKey.WriteString("prefixidx:")
		prefixIdxKey.Write(slotBe)
		prefixIdxKey.WriteString(":")
		prefixIdxKey.Write(kRaw[7:])

		err := txn.Set(prefixIdxKey.Bytes(), []byte{})
		if err == badger.ErrTxnTooBig {
			_ = txn.Commit()
			txn = indexDb.NewTransactionAt(math.MaxUint64, true)
			_ = txn.Set(prefixIdxKey.Bytes(), []byte{})
		}
	}

	txn.Commit()

	return nil
}

func SetIfSlotHigher(indexDb *badger.DB, k, v []byte) error {
	for {
		txn := indexDb.NewTransaction(true)
		defer txn.Discard()

		prefixedKey := pubKeyWithPrefix(k)
		currentValItem, err := txn.Get(prefixedKey)
		newSlot := binary.LittleEndian.Uint64(v)
		if err == nil {
			var existingSlot uint64
			currentValItem.Value(func(v []byte) error {
				existingSlot = binary.LittleEndian.Uint64(v)
				return nil
			});

			if existingSlot >= newSlot {
				return nil
			}

			oldSlotBe := make([]byte, 8)
			binary.BigEndian.PutUint64(oldSlotBe, existingSlot)
			oldPrefixIdxKey := new(bytes.Buffer)
			oldPrefixIdxKey.WriteString("prefixidx:")
			oldPrefixIdxKey.Write(oldSlotBe)
			oldPrefixIdxKey.WriteString(":")
			oldPrefixIdxKey.Write(k)

			err = txn.Delete(oldPrefixIdxKey.Bytes())
			if err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		err = txn.Set(prefixedKey, v)
		if err != nil {
			return err
		}

		slotBe := make([]byte, 8)
		binary.BigEndian.PutUint64(slotBe, newSlot)
		prefixIdxKey := new(bytes.Buffer)
		prefixIdxKey.WriteString("prefixidx:")
		// ensures lexicographic ordering
		prefixIdxKey.Write(slotBe)
		prefixIdxKey.WriteString(":")
		prefixIdxKey.Write(k)

		// set to empty value
		err = txn.Set(prefixIdxKey.Bytes(), []byte{})
		if err != nil {
			return err
		}

		err = txn.Commit()

		if err == badger.ErrConflict {
			continue
		}

		return err
	}
}

func (accountsDb *AccountsDb) StoreAccounts(accts []*accounts.Account, slot uint64) error {
	fileId := accountsDb.LargestFileId.Add(1)

	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
	appendVecFile, err := os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		mlog.Log.Debugf("unable to open appendvec file %s for writing to accountsdb", appendVecFileName)
		return err
	}
	defer appendVecFile.Close()

	appendVecAcctsBuf := new(bytes.Buffer)
	writer := new(bytes.Buffer)

	for _, acct := range accts {
		acct.Slot = slot

		// if vote account, do not serialize up and write into accountsdb - just save it in cache.
		if solana.PublicKeyFromBytes(acct.Owner[:]) == voteAcct {
			accountsDb.VoteAcctCache.Set(acct.Key, acct)
			continue
		}

		accountsDb.CommonAcctCache.Set(acct.Key, acct)

		// create index entry, encode it and write it to the index kv store
		// offset field is specified as the current num of bytes written to the appendvec buffer.
		writer.Reset()
		encoder := bin.NewBinEncoder(writer)

		indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: uint64(appendVecAcctsBuf.Len())}

		err = indexEntry.MarshalWithEncoder(encoder)
		if err != nil {
			mlog.Log.Debugf("error marshaling in Set on accountsdb for pubkey %s", acct.Key)
			return err
		}

		err = SetIfSlotHigher(accountsDb.IndexDb, acct.Key[:], writer.Bytes())
		if err != nil {
			mlog.Log.Debugf("error calling SetIfSlotHigher on accountsdb for pubkey %s", acct.Key)
			return err
		}

		msg := util.PrettyPrintAcct(acct)
		mlog.Log.Debugf("SLOT %d - wrote account %s to %s in StoreAccounts: %s", slot, acct.Key, appendVecFileName, msg)

		// marshal up the account as an appendvec style account and write it to the buffer
		appendVecAcct := AppendVecAccount{DataLen: uint64(len(acct.Data)), Pubkey: acct.Key, Lamports: acct.Lamports,
			RentEpoch: acct.RentEpoch, Owner: acct.Owner, Executable: acct.Executable, Data: acct.Data}

		err = appendVecAcct.Marshal(appendVecAcctsBuf)
		if err != nil {
			return err
		}
	}

	// write the appendvecs data into the file
	n, err := appendVecFile.Write(appendVecAcctsBuf.Bytes())
	if err != nil {
		return err
	} else if n != appendVecAcctsBuf.Len() {
		return fmt.Errorf("only wrote %d appendvec account bytes, rather than %d", n, appendVecAcctsBuf.Len())
	}

	return nil
}

func (accountsDb *AccountsDb) KeysBetweenPrefixes(startPrefix uint64, endPrefix uint64) []solana.PublicKey {
	keyObjs := make([]solana.PublicKey, 0)
	err := accountsDb.IndexDb.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("prefixidx:")
		opts.PrefetchValues = false

		slotBe := make([]byte, 8)
		binary.BigEndian.PutUint64(slotBe, startPrefix)

		minKeyBuf := new(bytes.Buffer)
		minKeyBuf.WriteString("prefixidx:")
		minKeyBuf.Write(slotBe)
		minKeyBuf.WriteString(":")

		minKey := minKeyBuf.Bytes()

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(minKey); it.Valid(); it.Next() {
			item := it.Item()
			kRaw := item.Key()
			// Slot is the 8 bytes after prefixidx:
			slot := kRaw[10:18]
			if binary.BigEndian.Uint64(slot) > endPrefix {
				break
			}
			// pubkey comes after the slot + ":"
			pubKeyRaw := kRaw[19:]

			keyObject := solana.PublicKeyFromBytes(pubKeyRaw)
			keyObjs = append(keyObjs, keyObject)

		}

		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("error in KeysBetweenPrefixes: %s", err))
	}

	return keyObjs
}

func (accountsDb *AccountsDb) AllKeys() [][]byte {
	keys := make([][]byte, 0)
	err := accountsDb.IndexDb.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("pubkey:")
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			kRaw := item.KeyCopy(nil)
			keys = append(keys, kRaw[7:]) // strip off the prefix
		}

		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("error in AllKeys: %s", err))
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return util.PubkeyCmpByteSlice(keys[i], keys[j])
	})

	return keys
}

func (accountsDb *AccountsDb) BankHash() [32]byte {
	return accountsDb.BankHashBytes
}
