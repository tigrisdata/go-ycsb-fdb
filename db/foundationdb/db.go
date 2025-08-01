// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	fdbClusterFile           = "fdb.clusterfile"
	fdbDatabase              = "fdb.dbname"
	fdbAPIVersion            = "fdb.apiversion"
	fdbDrReadEnabled         = "fdb.drreads"
	fdbUseCachedReadVersions = "fdb.usecachedreadversions"
	fdbCacheVersionTime      = "fdb.versioncachetime"
)

type fDB struct {
	db                    fdb.Database
	r                     *util.RowCodec
	bufPool               *util.BufPool
	drReadEnabled         bool
	useCachedReadVersions bool
	cachedReadVersion     int64
	readVersionCachedAt   time.Time
	versionCacheTime      time.Duration
}

func createDB(p *properties.Properties) (ycsb.DB, error) {
	clusterFile := p.GetString(fdbClusterFile, "/etc/foundationdb/fdb.cluster")
	database := p.GetString(fdbDatabase, "DB")
	apiVersion := p.GetInt(fdbAPIVersion, 730)
	drReadEnabled := p.GetBool(fdbDrReadEnabled, false)
	useCachedReadVersions := p.GetBool(fdbUseCachedReadVersions, false)
	versionCacheTime, err := time.ParseDuration(p.GetString(fdbCacheVersionTime, "2s"))
	if err != nil {
		if useCachedReadVersions {
			panic("Failed to parse version cache duration")
		}
	}

	fdb.MustAPIVersion(apiVersion)

	db, err := fdb.Open(clusterFile, []byte(database))
	if err != nil {
		return nil, err
	}

	bufPool := util.NewBufPool()

	return &fDB{
		db:                    db,
		r:                     util.NewRowCodec(p),
		bufPool:               bufPool,
		drReadEnabled:         drReadEnabled,
		useCachedReadVersions: useCachedReadVersions,
		versionCacheTime:      versionCacheTime,
	}, nil
}

func (db *fDB) ToSqlDB() *sql.DB {
	return nil
}

func (db *fDB) Close() error {
	return nil
}

func (db *fDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *fDB) CleanupThread(ctx context.Context) {
}

func (db *fDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *fDB) getEndRowKey(table string) []byte {
	// ';' is ':' + 1 in the ASCII
	return util.Slice(fmt.Sprintf("%s;", table))
}

func (db *fDB) isNewVersionNeeded() bool {
	if time.Now().Sub(db.readVersionCachedAt) < db.versionCacheTime {
		return false
	}
	return true
}

func (db *fDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	rowKey := db.getRowKey(table, key)
	row, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		if db.drReadEnabled {
			tr.Options().SetReadLockAware()
		}

		f := tr.Get(fdb.Key(rowKey))
		return f.Get()
	})

	if err != nil {
		if os.Getenv("FDB_PRINT_ERRORS") != "" {
			fmt.Println("Got fdb error: ", err)
		}
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row.([]byte), fields)
}

func (db *fDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, 0, len(keys))

	_, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		if db.drReadEnabled {
			tr.Options().SetReadLockAware()
		}

		futures := make([]fdb.FutureByteSlice, len(keys))
		for i, key := range keys {
			rowKey := db.getRowKey(table, key)
			futures[i] = tr.Get(fdb.Key(rowKey))
		}

		for _, fut := range futures {
			rowBytes, err := fut.Get()
			if err != nil {
				return nil, err
			}

			if rowBytes == nil {
				res = append(res, nil)
				continue
			}

			decoded, err := db.r.Decode(rowBytes, fields)
			if err != nil {
				return nil, err
			}
			res = append(res, decoded)
		}

		return nil, nil
	})

	if err != nil {
		if os.Getenv("FDB_PRINT_ERRORS") != "" {
			fmt.Println("Got fdb error: ", err)
		}
		return nil, err
	}

	return res, nil
}

func (db *fDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	rowKey := db.getRowKey(table, startKey)
	res, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		if db.drReadEnabled {
			tr.Options().SetReadLockAware()
		}

		r := fdb.KeyRange{
			Begin: fdb.Key(rowKey),
			End:   fdb.Key(db.getEndRowKey(table)),
		}
		ri := tr.GetRange(r, fdb.RangeOptions{Limit: count}).Iterator()
		res := make([]map[string][]byte, 0, count)
		for ri.Advance() {
			kv, err := ri.Get()
			if err != nil {
				return nil, err
			}

			if kv.Value == nil {
				res = append(res, nil)
			} else {
				v, err := db.r.Decode(kv.Value, fields)
				if err != nil {
					return nil, err
				}
				res = append(res, v)
			}

		}
		return res, nil
	})

	if err != nil {
		if os.Getenv("FDB_PRINT_ERRORS") != "" {
			fmt.Println("Got fdb error: ", err)
		}
		return nil, err
	}
	return res.([]map[string][]byte), nil
}

func (db *fDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		if db.drReadEnabled {
			tr.Options().SetReadLockAware()
		}

		f := tr.Get(fdb.Key(rowKey))
		row, err := f.Get()
		if err != nil {
			return nil, err
		} else if row == nil {
			return nil, nil
		}

		data, err := db.r.Decode(row, nil)
		if err != nil {
			return nil, err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer db.bufPool.Put(buf)

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return nil, err
		}

		tr.Set(fdb.Key(rowKey), buf)
		return
	})
	if err != nil && os.Getenv("FDB_PRINT_ERRORS") != "" {
		fmt.Println("Got fdb error: ", err)
	}

	return err
}

func (db *fDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		for keyIdx, key := range keys {
			rowKey := db.getRowKey(table, key)
			if db.drReadEnabled {
				tr.Options().SetReadLockAware()
			}

			f := tr.Get(fdb.Key(rowKey))
			row, err := f.Get()
			if err != nil {
				return nil, err
			} else if row == nil {
				return nil, nil
			}

			data, err := db.r.Decode(row, nil)
			if err != nil {
				return nil, err
			}

			singleKeyValues := values[keyIdx]
			for field, value := range singleKeyValues {
				data[field] = value
			}

			buf := db.bufPool.Get()
			defer db.bufPool.Put(buf)

			buf, err = db.r.Encode(buf, data)
			if err != nil {
				return nil, err
			}

			tr.Set(fdb.Key(rowKey), buf)
		}
		return
	})
	if err != nil && os.Getenv("FDB_PRINT_ERRORS") != "" {
		fmt.Println("Got fdb error: ", err)
	}

	return err
}

func (db *fDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)
	_, err = db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		tr.Set(fdb.Key(rowKey), buf)
		return
	})
	if err != nil && os.Getenv("FDB_PRINT_ERRORS") != "" {
		fmt.Println("Got fdb error: ", err)
	}
	return err
}

func (db *fDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var err error

	buffers := make([][]byte, len(keys))

	_, err = db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		for i, key := range keys {
			buf := db.bufPool.Get()
			val := values[i]
			buf, err = db.r.Encode(buf, val)
			if err != nil {
				db.bufPool.Put(buf)
				return nil, err
			}
			rowKey := db.getRowKey(table, key)
			tr.Set(fdb.Key(rowKey), buf)
			buffers[i] = buf // release at the end of the batch
		}
		return
	})
	if err != nil && os.Getenv("FDB_PRINT_ERRORS") != "" {
		fmt.Println("Got fdb error: ", err)
	}

	for _, buf := range buffers {
		db.bufPool.Put(buf)
	}

	return err
}
func (db *fDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		tr.Clear(fdb.Key(rowKey))
		return
	})
	if err != nil && os.Getenv("FDB_PRINT_ERRORS") != "" {
		fmt.Println("Got fdb error: ", err)
	}
	return err
}

func (db *fDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if db.useCachedReadVersions {
			if !db.isNewVersionNeeded() {
				tr.SetReadVersion(db.cachedReadVersion)
			} else {
				fresh := tr.GetReadVersion().MustGet()
				db.cachedReadVersion = fresh
				db.readVersionCachedAt = time.Now()
				tr.SetReadVersion(fresh)
			}
		}

		for _, key := range keys {
			rowKey := db.getRowKey(table, key)
			tr.Clear(fdb.Key(rowKey))
		}
		return
	})
	if err != nil && os.Getenv("FDB_PRINT_ERRORS") != "" {
		fmt.Println("Got fdb error: ", err)
	}
	return err
}

type fdbCreator struct {
}

func (c fdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	return createDB(p)
}

func init() {
	ycsb.RegisterDBCreator("fdb", fdbCreator{})
	ycsb.RegisterDBCreator("foundationdb", fdbCreator{})
}
