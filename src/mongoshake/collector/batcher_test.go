package collector

import (
	"fmt"
	"testing"

	"mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/oplog"

	"github.com/stretchr/testify/assert"
)

func mockSyncer() *OplogSyncer {
	length := 3
	syncer := &OplogSyncer{
		logsQueue: make([]chan []*oplog.GenericOplog, length),
		hasher:    &oplog.PrimaryKeyHasher{},
	}
	for i := 0; i < length; i++ {
		syncer.logsQueue[i] = make(chan []*oplog.GenericOplog, 100)
	}
	return syncer
}

// return oplogs array with length=input length, the ddlGiven array marks the ddl
func mockOplogs(length int, ddlGiven []int) []*oplog.GenericOplog {
	output := make([]*oplog.GenericOplog, length)
	j := 0
	for i := 0; i < length; i ++ {
		op := "u"
		if j < len(ddlGiven) && ddlGiven[j] == i {
			op = "c"
			j++
		}
		output[i] = &oplog.GenericOplog {
			Parsed: &oplog.PartialLog{
				Namespace: "a.b",
				Operation: op,
			},
		}
	}
	return output
}

func TestBatchMore(t *testing.T) {
	// test batchMore

	var nr int
	// normal
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = true

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, nil)
		syncer.logsQueue[2] <- mockOplogs(7, nil)

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 18, len(batchedOplog[0]), "should be equal")
	}

	// split by `conf.Options.AdaptiveBatchingMaxSize`
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 10
		conf.Options.ReplayerDMLOnly = true

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, nil)
		syncer.logsQueue[2] <- mockOplogs(7, nil)

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 11, len(batchedOplog[0]), "should be equal")
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
	}

	// has ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, []int{2})
		syncer.logsQueue[2] <- mockOplogs(7, nil)

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
	}

	// has several ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, []int{3})
		syncer.logsQueue[1] <- mockOplogs(6, []int{2})
		syncer.logsQueue[2] <- mockOplogs(7, []int{4, 5})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")

		// 3 in logsQ[0]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")

		// 2 in logsQ[1]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")

		// 4 in logsQ[2]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		// 5 in logsQ[2]
		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
	}

	// first one and last one are ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, []int{0})
		syncer.logsQueue[1] <- mockOplogs(6, nil)
		syncer.logsQueue[2] <- mockOplogs(7, []int{6})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 16, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil)

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 80, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(1), batcher.currentQueue(), "should be equal")
	}

	// all ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 100
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(3, []int{0, 1, 2})
		syncer.logsQueue[1] <- mockOplogs(1, []int{0})
		syncer.logsQueue[2] <- mockOplogs(1, []int{0})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil)

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 80, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(1), batcher.currentQueue(), "should be equal")
	}

	// the edge of `AdaptiveBatchingMaxSize` is ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.AdaptiveBatchingMaxSize = 8
		conf.Options.ReplayerDMLOnly = false

		syncer.logsQueue[0] <- mockOplogs(5, nil)
		syncer.logsQueue[1] <- mockOplogs(6, []int{5}) // last is ddl
		syncer.logsQueue[2] <- mockOplogs(7, []int{3})

		batchedOplog, barrier := batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(2), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(2), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")

		batchedOplog, barrier = batcher.batchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, uint64(0), batcher.currentQueue(), "should be equal")
	}
}