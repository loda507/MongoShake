package executor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"strings"
	"time"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"
	"mongoshake/collector/transform"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	DumpConflictToDB  = "db"
	DumpConflictToSDK = "sdk"
	NoDumpConflict    = "none"

	ExecuteOrdered = false

	OpInsert = 0x01
	OpUpdate = 0x02

	OplogsMaxGroupNum  = 1000
	OplogsMaxGroupSize = 12 * 1024 * 1024 // MongoDB limits 16MB
)

var (
	GlobalExecutorId int32  = -1
	ThresholdVersion string = "3.2.0"
)

type PartialLogWithCallbak struct {
	partialLog *oplog.PartialLog
	callback   func()
}

type BatchGroupExecutor struct {
	// multi executor
	executors []*Executor
	// worker id
	ReplayerId uint32
	// mongo url
	MongoUrl string
	// tranform namespace
	NsTrans *transform.NamespaceTransform
}

func (batchExecutor *BatchGroupExecutor) Start() {
	// max concurrent execute connection sets to 64. And the total
	// conns = number of executor * number of batchExecutor. Normally max
	// is 64. if collector hashed oplogRecords by _id and the number of collector
	// is bigger we will use single executer in respective batchExecutor
	parallel := conf.Options.ReplayerExecutor
	if len(conf.Options.TransformNamespace) > 0 {
		batchExecutor.NsTrans = transform.NewNamespaceTransform(conf.Options.TransformNamespace)
	}
	executors := make([]*Executor, parallel)
	for i := 0; i != len(executors); i++ {
		executors[i] = NewExecutor(GenerateExecutorId(), batchExecutor, batchExecutor.MongoUrl)
		go executors[i].start()
	}
	batchExecutor.executors = executors
}

func (batchExecutor *BatchGroupExecutor) Sync(rawLogs []*oplog.PartialLog, callback func()) {
	count := uint64(len(rawLogs))
	if count == 0 {
		// may be probe request
		return
	}

	logs := make([]*PartialLogWithCallbak, len(rawLogs), len(rawLogs))
	// populate the batch buffer first
	for i, rawLog := range rawLogs {
		logs[i] = &PartialLogWithCallbak{partialLog: rawLog}
	}
	// only the last oplog message would be notified
	logs[len(logs)-1].callback = callback

	batchExecutor.replay(logs)
}

func (batchExecutor *BatchGroupExecutor) replay(logs []*PartialLogWithCallbak) {
	// TODO: skip the oplogRecords which has been replayed
	//  lastTs := utils.TimestampToInt64(logs[len(logs)-1].partialLog.Timestamp)
	//	if batchExecutor.replayer.Ack >= lastTs {
	//		// every oplog in buffer have been alread executed in previously
	//		// so discard them simply. Even the smaller timestamp oplogRecords has
	//		// been changed(other collector or other mongos source)
	//		return
	//	}

	// executor needs to check pausing or throttle here.
	batchExecutor.replicateShouldStall(int64(len(logs)))

	// In mongo shard cluster. our request goes into mongos. it's safe for
	// unique index without collision detection
	var matrix CollisionMatrix = &NoopMatrix{}
	if conf.Options.ReplayerCollisionEnable {
		matrix = NewBarrierMatrix()
	}

	// firstly. we split the oplogRecords into segments which are the unit
	// of safety execution. it means there is no any operations
	// on the safe unique index in the single segment.
	var segments = matrix.split(logs)
	// secondly. in each segment, we analyze the dependence between
	// each oplogRecords. And
	for _, segment := range segments {
		toBeExecuted := matrix.convert(segment)
		batchExecutor.executeInParallel(toBeExecuted)
	}
}

// TODO
func (batchExecutor *BatchGroupExecutor) replicateShouldStall(n int64) {
}

func (batchExecutor *BatchGroupExecutor) executeInParallel(logs []*OplogRecord) {
	// prepare execution monitor
	latch := new(sync.WaitGroup)
	latch.Add(len(logs))
	// shard oplogRecords by _id primary key and make up callback chain
	var buffer = make([][]*OplogRecord, len(batchExecutor.executors))
	shardKey := oplog.PrimaryKeyHasher{}
	var completionList []func()
	for _, log := range logs {
		selected := shardKey.DistributeOplogByMod(log.original.partialLog, len(batchExecutor.executors))
		buffer[selected] = append(buffer[selected], log)
		if log.original.callback != nil {
			// should be ordered by the incoming sequence
			completionList = append(completionList, log.original.callback)
		}
	}
	for index, buf := range buffer {
		if len(buf) != 0 {
			nimo.AssertTrue(len(batchExecutor.executors[index].batchBlock) == 0, "executors buffer is not empty!")
			nimo.AssertTrue(batchExecutor.executors[index].finisher == nil, "executors await status is wrong!")
			batchExecutor.executors[index].finisher = latch
			// follow the MEMORY MODEL : finisher should be assigned
			// before batchBlock channel. it read after <- batchBlock
			batchExecutor.executors[index].batchBlock <- buf
		}
	}
	// wait for execute completely
	latch.Wait()
	// invoke all callbacks
	for _, callback := range completionList {
		callback()
	}
	// sweep executors' block buffer and await
	for _, exec := range batchExecutor.executors {
		exec.finisher = nil
	}
}

type Executor struct {
	// sequence index id in each replayer
	id int
	// batchExecutor, not owned
	batchExecutor *BatchGroupExecutor
	// records all oplogRecords into journal files
	journal *utils.Journal
	// mongo url
	MongoUrl string

	batchBlock chan []*OplogRecord
	finisher   *sync.WaitGroup

	// mongo connection
	session *mgo.Session

	// bulk insert or single insert
	bulkInsert bool
}

func GenerateExecutorId() int {
	return int(atomic.AddInt32(&GlobalExecutorId, 1))
}

func NewExecutor(id int, batchExecutor *BatchGroupExecutor, MongoUrl string) *Executor {
	return &Executor{
		id:            id,
		batchExecutor: batchExecutor,
		journal:       utils.NewJournal(utils.JournalFileName(fmt.Sprintf("direct.%03d", id))),
		MongoUrl:      MongoUrl,
		batchBlock:    make(chan []*OplogRecord, 1),
	}
}

func (exec *Executor) start() {
	for toBeExecuted := range exec.batchBlock {
		nimo.AssertTrue(len(toBeExecuted) != 0, "the size of being executed batch oplogRecords could not be zero")
		for exec.doSync(toBeExecuted) != nil {
			time.Sleep(time.Second)
		}
		// acknowledge all oplogRecords have been successfully executed
		exec.finisher.Add(-len(toBeExecuted))

		// records all oplogRecords if enabled. After write successfully
		for _, log := range toBeExecuted {
			exec.journal.WriteRecord(log.original.partialLog)
		}
	}
}

func (exec *Executor) doSync(logs []*OplogRecord) error {
	count := len(logs)

	transLogs := transformLogs(logs, exec.batchExecutor.NsTrans, conf.Options.DBRef)

	// split batched oplogRecords into (ns, op) groups. individual group
	// can be accomplished in single MongoDB request. groups
	// in this executor will be sequential
	oplogGroups := LogsGroupCombiner{maxGroupNr: OplogsMaxGroupNum,
		maxGroupSize: OplogsMaxGroupSize}.mergeToGroups(transLogs)
	for _, group := range oplogGroups {
		if err := exec.execute(group); err != nil {
			return err
		}
	}

	LOG.Info("Replayer-%d Executor-%d doSync oplogRecords received[%d] merged[%d]. merge to %.2f%% chunks",
		exec.batchExecutor.ReplayerId, exec.id, count, len(oplogGroups), float32(len(oplogGroups))*100.00/float32(count))
	return nil
}

// if no need to transform namespace, return original logs
// for no command log, transform namespace in DBRef by conf.Options.TransformDBRef
// for command log, need transform namespace/collection in object of oplog
func transformLogs(logs []*OplogRecord, nsTrans *transform.NamespaceTransform, transformRef bool) []*OplogRecord {
	if nsTrans == nil {
		return logs
	}
	for _, log := range logs {
		partialLog := log.original.partialLog
		transPartialLog := transformPartialLog(partialLog, nsTrans, transformRef)
		if transPartialLog != nil {
			log.original.partialLog = transPartialLog
		}
	}
	return logs
}

func transformPartialLog(partialLog *oplog.PartialLog, nsTrans *transform.NamespaceTransform, transformRef bool) *oplog.PartialLog {
	db := strings.SplitN(partialLog.Namespace, ".", 2)[0]
	if partialLog.Operation != "c" {
		// {"op" : "i", "ns" : "my.system.indexes", "o" : { "v" : 2, "key" : { "date" : 1 }, "name" : "date_1", "ns" : "my.tbl", "expireAfterSeconds" : 3600 }
		if strings.HasSuffix(partialLog.Namespace, "system.indexes") {
			value := oplog.GetKey(partialLog.Object, "ns")
			oplog.SetFiled(partialLog.Object, "ns", nsTrans.Transform(value.(string)))
		}
		partialLog.Namespace = nsTrans.Transform(partialLog.Namespace)
		if transformRef {
			partialLog.Object = transform.TransformDBRef(partialLog.Object, db, nsTrans)
		}
	} else {
		operation, found := extraCommandName(partialLog.Object)
		if !found {
			LOG.Warn("extraCommandName meets type[%s] which is not implemented, ignore!", operation)
			return nil
		}
		switch operation {
		case "create":
			// { "create" : "my", "idIndex" : { "v" : 2, "key" : { "_id" : 1 }, "name" : "_id_", "ns" : "my.my" }
			if idIndex, _ := oplog.GetKeyWithIndex(partialLog.Object, "idIndex"); idIndex != nil {
				ns := oplog.GetKey(idIndex.(bson.D), "ns")
				oplog.SetFiled(idIndex.(bson.D), "ns", nsTrans.Transform(ns.(string)))
				// partialLog.Object[id].Value = idIndex
			} else {
				LOG.Warn("transformLogs meet unknown create command: %v", partialLog.Object)
			}
			fallthrough
		case "collMod":
			fallthrough
		case "drop":
			fallthrough
		case "deleteIndex":
			fallthrough
		case "deleteIndexes":
			fallthrough
		case "dropIndex":
			fallthrough
		case "dropIndexes":
			fallthrough
		case "convertToCapped":
			fallthrough
		case "emptycapped":
			col := oplog.GetKey(partialLog.Object, operation)
			colS, ok := col.(string)
			if col == nil || !ok {
				LOG.Warn("extraCommandName meets {%v: %v} value is not string, ignore!",
					operation, colS)
				return nil
			}
			partialLog.Namespace = nsTrans.Transform(fmt.Sprintf("%s.%s", db, colS))
			// partialLog.Object[operation] = strings.SplitN(partialLog.Namespace, ".", 2)[1]
			oplog.SetFiled(partialLog.Object, operation, strings.SplitN(partialLog.Namespace, ".", 2)[1])
		case "renameCollection":
			// { "renameCollection" : "my.tbl", "to" : "my.my", "stayTemp" : false, "dropTarget" : false }
			fromNs, ok := oplog.GetKey(partialLog.Object, operation).(string)
			if !ok {
				LOG.Warn("extraCommandName meets {%v: %v} value is not string, ignore!",
					operation, oplog.GetKey(partialLog.Object, operation))
				return nil
			}
			toNs, ok := oplog.GetKey(partialLog.Object, "to").(string)
			if !ok {
				LOG.Warn("extraCommandName meets {to: %v} value is not string, ignore!",
					oplog.GetKey(partialLog.Object, "to"))
				return nil
			}
			partialLog.Namespace = nsTrans.Transform(fromNs)
			//partialLog.Object[operation] = partialLog.Namespace
			//partialLog.Object["to"] = nsTrans.Transform(toNs)
			oplog.SetFiled(partialLog.Object, operation, partialLog.Namespace)
			oplog.SetFiled(partialLog.Object, "to", nsTrans.Transform(toNs))
		case "applyOps":
			if ops := oplog.GetKey(partialLog.Object, "applyOps").([]bson.D); ops != nil {
				//transOps := make([]interface{}, 0)
				//for _, op := range ops {
				//	subLog := oplog.NewPartialLog(op.(bson.M))
				//	transSubLog := transformPartialLog(subLog, nsTrans, transformRef)
				//	if transSubLog == nil {
				//		LOG.Warn("transformPartialLog sublog %v return nil, ignore!", subLog)
				//		return nil
				//	}
				//	transOps = append(transOps, transSubLog.Dump())
				//}
				//partialLog.Object["applyOps"] = transOps
				for i, ele := range ops {
					m, keys := oplog.ConvertBsonD2M(ele)
					subLog := oplog.NewPartialLog(m)
					transSubLog := transformPartialLog(subLog, nsTrans, transformRef)
					if transSubLog == nil {
						LOG.Warn("transformPartialLog sublog %v return nil, ignore!", subLog)
						return nil
					}
					ops[i] = transSubLog.Dump(keys)
				}
			}
		default:
			partialLog.Namespace = nsTrans.Transform(partialLog.Namespace)
		}
	}
	return partialLog
}