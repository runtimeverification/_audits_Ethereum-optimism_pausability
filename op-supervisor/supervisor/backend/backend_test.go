package backend

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum-optimism/optimism/op-service/oppprof"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	"github.com/ethereum-optimism/optimism/op-service/testutils"
	"github.com/ethereum-optimism/optimism/op-supervisor/config"
	"github.com/ethereum-optimism/optimism/op-supervisor/metrics"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/depset"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/superevents"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/syncnode"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

const testChainIDOffset = 900

func fullConfigSet(t *testing.T, size int) depset.FullConfigSetMerged {
	staticDepSet := make(map[eth.ChainID]*depset.StaticConfigDependency, size)
	staticRollupCfgSet := make(map[eth.ChainID]*depset.StaticRollupConfig, size)
	zero := uint64(0)
	for i := 0; i < size; i++ {
		chainID := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))
		staticDepSet[chainID] = &depset.StaticConfigDependency{}
		staticRollupCfgSet[chainID] = &depset.StaticRollupConfig{
			InteropTime: &zero,
			BlockTime:   2,
		}
	}
	depSet, err := depset.NewStaticConfigDependencySet(staticDepSet)
	require.NoError(t, err)
	rollupCfgSet := depset.NewStaticRollupConfigSet(staticRollupCfgSet)
	fullCfgSet, err := depset.NewFullConfigSetMerged(rollupCfgSet, depSet)
	require.NoError(t, err)
	return fullCfgSet
}

func ExecMsgForLog(chain eth.ChainID, block eth.BlockRef, log_index uint32, log *types2.Log) *types2.Log {
	msg := types.Message{
		Identifier: types.Identifier{
			Origin:      log.Address,
			BlockNumber: block.Number,
			LogIndex:    log_index,
			Timestamp:   block.Time,
			ChainID:     chain,
		},
		PayloadHash: processors.LogToPayloadHash(log),
	}
	topics, data := msg.EncodeEvent()
	return &types2.Log{
		Address: params.InteropCrossL2InboxAddress,
		Data:    data,
		Topics:  topics,
	}
}

type ChainBlock struct {
	chain eth.ChainID
	block *eth.BlockRef
}

type ChainHeads struct {
	localSafe   uint64 // <= chain length
	localUnsafe uint64 // <= chain length
	crossSafe   uint64 // <= localSafe
	crossUnsafe uint64 // <= localUnsafe
}

type RandomChainParams struct {
	chainCount int

	minLength int
	maxLength int

	sameTimestampFrequency int // Percentage [0-100]
	dependencyChance       int // Percentage [0-100]
}

type RandomChain struct {
	cutoff       int
	chainIDs     []eth.ChainID
	allBlocks    []*ChainBlock
	chainSources map[eth.ChainID]*MockProcessorSource
	chainBlocks  map[eth.ChainID][]*eth.BlockRef
	chainHeads   map[eth.ChainID]*ChainHeads
}

func (p *RandomChainParams) MakeRandomChain(seed int64) (res RandomChain) {
	r := rand.New(rand.NewSource(seed))
	totalLength := r.Intn(p.maxLength-p.minLength) + p.minLength

	res = RandomChain{
		cutoff:       r.Intn(totalLength),
		chainIDs:     make([]eth.ChainID, 0, p.chainCount),
		allBlocks:    make([]*ChainBlock, 0, totalLength),
		chainSources: make(map[eth.ChainID]*MockProcessorSource),
		chainBlocks:  make(map[eth.ChainID][]*eth.BlockRef),
		chainHeads:   make(map[eth.ChainID]*ChainHeads),
	}

	for i := range p.chainCount {
		chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))
		res.chainBlocks[chain] = make([]*eth.BlockRef, 0)
		res.chainSources[chain] = &MockProcessorSource{}
		res.chainHeads[chain] = &ChainHeads{}
		res.chainIDs = append(res.chainIDs, chain)
	}

	//
	// Create array of all blocks
	//
	chainUninit := eth.ChainIDFromUInt64(0)
	timeStampCount := 1 // Can't be greater than p.chainCount
	var newBlock *ChainBlock
	for i := range totalLength {
		allBlocks := res.allBlocks
		if i == 0 {
			randomBlock := testutils.RandomBlockRef(r)
			randomBlock.Time = 10000
			newBlock = &ChainBlock{chainUninit, &randomBlock}
		} else {
			// Use NextRandomRef for timestamp coherence.
			randomBlock := testutils.NextRandomRef(r, *allBlocks[len(allBlocks)-1].block)
			if r.Intn(100) < p.sameTimestampFrequency && timeStampCount < p.chainCount {
				randomBlock.Time = allBlocks[len(allBlocks)-1].block.Time
				timeStampCount++
			} else {
				randomBlock.Time += 1 // Increment because NextRandomRef could return a block with the same timestamp
				timeStampCount = 1
			}
			newBlock = &ChainBlock{chainUninit, &randomBlock}
		}
		res.allBlocks = append(res.allBlocks, newBlock)
	}

	//
	// Assign blocks to random L2 chains
	//
	chainSelections := make([]eth.ChainID, p.chainCount)
	copy(chainSelections, res.chainIDs)
	shuffleChains := func() {
		r.Shuffle(len(chainSelections), func(i, j int) {
			chainSelections[i], chainSelections[j] = chainSelections[j], chainSelections[i]
		})
	}

	nextChain := 0
	var prevBlock *eth.BlockRef
	for i, cb := range res.allBlocks {
		block := cb.block
		if i == 0 || prevBlock.Time != block.Time {
			shuffleChains()
			nextChain = 0
		}
		chainid := chainSelections[nextChain]
		cb.chain = chainid
		nextChain++

		if len(res.chainBlocks[chainid]) == 0 {
			block.Number = 0
			block.ParentHash = common.Hash{}
		} else {
			chainBlocks := res.chainBlocks[chainid]
			lastblock := chainBlocks[len(chainBlocks)-1]
			block.Number = lastblock.Number + 1
			block.ParentHash = lastblock.Hash
		}

		if i <= res.cutoff {
			chainHeads := res.chainHeads[chainid]
			chainHeads.crossSafe = block.Number
			chainHeads.crossUnsafe = block.Number
		}

		res.chainSources[chainid].ExpectBlockRefByNumber(block.Number, *block, nil)
		res.chainBlocks[chainid] = append(res.chainBlocks[chainid], block)
		prevBlock = block
	}

	// Determine the local safe/unsafe heads for each chain
	for chain, blocks := range res.chainBlocks {
		chainLength := len(blocks)
		lastBlockNumber := blocks[chainLength-1].Number
		res.chainHeads[chain].localSafe = lastBlockNumber
		res.chainHeads[chain].localUnsafe = lastBlockNumber
	}

	//
	// Create random dependencies between all blocks
	//
	generatedLogs := make([][]*types2.Log, totalLength)
	for initIndex, cb := range res.allBlocks {
		block := cb.block
		if block.Number == 0 {
			continue
		}
		for r.Intn(100) < p.dependencyChance {
			execIndex := r.Intn(totalLength-initIndex) + initIndex
			cb := res.allBlocks[execIndex]
			execChain, execBlock := cb.chain, cb.block
			initiatingLog := testutils.RandomLog(r)
			initiatingLog.Index = uint(len(generatedLogs[initIndex]))
			execLog := ExecMsgForLog(execChain, *execBlock, uint32(len(generatedLogs[execIndex])), initiatingLog)
			execLog.Index = uint(len(generatedLogs[execIndex]))
			generatedLogs[initIndex] = append(generatedLogs[initIndex], initiatingLog)
			generatedLogs[execIndex] = append(generatedLogs[execIndex], execLog)
		}
	}
	for i, logs := range generatedLogs {
		cb := res.allBlocks[i]
		chain, block := cb.chain, cb.block
		rcpt := types2.Receipt{
			Logs: logs,
		}
		source := res.chainSources[chain]
		source.ExpectFetchReceipts(block.Hash, types2.Receipts{&rcpt}, nil)
	}

	return res
}

func TestBackendLifetime_InteropAtGenesis(t *testing.T) {
	logger := testlog.Logger(t, log.LvlInfo)
	m := metrics.NoopMetrics
	dataDir := t.TempDir()
	chainA := eth.ChainIDFromUInt64(testChainIDOffset)
	fullCfgSet := fullConfigSet(t, 2)
	rollupCfgSet := fullCfgSet.RollupConfigSet.(depset.StaticRollupConfigSet)

	anchor := eth.BlockRef{
		Hash:       common.Hash{0xff},
		Number:     0,
		ParentHash: common.Hash{}, // genesis has no parent hash
		Time:       10000,
	}

	rollupCfgSet[chainA].Genesis = depset.Genesis{
		L2: types.BlockSealFromRef(anchor),
	}

	cfg := &config.Config{
		Version:               "test",
		FullConfigSetSource:   fullCfgSet,
		SynchronousProcessors: true,
		MockRun:               false,
		SyncSources:           &syncnode.CLISyncNodes{},
		Datadir:               dataDir,
	}

	ex := event.NewGlobalSynchronous(context.Background())
	b, err := NewSupervisorBackend(context.Background(), logger, m, cfg, ex)
	require.NoError(t, err)
	t.Log("initialized!")

	l1Src := &testutils.MockL1Source{}
	src := &MockProcessorSource{}

	blockX := eth.BlockRef{
		Hash:       common.Hash{0xaa},
		Number:     anchor.Number + 1,
		ParentHash: anchor.Hash,
		Time:       anchor.Time + 2,
	}
	blockY := eth.BlockRef{
		Hash:       common.Hash{0xbb},
		Number:     blockX.Number + 1,
		ParentHash: blockX.Hash,
		Time:       blockX.Time + 2,
	}

	b.AttachL1Source(l1Src)
	require.NoError(t, b.AttachProcessorSource(chainA, src))

	require.FileExists(t, filepath.Join(cfg.Datadir, "900", "log.db"), "must have logs DB 900")
	require.FileExists(t, filepath.Join(cfg.Datadir, "901", "log.db"), "must have logs DB 901")
	require.FileExists(t, filepath.Join(cfg.Datadir, "900", "local_safe.db"), "must have local safe DB 900")
	require.FileExists(t, filepath.Join(cfg.Datadir, "901", "local_safe.db"), "must have local safe DB 901")
	require.FileExists(t, filepath.Join(cfg.Datadir, "900", "cross_safe.db"), "must have cross safe DB 900")
	require.FileExists(t, filepath.Join(cfg.Datadir, "901", "cross_safe.db"), "must have cross safe DB 901")

	err = b.Start(context.Background())
	require.NoError(t, err)
	t.Log("started!")

	_, err = b.LocalUnsafe(context.Background(), chainA)
	require.ErrorIs(t, err, types.ErrFuture, "no data yet, need local-unsafe")

	require.NoError(t, ex.Drain())
	// The database is initialized from the genesis interop block at startup.
	xunsafe, err := b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, anchor.ID(), xunsafe)
	xsafe, err := b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, anchor.ID(), xsafe.Derived)

	// Receive unsafe block Y from node

	src.ExpectBlockRefByNumber(1, blockX, nil)
	src.ExpectFetchReceipts(blockX.Hash, nil, nil)
	src.ExpectBlockRefByNumber(2, blockY, nil)
	src.ExpectFetchReceipts(blockY.Hash, nil, nil)
	b.emitter.Emit(context.Background(), superevents.LocalUnsafeReceivedEvent{
		ChainID:        chainA,
		NewLocalUnsafe: blockY,
	})
	require.NoError(t, ex.Drain())
	src.AssertExpectations(t)
	xunsafe, err = b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockY.ID(), xunsafe)
	// cross-safe still at anchor
	xsafe, err = b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, anchor.ID(), xsafe.Derived)

	// Revert cross-unafe back to block X
	err = b.chainDBs.UpdateCrossUnsafe(chainA, types.BlockSealFromRef(blockX))
	require.NoError(t, err)

	xunsafe, err = b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockX.ID(), xunsafe)

	// Receive derived block X from node

	b.emitter.Emit(context.Background(), superevents.LocalDerivedEvent{
		ChainID: chainA,
		Derived: types.DerivedBlockRefPair{
			Derived: blockX,
		},
	})
	require.NoError(t, ex.Drain())
	src.AssertExpectations(t)
	// cross-unsafe still at block X
	xunsafe, err = b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockY.ID(), xunsafe)
	// cross-safe now at block X
	xsafe, err = b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockX.ID(), xsafe.Derived)

	err = b.Stop(context.Background())
	require.NoError(t, err)
	t.Log("stopped!")
}

func TestBackendLifetime_InteropPostGenesis(t *testing.T) {
	logger := testlog.Logger(t, log.LvlInfo)
	m := metrics.NoopMetrics
	dataDir := t.TempDir()
	chainA := eth.ChainIDFromUInt64(testChainIDOffset)
	fullCfgSet := fullConfigSet(t, 2)
	rollupCfgSet := fullCfgSet.RollupConfigSet.(depset.StaticRollupConfigSet)

	block0 := eth.BlockRef{
		Hash:       common.Hash{0xff},
		Number:     0,
		ParentHash: common.Hash{}, // genesis has no parent hash
		Time:       10000,
	}
	blockX := eth.BlockRef{
		Hash:       common.Hash{0xaa},
		Number:     block0.Number + 1,
		ParentHash: block0.Hash,
		Time:       block0.Time + 2,
	}

	rollupCfgSet[chainA].InteropTime = &blockX.Time
	rollupCfgSet[chainA].Genesis = depset.Genesis{
		L2: types.BlockSealFromRef(block0),
	}

	cfg := &config.Config{
		Version:               "test",
		FullConfigSetSource:   fullCfgSet,
		SynchronousProcessors: true,
		MockRun:               false,
		SyncSources:           &syncnode.CLISyncNodes{},
		Datadir:               dataDir,
	}

	ex := event.NewGlobalSynchronous(context.Background())
	b, err := NewSupervisorBackend(context.Background(), logger, m, cfg, ex)
	require.NoError(t, err)
	t.Log("initialized!")

	l1Src := &testutils.MockL1Source{}
	src := &MockProcessorSource{}

	blockY := eth.BlockRef{
		Hash:       common.Hash{0xbb},
		Number:     blockX.Number + 1,
		ParentHash: blockX.Hash,
		Time:       blockX.Time + 2,
	}

	b.AttachL1Source(l1Src)
	require.NoError(t, b.AttachProcessorSource(chainA, src))

	require.FileExists(t, filepath.Join(cfg.Datadir, "900", "log.db"), "must have logs DB 900")
	require.FileExists(t, filepath.Join(cfg.Datadir, "901", "log.db"), "must have logs DB 901")
	require.FileExists(t, filepath.Join(cfg.Datadir, "900", "local_safe.db"), "must have local safe DB 900")
	require.FileExists(t, filepath.Join(cfg.Datadir, "901", "local_safe.db"), "must have local safe DB 901")
	require.FileExists(t, filepath.Join(cfg.Datadir, "900", "cross_safe.db"), "must have cross safe DB 900")
	require.FileExists(t, filepath.Join(cfg.Datadir, "901", "cross_safe.db"), "must have cross safe DB 901")

	err = b.Start(context.Background())
	require.NoError(t, err)
	t.Log("started!")

	_, err = b.LocalUnsafe(context.Background(), chainA)
	require.ErrorIs(t, err, types.ErrFuture, "no data yet, need local-unsafe")

	require.NoError(t, ex.Drain())
	// The database is not initialized from non-Interop genesis
	xunsafe, err := b.CrossUnsafe(context.Background(), chainA)
	require.ErrorIs(t, err, types.ErrFuture, "got xunsafe %v", xunsafe)
	xsafe, err := b.CrossSafe(context.Background(), chainA)
	require.ErrorIs(t, err, types.ErrFuture, "got xsafe %v", xsafe)

	// Receive unsafe block X, interop activation block, from node

	// src.ExpectBlockRefByNumber(1, blockX, nil)
	// src.ExpectFetchReceipts(blockX.Hash, nil, nil)
	b.emitter.Emit(context.Background(), superevents.LocalUnsafeReceivedEvent{
		ChainID:        chainA,
		NewLocalUnsafe: blockX,
	})
	require.NoError(t, ex.Drain())
	src.AssertExpectations(t)
	unsafe, err := b.LocalUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockX.ID(), unsafe)
	xunsafe, err = b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockX.ID(), xunsafe)
	// cross-safe still undefined
	_, err = b.CrossSafe(context.Background(), chainA)
	require.ErrorIs(t, err, types.ErrFuture, err)

	// Receive unsafe block Y from node

	src.ExpectBlockRefByNumber(blockY.Number, blockY, nil)
	src.ExpectFetchReceipts(blockY.Hash, nil, nil)
	b.emitter.Emit(context.Background(), superevents.LocalUnsafeReceivedEvent{
		ChainID:        chainA,
		NewLocalUnsafe: blockY,
	})
	require.NoError(t, ex.Drain())
	src.AssertExpectations(t)
	xunsafe, err = b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockY.ID(), xunsafe)

	// Receive derived block X from node

	b.emitter.Emit(context.Background(), superevents.LocalDerivedEvent{
		ChainID: chainA,
		Derived: types.DerivedBlockRefPair{
			Derived: blockX,
		},
	})
	require.NoError(t, ex.Drain())
	src.AssertExpectations(t)
	// cross-unsafe still at block Y
	xunsafe, err = b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockY.ID(), xunsafe)
	// cross-safe now at block X
	xsafe, err = b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockX.ID(), xsafe.Derived)

	// Receive derived block Y from node

	b.emitter.Emit(context.Background(), superevents.LocalDerivedEvent{
		ChainID: chainA,
		Derived: types.DerivedBlockRefPair{
			Derived: blockY,
		},
	})
	require.NoError(t, ex.Drain())
	// cross-safe now at block Y
	xsafe, err = b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)
	require.Equal(t, blockY.ID(), xsafe.Derived)

	err = b.Stop(context.Background())
	require.NoError(t, err)
	t.Log("stopped!")
}

func TestBackendCallsMetrics(t *testing.T) {
	logger := testlog.Logger(t, log.LvlInfo)
	mockMetrics := &MockMetrics{}
	dataDir := t.TempDir()
	chainA := eth.ChainIDFromUInt64(testChainIDOffset)

	// Set up mock metrics
	mockMetrics.Mock.On("RecordDBEntryCount", chainA, mock.AnythingOfType("string"), mock.AnythingOfType("int64")).Return()
	mockMetrics.Mock.On("RecordCrossUnsafe", chainA, mock.MatchedBy(func(_ types.BlockSeal) bool { return true })).Return()
	mockMetrics.Mock.On("RecordCrossSafe", chainA, mock.MatchedBy(func(_ types.BlockSeal) bool { return true })).Return()
	mockMetrics.Mock.On("RecordLocalSafe", chainA, mock.MatchedBy(func(_ types.BlockSeal) bool { return true })).Return()
	mockMetrics.Mock.On("RecordLocalUnsafe", chainA, mock.MatchedBy(func(_ types.BlockSeal) bool { return true })).Return()

	fullCfgSet := fullConfigSet(t, 1)
	cfg := &config.Config{
		Version:               "test",
		LogConfig:             oplog.CLIConfig{},
		MetricsConfig:         opmetrics.CLIConfig{},
		PprofConfig:           oppprof.CLIConfig{},
		RPC:                   oprpc.CLIConfig{},
		FullConfigSetSource:   fullCfgSet,
		SynchronousProcessors: true,
		MockRun:               false,
		SyncSources:           &syncnode.CLISyncNodes{},
		Datadir:               dataDir,
	}

	ex := event.NewGlobalSynchronous(context.Background())
	b, err := NewSupervisorBackend(context.Background(), logger, mockMetrics, cfg, ex)
	require.NoError(t, err)

	// Assert that the metrics are called at initialization
	mockMetrics.Mock.AssertCalled(t, "RecordDBEntryCount", chainA, "log", int64(0))
	mockMetrics.Mock.AssertCalled(t, "RecordDBEntryCount", chainA, "local_derived", int64(0))
	mockMetrics.Mock.AssertCalled(t, "RecordDBEntryCount", chainA, "cross_derived", int64(0))

	// Start the backend
	err = b.Start(context.Background())
	require.NoError(t, err)

	// Create a test block
	block := eth.BlockRef{
		Hash:       common.Hash{0xaa},
		Number:     42,
		ParentHash: common.Hash{0xbb},
		Time:       10000,
	}
	safe := types.DerivedBlockRefPair{
		Source:  block, // dummy value
		Derived: block,
	}
	// update local unsafe/safe, cross unsafe/safe
	b.chainDBs.OnEvent(context.Background(), superevents.SafeActivationBlockEvent{
		Safe:    safe,
		ChainID: chainA,
	})
	// Assert that metrics are called on safety level updates
	mockMetrics.Mock.AssertCalled(t, "RecordLocalUnsafe", chainA, mock.MatchedBy(func(ref types.BlockSeal) bool {
		return ref.Hash == block.Hash && ref.Number == block.Number && ref.Timestamp == block.Time
	}))
	mockMetrics.Mock.AssertCalled(t, "RecordLocalSafe", chainA, mock.MatchedBy(func(ref types.BlockSeal) bool {
		return ref.Hash == block.Hash && ref.Number == block.Number && ref.Timestamp == block.Time
	}))
	mockMetrics.Mock.AssertCalled(t, "RecordCrossUnsafe", chainA, mock.MatchedBy(func(ref types.BlockSeal) bool {
		return ref.Hash == block.Hash && ref.Number == block.Number && ref.Timestamp == block.Time
	}))
	mockMetrics.Mock.AssertCalled(t, "RecordCrossSafe", chainA, mock.MatchedBy(func(ref types.BlockSeal) bool {
		return ref.Hash == block.Hash && ref.Number == block.Number && ref.Timestamp == block.Time
	}))
	mockMetrics.Mock.AssertCalled(t, "RecordDBEntryCount", chainA, "cross_derived", int64(1))
	mockMetrics.Mock.AssertCalled(t, "RecordDBEntryCount", chainA, "local_derived", int64(1))
	// db entry: searchCheckpoint, canonicalHash
	mockMetrics.Mock.AssertCalled(t, "RecordDBEntryCount", chainA, "log", int64(2))

	// Stop the backend
	err = b.Stop(context.Background())
	require.NoError(t, err)
}

type MockMetrics struct {
	mock.Mock
	event.NoopMetrics
	opmetrics.NoopRPCMetrics
}

var _ Metrics = (*MockMetrics)(nil)

func (m *MockMetrics) CacheAdd(chainID eth.ChainID, label string, cacheSize int, evicted bool) {
	m.Mock.Called(chainID, label, cacheSize, evicted)
}

func (m *MockMetrics) CacheGet(chainID eth.ChainID, label string, hit bool) {
	m.Mock.Called(chainID, label, hit)
}

func (m *MockMetrics) RecordCrossUnsafe(chainID eth.ChainID, seal types.BlockSeal) {
	m.Mock.Called(chainID, seal)
}

func (m *MockMetrics) RecordCrossSafe(chainID eth.ChainID, seal types.BlockSeal) {
	m.Mock.Called(chainID, seal)
}

func (m *MockMetrics) RecordLocalSafe(chainID eth.ChainID, seal types.BlockSeal) {
	m.Mock.Called(chainID, seal)
}

func (m *MockMetrics) RecordLocalUnsafe(chainID eth.ChainID, seal types.BlockSeal) {
	m.Mock.Called(chainID, seal)
}

func (m *MockMetrics) RecordDBEntryCount(chainID eth.ChainID, kind string, count int64) {
	m.Mock.Called(chainID, kind, count)
}

func (m *MockMetrics) RecordDBSearchEntriesRead(chainID eth.ChainID, count int64) {
	m.Mock.Called(chainID, count)
}

func (m *MockMetrics) RecordAccessListVerifyFailure(chainID eth.ChainID) {
	m.Mock.Called(chainID)
}

type MockProcessorSource struct {
	mock.Mock
}

var _ processors.Source = (*MockProcessorSource)(nil)

func (m *MockProcessorSource) FetchReceipts(ctx context.Context, blockHash common.Hash) (types2.Receipts, error) {
	out := m.Mock.Called(blockHash)
	return out.Get(0).(types2.Receipts), out.Error(1)
}

func (m *MockProcessorSource) ExpectFetchReceipts(hash common.Hash, receipts types2.Receipts, err error) {
	m.Mock.On("FetchReceipts", hash).Once().Return(receipts, err)
}

func (m *MockProcessorSource) BlockRefByNumber(ctx context.Context, num uint64) (eth.BlockRef, error) {
	out := m.Mock.Called(num)
	return out.Get(0).(eth.BlockRef), out.Error(1)
}

func (m *MockProcessorSource) ExpectBlockRefByNumber(num uint64, ref eth.BlockRef, err error) {
	m.Mock.On("BlockRefByNumber", num).Return(ref, err)
}

// fakeSyncSource implements syncnode.SyncSource for testing asyncVerifyAccessWithRPC.
type fakeSyncSource struct {
	chainID eth.ChainID
	seal    types.BlockSeal
	err     error
}

func (f *fakeSyncSource) Contains(_ context.Context, _ types.ContainsQuery) (types.BlockSeal, error) {
	return f.seal, f.err
}

func (f *fakeSyncSource) ChainID(_ context.Context) (eth.ChainID, error) {
	return f.chainID, nil
}

func (f *fakeSyncSource) BlockRefByNumber(_ context.Context, _ uint64) (eth.BlockRef, error) {
	panic("should not be called")
}

func (f *fakeSyncSource) FetchReceipts(_ context.Context, _ common.Hash) (types2.Receipts, error) {
	panic("should not be called")
}

func (f *fakeSyncSource) OutputV0AtTimestamp(_ context.Context, _ uint64) (*eth.OutputV0, error) {
	panic("should not be called")
}

func (f *fakeSyncSource) PendingOutputV0AtTimestamp(_ context.Context, _ uint64) (*eth.OutputV0, error) {
	panic("should not be called")
}

func (f *fakeSyncSource) L2BlockRefByTimestamp(_ context.Context, _ uint64) (eth.L2BlockRef, error) {
	panic("should not be called")
}

func (f *fakeSyncSource) String() string {
	return "fakeSyncSource"
}

// TestAsyncVerifyAccessWithRPC exercises the asyncVerifyAccessWithRPC method against various RPC error and block match/mismatch scenarios.
// The method is responsible for asynchronously verifying RPC access checks (checksum and block ID matching),
// and recording metrics when discrepancies are found.
//
// The test checks four key scenarios:
// 1. ErrConflict error + block ID mismatch: Should record 2 failures (one for checksum, one for mismatch)
// 2. ErrConflict error + matching block ID: Still records a failure for the checksum error
// 3. Other error (e.g. ErrFuture) + mismatch: Should record failure only for the block mismatch
// 4. No error + matching block ID: Should record no failures
func TestAsyncVerifyAccessWithRPC(t *testing.T) {
	logger := testlog.Logger(t, log.LevelInfo)
	// Setup a single-chain dependency set
	chainID := eth.ChainIDFromUInt64(testChainIDOffset)
	fullCfgSet := fullConfigSet(t, 1)

	// Create and set up mock metrics
	mockMetrics := &MockMetrics{}
	// Set up the required method calls that happen during initialization
	mockMetrics.Mock.On("RecordDBEntryCount", chainID, "log", int64(0)).Return()
	mockMetrics.Mock.On("RecordDBEntryCount", chainID, "local_derived", int64(0)).Return()
	mockMetrics.Mock.On("RecordDBEntryCount", chainID, "cross_derived", int64(0)).Return()

	// Initialize backend with mock metrics
	cfg := &config.Config{
		Version:               "test",
		LogConfig:             oplog.CLIConfig{},
		MetricsConfig:         opmetrics.CLIConfig{},
		PprofConfig:           oppprof.CLIConfig{},
		RPC:                   oprpc.CLIConfig{},
		FullConfigSetSource:   fullCfgSet,
		SynchronousProcessors: true,
		MockRun:               false,
		SyncSources:           &syncnode.CLISyncNodes{},
		Datadir:               t.TempDir(),
	}
	ex := event.NewGlobalSynchronous(context.Background())
	b, err := NewSupervisorBackend(context.Background(), logger, mockMetrics, cfg, ex)
	require.NoError(t, err)

	// Prepare the access object (only ChainID matters for metrics)
	acc := types.Access{ChainID: chainID}

	// Helper to run a scenario and assert metrics calls
	runScenario := func(name string, stubSeal types.BlockSeal, stubErr error, dbBlock eth.BlockID) {
		t.Run(name, func(t *testing.T) {
			// Reset recorded calls
			mockMetrics.Mock = mock.Mock{}

			// Based on the log output, we observe:
			// 1. When err=ErrConflict: Logs "RPC access checksum failed" and calls RecordAccessListVerifyFailure
			// 2. When err!=ErrConflict: Logs "RPC access check failed mechanically" but doesn't record a metric
			// 3. When seal.ID() != dbBlock: Logs "DB access check result did not match" and calls RecordAccessListVerifyFailure

			// Set expectations for the actual behavior observed
			if errors.Is(stubErr, types.ErrConflict) {
				// Error for checksum failure
				mockMetrics.Mock.On("RecordAccessListVerifyFailure", chainID).Return()
			}

			// Block ID mismatch will always trigger a metrics call
			if seal := stubSeal.ID(); seal != dbBlock {
				mockMetrics.Mock.On("RecordAccessListVerifyFailure", chainID).Return()
			}

			// Override the sync source to return our stubbed result
			b.syncSources.Set(chainID, &fakeSyncSource{chainID: chainID, seal: stubSeal, err: stubErr})

			// Invoke the async verification
			b.asyncVerifyAccessWithRPC(context.Background(), acc, dbBlock)

			// Verify that our expectations were met
			mockMetrics.Mock.AssertExpectations(t)
		})
	}

	// Define a couple of block seals for match vs mismatch
	sealA := types.BlockSeal{Hash: common.HexToHash("0x1"), Number: 10, Timestamp: 100}
	idA := sealA.ID()
	sealB := types.BlockSeal{Hash: common.HexToHash("0x2"), Number: 20, Timestamp: 200}
	idB := sealB.ID()

	// ErrConflict + mismatch => 2 failures (checksum + mismatch)
	runScenario("ErrConflict_mismatch", sealA, types.ErrConflict, idB)
	// ErrConflict + match    => 1 failure  (checksum only)
	runScenario("ErrConflict_match", sealA, types.ErrConflict, idA)
	// Other non-conflict error + mismatch => 1 failure (mismatch only)
	runScenario("OtherErr_mismatch", sealA, types.ErrFuture, idB)
	// No error + match         => 0 failures
	runScenario("NoErr_match", sealA, nil, idA)
}
