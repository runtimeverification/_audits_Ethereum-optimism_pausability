package backend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup/event"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum-optimism/optimism/op-service/oppprof"
	oprpc "github.com/ethereum-optimism/optimism/op-service/rpc"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	"github.com/ethereum-optimism/optimism/op-service/testutils"
	"github.com/ethereum-optimism/optimism/op-supervisor/config"
	"github.com/ethereum-optimism/optimism/op-supervisor/metrics"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/depset"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/superevents"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/syncnode"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

func FuzzCrossUnsafeUpdateInvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(2), uint64(1)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, chainALength uint64, chainBLength uint64, crossUnsafeHeadIndex uint64, localSafeHeadIndex uint64, crossSafeHeadIndex uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d, Cross Unsafe Head Index: %d", chainALength, chainBLength, crossUnsafeHeadIndex)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10
		crossUnsafeHeadIndex = crossUnsafeHeadIndex % chainALength
		localSafeHeadIndex = localSafeHeadIndex % chainALength
		if localSafeHeadIndex > 0 {
			crossSafeHeadIndex = crossSafeHeadIndex % localSafeHeadIndex
		} else {
			crossSafeHeadIndex = 0
		}

		crossUnsafeHead, _, _ := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("Cross Unsafe Update", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.UpdateCrossUnsafeRequestEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					// We expect the UpdateCrossUnsafeRequestEvent to be emitted
					return ev == superevents.UpdateCrossUnsafeRequestEvent{ChainID: chainA}
				}, false))
			t.Log("Cross Unsafe Update processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)

		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func FuzzCrossSafeUpdateInvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(2), uint64(1)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, chainALength uint64, chainBLength uint64, crossUnsafeHeadIndex uint64, localSafeHeadIndex uint64, crossSafeHeadIndex uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d, Cross Unsafe Head Index: %d", chainALength, chainBLength, crossUnsafeHeadIndex)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10
		crossUnsafeHeadIndex = crossUnsafeHeadIndex % chainALength
		localSafeHeadIndex = localSafeHeadIndex % chainALength
		if localSafeHeadIndex > 0 {
			crossSafeHeadIndex = crossSafeHeadIndex % localSafeHeadIndex
		} else {
			crossSafeHeadIndex = 0
		}

		crossUnsafeHead, _, _ := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("Cross Safe Update", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.UpdateCrossSafeRequestEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					// We expect the UpdateCrossUnsafeRequestEvent to be emitted
					return ev == superevents.UpdateCrossSafeRequestEvent{ChainID: chainA}
				}, false))

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func ExecutorBackendInit(t *testing.T, chainA eth.ChainID, chainB eth.ChainID) (ex *event.GlobalSyncExec, b *SupervisorBackend, l1Src *testutils.MockL1Source, srcChainA *MockProcessorSource, srcChainB *MockProcessorSource) {
	logger := testlog.Logger(t, log.LvlInfo)
	dataDir := t.TempDir()

	depSet, err := depset.NewStaticConfigDependencySet(
		map[eth.ChainID]*depset.StaticConfigDependency{
			chainA: {
				ChainIndex:     900,
				ActivationTime: 42,
				HistoryMinTime: 100,
			},
			chainB: {
				ChainIndex:     901,
				ActivationTime: 30,
				HistoryMinTime: 20,
			},
		})
	require.NoError(t, err)
	cfg := &config.Config{
		Version:               "test",
		LogConfig:             oplog.CLIConfig{},
		MetricsConfig:         opmetrics.CLIConfig{},
		PprofConfig:           oppprof.CLIConfig{},
		RPC:                   oprpc.CLIConfig{},
		DependencySetSource:   depSet,
		SynchronousProcessors: true,
		MockRun:               false,
		SyncSources:           &syncnode.CLISyncNodes{},
		Datadir:               dataDir,
	}

	ex = event.NewGlobalSynchronous(context.Background())
	b, err = NewSupervisorBackend(context.Background(), logger, metrics.NoopMetrics, cfg, ex)
	require.NoError(t, err)
	t.Log("initialized!")

	l1Src = &testutils.MockL1Source{}
	b.AttachL1Source(l1Src)

	srcChainA = &MockProcessorSource{}
	require.NoError(t, b.AttachProcessorSource(chainA, srcChainA))

	srcChainB = &MockProcessorSource{}
	require.NoError(t, b.AttachProcessorSource(chainB, srcChainB))

	err = b.Start(context.Background())
	require.NoError(t, err)
	t.Log("started!")

	return ex, b, l1Src, srcChainA, srcChainB
}

func ChainAInit(t *testing.T,
	b *SupervisorBackend,
	ex *event.GlobalSyncExec,
	chainA eth.ChainID,
	srcChainA *MockProcessorSource,
	chainALength uint64,
	crossUnsafeHeadIndex uint64,
	localSafeHeadIndex uint64,
	crossSafeHeadIndex uint64) (types.BlockSeal, eth.BlockRef, eth.BlockRef) {
	t.Log("Initializing Chain A")
	t.Logf("Chain A length: %d", chainALength)

	// Initialize the chainA source with a genesis block
	block := eth.BlockRef{
		Hash:       common.BytesToHash([]byte{0xaa, 0x00}),
		Number:     0,
		ParentHash: common.Hash{}, // genesis has no parent hash
		Time:       uint64(time.Now().Unix()),
	}
	crossUnsafeHead, localSafeHead, crossSafeHead := block, block, block
	t.Logf("Chain A genesis block:%s", block.Hash.Hex())
	srcChainA.ExpectBlockRefByNumber(0, block, nil)
	srcChainA.ExpectFetchReceipts(block.Hash, nil, nil)
	// Emit the anchor event for chain A with the genesis block
	// This is necessary to initialize the database with the genesis block
	b.emitter.Emit(superevents.AnchorEvent{
		ChainID: chainA,
		Anchor: types.DerivedBlockRefPair{
			Derived: block,
			Source:  eth.L1BlockRef{},
		}})
	require.NoError(t, ex.Drain())

	i := 1
	for ; i < int(chainALength); i++ {
		block = eth.BlockRef{
			Hash:       common.BytesToHash([]byte{0xaa, byte(i)}),
			Number:     uint64(i),
			ParentHash: common.BytesToHash([]byte{0xaa, byte(i - 1)}),
			Time:       uint64(time.Now().Add(time.Duration(i*5) * time.Minute).Unix()),
		}
		t.Logf("Chain A block %d: %s\t Timestamp:%d", i, block.Hash.Hex(), block.Time)
		// Expect the source to return the block by number
		srcChainA.ExpectBlockRefByNumber(uint64(i), block, nil)
		srcChainA.ExpectFetchReceipts(block.Hash, nil, nil)

		b.emitter.Emit(superevents.LocalUnsafeReceivedEvent{
			ChainID:        chainA,
			NewLocalUnsafe: block,
		})

		if i <= int(crossUnsafeHeadIndex) {
			// Set the cross unsafe head to a specific block
			crossUnsafeHead = block
			//b.chainDBs.UpdateCrossUnsafe(chainA, types.BlockSealFromRef(crossUnsafeHead))
		}
		if i <= int(localSafeHeadIndex) {
			// TODO create L1 source blocks
			b.emitter.Emit(superevents.LocalDerivedEvent{
				ChainID: chainA,
				Derived: types.DerivedBlockRefPair{
					Derived: block,
					Source:  eth.L1BlockRef{},
				},
				NodeID: "test-node",
			})
		}
	}
	srcChainA.ExpectBlockRefByNumber(uint64(i), eth.L1BlockRef{}, ethereum.NotFound)

	require.NoError(t, ex.DrainUntil(
		func(ev event.Event) bool {
			return ev == superevents.UpdateCrossSafeRequestEvent{ChainID: chainA}
		}, true))

	return types.BlockSealFromRef(crossUnsafeHead), localSafeHead, crossSafeHead
}

func ChainBInit(t *testing.T, b *SupervisorBackend, chainB eth.ChainID, srcChainB *MockProcessorSource, chainBLength uint64) {
	t.Log("Initializing Chain B")
	t.Logf("Chain B length: %d", chainBLength)

	if chainBLength > 0 {
		// Initialize the chainB source with a genesis block
		genesisBlock := eth.BlockRef{
			Hash:       common.BytesToHash([]byte{0xbb, 0x00}),
			Number:     0,
			ParentHash: common.Hash{}, // genesis has no parent hash
			Time:       uint64(time.Now().Unix()),
		}
		t.Logf("Chain B genesis block: %s", genesisBlock.Hash.Hex())

		srcChainB.ExpectBlockRefByNumber(0, genesisBlock, nil)
		srcChainB.ExpectFetchReceipts(genesisBlock.Hash, nil, nil)
		// Emit the anchor event for chain B
		b.emitter.Emit(superevents.AnchorEvent{
			ChainID: chainB,
			Anchor: types.DerivedBlockRefPair{
				Derived: genesisBlock,
				Source:  eth.L1BlockRef{},
			}})
		i := 1
		for ; i < int(chainBLength); i++ {
			block := eth.BlockRef{
				Hash:       common.BytesToHash([]byte{0xbb, byte(i)}),
				Number:     uint64(i),
				ParentHash: common.Hash{0xbb, byte(i - 1)},
				Time:       uint64(time.Now().Add(time.Duration(i*10) * time.Minute).Unix()),
			}
			t.Logf("Chain B block %d: %s\t Timestamp:%d", i, block.Hash.Hex(), block.Time)
			// Expect the source to return the block by number
			srcChainB.ExpectBlockRefByNumber(uint64(i), block, nil)
			srcChainB.ExpectFetchReceipts(block.Hash, nil, nil)
		}
		srcChainB.ExpectBlockRefByNumber(uint64(i), eth.L1BlockRef{}, ethereum.NotFound)
	} else {
		t.Log("Chain B has no blocks to initialize")
	}
}

func InitialState(t *testing.T, b *SupervisorBackend, ex *event.GlobalSyncExec, chainA eth.ChainID, crossUnsafeHead types.BlockSeal, crossSafeHeadIndex uint64) {
	err := b.chainDBs.UpdateCrossUnsafe(chainA, crossUnsafeHead)
	require.NoError(t, err)

	for i := 0; i < int(crossSafeHeadIndex); i++ {
		ex.Enqueue(event.AnnotatedEvent{
			Event: superevents.UpdateCrossSafeRequestEvent{
				ChainID: chainA,
			},
			EmitPriority: event.High,
		})

		require.NoError(t, ex.DrainUntil(
			func(ev event.Event) bool {
				// We expect the UpdateCrossUnsafeRequestEvent to be emitted
				return ev == superevents.UpdateCrossSafeRequestEvent{ChainID: chainA}
			}, false))
	}

	t.Log("Initial state for Chain A")

	localUnsafe, err := b.LocalUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	t.Logf("Local Unsafe head: %d", localUnsafe.Number)

	crossUnsafe, err := b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	t.Logf("Cross Unsafe head: %d", crossUnsafe.Number)

	localSafe, err := b.LocalSafe(context.Background(), chainA)
	require.NoError(t, err)
	t.Logf("Local Safe head: %d", localSafe.Derived.Number)

	crossSafe, err := b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)
	t.Logf("Cross Safe head: %d", crossSafe.Derived.Number)
}

func CrossUnsafe_LE_LocalUnsafe(t *testing.T, b *SupervisorBackend, chainA eth.ChainID) {

	localUnsafe, err := b.LocalUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	crossUnsafe, err := b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)

	t.Logf("Cross Unsafe head for Chain A: %d <= Local Unsafe head for Chain A: %d", crossUnsafe.Number, localUnsafe.Number)

	require.LessOrEqual(t, crossUnsafe.Number, localUnsafe.Number)
}

func CrossSafe_LE_LocalSafe(t *testing.T, b *SupervisorBackend, chainA eth.ChainID) {

	localSafe, err := b.LocalSafe(context.Background(), chainA)
	require.NoError(t, err)
	crossSafe, err := b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)

	t.Logf("Cross Safe head for Chain A: %d <= Local Safe head for Chain A: %d", crossSafe.Derived.Number, localSafe.Derived.Number)

	require.LessOrEqual(t, crossSafe.Derived.Number, localSafe.Derived.Number)
}
