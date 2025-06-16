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

func FuzzCrossUpdate(f *testing.F) {

	f.Add(uint(4), uint(2), uint(2)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, chainALength uint, chainBLength uint, crossUnsafeHeadIndex uint) {
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, srcChainB := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		chainBLength = chainBLength % 10
		crossUnsafeHeadIndex = crossUnsafeHeadIndex % chainALength
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d, Cross Unsafe Head Index: %d", chainALength, chainBLength, crossUnsafeHeadIndex)

		crossUnsafeHead := ChainAInit(t, b, chainA, srcChainA, chainALength, crossUnsafeHeadIndex)
		ChainBInit(t, b, chainB, srcChainB, chainBLength)

		require.NoError(t, ex.Drain())

		t.Run("Cross Unsafe Update", func(t *testing.T) {
			InitialState(t, b, chainA, crossUnsafeHead)
			b.emitter.Emit(superevents.UpdateCrossUnsafeRequestEvent{ChainID: chainA})
			require.NoError(t, ex.Drain())

			Invariant1(t, b, chainA)
		})

		t.Run("Cross Safe Update", func(t *testing.T) {
			InitialState(t, b, chainA, crossUnsafeHead)
			b.emitter.Emit(superevents.UpdateCrossSafeRequestEvent{ChainID: chainA})
			require.NoError(t, ex.Drain())

			Invariant1(t, b, chainA)
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

func ChainAInit(t *testing.T, b *SupervisorBackend, chainA eth.ChainID, srcChainA *MockProcessorSource, chainALength uint, crossUnsafeHeadIndex uint) types.BlockSeal {
	t.Log("Initializing Chain A")
	t.Logf("Chain A length: %d", chainALength)

	var block, crossUnsafeHead eth.BlockRef

	if chainALength > 0 {
		// Initialize the chainA source with a genesis block
		block = eth.BlockRef{
			Hash:       common.BytesToHash([]byte{0xaa, 0x00}),
			Number:     0,
			ParentHash: common.Hash{}, // genesis has no parent hash
			Time:       uint64(time.Now().Unix()),
		}
		crossUnsafeHead = block
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
		i := 1
		for ; i < int(chainALength); i++ {
			block = eth.BlockRef{
				Hash:       common.BytesToHash([]byte{0xaa, byte(i)}),
				Number:     uint64(i),
				ParentHash: common.BytesToHash([]byte{0xaa, byte(i - 1)}),
				Time:       uint64(time.Now().Add(time.Duration(i*5) * time.Minute).Unix()),
			}
			if i == int(crossUnsafeHeadIndex) {
				// Set the cross unsafe head to a specific block
				crossUnsafeHead = block
			}
			t.Logf("Chain A block %d: %s\t Timestamp:%d", i, block.Hash.Hex(), block.Time)
			// Expect the source to return the block by number
			srcChainA.ExpectBlockRefByNumber(uint64(i), block, nil)
			srcChainA.ExpectFetchReceipts(block.Hash, nil, nil)
		}
		srcChainA.ExpectBlockRefByNumber(uint64(i), eth.L1BlockRef{}, ethereum.NotFound)
		// After the anchor event, the database is initialized, and the call to update
		// from the LocalUnsafe event will succeed.
		b.emitter.Emit(superevents.LocalUnsafeReceivedEvent{
			ChainID:        chainA,
			NewLocalUnsafe: block,
		})
		t.Log("Emitted LocalUnsafeReceivedEvent for Chain A")
		return types.BlockSealFromRef(crossUnsafeHead)
	} else {
		t.Log("Chain A has no blocks to initialize")
		return types.BlockSeal{}
	}
}

func ChainBInit(t *testing.T, b *SupervisorBackend, chainB eth.ChainID, srcChainB *MockProcessorSource, chainBLength uint) {
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

func InitialState(t *testing.T, b *SupervisorBackend, chainA eth.ChainID, crossUnsafeHead types.BlockSeal) {
	err := b.chainDBs.UpdateCrossUnsafe(chainA, crossUnsafeHead)
	require.NoError(t, err)

	localUnsafe, err := b.LocalUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	t.Logf("Local Unsafe head for Chain A: %d", localUnsafe.Number)

	crossUnsafe, err := b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	t.Logf("Cross Unsafe head for Chain A: %d", crossUnsafe.Number)
}

func Invariant1(t *testing.T, b *SupervisorBackend, chainA eth.ChainID) {

	localUnsafe, err := b.LocalUnsafe(context.Background(), chainA)
	require.NoError(t, err)
	crossUnsafe, err := b.CrossUnsafe(context.Background(), chainA)
	require.NoError(t, err)

	t.Logf("Cross Unsafe head for Chain A: %d <= Local Unsafe head for Chain A: %d", crossUnsafe.Number, localUnsafe.Number)

	require.LessOrEqual(t, crossUnsafe.Number, localUnsafe.Number)
}
