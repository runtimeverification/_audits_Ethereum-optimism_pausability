package backend

import (
	"context"
	"math/rand"
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

func TestCrossUpdate(t *testing.T) {
	logger := testlog.Logger(t, log.LvlInfo)
	m := metrics.NoopMetrics
	dataDir := t.TempDir()
	chainA := eth.ChainIDFromUInt64(900)
	chainB := eth.ChainIDFromUInt64(901)
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

	ex := event.NewGlobalSynchronous(context.Background())
	b, err := NewSupervisorBackend(context.Background(), logger, m, cfg, ex)
	require.NoError(t, err)
	t.Log("initialized!")

	l1Src := &testutils.MockL1Source{}
	b.AttachL1Source(l1Src)

	srcChainA := &MockProcessorSource{}
	require.NoError(t, b.AttachProcessorSource(chainA, srcChainA))

	t.Run("ChainA Initialization", func(t *testing.T) {
		//t.Parallel()
		chainALength := rand.Intn(10)
		t.Logf("Chain A length: %d", chainALength)

		if chainALength > 0 {
			// Initialize the chainA source with a genesis block
			genesisBlock := eth.BlockRef{
				Hash:       common.BytesToHash([]byte{0xaa, 0x00}),
				Number:     0,
				ParentHash: common.Hash{}, // genesis has no parent hash
				Time:       uint64(time.Now().Unix()),
			}
			t.Logf("Chain A genesis block:%s", genesisBlock.Hash.Hex())

			srcChainA.ExpectBlockRefByNumber(0, genesisBlock, nil)
			srcChainA.ExpectFetchReceipts(genesisBlock.Hash, nil, nil)
			// Emit the anchor event for chain A with the genesis block
			// This is necessary to initialize the database with the genesis block
			b.emitter.Emit(superevents.AnchorEvent{
				ChainID: chainA,
				Anchor: types.DerivedBlockRefPair{
					Derived: genesisBlock,
					Source:  eth.L1BlockRef{},
				}})
		} else {
			t.Log("Chain A has no blocks to initialize")
		}
		i := 1
		for ; i < chainALength; i++ {
			block := eth.BlockRef{
				Hash:       common.BytesToHash([]byte{0xaa, byte(i)}),
				Number:     uint64(i),
				ParentHash: common.Hash{0xaa, byte(i - 1)},
				Time:       uint64(time.Now().Add(time.Duration(i*10) * time.Minute).Unix()),
			}
			t.Logf("Chain A block %d: %s\t Timestamp:%d", i, block.Hash.Hex(), block.Time)
			// Expect the source to return the block by number
			srcChainA.ExpectBlockRefByNumber(uint64(i), block, nil)
			srcChainA.ExpectFetchReceipts(block.Hash, nil, nil)
		}

		srcChainA.ExpectBlockRefByNumber(uint64(i), eth.L1BlockRef{}, ethereum.NotFound)

	})

	srcChainB := &MockProcessorSource{}
	require.NoError(t, b.AttachProcessorSource(chainB, srcChainB))

	t.Run("ChainB Initialization", func(t *testing.T) {
		//t.Parallel()
		chainBLength := rand.Intn(10)
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
			// This is necessary to initialize the database with the genesis block
			b.emitter.Emit(superevents.AnchorEvent{
				ChainID: chainA,
				Anchor: types.DerivedBlockRefPair{
					Derived: genesisBlock,
					Source:  eth.L1BlockRef{},
				}})
		} else {
			t.Log("Chain B has no blocks to initialize")
		}
		i := 1
		for ; i < chainBLength; i++ {
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
	})

	err = b.Start(context.Background())
	require.NoError(t, err)
	t.Log("started!")

	// After the anchor event, the database is initialized, and the call to update
	// from the LocalUnsafe event will succeed.

	require.NoError(t, ex.Drain())

	err = b.Stop(context.Background())
	require.NoError(t, err)
	t.Log("stopped!")

}
