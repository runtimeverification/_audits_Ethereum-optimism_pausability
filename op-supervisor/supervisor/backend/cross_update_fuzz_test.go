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

	logger := testlog.Logger(f, log.LvlInfo)
	m := metrics.NoopMetrics
	dataDir := f.TempDir()
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
	require.NoError(f, err)
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
	require.NoError(f, err)
	f.Log("initialized!")

	l1Src := &testutils.MockL1Source{}
	b.AttachL1Source(l1Src)

	srcChainA := &MockProcessorSource{}
	require.NoError(f, b.AttachProcessorSource(chainA, srcChainA))

	srcChainB := &MockProcessorSource{}
	require.NoError(f, b.AttachProcessorSource(chainB, srcChainB))

	err = b.Start(context.Background())
	require.NoError(f, err)
	f.Log("started!")

	f.Add(uint(4), uint(1))

	f.Fuzz(func(t *testing.T, chainALength uint, chainBLength uint) {

		t.Run("ChainA Initialization", func(t *testing.T) {
			//t.Parallel()
			chainALength = chainALength % 10
			t.Logf("Chain A length: %d", chainALength)

			var block eth.BlockRef

			if chainALength > 0 {
				// Initialize the chainA source with a genesis block
				block = eth.BlockRef{
					Hash:       common.BytesToHash([]byte{0xaa, 0x00}),
					Number:     0,
					ParentHash: common.Hash{}, // genesis has no parent hash
					Time:       uint64(time.Now().Unix()),
				}
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
						Time:       uint64(time.Now().Add(time.Duration(i*10) * time.Minute).Unix()),
					}
					t.Logf("Chain A block %d: %s\t Timestamp:%d", i, block.Hash.Hex(), block.Time)
					// Expect the source to return the block by number
					srcChainA.ExpectBlockRefByNumber(uint64(i), block, nil)
					srcChainA.ExpectFetchReceipts(block.Hash, nil, nil)
				}
				srcChainA.ExpectBlockRefByNumber(uint64(i), eth.L1BlockRef{}, ethereum.NotFound)

				b.emitter.Emit(superevents.LocalUnsafeReceivedEvent{
					ChainID:        chainA,
					NewLocalUnsafe: block,
				})
				t.Log("Emitted LocalUnsafeReceivedEvent for Chain A")
			} else {
				t.Log("Chain A has no blocks to initialize")
			}

		})

		t.Run("ChainB Initialization", func(t *testing.T) {
			//t.Parallel()
			t.Logf("Chain B length: %d", chainBLength%10)
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
		})
	})

	// After the anchor event, the database is initialized, and the call to update
	// from the LocalUnsafe event will succeed.

	//require.NoError(f, ex.DrainUntil(
	//	func(ev event.Event) bool {
	//		return ev == superevents.UpdateCrossUnsafeRequestEvent{
	//			ChainID: chainA,
	//		}
	//	},
	//	true))
	require.NoError(f, ex.Drain())

	localUnsafe, err := b.LocalUnsafe(context.Background(), chainA)
	require.NoError(f, err)
	f.Logf("Local Unsafe head for Chain A: %d", localUnsafe.Number)

	crossUnsafe, err := b.CrossUnsafe(context.Background(), chainA)
	require.NoError(f, err)
	f.Logf("Cross Unsafe head for Chain A: %d", crossUnsafe.Number)

	err = b.Stop(context.Background())
	require.NoError(f, err)
	f.Log("stopped!")
}
