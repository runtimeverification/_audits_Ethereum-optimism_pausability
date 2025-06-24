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

// Missing:

// 4 - FinalizedL1RequestEvent
// 9 - LocalDerivedOriginUpdateEvent
// 10 - AnchorEvent
// 12 - RewindL1Event
// 13 - ReplaceBlockEvent

// Done:
// 1 - UpdateCrossUnsafeRequestEvent
// 2 - CrossUnsafeUpdateEvent
// 3 - UpdateCrossSafeRequestEvent
// 4 - LocalSafeUpdateEvent
// 5 - CrossSafeUpdateEvent
// 6 - LocalUnsafeUpdateEvent
// 7 - ChainProcessEvent
// 8 - LocalUnsafeReceivedEvent
// 9 - FinalizedL1UpdateEvent
// 10 - FinalizedL2UpdateEvent
// 11 - InvalidateLocalSafeEvent
// 12 - ChainRewoundEvent
// 13 - UpdateLocalSafeFailedEvent
// 14 - LocalDerivedEvent

func FuzzUpdateCrossUnsafeInvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(3), uint64(1)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, chainALength uint64, chainBLength uint64, crossUnsafeHeadIndex uint64, localSafeHeadIndex uint64, crossSafeHeadIndex uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d", chainALength, chainBLength)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10

		crossUnsafeHead, _, crossSafeHeadIndex := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		srcChainA.ExpectBlockRefByNumber(uint64(chainALength), eth.L1BlockRef{}, ethereum.NotFound)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("UpdateCrossUnsafeRequestEvent", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.UpdateCrossUnsafeRequestEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateCrossUnsafeRequestEvent{ChainID: chainA}
				}, false))
			t.Log("UpdateCrossUnsafeRequestEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func FuzzUpdateCrossSafeInvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(3), uint64(1)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, chainALength uint64, chainBLength uint64, crossUnsafeHeadIndex uint64, localSafeHeadIndex uint64, crossSafeHeadIndex uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d", chainALength, chainBLength)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10

		crossUnsafeHead, _, crossSafeHeadIndex := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		srcChainA.ExpectBlockRefByNumber(uint64(chainALength), eth.L1BlockRef{}, ethereum.NotFound)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("UpdateCrossSafeRequestEvent", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.UpdateCrossSafeRequestEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateCrossSafeRequestEvent{ChainID: chainA}
				}, false))

			t.Log("UpdateCrossSafeRequestEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func FuzzUpdateLocalSafeInvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(2), uint64(1), bool(false)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T,
		chainALength uint64,
		chainBLength uint64,
		crossUnsafeHeadIndex uint64,
		localSafeHeadIndex uint64,
		crossSafeHeadIndex uint64,
		equalUnsafeChain bool) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d", chainALength, chainBLength)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10

		crossUnsafeHead, localSafeHeadIndex, crossSafeHeadIndex := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		srcChainA.ExpectBlockRefByNumber(uint64(chainALength), eth.L1BlockRef{}, ethereum.NotFound)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("LocalSafeUpdateEvent Event", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)
			var hashDerived common.Hash
			if equalUnsafeChain {
				hashDerived = common.BytesToHash([]byte{0xaa, byte(localSafeHeadIndex + 1)})
			} else {
				hashDerived = common.BytesToHash([]byte{0xbb, byte(localSafeHeadIndex + 1)}) // Ensure the hash is different from the cross unsafe head
			}
			newLocalSafe := types.DerivedBlockSealPair{
				Derived: types.BlockSealFromRef(eth.BlockRef{
					Hash:       hashDerived,
					Number:     localSafeHeadIndex + 1,
					ParentHash: common.BytesToHash([]byte{0xaa, byte(localSafeHeadIndex)}),
					Time:       uint64(time.Now().Unix()),
				}),
				Source: types.BlockSeal{},
			}
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.LocalSafeUpdateEvent{
					ChainID:      chainA,
					NewLocalSafe: newLocalSafe,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalSafeUpdateEvent{
						ChainID:      chainA,
						NewLocalSafe: newLocalSafe,
					}
				}, false))

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func FuzzLocalDerivedEventnvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(2), uint64(1), uint64(3)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T,
		chainALength uint64,
		chainBLength uint64,
		crossUnsafeHeadIndex uint64,
		localSafeHeadIndex uint64,
		crossSafeHeadIndex uint64,
		localSafetoUpdate uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d", chainALength, chainBLength)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10

		crossUnsafeHead, localSafeHeadIndex, crossSafeHeadIndex := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		srcChainA.ExpectBlockRefByNumber(uint64(chainALength), eth.L1BlockRef{}, ethereum.NotFound)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("LocalDerivedEvent Event", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)
			localSafetoUpdate = localSafetoUpdate % (localSafeHeadIndex + 3) // Allow it to be greater than the next current local safe head
			derived := types.DerivedBlockRefPair{
				Derived: eth.BlockRef{
					Hash:       common.BytesToHash([]byte{0xaa, byte(localSafetoUpdate)}),
					Number:     localSafetoUpdate,
					ParentHash: common.BytesToHash([]byte{0xaa, byte(localSafetoUpdate) - 1}),
					Time:       uint64(time.Now().Unix()),
				},
				Source: eth.BlockRef{},
			}
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.LocalDerivedEvent{
					ChainID: chainA,
					Derived: derived,
					NodeID:  "test-node",
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalDerivedEvent{
						ChainID: chainA,
						Derived: derived,
						NodeID:  "test-node",
					}
				}, false))

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})
}

func FuzzEventsPreserveState(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(3), uint64(1)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, chainALength uint64, chainBLength uint64, crossUnsafeHeadIndex uint64, localSafeHeadIndex uint64, crossSafeHeadIndex uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d", chainALength, chainBLength)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10

		crossUnsafeHead, _, crossSafeHeadIndex := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		srcChainA.ExpectBlockRefByNumber(uint64(chainALength), eth.L1BlockRef{}, ethereum.NotFound)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)

		t.Run("LocalUnsafeUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.LocalUnsafeUpdateEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalUnsafeUpdateEvent{ChainID: chainA}
				}, false))
			t.Log("LocalUnsafeUpdateEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("LocalUnsafeReceivedEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.LocalUnsafeReceivedEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalUnsafeReceivedEvent{ChainID: chainA}
				}, false))
			t.Log("LocalUnsafeReceivedEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("CrossUnsafeUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.CrossUnsafeUpdateEvent{
					ChainID:        chainA,
					NewCrossUnsafe: types.BlockSeal{},
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.CrossUnsafeUpdateEvent{ChainID: chainA}
				}, false))
			t.Log("CrossUnsafeUpdateEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("CrossSafeUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.CrossSafeUpdateEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.CrossSafeUpdateEvent{ChainID: chainA}
				}, false))
			t.Log("CrossSafeUpdateEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("FinalizedL1UpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.FinalizedL1UpdateEvent{
					FinalizedL1: eth.BlockRef{},
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.FinalizedL1UpdateEvent{FinalizedL1: eth.BlockRef{}}
				}, false))
			t.Log("FinalizedL1UpdateEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("FinalizedL2UpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.FinalizedL2UpdateEvent{
					ChainID:     chainA,
					FinalizedL2: types.BlockSeal{},
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.FinalizedL2UpdateEvent{
						ChainID:     chainA,
						FinalizedL2: types.BlockSeal{},
					}
				}, false))
			t.Log("FinalizedL2UpdateEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("InvalidateLocalSafeEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.InvalidateLocalSafeEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.InvalidateLocalSafeEvent{
						ChainID: chainA,
					}
				}, false))
			t.Log("InvalidateLocalSafeEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("ChainRewoundEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.ChainRewoundEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.ChainRewoundEvent{
						ChainID: chainA,
					}
				}, false))
			t.Log("ChainRewoundEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		t.Run("UpdateLocalSafeFailedEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.UpdateLocalSafeFailedEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateLocalSafeFailedEvent{
						ChainID: chainA,
					}
				}, false))
			t.Log("UpdateLocalSafeFailedEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func FuzzChainProcessEventInvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(3), uint64(2), uint64(1), uint64(7)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T,
		chainALength uint64,
		chainBLength uint64,
		crossUnsafeHeadIndex uint64,
		localSafeHeadIndex uint64,
		crossSafeHeadIndex uint64,
		target uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d", chainALength, chainBLength)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		target = target % (chainALength + 2)
		//chainBLength = chainBLength % 10

		crossUnsafeHead, _, crossSafeHeadIndex := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("ChainProcessEvent Event", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)
			newLocalUnsafe := eth.BlockRef{
				Hash:       common.BytesToHash([]byte{0xaa, byte(target)}),
				Number:     target,
				ParentHash: common.BytesToHash([]byte{0xaa, byte(target - 1)}),
				Time:       uint64(time.Now().Add(time.Duration(target*5) * time.Minute).Unix()),
			}

			t.Logf("Chain A block %d: %s\t Timestamp:%d", target, newLocalUnsafe.Hash.Hex(), newLocalUnsafe.Time)

			srcChainA.ExpectBlockRefByNumber(uint64(chainALength), newLocalUnsafe, nil)
			srcChainA.ExpectFetchReceipts(newLocalUnsafe.Hash, nil, nil)

			srcChainA.ExpectBlockRefByNumber(uint64(chainALength+1), eth.L1BlockRef{}, ethereum.NotFound)

			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.ChainProcessEvent{
					ChainID: chainA,
					Target:  target,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.ChainProcessEvent{
						ChainID: chainA,
						Target:  target,
					}
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
	crossSafeHeadIndex uint64) (types.BlockSeal, uint64, uint64) {
	t.Log("Initializing Chain A")
	crossUnsafeHeadIndex = crossUnsafeHeadIndex % chainALength
	localSafeHeadIndex = localSafeHeadIndex % chainALength
	if localSafeHeadIndex > 0 {
		crossSafeHeadIndex = crossSafeHeadIndex % localSafeHeadIndex
	} else {
		crossSafeHeadIndex = 0
	}

	t.Logf("Local-Unsafe Number: %d", chainALength-1)
	t.Logf("Cross-Unsafe Number: %d", crossUnsafeHeadIndex)
	t.Logf("Local-Safe Number: %d", localSafeHeadIndex)
	t.Logf("Cross-Safe Number: %d", crossSafeHeadIndex)

	// Initialize the chainA source with a genesis block
	block := eth.BlockRef{
		Hash:       common.BytesToHash([]byte{0xaa, 0x00}),
		Number:     0,
		ParentHash: common.Hash{}, // genesis has no parent hash
		Time:       uint64(time.Now().Unix()),
	}
	crossUnsafeHead := block
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

	ex.DrainUntil(
		func(ev event.Event) bool {
			return ev == superevents.UpdateCrossSafeRequestEvent{ChainID: chainA}
		}, true)

	return types.BlockSealFromRef(crossUnsafeHead), localSafeHeadIndex, crossSafeHeadIndex
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

	require.LessOrEqual(t, crossUnsafe.Number, localUnsafe.Number, "Cross Unsafe head: %d is not less or equal than Local Unsafe head: %d", crossUnsafe.Number, localUnsafe.Number)
}

func CrossSafe_LE_LocalSafe(t *testing.T, b *SupervisorBackend, chainA eth.ChainID) {

	localSafe, err := b.LocalSafe(context.Background(), chainA)
	require.NoError(t, err)
	crossSafe, err := b.CrossSafe(context.Background(), chainA)
	require.NoError(t, err)

	t.Logf("Cross Safe head for Chain A: %d <= Local Safe head for Chain A: %d", crossSafe.Derived.Number, localSafe.Derived.Number)

	require.LessOrEqual(t, crossSafe.Derived.Number, localSafe.Derived.Number, "Cross Safe head: %d is not less or equal than Local Safe head: %d", crossSafe.Derived.Number, localSafe.Derived.Number)
}
