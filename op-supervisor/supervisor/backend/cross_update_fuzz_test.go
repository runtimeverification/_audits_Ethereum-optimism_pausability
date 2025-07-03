package backend

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
// 10 - AnchorEvent
// 12 - RewindL1Event

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
// 15 - LocalDerivedOriginUpdateEvent
// 16 - ReplaceBlockEvent
// 17 - FinalizedL1RequestEvent

type State struct {
	chainHeads map[eth.ChainID]*ChainHeads
}

func FuzzRandomChains(f *testing.F) {
	params := RandomChainParams{
		chainCount: 3,
		minLength:  10,
		maxLength:  30,

		sameTimestampFrequency: 80,
		dependencyChance:       50,
	}
	f.Add(int64(-57))

	f.Fuzz(func(t *testing.T, seed int64) {
		randomChain := params.MakeRandomChain(seed)

		for _, cb := range randomChain.allBlocks {
			t.Logf("    %s, %2d, %d", cb.chain, cb.block.Number, cb.block.Time)
		}

		for _, chain := range randomChain.chainIDs {
			chainHeads := randomChain.chainHeads[chain]
			localUnsafe := randomChain.chainBlocks[chain][len(randomChain.chainBlocks[chain])-1].Number
			crossUnsafe := randomChain.chainBlocks[chain][chainHeads.crossUnsafe]
			localSafe := randomChain.chainBlocks[chain][chainHeads.localSafe].Number
			crossSafe := randomChain.chainBlocks[chain][chainHeads.crossSafe]

			t.Logf("Chain %d LocalUnsafe: %d CrossUnsafe: %d LocalSafe: %d CrossSafe: %d", chain, localUnsafe, crossUnsafe.Number, localSafe, crossSafe.Number)

			for _, block := range randomChain.chainBlocks[chain] {
				t.Logf("Chain %d block %d: %s\t Timestamp:%d", chain, block.Number, block.Hash.Hex(), block.Time)
			}
		}

		for exec, inits := range randomChain.dependencies {
			for _, init := range inits {
				t.Logf("(%s, %2d) <- (%s, %2d)", init.chain, init.block.Number, exec.chain, exec.block.Number)
			}
		}
		for cb, logs := range randomChain.generatedLogs {
			chain := cb.chain
			block := cb.block
			t.Logf("Generating receipt for (%s, %2d, %s) with %d logs", chain, block.Number, block.Hash, len(logs))
		}
	})
}

var chainParams = RandomChainParams{
	chainCount: 3,
	minLength:  10,
	maxLength:  30,

	sameTimestampFrequency: 80,
	dependencyChance:       50,
}

func FuzzUpdateCrossUnsafeSucceeds(f *testing.F) {

	f.Add(int64(-57))

	f.Fuzz(func(t *testing.T, seed int64) {
		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)

		t.Run("UpdateCrossUnsafeRequestEvent Success", func(t *testing.T) {
			ChainsInit(t, b, ex, randomChain)

			// Ensure the invariants hold in the intiial state
			t.Log("Initial State")
			preState := AssertInvariants(t, b, randomChain)

			// Enqueue the UpdateCrossUnsafeRequestEvent
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.UpdateCrossUnsafeRequestEvent{},
				EmitPriority: event.High,
			})

			// Drain the event until it is processed
			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateCrossUnsafeRequestEvent{}
				}, false))
			t.Log("UpdateCrossUnsafeRequestEvent processed")

			t.Log("Final State")
			// Assert the invariants hold after handling the event - Safety properties
			posState := AssertInvariants(t, b, randomChain)

			// Check that the state has changed as expected - Liveness property
			AssertCrossUnsafeHeadUpdate(t, randomChain, preState, posState, eth.ChainIDFromUInt64(0))
		})

		t.Run("Cross-unsafe reaches local-unsafe", func(t *testing.T) {

			// Ensure the invariants hold in the intiial state
			t.Log("Initial State")
			preState := AssertInvariants(t, b, randomChain)

			// Drain the event until it is processed
			require.NoError(t, ex.Drain())
			t.Log("All Events processed")

			t.Log("Final State")
			// Assert the invariants hold after handling the event - Safety properties
			posState := AssertInvariants(t, b, randomChain)

			// Check that all cross-unsafe heads got equal to respective local-unsafe heads - Liveness property
			for _, chain := range randomChain.chainIDs {
				preLocalUnsafe := preState.chainHeads[chain].localUnsafe
				posCrossUnsafe := posState.chainHeads[chain].crossUnsafe

				require.Equal(t, posCrossUnsafe, preLocalUnsafe)
			}
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})
}

func FuzzUpdateCrossUnsafeFails(f *testing.F) {

	f.Add(int64(30))

	f.Fuzz(func(t *testing.T, seed int64) {
		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)

		t.Run("UpdateCrossUnsafeRequestEvent Fails", func(t *testing.T) {
			// Invalidate a block
			crossUnsafeCandidate := GetCrossUnsafeCandidate(randomChain)
			InvalidateBlock(t, &randomChain, crossUnsafeCandidate)
			ChainsInit(t, b, ex, randomChain)

			// Ensure the invariants hold in the intiial state
			t.Log("Initial State")
			preState := AssertInvariants(t, b, randomChain)

			// Enqueue the UpdateCrossUnsafeRequestEvent
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.UpdateCrossUnsafeRequestEvent{},
				EmitPriority: event.High,
			})

			// Drain the event until it is processed
			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateCrossUnsafeRequestEvent{}
				}, false))
			t.Log("UpdateCrossUnsafeRequestEvent processed")

			t.Log("Final State")
			// Assert the invariants hold after handling the event - Safety properties
			posState := AssertInvariants(t, b, randomChain)

			// Check that the state has changed as expected - Liveness property
			AssertCrossUnsafeHeadUpdate(t, randomChain, preState, posState, crossUnsafeCandidate.chain)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func FuzzUpdateCrossSafeSucceeds(f *testing.F) {

	f.Add(int64(30))

	f.Fuzz(func(t *testing.T, seed int64) {
		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)

		t.Run("UpdateCrossSafeRequestEvent Succeeds", func(t *testing.T) {
			ChainsInit(t, b, ex, randomChain)

			// Ensure the invariants hold in the intiial state
			t.Log("Initial State")
			preState := AssertInvariants(t, b, randomChain)

			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.UpdateCrossSafeRequestEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateCrossSafeRequestEvent{}
				}, false))

			t.Log("UpdateCrossSafeRequestEvent processed")

			t.Log("Final State")
			// Assert the invariants hold after handling the event - Safety properties
			posState := AssertInvariants(t, b, randomChain)

			// Check that the state has changed as expected - Liveness property
			AssertCrossSafeHeadUpdate(t, randomChain, preState, posState, eth.ChainIDFromUInt64(0))
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})
}

func FuzzUpdateCrossSafeFails(f *testing.F) {

	f.Add(int64(30))

	f.Fuzz(func(t *testing.T, seed int64) {
		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)

		t.Run("UpdateCrossSafeRequestEvent Fails", func(t *testing.T) {
			invalidCandidate := GetCrossSafeCandidate(randomChain)
			InvalidateBlock(t, &randomChain, invalidCandidate)
			ChainsInit(t, b, ex, randomChain)

			// Ensure the invariants hold in the intiial state
			t.Log("Initial State")
			preState := AssertInvariants(t, b, randomChain)

			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.UpdateCrossSafeRequestEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateCrossSafeRequestEvent{}
				}, false))

			t.Log("UpdateCrossSafeRequestEvent processed")

			t.Log("Final State")
			// Assert the invariants hold after handling the event - Safety properties
			posState := AssertInvariants(t, b, randomChain)

			// Check that the state has changed as expected - Liveness property
			AssertCrossSafeHeadUpdate(t, randomChain, preState, posState, invalidCandidate.chain)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})
}

func FuzzUpdateLocalSafeInvariants(f *testing.F) {

	f.Add(int64(30), bool(false)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, seed int64, equalUnsafeChain bool) {

		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)
		ChainsInit(t, b, ex, randomChain)

		t.Run("LocalSafeUpdateEvent Event", func(t *testing.T) {
			// Ensure the invariants hold in the initial state
			t.Log("Initial State")
			AssertInvariants(t, b, randomChain)

			chainA := randomChain.chainIDs[0]
			localSafeHead := randomChain.chainHeads[chainA].localSafe

			var hashDerived common.Hash
			if equalUnsafeChain {
				hashDerived = common.BytesToHash([]byte{0xaa, byte(localSafeHead + 1)})
			} else {
				hashDerived = common.BytesToHash([]byte{0xbb, byte(localSafeHead + 1)}) // Ensure the hash is different from the unsafe chain
			}
			newLocalSafe := types.DerivedBlockSealPair{
				Derived: types.BlockSealFromRef(eth.BlockRef{
					Hash:       hashDerived,
					Number:     localSafeHead + 1,
					ParentHash: common.BytesToHash([]byte{0xaa, byte(localSafeHead)}),
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
			t.Log("LocalSafeUpdateEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func FuzzLocalDerivedEventInvariants(f *testing.F) {

	f.Add(int64(30), uint64(5))

	f.Fuzz(func(t *testing.T, seed int64, localSafetoUpdate uint64) {

		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)
		ChainsInit(t, b, ex, randomChain)

		t.Run("LocalDerivedEvent Event", func(t *testing.T) {
			// Ensure the invariants hold in the initial state
			t.Log("Initial State")
			AssertInvariants(t, b, randomChain)

			chainA := randomChain.chainIDs[0]
			localSafeHead := randomChain.chainHeads[chainA].localSafe
			localSafetoUpdate = localSafetoUpdate % (localSafeHead + 3) // Allow it to be greater than the next current local safe head

			derived := types.DerivedBlockRefPair{
				Derived: *randomChain.chainBlocks[chainA][localSafetoUpdate],
				Source:  eth.BlockRef{},
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
			t.Log("LocalDerivedEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})
}

func FuzzReplaceBlockEventInvariants(f *testing.F) {

	f.Add(int64(30))

	f.Fuzz(func(t *testing.T, seed int64) {

		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)
		ChainsInit(t, b, ex, randomChain)

		t.Run("ReplaceBlockEvent Event", func(t *testing.T) {
			// Ensure the invariants hold in the initial state
			t.Log("Initial State")
			AssertInvariants(t, b, randomChain)

			chainA := randomChain.chainIDs[0]
			crossSafeHeadCandidate := randomChain.chainHeads[chainA].crossSafe + 1

			invalidated := types.DerivedBlockRefPair{
				Derived: *randomChain.chainBlocks[chainA][crossSafeHeadCandidate],
				Source:  eth.BlockRef{},
			}
			b.chainDBs.InvalidateLocalSafe(chainA, invalidated)

			newHash := make([]byte, 32)
			rand.Read(newHash)
			replacementBlock := eth.BlockRef{
				Hash:       common.BytesToHash(newHash),
				Number:     crossSafeHeadCandidate,
				ParentHash: invalidated.Derived.ParentHash,
				Time:       uint64(time.Now().Unix()),
			}
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.ReplaceBlockEvent{
					ChainID: chainA,
					Replacement: types.BlockReplacement{
						Replacement: replacementBlock,
						Invalidated: invalidated.Derived.Hash,
					},
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.ReplaceBlockEvent{
						ChainID: chainA,
						Replacement: types.BlockReplacement{
							Replacement: replacementBlock,
							Invalidated: invalidated.Derived.Hash,
						},
					}
				}, false))

			t.Log("ReplaceBlockEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})
}

func FuzzChainProcessEventInvariants(f *testing.F) {

	f.Add(int64(30), uint64(8))

	f.Fuzz(func(t *testing.T, seed int64, target uint64) {

		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)
		ChainsInit(t, b, ex, randomChain)

		chainA := randomChain.chainIDs[0]
		srcChainA := randomChain.chainSources[chainA]
		target = target % (randomChain.chainHeads[chainA].localUnsafe + 2)

		t.Run("ChainProcessEvent Event", func(t *testing.T) {
			// Ensure the invariants hold in the initial state
			t.Log("Initial State")
			AssertInvariants(t, b, randomChain)

			newHash := make([]byte, 32)
			rand.Read(newHash)

			newLocalUnsafe := eth.BlockRef{
				Hash:       common.BytesToHash(newHash),
				Number:     target,
				ParentHash: randomChain.chainBlocks[chainA][target-1].Hash,
				Time:       uint64(time.Now().Unix()),
			}

			t.Logf("Chain A block %d: %s\t Timestamp:%d", target, newLocalUnsafe.Hash.Hex(), newLocalUnsafe.Time)

			srcChainA.ExpectBlockRefByNumber(target, newLocalUnsafe, nil)
			srcChainA.ExpectFetchReceipts(newLocalUnsafe.Hash, nil, nil)

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
			t.Log("ChainProcessEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

// FuzzEventsPreserveState tests that various events preserve the state of the backend
func FuzzEventsPreserveState(f *testing.F) {

	f.Add(int64(30), uint64(8))

	f.Fuzz(func(t *testing.T, seed int64, target uint64) {

		randomChain := chainParams.MakeRandomChain(seed)
		ex, b := ExecutorBackendInit(t, randomChain)
		ChainsInit(t, b, ex, randomChain)

		t.Log("Initial State")
		AssertInvariants(t, b, randomChain)

		t.Run("LocalUnsafeUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.LocalUnsafeUpdateEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalUnsafeUpdateEvent{}
				}, false))
			t.Log("LocalUnsafeUpdateEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("LocalUnsafeReceivedEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.LocalUnsafeReceivedEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalUnsafeReceivedEvent{}
				}, false))
			t.Log("LocalUnsafeReceivedEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("CrossUnsafeUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.CrossUnsafeUpdateEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.CrossUnsafeUpdateEvent{}
				}, false))
			t.Log("CrossUnsafeUpdateEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("CrossSafeUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.CrossSafeUpdateEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.CrossSafeUpdateEvent{}
				}, false))
			t.Log("CrossSafeUpdateEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
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

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("FinalizedL2UpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.FinalizedL2UpdateEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.FinalizedL2UpdateEvent{}
				}, false))
			t.Log("FinalizedL2UpdateEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("InvalidateLocalSafeEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.InvalidateLocalSafeEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.InvalidateLocalSafeEvent{}
				}, false))
			t.Log("InvalidateLocalSafeEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("ChainRewoundEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.ChainRewoundEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.ChainRewoundEvent{}
				}, false))
			t.Log("ChainRewoundEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("UpdateLocalSafeFailedEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.UpdateLocalSafeFailedEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.UpdateLocalSafeFailedEvent{}
				}, false))
			t.Log("UpdateLocalSafeFailedEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("LocalDerivedOriginUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.LocalDerivedOriginUpdateEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalDerivedOriginUpdateEvent{}
				}, false))
			t.Log("LocalDerivedOriginUpdateEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		t.Run("FinalizedL1RequestEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event:        superevents.FinalizedL1RequestEvent{},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.FinalizedL1RequestEvent{}
				}, false))
			t.Log("FinalizedL1RequestEvent processed")

			t.Log("Final State")
			AssertInvariants(t, b, randomChain)
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

func ExecutorBackendInit(t *testing.T, randomChain RandomChain) (ex *event.GlobalSyncExec, b *SupervisorBackend) {
	logger := testlog.Logger(t, log.LvlInfo)
	dataDir := t.TempDir()
	dependencies := make(map[eth.ChainID]*depset.StaticConfigDependency)

	for i, chain := range randomChain.chainIDs {
		chainint, _ := chain.Uint64()

		dependencies[chain] = &depset.StaticConfigDependency{
			ChainIndex:     types.ChainIndex(uint32(chainint)),
			ActivationTime: uint64(42 + i),
			HistoryMinTime: uint64(100 + i),
		}
	}
	depSet, err := depset.NewStaticConfigDependencySet(dependencies)
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
	t.Log("Initialized!")

	l1Src := &testutils.MockL1Source{}
	b.AttachL1Source(l1Src)

	for _, chain := range randomChain.chainIDs {
		srcChain := randomChain.chainSources[chain]
		require.NoError(t, b.AttachProcessorSource(chain, srcChain))
	}

	err = b.Start(context.Background())
	require.NoError(t, err)
	t.Log("started!")

	return ex, b
}

func ChainsInitOld(t *testing.T, b *SupervisorBackend, ex *event.GlobalSyncExec, randomChain RandomChain) {
	for exec, inits := range randomChain.dependencies {
		for _, init := range inits {
			t.Logf("(%s, %2d) <- (%s, %2d)", init.chain, init.block.Number, exec.chain, exec.block.Number)
		}
	}
	for cb, logs := range randomChain.generatedLogs {
		chain := cb.chain
		block := cb.block
		t.Logf("Generating receipt for (%s, %2d, %s) with %d logs", chain, block.Number, block.Hash, len(logs))
	}
	for _, chain := range randomChain.chainIDs {
		chainHeads := randomChain.chainHeads[chain]
		localUnsafe := randomChain.chainBlocks[chain][len(randomChain.chainBlocks[chain])-1].Number
		crossUnsafe := randomChain.chainBlocks[chain][chainHeads.crossUnsafe]
		localSafe := randomChain.chainBlocks[chain][chainHeads.localSafe].Number
		crossSafe := randomChain.chainBlocks[chain][chainHeads.crossSafe]

		t.Logf("Chain %d LocalUnsafe: %d CrossUnsafe: %d LocalSafe: %d CrossSafe: %d", chain, localUnsafe, crossUnsafe.Number, localSafe, crossSafe.Number)

		for _, block := range randomChain.chainBlocks[chain] {
			t.Logf("Chain %d block %d: %s\t Timestamp:%d", chain, block.Number, block.Hash.Hex(), block.Time)
		}

		ex.Enqueue(event.AnnotatedEvent{
			Event: superevents.AnchorEvent{
				ChainID: chain,
				Anchor: types.DerivedBlockRefPair{
					Derived: *crossSafe,
					Source:  eth.L1BlockRef{},
				}},
			EmitPriority: event.High,
		})

		t.Logf("AnchorEvent for chain %d Emmitted", chain)

		ex.DrainUntil(
			func(ev event.Event) bool {
				return ev == superevents.AnchorEvent{
					ChainID: chain,
					Anchor: types.DerivedBlockRefPair{
						Derived: *crossSafe,
						Source:  eth.L1BlockRef{},
					}}
			}, false)

		t.Logf("AnchorEvent for chain %d processed", chain)

		for i := crossSafe.Number + 1; i <= localUnsafe; i++ {
			block := randomChain.chainBlocks[chain][i]

			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.ChainProcessEvent{
					ChainID: chain,
					Target:  block.Number,
				},
				EmitPriority: event.High,
			})
			ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.ChainProcessEvent{
						ChainID: chain,
						Target:  block.Number}
				}, false)
			t.Logf("ChainProcessEvent for chain %d block %d processed", chain, block.Number)

			if block.Number <= localSafe {
				localSafe := superevents.LocalDerivedEvent{
					ChainID: chain,
					Derived: types.DerivedBlockRefPair{
						Derived: *block,
						Source:  eth.L1BlockRef{},
					},
					NodeID: "test-node",
				}
				ex.Enqueue(event.AnnotatedEvent{
					Event:        localSafe,
					EmitPriority: event.High,
				})
				ex.DrainUntil(
					func(ev event.Event) bool {
						return ev == localSafe
					}, false)
				t.Logf("LocalDerivedEvent for chain %d block %d processed", chain, block.Number)
			}
		}

		err := b.chainDBs.UpdateCrossUnsafe(chain, types.BlockSealFromRef(*crossUnsafe))
		require.NoError(t, err)
	}
	t.Log("Chains initialized!")
}

func ChainsInit(t *testing.T, b *SupervisorBackend, ex *event.GlobalSyncExec, randomChain RandomChain) {
	GenerateReceiptsFromLogs(&randomChain)

	for _, chain := range randomChain.chainIDs {
		chainHeads := randomChain.chainHeads[chain]
		localUnsafe := randomChain.chainBlocks[chain][len(randomChain.chainBlocks[chain])-1].Number
		crossUnsafe := randomChain.chainBlocks[chain][chainHeads.crossUnsafe]
		localSafe := randomChain.chainBlocks[chain][chainHeads.localSafe].Number
		crossSafe := randomChain.chainBlocks[chain][chainHeads.crossSafe]

		t.Logf("Chain %d LocalUnsafe: %d CrossUnsafe: %d LocalSafe: %d CrossSafe: %d", chain, localUnsafe, crossUnsafe.Number, localSafe, crossSafe.Number)

		ex.Enqueue(event.AnnotatedEvent{
			Event: superevents.AnchorEvent{
				ChainID: chain,
				Anchor: types.DerivedBlockRefPair{
					Derived: *randomChain.chainBlocks[chain][0],
					Source:  eth.L1BlockRef{},
				}},
			EmitPriority: event.High,
		})

		t.Logf("AnchorEvent for chain %d Emmitted", chain)

		ex.DrainUntil(
			func(ev event.Event) bool {
				return ev == superevents.AnchorEvent{
					ChainID: chain,
					Anchor: types.DerivedBlockRefPair{
						Derived: *randomChain.chainBlocks[chain][0],
						Source:  eth.L1BlockRef{},
					}}
			}, false)

		t.Logf("AnchorEvent for chain %d processed", chain)

		for _, block := range randomChain.chainBlocks[chain] {
			t.Logf("Chain %d block %d: %s\t Timestamp:%d", chain, block.Number, block.Hash.Hex(), block.Time)

			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.ChainProcessEvent{
					ChainID: chain,
					Target:  block.Number,
				},
				EmitPriority: event.High,
			})
			ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.ChainProcessEvent{
						ChainID: chain,
						Target:  block.Number}
				}, false)

			if block.Number <= crossSafe.Number {
				localSafe := superevents.LocalDerivedEvent{
					ChainID: chain,
					Derived: types.DerivedBlockRefPair{
						Derived: *block,
						Source:  eth.L1BlockRef{},
					},
					NodeID: "test-node",
				}
				ex.Enqueue(event.AnnotatedEvent{
					Event:        localSafe,
					EmitPriority: event.High,
				})
				ex.DrainUntil(func(ev event.Event) bool { return ev == localSafe }, false)
			}
		}
	}

	for _, chain := range randomChain.chainIDs {
		chainHeads := randomChain.chainHeads[chain]
		localSafe := randomChain.chainBlocks[chain][chainHeads.localSafe].Number
		crossSafe := randomChain.chainBlocks[chain][chainHeads.crossSafe]

		for i := crossSafe.Number + 1; i <= localSafe; i++ {
			block := randomChain.chainBlocks[chain][i]

			localSafe := superevents.LocalDerivedEvent{
				ChainID: chain,
				Derived: types.DerivedBlockRefPair{
					Derived: *block,
					Source:  eth.L1BlockRef{},
				},
				NodeID: "test-node",
			}
			ex.Enqueue(event.AnnotatedEvent{
				Event:        localSafe,
				EmitPriority: event.High,
			})
			ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == localSafe
				}, false)
		}
		crossUnsafe := randomChain.chainBlocks[chain][chainHeads.crossUnsafe]
		err := b.chainDBs.UpdateCrossUnsafe(chain, types.BlockSealFromRef(*crossUnsafe))
		require.NoError(t, err)
	}
	t.Log("Chains initialized!")
}

func CrossUnsafe_LE_LocalUnsafe(t *testing.T, b *SupervisorBackend, chain eth.ChainID, state State) {

	localUnsafe, err := b.LocalUnsafe(context.Background(), chain)
	require.NoError(t, err)
	crossUnsafe, err := b.CrossUnsafe(context.Background(), chain)
	require.NoError(t, err)

	t.Logf("\t- Cross Unsafe head %d <= Local Unsafe head %d", crossUnsafe.Number, localUnsafe.Number)
	state.chainHeads[chain].crossUnsafe = crossUnsafe.Number
	state.chainHeads[chain].localUnsafe = localUnsafe.Number
	require.LessOrEqual(t, crossUnsafe.Number, localUnsafe.Number, "Cross Unsafe head: %d is not less or equal than Local Unsafe head: %d", crossUnsafe.Number, localUnsafe.Number)
}

func CrossSafe_LE_LocalSafe(t *testing.T, b *SupervisorBackend, chain eth.ChainID, state State) {

	localSafe, err := b.LocalSafe(context.Background(), chain)
	crossSafe, _ := b.CrossSafe(context.Background(), chain)

	state.chainHeads[chain].crossSafe = crossSafe.Derived.Number
	state.chainHeads[chain].localSafe = localSafe.Derived.Number

	t.Logf("\t- Cross Safe head %d <= Local Safe head %d", crossSafe.Derived.Number, localSafe.Derived.Number)
	if err == types.ErrAwaitReplacementBlock {
		return
	}
	require.LessOrEqual(t, crossSafe.Derived.Number, localSafe.Derived.Number, "Cross Safe head: %d is not less or equal than Local Safe head: %d", crossSafe.Derived.Number, localSafe.Derived.Number)
}

func AssertInvariants(t *testing.T, b *SupervisorBackend, rc RandomChain) (state State) {
	state.chainHeads = make(map[eth.ChainID]*ChainHeads)
	for _, chain := range rc.chainIDs {
		t.Logf("Chain %d:", chain)
		state.chainHeads[chain] = &ChainHeads{}

		CrossUnsafe_LE_LocalUnsafe(t, b, chain, state)
		CrossSafe_LE_LocalSafe(t, b, chain, state)
	}
	return state
}

func AssertCrossUnsafeHeadUpdate(t *testing.T, rc RandomChain, preState State, posState State, expectNoUpdate eth.ChainID) {
	crossUnsafeUpdates := 0
	chainsToBeUpdated := len(rc.chainIDs)
	for _, chain := range rc.chainIDs {
		preCrossUnsafe := preState.chainHeads[chain].crossUnsafe
		preLocalUnsafe := preState.chainHeads[chain].localUnsafe
		posCrossUnsafe := posState.chainHeads[chain].crossUnsafe
		if chain != expectNoUpdate {
			if preCrossUnsafe < preLocalUnsafe {
				// Ensure the cross unsafe head has been updated
				if posCrossUnsafe == preCrossUnsafe+1 {
					crossUnsafeUpdates++
					t.Logf("Cross Unsafe head for chain %d has been updated from %d to %d", chain, preCrossUnsafe, posCrossUnsafe)
				}
			} else {
				chainsToBeUpdated--
				require.Equal(t, posCrossUnsafe, preCrossUnsafe)
				t.Logf("Cross Unsafe head for chain %d was already equal to Local Unsafe", chain)
			}
		} else {
			chainsToBeUpdated--
			require.Equal(t, posCrossUnsafe, preCrossUnsafe, "Cross Unsafe head unexpectedly updated for chain %d", chain)
			t.Logf("Cross Unsafe head for chain %d was not updated because the candidate was invalid", chain)
		}
	}
	if chainsToBeUpdated > 0 {
		require.Greater(t, crossUnsafeUpdates, 0)
	} else {
		require.Equal(t, crossUnsafeUpdates, 0)
	}
}

func AssertCrossSafeHeadUpdate(t *testing.T, rc RandomChain, preState State, posState State, expectNoUpdate eth.ChainID) {
	for _, chain := range rc.chainIDs {
		preCrossSafe := preState.chainHeads[chain].crossSafe
		preLocalSafe := preState.chainHeads[chain].localSafe
		posCrossSafe := posState.chainHeads[chain].crossSafe
		if chain != expectNoUpdate {
			if preCrossSafe < preLocalSafe {
				// Ensure the cross unsafe head has been updated
				require.Equal(t, posCrossSafe, preCrossSafe+1,
					"Cross Safe head did not update for chain %d", chain)
				t.Logf("Cross Safe head for chain %d has been updated from %d to %d", chain, preCrossSafe, posCrossSafe)
			} else {
				require.Equal(t, posCrossSafe, preCrossSafe)
				t.Logf("Cross Safe head for chain %d was already equal to Local Safe", chain)
			}
		} else {
			require.Equal(t, posCrossSafe, preCrossSafe, "Cross Safe head unexpectedly updated for chain %d", chain)
			t.Logf("Cross Safe head for chain %d was not updated because the candidate was invalid", chain)
		}
	}
}
