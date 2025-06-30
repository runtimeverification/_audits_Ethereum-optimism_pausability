package backend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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

func FuzzRandomChains(f *testing.F) {
	params := RandomChainParams{
		chainCount: 4,
		minLength:  50,
		maxLength:  100,

		sameTimestampFrequency: 60,
		dependencyChance:       20,
	}
	f.Add(int64(30))

	f.Fuzz(func(t *testing.T, seed int64) {
		randomChain := params.MakeRandomChain(seed)

		for _, cb := range randomChain.allBlocks {
			head := ""
			if cb.block.Number == randomChain.chainHeads[cb.chain].crossSafe {
				head += " <-- Cross Safe"
			}
			if cb.block.Number == randomChain.chainHeads[cb.chain].crossUnsafe {
				head += " <-- Cross Unsafe"
			}
			if cb.block.Number == randomChain.chainHeads[cb.chain].localSafe {
				head += " <-- Local Safe"
			}
			if cb.block.Number == randomChain.chainHeads[cb.chain].localUnsafe {
				head += " <-- Local Unsafe"
			}
			t.Logf("    %s, %2d, %d, %s", cb.chain, cb.block.Number, cb.block.Time, head)
		}

		for exec, inits := range randomChain.dependencies {
			for _, init := range inits {
				t.Logf("(%s, %2d) <- (%s, %2d)", init.chain, init.block.Number, exec.chain, exec.block.Number)
			}
		}
	})
}

var chainParams = RandomChainParams{
	chainCount: 2,
	minLength:  5,
	maxLength:  14,

	sameTimestampFrequency: 60,
	dependencyChance:       20,
}

func FuzzUpdateCrossUnsafeInvariants(f *testing.F) {

	f.Add(int64(30)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T, seed int64) {
		randomChain := chainParams.MakeRandomChain(seed)

		ex, b := ExecutorBackendInit(t, randomChain)

		ChainsInit(t, b, ex, randomChain)

		t.Run("UpdateCrossUnsafeRequestEvent", func(t *testing.T) {
			// Ensure the invariants hold in the intiial state
			t.Log("Initial State")
			for i := range chainParams.chainCount {
				chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))
				CrossUnsafe_LE_LocalUnsafe(t, b, chain)
				CrossSafe_LE_LocalSafe(t, b, chain)
			}

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
			for i := range chainParams.chainCount {
				chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))
				CrossUnsafe_LE_LocalUnsafe(t, b, chain)
				CrossSafe_LE_LocalSafe(t, b, chain)
			}
		})

		err := b.Stop(context.Background())
		require.NoError(t, err)
		t.Log("stopped!")
	})

}

/*
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

func FuzzLocalDerivedEventInvariants(f *testing.F) {

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

func FuzzReplaceBlockEventInvariants(f *testing.F) {

	f.Add(uint64(5), uint64(2), uint64(4), uint64(3), uint64(1)) // Add initial values for fuzzing

	f.Fuzz(func(t *testing.T,
		chainALength uint64,
		chainBLength uint64,
		crossUnsafeHeadIndex uint64,
		localSafeHeadIndex uint64,
		crossSafeHeadIndex uint64) {
		t.Logf("Fuzzing with Chain A length: %d, Chain B length: %d", chainALength, chainBLength)
		chainA := eth.ChainIDFromUInt64(900)
		chainB := eth.ChainIDFromUInt64(901)

		ex, b, _, srcChainA, _ := ExecutorBackendInit(t, chainA, chainB)

		chainALength = chainALength%10 + 1 // ChainA can't be empty
		//chainBLength = chainBLength % 10

		crossUnsafeHead, _, crossSafeHeadIndex := ChainAInit(t, b, ex, chainA, srcChainA, chainALength, crossUnsafeHeadIndex, localSafeHeadIndex, crossSafeHeadIndex)
		srcChainA.ExpectBlockRefByNumber(uint64(chainALength), eth.L1BlockRef{}, ethereum.NotFound)
		//ChainBInit(t, b, chainB, srcChainB, chainBLength)

		t.Run("ReplaceBlockEvent Event", func(t *testing.T) {
			InitialState(t, b, ex, chainA, crossUnsafeHead, crossSafeHeadIndex)

			invalidated := types.DerivedBlockRefPair{
				Derived: eth.BlockRef{
					Hash:       common.BytesToHash([]byte{0xaa, byte(crossSafeHeadIndex) + 1}),
					Number:     crossSafeHeadIndex + 1,
					ParentHash: common.BytesToHash([]byte{0xaa, byte(crossSafeHeadIndex)}),
					Time:       uint64(time.Now().Add(time.Duration((crossSafeHeadIndex+1)*5) * time.Minute).Unix()),
				},
				Source: eth.BlockRef{},
			}
			b.chainDBs.InvalidateLocalSafe(chainA, invalidated)

			replacementBlock := eth.BlockRef{
				Hash:       common.BytesToHash([]byte{0xbb, byte(crossSafeHeadIndex) + 1}),
				Number:     crossSafeHeadIndex + 1,
				ParentHash: common.BytesToHash([]byte{0xaa, byte(crossSafeHeadIndex)}),
				Time:       uint64(time.Now().Add(time.Duration((crossSafeHeadIndex+1)*10) * time.Minute).Unix()),
			}
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.ReplaceBlockEvent{
					ChainID: chainA,
					Replacement: types.BlockReplacement{
						Replacement: replacementBlock,
						Invalidated: common.BytesToHash([]byte{0xaa, byte(crossSafeHeadIndex + 1)}),
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
							Invalidated: common.BytesToHash([]byte{0xaa, byte(crossSafeHeadIndex + 1)}),
						},
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

		t.Run("LocalDerivedOriginUpdateEvent", func(t *testing.T) {
			ex.Enqueue(event.AnnotatedEvent{
				Event: superevents.LocalDerivedOriginUpdateEvent{
					ChainID: chainA,
				},
				EmitPriority: event.High,
			})

			require.NoError(t, ex.DrainUntil(
				func(ev event.Event) bool {
					return ev == superevents.LocalDerivedOriginUpdateEvent{
						ChainID: chainA,
					}
				}, false))
			t.Log("LocalDerivedOriginUpdateEvent processed")

			CrossUnsafe_LE_LocalUnsafe(t, b, chainA)
			CrossSafe_LE_LocalSafe(t, b, chainA)
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
*/

func ExecutorBackendInit(t *testing.T, randomChain RandomChain) (ex *event.GlobalSyncExec, b *SupervisorBackend) {
	logger := testlog.Logger(t, log.LvlInfo)
	dataDir := t.TempDir()
	dependencies := make(map[eth.ChainID]*depset.StaticConfigDependency)

	for i := range chainParams.chainCount {
		chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))

		dependencies[chain] = &depset.StaticConfigDependency{
			ChainIndex:     types.ChainIndex(900 + i),
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

	for i := range chainParams.chainCount {
		chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))
		srcChain := randomChain.chainSources[chain]
		require.NoError(t, b.AttachProcessorSource(chain, srcChain))
	}

	err = b.Start(context.Background())
	require.NoError(t, err)
	t.Log("started!")

	return ex, b
}

func ChainsInit(t *testing.T, b *SupervisorBackend, ex *event.GlobalSyncExec, randomChain RandomChain) {
	for exec, inits := range randomChain.dependencies {
		for _, init := range inits {
			t.Logf("(%s, %2d) <- (%s, %2d)", init.chain, init.block.Number, exec.chain, exec.block.Number)
		}
	}
	// Initialize Databases
	for i := range chainParams.chainCount {
		chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))
		anchor := randomChain.chainBlocks[chain][0]
		b.emitter.Emit(superevents.AnchorEvent{
			ChainID: chain,
			Anchor: types.DerivedBlockRefPair{
				Derived: *anchor,
				Source:  eth.L1BlockRef{},
			}})
		t.Logf("Chain %d genesis block:%s", chain, anchor.Hash.Hex())
	}
	require.NoError(t, ex.Drain())

	for i := range chainParams.chainCount {
		chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))

		chainHeads := randomChain.chainHeads[chain]
		localUnsafe := randomChain.chainBlocks[chain][len(randomChain.chainBlocks[chain])-1].Number
		crossUnsafe := randomChain.chainBlocks[chain][chainHeads.crossUnsafe].Number
		localSafe := randomChain.chainBlocks[chain][chainHeads.localSafe].Number
		crossSafe := randomChain.chainBlocks[chain][chainHeads.crossSafe].Number

		t.Logf("Chain %d LocalUnsafe: %d CrossUnsafe: %d LocalSafe: %d CrossSafe: %d", chain, localUnsafe, crossUnsafe, localSafe, crossSafe)

		for _, block := range randomChain.chainBlocks[chain] {
			if block.Number == 0 {
				continue
			}
			t.Logf("Chain %d block %d: %s\t Timestamp:%d", chain, block.Number, block.Hash.Hex(), block.Time)
			b.emitter.Emit(superevents.ChainProcessEvent{
				ChainID: chain,
				Target:  block.Number,
			})
			if block.Number <= localSafe {
				b.emitter.Emit(superevents.LocalDerivedEvent{
					ChainID: chain,
					Derived: types.DerivedBlockRefPair{
						Derived: *block,
						Source:  eth.L1BlockRef{},
					},
					NodeID: "test-node",
				})
			}
		}
	}
	ex.DrainUntil(
		func(ev event.Event) bool {
			return StopCrossSafeRequest(ev)
		}, true)

	for i := range chainParams.chainCount {
		chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))

		chainHeads := randomChain.chainHeads[chain]
		crossUnsafe := randomChain.chainBlocks[chain][chainHeads.crossUnsafe]
		crossSafe := randomChain.chainBlocks[chain][chainHeads.crossSafe].Number

		for _, block := range randomChain.chainBlocks[chain] {
			if block.Number == 0 {
				continue
			}
			if block.Number <= crossSafe {
				ex.Enqueue(event.AnnotatedEvent{
					Event: superevents.UpdateCrossSafeRequestEvent{
						ChainID: chain,
					},
					EmitPriority: event.High,
				})
			}
		}
		ex.DrainUntil(
			func(ev event.Event) bool {
				return ev != superevents.UpdateCrossSafeRequestEvent{ChainID: chain}
			}, true)

		err := b.chainDBs.UpdateCrossUnsafe(chain, types.BlockSealFromRef(*crossUnsafe))
		require.NoError(t, err)
	}

	t.Log("Chains initialized!")
}

func CrossUnsafe_LE_LocalUnsafe(t *testing.T, b *SupervisorBackend, chain eth.ChainID) {

	localUnsafe, _ := b.LocalUnsafe(context.Background(), chain)
	//require.NoError(t, err)
	crossUnsafe, _ := b.CrossUnsafe(context.Background(), chain)
	//require.NoError(t, err)

	t.Logf("Chain %d: Cross Unsafe head %d <= Local Unsafe head %d", chain, crossUnsafe.Number, localUnsafe.Number)

	require.LessOrEqual(t, crossUnsafe.Number, localUnsafe.Number, "Cross Unsafe head: %d is not less or equal than Local Unsafe head: %d", crossUnsafe.Number, localUnsafe.Number)
}

func CrossSafe_LE_LocalSafe(t *testing.T, b *SupervisorBackend, chain eth.ChainID) {

	localSafe, _ := b.LocalSafe(context.Background(), chain)
	//require.NoError(t, err)
	crossSafe, _ := b.CrossSafe(context.Background(), chain)
	//require.NoError(t, err)

	t.Logf("Chain %d: Cross Safe head %d <= Local Safe head %d", chain, crossSafe.Derived.Number, localSafe.Derived.Number)

	require.LessOrEqual(t, crossSafe.Derived.Number, localSafe.Derived.Number, "Cross Safe head: %d is not less or equal than Local Safe head: %d", crossSafe.Derived.Number, localSafe.Derived.Number)
}

func StopCrossSafeRequest(ev event.Event) bool {
	for i := range chainParams.chainCount {
		chain := eth.ChainIDFromUInt64(testChainIDOffset + uint64(i))
		if (ev == superevents.UpdateCrossUnsafeRequestEvent{ChainID: chain}) {
			return true
		}
		if (ev == superevents.UpdateCrossSafeRequestEvent{ChainID: chain}) {
			return true
		}
	}
	return false
}
