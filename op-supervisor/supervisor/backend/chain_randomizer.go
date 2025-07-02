package backend

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/core/types"
	params2 "github.com/ethereum/go-ethereum/params"
	"github.com/stretchr/testify/mock"

	"github.com/ethereum-optimism/optimism/op-node/params"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/testutils"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/processors"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/types"
)

const testChainIDOffset = 900

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
		Address: params2.InteropCrossL2InboxAddress,
		Data:    data,
		Topics:  topics,
		Index:   uint(log_index),
	}
}

type ChainBlock struct {
	chain eth.ChainID
	block *eth.BlockRef
}

type ChainHeads struct {
	// These are block numbers on the chain
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

type L1Assignments struct {
	L1Block  eth.BlockRef
	L2Blocks []*ChainBlock
}

type RandomChain struct {
	cutoffs struct {
		crossUnsafe int
		crossSafe   int
		localUnsafe int
		localSafe   int
	}
	chainIDs      []eth.ChainID
	allBlocks     []*ChainBlock
	generatedLogs map[ChainBlock][]*types2.Log
	dependencies  map[ChainBlock][]*ChainBlock
	chainSources  map[eth.ChainID]*MockProcessorSource
	chainBlocks   map[eth.ChainID][]*eth.BlockRef
	chainHeads    map[eth.ChainID]*ChainHeads
	l1Blocks      []L1Assignments
}

func (rc *RandomChain) ChainInfo(chainid eth.ChainID) (blocks []*eth.BlockRef, heads ChainHeads) {
	blocks = rc.chainBlocks[chainid]
	heads = *rc.chainHeads[chainid]
	return blocks, heads
}

func (p *RandomChainParams) MakeRandomChain(seed int64) (res RandomChain) {
	r := rand.New(rand.NewSource(seed))
	totalLength := r.Intn(p.maxLength-p.minLength) + p.minLength

	localUnsafe := totalLength - 1
	localSafe := r.Intn(totalLength-1) + 1
	crossSafe := r.Intn(localSafe)
	crossUnsafe := r.Intn(localUnsafe-crossSafe) + crossSafe
	res = RandomChain{
		cutoffs: struct {
			crossUnsafe int
			crossSafe   int
			localUnsafe int
			localSafe   int
		}{
			crossUnsafe: crossUnsafe,
			crossSafe:   crossSafe,
			localUnsafe: localUnsafe,
			localSafe:   localSafe,
		},
		chainIDs:      make([]eth.ChainID, 0, p.chainCount),
		allBlocks:     make([]*ChainBlock, 0, totalLength),
		generatedLogs: make(map[ChainBlock][]*types2.Log),
		dependencies:  make(map[ChainBlock][]*ChainBlock),
		chainSources:  make(map[eth.ChainID]*MockProcessorSource),
		chainBlocks:   make(map[eth.ChainID][]*eth.BlockRef),
		chainHeads:    make(map[eth.ChainID]*ChainHeads),
		l1Blocks:      make([]L1Assignments, 0),
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

		// Assign the cross/local heads based on where the cutoffs are
		if i <= res.cutoffs.localSafe {
			res.chainHeads[chainid].localSafe = block.Number
		}
		if i <= res.cutoffs.localUnsafe {
			res.chainHeads[chainid].localUnsafe = block.Number
		}
		if i <= res.cutoffs.crossSafe {
			res.chainHeads[chainid].crossSafe = block.Number
		}
		if i <= res.cutoffs.crossUnsafe {
			res.chainHeads[chainid].crossUnsafe = block.Number
		}

		res.chainSources[chainid].ExpectBlockRefByNumber(block.Number, *block, nil)
		res.chainBlocks[chainid] = append(res.chainBlocks[chainid], block)
		prevBlock = block
	}

	//
	// Create random dependencies between all blocks
	//
	for initIndex, initcb := range res.allBlocks {
		block := initcb.block
		if block.Number == 0 {
			continue
		}
		for r.Intn(100) < p.dependencyChance {
			execIndex := r.Intn(totalLength-initIndex) + initIndex
			execcb := res.allBlocks[execIndex]
			initiatingLog := addRandomInitiatingMessage(r, &res, initcb)
			addExecutingMessage(&res, execcb, initcb, initiatingLog)
		}
	}

	//
	// Make L1 derivation info
	//
	taken := 0
	nextL1 := testutils.RandomBlockRef(r)
	for taken < totalLength {
		nextL1 = testutils.NextRandomRef(r, nextL1)
		take := r.Intn(4) + 1 // Take 1-4 L2 blocks
		take = min(totalLength-taken, take)
		l1Derivation := L1Assignments{
			L1Block:  nextL1,
			L2Blocks: res.allBlocks[taken : taken+take],
		}
		res.l1Blocks = append(res.l1Blocks, l1Derivation)
		taken += take
	}

	return res
}

func addRandomInitiatingMessage(r *rand.Rand, res *RandomChain, initcb *ChainBlock) *types2.Log {
	initiatingLog := testutils.RandomLog(r)
	initiatingLog.Index = uint(len(res.generatedLogs[*initcb]))
	res.generatedLogs[*initcb] = append(res.generatedLogs[*initcb], initiatingLog)
	return initiatingLog
}

func addExecutingMessage(res *RandomChain, execcb *ChainBlock, initcb *ChainBlock, initiatingLog *types2.Log) {
	execLog := ExecMsgForLog(initcb.chain, *initcb.block, uint32(initiatingLog.Index), initiatingLog)
	execLog.Index = uint(len(res.generatedLogs[*execcb]))
	res.generatedLogs[*execcb] = append(res.generatedLogs[*execcb], execLog)
	res.dependencies[*execcb] = append(res.dependencies[*execcb], initcb)
}

func GenerateReceiptsFromLogs(res *RandomChain) {
	for _, cb := range res.allBlocks {
		chain, block := cb.chain, cb.block
		logs := res.generatedLogs[*cb]
		rcpt := types2.Receipt{
			Logs: logs,
		}
		source := res.chainSources[chain]
		source.ExpectFetchReceipts(block.Hash, types2.Receipts{&rcpt}, nil)
	}
}

// Returns a random integer in the interval [lowerIncluding, upperExcluding)
func randomInRange(r *rand.Rand, lowerIncluding int, upperExcluding int) int {
	return r.Intn(upperExcluding-lowerIncluding) + lowerIncluding
}

/*
func InsertMessageWithInvalidIdentifier(r *rand.Rand, res *RandomChain, candidateIndex int) {
	candidateBlock := res.allBlocks[candidateIndex]
	randomIndex := r.Intn(candidateIndex + 1)
	randomBlock := res.allBlocks[randomIndex]
	randomLogIndex := r.Intn(len(res.generatedLogs[*randomBlock]))
	randomLog := res.generatedLogs[*randomBlock][randomLogIndex]

	switch r.Intn(5) {
	case 0:
		// Invalid origin
	case 1:
		// Invalid block number
	case 2:
		// Invalid log index
	case 3:
		// Invalid timestamp
	case 4:
		// Invalid chain ID
	}
}
*/

func InvalidateBlock(t *testing.T, r *rand.Rand, res *RandomChain, candidate *ChainBlock) {
	switch r.Intn(4) {
	case 0:
		InsertFutureDependency(r, res, FindRandomChainIndex(res, candidate))
	case 1:
		InsertDependencyToExpiredMessage(t, r, res, FindRandomChainIndex(res, candidate))
	case 2:
		InsertSelfDependency(r, res, candidate)
	case 3:
		InsertCycle(t, r, res, candidate)
	default:
	}
}

func FindRandomChainIndex(res *RandomChain, candidate *ChainBlock) (index int) {
	for i, cb := range res.allBlocks {
		if cb.chain == candidate.chain && cb.block == candidate.block {
			return i
		}
	}
	return -1 // Return -1 if not found
}

func InsertFutureDependency(r *rand.Rand, res *RandomChain, candidateIndex int) {
	candidateBlock := res.allBlocks[candidateIndex]
	futureIndex := randomInRange(r, candidateIndex, len(res.allBlocks))
	futureBlock := res.allBlocks[futureIndex]
	initiatingLog := addRandomInitiatingMessage(r, res, futureBlock)
	addExecutingMessage(res, candidateBlock, futureBlock, initiatingLog)
}

func InsertDependencyToExpiredMessage(t *testing.T, r *rand.Rand, res *RandomChain, candidateIndex int) {
	candidate := res.allBlocks[candidateIndex]

	// Any timestamp below this is expired
	// TODO: Ensure there is always an expired block to avoid overflow
	expiryTimestamp := candidate.block.Time - params.MessageExpiryTimeSecondsInterop
	require.Less(t, candidate.block.Time, math.MaxInt)

	// Iterate until we find the first unexpired block
	i := 0
	for res.allBlocks[i].block.Time < expiryTimestamp {
		i++
	}

	// TODO: Ensure there is always an expired block
	expiredIndex := r.Intn(i)
	expiredBlock := res.allBlocks[expiredIndex]
	initiatingLog := addRandomInitiatingMessage(r, res, expiredBlock)
	addExecutingMessage(res, candidate, expiredBlock, initiatingLog)
}

func InsertSelfDependency(r *rand.Rand, res *RandomChain, candidate *ChainBlock) {
	// Create a random initiating message to be inserted at index N+1
	initiatingLog := testutils.RandomLog(r)
	initiatingLog.Index = uint(len(res.generatedLogs[*candidate]) + 1)

	// Insert executing message at index N
	addExecutingMessage(res, candidate, candidate, initiatingLog)

	// Insert initiating message at index N+1
	res.generatedLogs[*candidate] = append(res.generatedLogs[*candidate], initiatingLog)
}

func listHazards(t *testing.T, res *RandomChain, candidate *ChainBlock) []*ChainBlock {
	hazards := make([]*ChainBlock, 0)
	includedHazards := make(map[eth.ChainID]*ChainBlock)

	// Add the candidate itself as a hazard
	stack := []*ChainBlock{candidate}

	for len(stack) > 0 {
		// Pop hazard from the stack
		hazard := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Check if we already found a hazard from this chain
		includedHazard, ok := includedHazards[hazard.chain]
		if ok {
			// Ensure that there are not two different hazards from the same chain
			require.Equal(t, includedHazard.block.ID(), hazard.block.ID())
		} else {
			// If not already included, add hazard to the list
			hazards = append(hazards, hazard)
			includedHazards[hazard.chain] = hazard

			// For each new hazard, add all dependencies with the same timestamp to the stack
			for _, dependency := range res.dependencies[*hazard] {
				if dependency.block.Time == candidate.block.Time {
					stack = append(stack, dependency)
				}
			}
		}
	}

	return hazards
}

func InsertCycle(t *testing.T, r *rand.Rand, res *RandomChain, candidate *ChainBlock) {
	t.Logf("Inserting a cycle in candidate (%s, %2d)'s hazard set", candidate.chain, candidate.block.Number)

	candidateHazards := listHazards(t, res, candidate)
	cycleStart := candidateHazards[r.Intn(len(candidateHazards))]
	t.Logf("Picked random hazard set element to start the cycle: (%s, %2d)", cycleStart.chain, cycleStart.block.Number)

	// If the random element is equal to the candidate, no need to compute the hazards again
	var subHazards []*ChainBlock
	if cycleStart.chain == candidate.chain {
		require.Equal(t, cycleStart.block.Number, candidate.block.Number)
		subHazards = candidateHazards
	} else {
		subHazards = listHazards(t, res, cycleStart)
	}

	cycleEnd := subHazards[r.Intn(len(subHazards))]
	t.Logf("Picked random hazard set element to end the cycle: (%s, %2d)", cycleEnd.chain, cycleEnd.block.Number)

	// Add executing message from cycleEnd to the first log of cycleStart
	initiatingLog := res.generatedLogs[*cycleStart][0]
	addExecutingMessage(res, cycleEnd, cycleStart, initiatingLog)
	t.Logf("Added cyclic dependency: (%s, %2d) -> (%s, %2d)", cycleEnd.chain, cycleEnd.block.Number, cycleStart.chain, cycleStart.block.Number)
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
	m.Mock.On("FetchReceipts", hash).Return(receipts, err)
}

func (m *MockProcessorSource) BlockRefByNumber(ctx context.Context, num uint64) (eth.BlockRef, error) {
	out := m.Mock.Called(num)
	return out.Get(0).(eth.BlockRef), out.Error(1)
}

func (m *MockProcessorSource) ExpectBlockRefByNumber(num uint64, ref eth.BlockRef, err error) {
	m.Mock.On("BlockRefByNumber", num).Return(ref, err)
}
