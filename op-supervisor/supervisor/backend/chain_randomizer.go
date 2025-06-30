package backend

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/stretchr/testify/mock"

	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/testutils"
	"github.com/ethereum-optimism/optimism/op-supervisor/supervisor/backend/cross"
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
		Address: params.InteropCrossL2InboxAddress,
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

type RandomChain struct {
	cutoff       int
	chainIDs     []eth.ChainID
	allBlocks    []*ChainBlock
	dependencies map[ChainBlock][]*ChainBlock
	chainSources map[eth.ChainID]*MockProcessorSource
	chainBlocks  map[eth.ChainID][]*eth.BlockRef
	chainHeads   map[eth.ChainID]*ChainHeads
}

func (rc *RandomChain) ChainInfo(chainid eth.ChainID) (blocks []*eth.BlockRef, heads ChainHeads) {
	blocks = rc.chainBlocks[chainid]
	heads = *rc.chainHeads[chainid]
	return blocks, heads
}

func (p *RandomChainParams) MakeRandomChain(seed int64) (res RandomChain) {
	r := rand.New(rand.NewSource(seed))
	totalLength := r.Intn(p.maxLength-p.minLength) + p.minLength

	res = RandomChain{
		cutoff:       r.Intn(totalLength),
		chainIDs:     make([]eth.ChainID, 0, p.chainCount),
		allBlocks:    make([]*ChainBlock, 0, totalLength),
		dependencies: make(map[ChainBlock][]*ChainBlock),
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
	chainCutoffs := make(map[eth.ChainID]uint64)
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
			chainCutoffs[chainid] = block.Number
		}

		res.chainSources[chainid].ExpectBlockRefByNumber(block.Number, *block, nil)
		res.chainBlocks[chainid] = append(res.chainBlocks[chainid], block)
		prevBlock = block
	}

	// Determine the local safe/unsafe heads for each chain
	for chain, blocks := range res.chainBlocks {
		cutoff := int(chainCutoffs[chain])
		heads := res.chainHeads[chain]
		chainLength := len(blocks)
		lastBlockNumber := int(blocks[chainLength-1].Number)
		heads.localSafe = uint64(cutoff + r.Intn(lastBlockNumber-cutoff+1))
		heads.localUnsafe = uint64(len(res.chainBlocks[chain]) - 1) //uint64(cutoff + r.Intn(lastBlockNumber-cutoff+1))

		heads.crossSafe = uint64(r.Intn(int(cutoff + 1)))
		heads.crossUnsafe = uint64(r.Intn(int(cutoff + 1)))
	}

	//
	// Create random dependencies between all blocks
	//
	generatedLogs := make([][]*types2.Log, totalLength)
	for initIndex, initcb := range res.allBlocks {
		block := initcb.block
		if block.Number == 0 {
			continue
		}
		for r.Intn(100) < p.dependencyChance {
			execIndex := r.Intn(totalLength-initIndex) + initIndex
			execcb := res.allBlocks[execIndex]
			execChain, _ := execcb.chain, execcb.block
			if execChain == initcb.chain {
				continue
			}
			initiatingLog := testutils.RandomLog(r)
			initiatingLog.Index = uint(len(generatedLogs[initIndex]))
			generatedLogs[initIndex] = append(generatedLogs[initIndex], initiatingLog)
			execLog := ExecMsgForLog(initcb.chain, *block, uint32(len(generatedLogs[execIndex])), initiatingLog)
			execLog.Index = uint(len(generatedLogs[execIndex]))
			generatedLogs[execIndex] = append(generatedLogs[execIndex], execLog)
			res.dependencies[*execcb] = append(res.dependencies[*execcb], initcb)
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

		for i, cb := range randomChain.allBlocks {
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
			if i == randomChain.cutoff {
				t.Log("    --- Cutoff point ---")
			}
		}

		for exec, inits := range randomChain.dependencies {
			for _, init := range inits {
				t.Logf("(%s, %2d) <- (%s, %2d)", init.chain, init.block.Number, exec.chain, exec.block.Number)
			}
		}
	})
}

func listHazards(t *testing.T, res *RandomChain, deps cross.HazardDeps, logger log.Logger, candidate *ChainBlock) []*ChainBlock {
	hazards := make([]*ChainBlock, 0)

	// Compute hazard set for the candidate
	hazardSet, err := cross.NewHazardSet(deps, logger, candidate.chain, types.BlockSealFromRef(*candidate.block))
	require.NoError(t, err)

	// Add the candidate itself to the list
	hazards = append(hazards, candidate)

	// Add every block in the hazard set to the list
	for chainIndex, hazard := range hazardSet.Entries() {
		chainID := eth.ChainIDFromUInt64(uint64(chainIndex))
		block := res.chainBlocks[chainID][hazard.Number]
		require.Equal(t, block.Number, hazard.Number)
		require.Equal(t, block.Hash, hazard.Hash)
		chainBlock := &ChainBlock{chainID, block}
		hazards = append(hazards, chainBlock)
	}

	return hazards
}

func InsertCycle(t *testing.T, r *rand.Rand, res *RandomChain, deps cross.HazardDeps, logger log.Logger, candidate *ChainBlock) {
	t.Logf("Inserting a cycle in candidate (%s, %2d)'s hazard set", candidate.chain, candidate.block.Number)

	candidateHazards := listHazards(t, res, deps, logger, candidate)
	cycleStart := candidateHazards[r.Intn(len(candidateHazards))]
	t.Logf("Picked random hazard set element to start the cycle: (%s, %2d)", cycleStart.chain, cycleStart.block.Number)

	// If the random element is equal to the candidate, no need to compute the hazards again
	var subHazards []*ChainBlock
	if cycleStart.chain == candidate.chain {
		require.Equal(t, cycleStart.block.Number, candidate.block.Number)
		subHazards = candidateHazards
	} else {
		subHazards = listHazards(t, res, deps, logger, cycleStart)
	}

	cycleEnd := subHazards[r.Intn(len(subHazards))]
	t.Logf("Picked random hazard set element to end the cycle: (%s, %2d)", cycleEnd.chain, cycleEnd.block.Number)

	res.dependencies[*cycleEnd] = append(res.dependencies[*cycleEnd], cycleStart)
	// TODO: Create executing message
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
