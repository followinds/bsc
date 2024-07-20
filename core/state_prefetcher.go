// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"time"
)

const prefetchThread = 3
const checkInterval = 10

// statePrefetcher is a basic Prefetcher, which blindly executes a block on top
// of an arbitrary state with the goal of prefetching potentially useful state
// data from disk before the main block processor start executing.
type statePrefetcher struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
	badTxs map[common.Hash]int64
}

// NewStatePrefetcher initialises a new statePrefetcher.
func NewStatePrefetcher(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *statePrefetcher {
	return &statePrefetcher{
		config: config,
		bc:     bc,
		engine: engine,
		badTxs: make(map[common.Hash]int64),
	}
}

// Prefetch processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to pre-cache transaction signatures and state trie nodes.
func (p *statePrefetcher) Prefetch(block *types.Block, statedb *state.StateDB, cfg *vm.Config, interruptCh <-chan struct{}) {
	var (
		header = block.Header()
		signer = types.MakeSigner(p.config, header.Number, header.Time)
	)
	transactions := block.Transactions()
	txChan := make(chan int, prefetchThread)
	// No need to execute the first batch, since the main processor will do it.
	for i := 0; i < prefetchThread; i++ {
		go func() {
			newStatedb := statedb.CopyDoPrefetch()
			if !p.config.IsHertzfix(header.Number) {
				newStatedb.EnableWriteOnSharedStorage()
			}
			gaspool := new(GasPool).AddGas(block.GasLimit())
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, *cfg)
			// Iterate over and process the individual transactions
			for {
				select {
				case txIndex := <-txChan:
					tx := transactions[txIndex]
					// Convert the transaction into an executable message and pre-cache its sender
					msg, err := TransactionToMessage(tx, signer, header.BaseFee)
					msg.SkipAccountChecks = true
					if err != nil {
						return // Also invalid block, bail out
					}
					newStatedb.SetTxContext(tx.Hash(), txIndex)
					precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)

				case <-interruptCh:
					// If block precaching was interrupted, abort
					return
				}
			}
		}()
	}

	// it should be in a separate goroutine, to avoid blocking the critical path.
	for i := 0; i < len(transactions); i++ {
		select {
		case txChan <- i:
		case <-interruptCh:
			return
		}
	}
}

// PrefetchMining processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to pre-cache transaction signatures and snapshot clean state. Only used for mining stage
func (p *statePrefetcher) PrefetchMining(txs TransactionsByPriceAndNonce, header *types.Header, gasLimit uint64, statedb *state.StateDB, cfg vm.Config, interruptCh <-chan struct{}, txCurr **types.Transaction) {
	var signer = types.MakeSigner(p.config, header.Number, header.Time)

	txCh := make(chan *types.Transaction, 2*prefetchThread)
	for i := 0; i < prefetchThread; i++ {
		go func(startCh <-chan *types.Transaction, stopCh <-chan struct{}) {
			idx := 0
			newStatedb := statedb.CopyDoPrefetch()
			if !p.config.IsHertzfix(header.Number) {
				newStatedb.EnableWriteOnSharedStorage()
			}
			gaspool := new(GasPool).AddGas(gasLimit)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
			// Iterate over and process the individual transactions
			for {
				select {
				case tx := <-startCh:
					// Convert the transaction into an executable message and pre-cache its sender
					msg, err := TransactionToMessage(tx, signer, header.BaseFee)
					msg.SkipAccountChecks = true
					if err != nil {
						return // Also invalid block, bail out
					}
					idx++
					newStatedb.SetTxContext(tx.Hash(), idx)
					precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)
					gaspool = new(GasPool).AddGas(gasLimit)
				case <-stopCh:
					return
				}
			}
		}(txCh, interruptCh)
	}
	go func(txset TransactionsByPriceAndNonce) {
		count := 0
		for {
			select {
			case <-interruptCh:
				return
			default:
				if count++; count%checkInterval == 0 {
					txset.Forward(*txCurr)
				}
				tx := txset.PeekWithUnwrap()
				if tx == nil {
					return
				}

				select {
				case <-interruptCh:
					return
				case txCh <- tx:
				}

				txset.Shift()
			}
		}
	}(txs)
}

func (p *statePrefetcher) PrefetchMiningPendingOrders(txs TransactionsByPriceAndNonce, header *types.Header, gasLimit uint64, statedb *state.StateDB, cfg vm.Config, interruptCh <-chan struct{}, txCurr **types.Transaction, pairs []common.Address) []uint64 {
	var signer = types.MakeSigner(p.config, header.Number, header.Time)
	count := 0
	idx := 0
	newStatedb := statedb.CopyDoPrefetch()
	if !p.config.IsHertzfix(header.Number) {
		newStatedb.EnableWriteOnSharedStorage()
	}
	gaspool := new(GasPool).AddGas(gasLimit)
	blockContext := NewEVMBlockContext(header, p.bc, nil)
	evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
	mp := make(map[common.Address]bool)
	for _, v := range pairs {
		mp[v] = true
	}
	var orderList []types.PendingOrder
	maxGasUsed := uint64(1500000)
	if len(p.badTxs) > 1000 {
		for k, v := range p.badTxs {
			if time.Now().Unix()-v > 300 {
				delete(p.badTxs, k)
			}
		}
	}
	var simulateTotal int
	var shift int
	//t1 := time.Now().UnixMilli()
	for {
		if count++; count%checkInterval == 0 {
			txs.Forward(*txCurr)
		}
		tx := txs.PeekWithUnwrap()
		if tx == nil {
			break
		}
		msg, err := TransactionToMessage(tx, signer, header.BaseFee)
		from, err := types.Sender(signer, tx)
		if err == nil && from == common.HexToAddress("0xc78af1b184ee130b5cd5e46ec873862a90bdd156") {
			log.Info("simulate_localTx", "txhash", tx.Hash(), "blockNumber", header.Number, "block_ts", header.Time, "time", time.Now().UnixMilli())
		}
		msg.SkipAccountChecks = true
		if err != nil {
			break
		}
		idx++
		newStatedb.SetTxContext(tx.Hash(), idx)
		codeSize := 0
		if tx.To() != nil {
			codeSize = statedb.GetCodeSize(*tx.To())
		}

		if p.badTxs[tx.Hash()] != 0 || tx.To() == nil || time.Since(tx.Time()).Seconds() > 100 || codeSize == 0 {
			//log.Info("PrefetchMiningPendingOrders:", "tx_hash", tx.Hash().String(), "gap", time.Since(tx.Time()).Seconds())
			shift++
			txs.Shift()
			continue
		}

		res, orders := precacheTransactionPendingOrders(msg, p.config, gaspool, newStatedb, header, evm, tx, mp)
		simulateTotal++
		if res != nil && res.UsedGas > maxGasUsed {
			p.badTxs[tx.Hash()] = time.Now().Unix()
		}
		if len(orders) > 0 {
			orderList = append(orderList, orders...)
		}
		gaspool = new(GasPool).AddGas(gasLimit)
		shift++
		txs.Shift()
	}
	//log.Info("PrefetchMiningPendingOrders_total:", "simulateTotal:", simulateTotal, "timeuse:", time.Now().UnixMilli()-t1, "total:", shift, "block", header.Number)
	//把各个pair最高的GasPrice找出来
	highestGasPrices := make(map[common.Address]uint64)
	for _, order := range orderList {
		if gasPrice, exists := highestGasPrices[order.Pair]; !exists || order.GasPrice > gasPrice {
			highestGasPrices[order.Pair] = order.GasPrice
		}
	}
	result := make([]uint64, len(pairs))
	for i, pair := range pairs {
		if gasPrice, exists := highestGasPrices[pair]; exists {
			result[i] = gasPrice
		} else {
			result[i] = 0 // 如果没有对应的订单，设置为 0 或其他默认值
		}
	}
	return result
}

func precacheTransactionPendingOrders(msg *Message, config *params.ChainConfig, gaspool *GasPool, statedb *state.StateDB, header *types.Header, evm *vm.EVM, tx *types.Transaction, pairs map[common.Address]bool) (*ExecutionResult, []types.PendingOrder) {
	// Update the evm with the new transaction context.
	evm.Reset(NewEVMTxContext(msg), statedb)
	// Add addresses to access list if applicable
	res, err := ApplyMessage(evm, msg, gaspool)
	if err == nil {
		statedb.Finalise(true)
	}

	hashMap := map[common.Hash]bool{
		common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"): true, // DefiSwap::UNIV2 | DefiSwap::SUSHIV2 | DefiSwap::HOPEV2 | DefiSwap::PANCAKEV2
		common.HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"): true, // DefiSwap::UNIV3 | DefiSwap::SUSHIV3
		common.HexToHash("0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"): true, // DefiSwap::PANCAKEV3
	}
	orderList := make([]types.PendingOrder, 0)
	logs := statedb.GetLogs(tx.Hash(), 1, tx.Hash())
	for _, l := range logs {
		if pairs[l.Address] && hashMap[l.Topics[0]] {
			orderList = append(orderList,
				types.PendingOrder{
					Pair:     l.Address,
					GasPrice: tx.GasTipCap().Uint64(),
					Buy:      true,
				})
			//log.Info("PrefetchMiningPendingOrders_total tx", "txhash", tx.Hash(), "gasprice", tx.GasPrice(), "gasFeeCap", tx.GasFeeCap(), "gasTipCap", tx.GasTipCap())
		}
		//if l.Address==common.HexToAddress("0x2c533e2c2b4fd1172b5a0a0178805be1526a15a7")&& hashMap[l.Topics[0]]{
		//	log.Info("lista tx", "txhash", tx.Hash(), "gasprice", tx.GasPrice(), "gasTipCap", tx.GasTipCap(),"time",time.Now().UnixMilli())
		//}
	}
	return res, orderList
}

// precacheTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. The goal is not to execute
// the transaction successfully, rather to warm up touched data slots.
func precacheTransaction(msg *Message, config *params.ChainConfig, gaspool *GasPool, statedb *state.StateDB, header *types.Header, evm *vm.EVM) error {
	// Update the evm with the new transaction context.
	evm.Reset(NewEVMTxContext(msg), statedb)
	// Add addresses to access list if applicable
	_, err := ApplyMessage(evm, msg, gaspool)
	if err == nil {
		statedb.Finalise(true)
	}
	return err
}
