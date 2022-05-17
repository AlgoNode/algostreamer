package utils

import (
	"fmt"

	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/indexer/api/generated/v2"
)

func GenerateBlock(block *bookkeeping.Block) (*generated.Block, error) {
	var ret generated.Block
	blockHeader := block.BlockHeader

	rewards := generated.BlockRewards{
		FeeSink:                 blockHeader.FeeSink.String(),
		RewardsCalculationRound: uint64(blockHeader.RewardsRecalculationRound),
		RewardsLevel:            blockHeader.RewardsLevel,
		RewardsPool:             blockHeader.RewardsPool.String(),
		RewardsRate:             blockHeader.RewardsRate,
		RewardsResidue:          blockHeader.RewardsResidue,
	}

	upgradeState := generated.BlockUpgradeState{
		CurrentProtocol:        string(blockHeader.CurrentProtocol),
		NextProtocol:           strPtr(string(blockHeader.NextProtocol)),
		NextProtocolApprovals:  uint64Ptr(blockHeader.NextProtocolApprovals),
		NextProtocolSwitchOn:   uint64Ptr(uint64(blockHeader.NextProtocolSwitchOn)),
		NextProtocolVoteBefore: uint64Ptr(uint64(blockHeader.NextProtocolVoteBefore)),
	}

	upgradeVote := generated.BlockUpgradeVote{
		UpgradeApprove: boolPtr(blockHeader.UpgradeApprove),
		UpgradeDelay:   uint64Ptr(uint64(blockHeader.UpgradeDelay)),
		UpgradePropose: strPtr(string(blockHeader.UpgradePropose)),
	}

	ret = generated.Block{
		GenesisHash:       blockHeader.GenesisHash[:],
		GenesisId:         blockHeader.GenesisID,
		PreviousBlockHash: blockHeader.Branch[:],
		Rewards:           &rewards,
		Round:             uint64(blockHeader.Round),
		Seed:              blockHeader.Seed[:],
		Timestamp:         uint64(blockHeader.TimeStamp),
		Transactions:      nil,
		TransactionsRoot:  blockHeader.TxnRoot[:],
		TxnCounter:        uint64Ptr(blockHeader.TxnCounter),
		UpgradeState:      &upgradeState,
		UpgradeVote:       &upgradeVote,
	}

	results := make([]generated.Transaction, 0)
	if err := genTransactions(block, block.Payset, results); err != nil {
		return nil, err
	}

	ret.Transactions = &results
	return &ret, nil
}

func genTransactions(block *bookkeeping.Block, modifiedTxns []transactions.SignedTxnInBlock, results []generated.Transaction) error {
	intra := uint(0)
	for idx, stib := range modifiedTxns {
		// Do not include inner transactions.
		// if txrow.RootTxn != nil {
		// 	continue
		// }

		var stxnad transactions.SignedTxnWithAD
		var err error
		// This function makes sure to set correct genesis information so we can get the
		// correct transaction hash.
		stxnad.SignedTxn, stxnad.ApplyData, err = block.BlockHeader.DecodeSignedTxn(stib)
		if err != nil {
			return fmt.Errorf("decode signed txn err: %w", err)
		}

		// txn := &stxnad.Txn
		// typeenum, ok := idb.GetTypeEnum(txn.Type)
		// if !ok {
		// 	return nil, fmt.Errorf("get type enum")
		// }
		assetid, err := transactionAssetID(&stxnad, intra, block)
		if err != nil {
			return err
		}
		// id := txn.ID().String()

		extra := rowData{
			Round:            uint64(block.BlockHeader.Round),
			RoundTime:        block.TimeStamp,
			Intra:            intra,
			AssetID:          assetid,
			AssetCloseAmount: block.Payset[idx].ApplyData.AssetClosingAmount,
		}

		tx, err := signedTxnWithAdToTransaction(&stxnad, extra)
		if err != nil {
			return err
		}

		results = append(results, tx)

		//TODO inners
	}
	return nil
}
