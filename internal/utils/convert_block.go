package utils

import (
	"fmt"

	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
)

func GenerateBlock(block *bookkeeping.Block) (*BlockResponse, error) {
	var ret BlockResponse
	blockHeader := block.BlockHeader

	rewards := BlockRewards{
		FeeSink:                 blockHeader.FeeSink.String(),
		RewardsCalculationRound: uint64(blockHeader.RewardsRecalculationRound),
		RewardsLevel:            blockHeader.RewardsLevel,
		RewardsPool:             blockHeader.RewardsPool.String(),
		RewardsRate:             blockHeader.RewardsRate,
		RewardsResidue:          blockHeader.RewardsResidue,
	}

	upgradeState := BlockUpgradeState{
		CurrentProtocol:        string(blockHeader.CurrentProtocol),
		NextProtocol:           strPtr(string(blockHeader.NextProtocol)),
		NextProtocolApprovals:  uint64Ptr(blockHeader.NextProtocolApprovals),
		NextProtocolSwitchOn:   uint64Ptr(uint64(blockHeader.NextProtocolSwitchOn)),
		NextProtocolVoteBefore: uint64Ptr(uint64(blockHeader.NextProtocolVoteBefore)),
	}

	upgradeVote := BlockUpgradeVote{
		UpgradeApprove: boolPtr(blockHeader.UpgradeApprove),
		UpgradeDelay:   uint64Ptr(uint64(blockHeader.UpgradeDelay)),
		UpgradePropose: strPtr(string(blockHeader.UpgradePropose)),
	}

	ret = BlockResponse{
		GenesisHash:       blockHeader.GenesisHash[:],
		GenesisId:         blockHeader.GenesisID,
		PreviousBlockHash: blockHeader.Branch[:],
		Rewards:           &rewards,
		Round:             uint64(blockHeader.Round),
		Seed:              blockHeader.Seed[:],
		Timestamp:         uint64(blockHeader.TimeStamp),
		Transactions:      nil,
		TransactionsRoot:  blockHeader.TxnCommitments.NativeSha512_256Commitment[:],
		TxnCounter:        uint64Ptr(blockHeader.TxnCounter),
		UpgradeState:      &upgradeState,
		UpgradeVote:       &upgradeVote,
	}

	txn, err := genTransactions(block, block.Payset)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Block %d, txns %d\n", block.BlockHeader.Round, len(txn))

	ret.Transactions = &txn
	return &ret, nil
}

func genTransactions(block *bookkeeping.Block, modifiedTxns []transactions.SignedTxnInBlock) ([]Transaction, error) {
	intra := uint(0)
	results := make([]Transaction, 0)
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
			return nil, fmt.Errorf("decode signed txn err: %w", err)
		}

		// txn := &stxnad.Txn
		// typeenum, ok := idb.GetTypeEnum(txn.Type)
		// if !ok {
		// 	return nil, fmt.Errorf("get type enum")
		// }
		assetid, err := transactionAssetID(&stxnad, intra, block)
		if err != nil {
			return nil, err
		}
		// id := txn.ID().String()

		extra := rowData{
			Round:            uint64(block.BlockHeader.Round),
			RoundTime:        block.TimeStamp,
			Intra:            intra,
			AssetID:          assetid,
			AssetCloseAmount: block.Payset[idx].ApplyData.AssetClosingAmount,
		}

		sig := TransactionSignature{
			Logicsig: lsigToTransactionLsig(stxnad.Lsig),
			Multisig: msigToTransactionMsig(stxnad.Msig),
			Sig:      sigToTransactionSig(stxnad.Sig),
		}

		tx, nextintra, err := signedTxnWithAdToTransaction(&stxnad, intra, extra)
		intra = nextintra
		if err != nil {
			return nil, err
		}

		txid := stxnad.Txn.ID().String()
		tx.Id = &txid
		tx.Signature = &sig

		results = append(results, tx)

	}
	return results, nil
}
