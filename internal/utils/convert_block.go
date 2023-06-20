package utils

import (
	"fmt"
	"sort"

	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/protocol"
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

	// var partUpdates *bookkeeping.ParticipationUpdates
	// if len(blockHeader.ExpiredParticipationAccounts) > 0 {
	// 	addrs := make([]string, len(blockHeader.ExpiredParticipationAccounts))
	// 	for i := 0; i < len(addrs); i++ {
	// 		addrs[i] = blockHeader.ExpiredParticipationAccounts[i].String()
	// 	}
	// 	partUpdates = &bookkeeping.ParticipationUpdates{
	// 		ExpiredParticipationAccounts: strArrayPtr(addrs),
	// 	}
	// } else {
	// 	partUpdates = nil
	// }

	orderedTrackingTypes := make([]protocol.StateProofType, len(blockHeader.StateProofTracking))
	trackingArray := make([]StateProofTracking, len(blockHeader.StateProofTracking))
	elems := 0
	for key := range blockHeader.StateProofTracking {
		orderedTrackingTypes[elems] = key
		elems++
	}
	sort.Slice(orderedTrackingTypes, func(i, j int) bool { return orderedTrackingTypes[i] < orderedTrackingTypes[j] })
	for i := 0; i < len(orderedTrackingTypes); i++ {
		stpfTracking := blockHeader.StateProofTracking[orderedTrackingTypes[i]]
		thing1 := StateProofTracking{
			NextRound:         uint64Ptr(uint64(stpfTracking.StateProofNextRound)),
			Type:              uint64Ptr(uint64(orderedTrackingTypes[i])),
			VotersCommitment:  byteSliceOmitZeroPtr(stpfTracking.StateProofVotersCommitment),
			OnlineTotalWeight: uint64Ptr(stpfTracking.StateProofOnlineTotalWeight.Raw),
		}
		trackingArray[orderedTrackingTypes[i]] = thing1
	}

	ret = BlockResponse{
		GenesisHash:            blockHeader.GenesisHash[:],
		GenesisId:              blockHeader.GenesisID,
		PreviousBlockHash:      blockHeader.Branch[:],
		Rewards:                &rewards,
		Round:                  uint64(blockHeader.Round),
		Seed:                   blockHeader.Seed[:],
		StateProofTracking:     &trackingArray,
		Timestamp:              uint64(blockHeader.TimeStamp),
		Transactions:           nil,
		TransactionsRoot:       blockHeader.TxnCommitments.NativeSha512_256Commitment[:],
		TransactionsRootSha256: blockHeader.TxnCommitments.Sha256Commitment[:],
		TxnCounter:             uint64Ptr(blockHeader.TxnCounter),
		UpgradeState:           &upgradeState,
		UpgradeVote:            &upgradeVote,
	}

	txn, err := genTransactions(block, block.Payset)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Block %d, txns %d\n", block.BlockHeader.Round, len(txn))

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
