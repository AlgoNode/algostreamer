package utils

import (
	"sort"

	"github.com/algorand/go-algorand-sdk/v2/crypto"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/algorand/indexer/v3/api/generated/v2"
)

func GenerateBlock(block *sdk.Block) (*generated.Block, error) {
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

	var partUpdates *generated.ParticipationUpdates = &generated.ParticipationUpdates{}
	if len(blockHeader.ExpiredParticipationAccounts) > 0 {
		addrs := make([]string, len(blockHeader.ExpiredParticipationAccounts))
		for i := 0; i < len(addrs); i++ {
			addrs[i] = blockHeader.ExpiredParticipationAccounts[i].String()
		}
		partUpdates.ExpiredParticipationAccounts = strArrayPtr(addrs)
	}
	if len(blockHeader.AbsentParticipationAccounts) > 0 {
		addrs := make([]string, len(blockHeader.AbsentParticipationAccounts))
		for i := 0; i < len(addrs); i++ {
			addrs[i] = blockHeader.AbsentParticipationAccounts[i].String()
		}
		partUpdates.AbsentParticipationAccounts = strArrayPtr(addrs)
	}
	if *partUpdates == (generated.ParticipationUpdates{}) {
		partUpdates = nil
	}

	// order these so they're deterministic
	orderedTrackingTypes := make([]sdk.StateProofType, len(blockHeader.StateProofTracking))
	trackingArray := make([]generated.StateProofTracking, len(blockHeader.StateProofTracking))
	elems := 0
	for key := range blockHeader.StateProofTracking {
		orderedTrackingTypes[elems] = key
		elems++
	}
	sort.Slice(orderedTrackingTypes, func(i, j int) bool { return orderedTrackingTypes[i] < orderedTrackingTypes[j] })
	for i := 0; i < len(orderedTrackingTypes); i++ {
		stpfTracking := blockHeader.StateProofTracking[orderedTrackingTypes[i]]
		thing1 := generated.StateProofTracking{
			NextRound:         uint64Ptr(uint64(stpfTracking.StateProofNextRound)),
			Type:              uint64Ptr(uint64(orderedTrackingTypes[i])),
			VotersCommitment:  byteSliceOmitZeroPtr(stpfTracking.StateProofVotersCommitment),
			OnlineTotalWeight: uint64Ptr(uint64(stpfTracking.StateProofOnlineTotalWeight)),
		}
		trackingArray[orderedTrackingTypes[i]] = thing1
	}

	ret := generated.Block{
		Bonus:                  uint64PtrOrNil(uint64(blockHeader.Bonus)),
		FeesCollected:          uint64PtrOrNil(uint64(blockHeader.FeesCollected)),
		GenesisHash:            blockHeader.GenesisHash[:],
		GenesisId:              blockHeader.GenesisID,
		ParticipationUpdates:   partUpdates,
		PreviousBlockHash:      blockHeader.Branch[:],
		Proposer:               addrPtr(block.BlockHeader.Proposer),
		ProposerPayout:         uint64PtrOrNil(uint64(blockHeader.ProposerPayout)),
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

	txn, err := genTransactions(block)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Block %d, txns %d\n", block.BlockHeader.Round, len(txn))

	ret.Transactions = &txn
	return &ret, nil
}

func genTransactions(block *sdk.Block) ([]generated.Transaction, error) {
	intra := uint(0)
	results := make([]generated.Transaction, 0)
	for idx, stib := range block.Payset {

		var stxnad sdk.SignedTxnWithAD
		var err error
		stxnad = stib.SignedTxnWithAD

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

		sig := generated.TransactionSignature{
			Logicsig: lsigToTransactionLsig(stxnad.Lsig),
			Multisig: msigToTransactionMsig(stxnad.Msig),
			Sig:      sigToTransactionSig(stxnad.Sig),
		}

		tx, nextintra, err := signedTxnWithAdToTransaction(&stxnad, intra, extra)
		intra = nextintra
		if err != nil {
			return nil, err
		}

		txid := crypto.TransactionIDString(stxnad.Txn)
		tx.Id = &txid
		tx.Signature = &sig

		results = append(results, tx)

	}
	return results, nil
}
