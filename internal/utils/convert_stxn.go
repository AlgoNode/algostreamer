package utils

import (
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/protocol"
)

func onCompletionToTransactionOnCompletion(oc transactions.OnCompletion) OnCompletion {
	switch oc {
	case transactions.NoOpOC:
		return "noop"
	case transactions.OptInOC:
		return "optin"
	case transactions.CloseOutOC:
		return "closeout"
	case transactions.ClearStateOC:
		return "clear"
	case transactions.UpdateApplicationOC:
		return "update"
	case transactions.DeleteApplicationOC:
		return "delete"
	}
	return "unknown"
}

// The state delta bits need to be sorted for testing. Maybe it would be
// for end users too, people always seem to notice results changing.
func stateDeltaToStateDelta(d basics.StateDelta) *StateDelta {
	if len(d) == 0 {
		return nil
	}
	var delta StateDelta
	keys := make([]string, 0)
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := d[k]
		delta = append(delta, EvalDeltaKeyValue{
			Key: base64.StdEncoding.EncodeToString([]byte(k)),
			Value: EvalDelta{
				Action: uint64(v.Action),
				Bytes:  strPtr(base64.StdEncoding.EncodeToString([]byte(v.Bytes))),
				Uint:   uint64Ptr(v.Uint),
			},
		})
	}
	return &delta
}

type rowData struct {
	Round            uint64
	RoundTime        int64
	Intra            uint
	AssetID          uint64
	AssetCloseAmount uint64
}

func signedTxnWithAdToTransaction(stxn *transactions.SignedTxnWithAD, intra uint, extra rowData) (Transaction, uint, error) {
	var payment *TransactionPayment
	var keyreg *TransactionKeyreg
	var assetConfig *TransactionAssetConfig
	var assetFreeze *TransactionAssetFreeze
	var assetTransfer *TransactionAssetTransfer
	var application *TransactionApplication

	switch stxn.Txn.Type {
	case protocol.PaymentTx:
		p := TransactionPayment{
			CloseAmount:      uint64Ptr(stxn.ApplyData.ClosingAmount.Raw),
			CloseRemainderTo: addrPtr(stxn.Txn.CloseRemainderTo),
			Receiver:         stxn.Txn.Receiver.String(),
			Amount:           stxn.Txn.Amount.Raw,
		}
		payment = &p
	case protocol.KeyRegistrationTx:
		k := TransactionKeyreg{
			NonParticipation:          boolPtr(stxn.Txn.Nonparticipation),
			SelectionParticipationKey: byteSliceOmitZeroPtr(stxn.Txn.SelectionPK[:]),
			VoteFirstValid:            uint64Ptr(uint64(stxn.Txn.VoteFirst)),
			VoteLastValid:             uint64Ptr(uint64(stxn.Txn.VoteLast)),
			VoteKeyDilution:           uint64Ptr(stxn.Txn.VoteKeyDilution),
			VoteParticipationKey:      byteSliceOmitZeroPtr(stxn.Txn.VotePK[:]),
			StateProofKey:             byteSliceOmitZeroPtr(stxn.Txn.StateProofPK[:]),
		}
		keyreg = &k
	case protocol.AssetConfigTx:
		assetParams := AssetParams{
			Clawback:      addrPtr(stxn.Txn.AssetParams.Clawback),
			Creator:       stxn.Txn.Sender.String(),
			Decimals:      uint64(stxn.Txn.AssetParams.Decimals),
			DefaultFrozen: boolPtr(stxn.Txn.AssetParams.DefaultFrozen),
			Freeze:        addrPtr(stxn.Txn.AssetParams.Freeze),
			Manager:       addrPtr(stxn.Txn.AssetParams.Manager),
			MetadataHash:  byteSliceOmitZeroPtr(stxn.Txn.AssetParams.MetadataHash[:]),
			Name:          strPtr(PrintableUTF8OrEmpty(stxn.Txn.AssetParams.AssetName)),
			NameB64:       byteSlicePtr([]byte(stxn.Txn.AssetParams.AssetName)),
			Reserve:       addrPtr(stxn.Txn.AssetParams.Reserve),
			Total:         stxn.Txn.AssetParams.Total,
			UnitName:      strPtr(PrintableUTF8OrEmpty(stxn.Txn.AssetParams.UnitName)),
			UnitNameB64:   byteSlicePtr([]byte(stxn.Txn.AssetParams.UnitName)),
			Url:           strPtr(PrintableUTF8OrEmpty(stxn.Txn.AssetParams.URL)),
			UrlB64:        byteSlicePtr([]byte(stxn.Txn.AssetParams.URL)),
		}
		config := TransactionAssetConfig{
			AssetId: uint64Ptr(uint64(stxn.Txn.ConfigAsset)),
			Params:  &assetParams,
		}
		assetConfig = &config
	case protocol.AssetTransferTx:
		t := TransactionAssetTransfer{
			Amount:      stxn.Txn.AssetAmount,
			AssetId:     uint64(stxn.Txn.XferAsset),
			CloseTo:     addrPtr(stxn.Txn.AssetCloseTo),
			Receiver:    stxn.Txn.AssetReceiver.String(),
			Sender:      addrPtr(stxn.Txn.AssetSender),
			CloseAmount: uint64Ptr(extra.AssetCloseAmount),
		}
		assetTransfer = &t
	case protocol.AssetFreezeTx:
		f := TransactionAssetFreeze{
			Address:         stxn.Txn.FreezeAccount.String(),
			AssetId:         uint64(stxn.Txn.FreezeAsset),
			NewFreezeStatus: stxn.Txn.AssetFrozen,
		}
		assetFreeze = &f
	case protocol.ApplicationCallTx:
		args := make([]string, 0)
		for _, v := range stxn.Txn.ApplicationArgs {
			args = append(args, base64.StdEncoding.EncodeToString(v))
		}

		accts := make([]string, 0)
		for _, v := range stxn.Txn.Accounts {
			accts = append(accts, v.String())
		}

		apps := make([]uint64, 0)
		for _, v := range stxn.Txn.ForeignApps {
			apps = append(apps, uint64(v))
		}

		assets := make([]uint64, 0)
		for _, v := range stxn.Txn.ForeignAssets {
			assets = append(assets, uint64(v))
		}

		a := TransactionApplication{
			Accounts:          &accts,
			ApplicationArgs:   &args,
			ApplicationId:     uint64(stxn.Txn.ApplicationID),
			ApprovalProgram:   byteSliceOmitZeroPtr(stxn.Txn.ApprovalProgram),
			ClearStateProgram: byteSliceOmitZeroPtr(stxn.Txn.ClearStateProgram),
			ForeignApps:       &apps,
			ForeignAssets:     &assets,
			GlobalStateSchema: &StateSchema{
				NumByteSlice: stxn.Txn.GlobalStateSchema.NumByteSlice,
				NumUint:      stxn.Txn.GlobalStateSchema.NumUint,
			},
			LocalStateSchema: &StateSchema{
				NumByteSlice: stxn.Txn.LocalStateSchema.NumByteSlice,
				NumUint:      stxn.Txn.LocalStateSchema.NumUint,
			},
			OnCompletion:      onCompletionToTransactionOnCompletion(stxn.Txn.OnCompletion),
			ExtraProgramPages: uint64PtrOrNil(uint64(stxn.Txn.ExtraProgramPages)),
		}

		application = &a
	}

	var localStateDelta *[]AccountStateDelta
	type tuple struct {
		key     uint64
		address basics.Address
	}
	if len(stxn.ApplyData.EvalDelta.LocalDeltas) > 0 {
		keys := make([]tuple, 0)
		for k := range stxn.ApplyData.EvalDelta.LocalDeltas {
			if k == 0 {
				keys = append(keys, tuple{
					key:     0,
					address: stxn.Txn.Sender,
				})
			} else {
				addr := basics.Address{}
				copy(addr[:], stxn.Txn.Accounts[k-1][:])
				keys = append(keys, tuple{
					key:     k,
					address: addr,
				})
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].key < keys[j].key })
		d := make([]AccountStateDelta, 0)
		for _, k := range keys {
			v := stxn.ApplyData.EvalDelta.LocalDeltas[k.key]
			delta := stateDeltaToStateDelta(v)
			if delta != nil {
				d = append(d, AccountStateDelta{
					Address: k.address.String(),
					Delta:   *delta,
				})
			}
		}
		localStateDelta = &d
	}

	var logs *[][]byte
	if len(stxn.ApplyData.EvalDelta.Logs) > 0 {
		l := make([][]byte, 0, len(stxn.ApplyData.EvalDelta.Logs))
		for _, v := range stxn.ApplyData.EvalDelta.Logs {
			l = append(l, []byte(v))
		}
		logs = &l
	}
	intra++
	var inners *[]Transaction
	if len(stxn.ApplyData.EvalDelta.InnerTxns) > 0 {
		itxns := make([]Transaction, 0, len(stxn.ApplyData.EvalDelta.InnerTxns))
		for _, t := range stxn.ApplyData.EvalDelta.InnerTxns {
			extra2 := extra
			if t.Txn.Type == protocol.ApplicationCallTx {
				extra2.AssetID = uint64(t.ApplyData.ApplicationID)
			} else if t.Txn.Type == protocol.AssetConfigTx {
				extra2.AssetID = uint64(t.ApplyData.ConfigAsset)
			} else {
				extra2.AssetID = 0
			}
			extra2.AssetCloseAmount = t.ApplyData.AssetClosingAmount

			itxn, nextintra, err := signedTxnWithAdToTransaction(&t, intra, extra2)
			intra = nextintra
			if err != nil {
				return Transaction{}, intra, err
			}
			itxns = append(itxns, itxn)
		}

		inners = &itxns
	}

	txn := Transaction{
		ApplicationTransaction:   application,
		AssetConfigTransaction:   assetConfig,
		AssetFreezeTransaction:   assetFreeze,
		AssetTransferTransaction: assetTransfer,
		PaymentTransaction:       payment,
		KeyregTransaction:        keyreg,
		ClosingAmount:            uint64Ptr(stxn.ClosingAmount.Raw),
		ConfirmedRound:           uint64Ptr(extra.Round),
		IntraRoundOffset:         uint64Ptr(uint64(extra.Intra)),
		RoundTime:                uint64Ptr(uint64(extra.RoundTime)),
		Fee:                      stxn.Txn.Fee.Raw,
		FirstValid:               uint64(stxn.Txn.FirstValid),
		GenesisHash:              byteSliceOmitZeroPtr(stxn.SignedTxn.Txn.GenesisHash[:]),
		GenesisId:                strPtr(stxn.SignedTxn.Txn.GenesisID),
		Group:                    byteSliceOmitZeroPtr(stxn.Txn.Group[:]),
		LastValid:                uint64(stxn.Txn.LastValid),
		Lease:                    byteSliceOmitZeroPtr(stxn.Txn.Lease[:]),
		Note:                     byteSliceOmitZeroPtr(stxn.Txn.Note[:]),
		Sender:                   stxn.Txn.Sender.String(),
		ReceiverRewards:          uint64Ptr(stxn.ReceiverRewards.Raw),
		CloseRewards:             uint64Ptr(stxn.CloseRewards.Raw),
		SenderRewards:            uint64Ptr(stxn.SenderRewards.Raw),
		TxType:                   string(stxn.Txn.Type),
		RekeyTo:                  addrPtr(stxn.Txn.RekeyTo),
		GlobalStateDelta:         stateDeltaToStateDelta(stxn.EvalDelta.GlobalDelta),
		LocalStateDelta:          localStateDelta,
		Logs:                     logs,
		InnerTxns:                inners,
	}

	if stxn.Txn.Type == protocol.AssetConfigTx {
		if txn.AssetConfigTransaction != nil && txn.AssetConfigTransaction.AssetId != nil && *txn.AssetConfigTransaction.AssetId == 0 {
			txn.CreatedAssetIndex = uint64Ptr(extra.AssetID)
		}
	}

	if stxn.Txn.Type == protocol.ApplicationCallTx {
		if txn.ApplicationTransaction != nil && txn.ApplicationTransaction.ApplicationId == 0 {
			if extra.AssetID > 0 {
				txn.CreatedApplicationIndex = uint64Ptr(extra.AssetID)
			}
		}
	}

	return txn, intra, nil
}

func transactionAssetID(stxnad *transactions.SignedTxnWithAD, intra uint, block *bookkeeping.Block) (uint64, error) {
	assetid := uint64(0)

	switch stxnad.Txn.Type {
	case protocol.ApplicationCallTx:
		assetid = uint64(stxnad.Txn.ApplicationID)
		if assetid == 0 {
			assetid = uint64(stxnad.ApplyData.ApplicationID)
		}
		if assetid == 0 {
			if block == nil {
				return 0, fmt.Errorf("transactionAssetID(): Missing ApplicationID for transaction: %s", stxnad.ID())
			}
			// pre v30 transactions do not have ApplyData.ConfigAsset or InnerTxns
			// so txn counter + payset pos calculation is OK
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	case protocol.AssetConfigTx:
		assetid = uint64(stxnad.Txn.ConfigAsset)
		if assetid == 0 {
			assetid = uint64(stxnad.ApplyData.ConfigAsset)
		}
		if assetid == 0 {
			if block == nil {
				return 0, fmt.Errorf("transactionAssetID(): Missing ConfigAsset for transaction: %s", stxnad.ID())
			}
			// pre v30 transactions do not have ApplyData.ApplicationID or InnerTxns
			// so txn counter + payset pos calculation is OK
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	case protocol.AssetTransferTx:
		assetid = uint64(stxnad.Txn.XferAsset)
	case protocol.AssetFreezeTx:
		assetid = uint64(stxnad.Txn.FreezeAsset)
	}

	return assetid, nil
}

func sigToTransactionSig(sig crypto.Signature) *[]byte {
	if sig == (crypto.Signature{}) {
		return nil
	}

	tsig := sig[:]
	return &tsig
}

func msigToTransactionMsig(msig crypto.MultisigSig) *TransactionSignatureMultisig {
	if msig.Blank() {
		return nil
	}

	subsigs := make([]TransactionSignatureMultisigSubsignature, 0)
	for _, subsig := range msig.Subsigs {
		subsigs = append(subsigs, TransactionSignatureMultisigSubsignature{
			PublicKey: byteSliceOmitZeroPtr(subsig.Key[:]),
			Signature: sigToTransactionSig(subsig.Sig),
		})
	}

	ret := TransactionSignatureMultisig{
		Subsignature: &subsigs,
		Threshold:    uint64Ptr(uint64(msig.Threshold)),
		Version:      uint64Ptr(uint64(msig.Version)),
	}
	return &ret
}

func lsigToTransactionLsig(lsig transactions.LogicSig) *TransactionSignatureLogicsig {
	if lsig.Blank() {
		return nil
	}

	args := make([]string, 0)
	for _, arg := range lsig.Args {
		args = append(args, base64.StdEncoding.EncodeToString(arg))
	}

	ret := TransactionSignatureLogicsig{
		Args:              &args,
		Logic:             lsig.Logic,
		MultisigSignature: msigToTransactionMsig(lsig.Msig),
		Signature:         sigToTransactionSig(lsig.Sig),
	}

	return &ret
}
