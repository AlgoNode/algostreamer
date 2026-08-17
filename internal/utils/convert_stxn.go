package utils

import (
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/algorand/go-algorand-sdk/v2/crypto"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/algorand/indexer/v3/api/generated/v2"
)

func onCompletionToTransactionOnCompletion(oc sdk.OnCompletion) generated.OnCompletion {
	switch oc {
	case sdk.NoOpOC:
		return "noop"
	case sdk.OptInOC:
		return "optin"
	case sdk.CloseOutOC:
		return "closeout"
	case sdk.ClearStateOC:
		return "clear"
	case sdk.UpdateApplicationOC:
		return "update"
	case sdk.DeleteApplicationOC:
		return "delete"
	}
	return "unknown"
}

// The state delta bits need to be sorted for testing. Maybe it would be
// for end users too, people always seem to notice results changing.
func stateDeltaToStateDelta(d sdk.StateDelta) *generated.StateDelta {
	if len(d) == 0 {
		return nil
	}
	var delta generated.StateDelta
	keys := make([]string, 0)
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := d[k]
		delta = append(delta, generated.EvalDeltaKeyValue{
			Key: base64.StdEncoding.EncodeToString([]byte(k)),
			Value: generated.EvalDelta{
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

func signedTxnWithAdToTransaction(stxn *sdk.SignedTxnWithAD, intra uint, extra rowData) (generated.Transaction, uint, error) {
	var payment *generated.TransactionPayment
	var keyreg *generated.TransactionKeyreg
	var assetConfig *generated.TransactionAssetConfig
	var assetFreeze *generated.TransactionAssetFreeze
	var assetTransfer *generated.TransactionAssetTransfer
	var application *generated.TransactionApplication
	var stateProof *generated.TransactionStateProof
	var heartbeat *generated.TransactionHeartbeat

	switch stxn.Txn.Type {
	case sdk.PaymentTx:
		p := generated.TransactionPayment{
			CloseAmount:      uint64Ptr(uint64(stxn.ApplyData.ClosingAmount)),
			CloseRemainderTo: addrPtr(stxn.Txn.CloseRemainderTo),
			Receiver:         stxn.Txn.Receiver.String(),
			Amount:           uint64(stxn.Txn.Amount),
		}
		payment = &p
	case sdk.KeyRegistrationTx:
		k := generated.TransactionKeyreg{
			NonParticipation:          boolPtr(stxn.Txn.Nonparticipation),
			SelectionParticipationKey: byteSliceOmitZeroPtr(stxn.Txn.SelectionPK[:]),
			VoteFirstValid:            uint64Ptr(uint64(stxn.Txn.VoteFirst)),
			VoteLastValid:             uint64Ptr(uint64(stxn.Txn.VoteLast)),
			VoteKeyDilution:           uint64Ptr(stxn.Txn.VoteKeyDilution),
			VoteParticipationKey:      byteSliceOmitZeroPtr(stxn.Txn.VotePK[:]),
			StateProofKey:             byteSliceOmitZeroPtr(stxn.Txn.StateProofPK[:]),
		}
		keyreg = &k
	case sdk.AssetConfigTx:
		var assetParams *generated.AssetParams
		if !stxn.Txn.AssetParams.IsZero() {
			assetParams = &generated.AssetParams{
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
		}
		config := generated.TransactionAssetConfig{
			AssetId: uint64Ptr(uint64(stxn.Txn.ConfigAsset)),
			Params:  assetParams,
		}
		assetConfig = &config
	case sdk.AssetTransferTx:
		t := generated.TransactionAssetTransfer{
			Amount:      stxn.Txn.AssetAmount,
			AssetId:     uint64(stxn.Txn.XferAsset),
			CloseTo:     addrPtr(stxn.Txn.AssetCloseTo),
			Receiver:    stxn.Txn.AssetReceiver.String(),
			Sender:      addrPtr(stxn.Txn.AssetSender),
			CloseAmount: uint64Ptr(extra.AssetCloseAmount),
		}
		assetTransfer = &t
	case sdk.AssetFreezeTx:
		f := generated.TransactionAssetFreeze{
			Address:         stxn.Txn.FreezeAccount.String(),
			AssetId:         uint64(stxn.Txn.FreezeAsset),
			NewFreezeStatus: stxn.Txn.AssetFrozen,
		}
		assetFreeze = &f
	case sdk.ApplicationCallTx:
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

		boxRefs := make([]generated.BoxReference, 0, len(stxn.Txn.BoxReferences))
		for _, v := range stxn.Txn.BoxReferences {
			var appID uint64
			if v.ForeignAppIdx == 0 {
				appID = 0
			} else if int(v.ForeignAppIdx-1) < len(stxn.Txn.ForeignApps) {
				// Indexes are 1-based, so we subtract 1
				appID = uint64(stxn.Txn.ForeignApps[v.ForeignAppIdx-1])
			} else {
				continue
			}
			boxRefs = append(boxRefs, generated.BoxReference{
				App:  appID,
				Name: v.Name,
			})
		}

		access := make([]generated.ResourceRef, 0, len(stxn.Txn.Access))
		for _, v := range stxn.Txn.Access {
			resourceRef := generated.ResourceRef{}

			// Only should be setting a single field on resourceRef
			if v.Address != (sdk.Address{}) {
				resourceRef.Address = strPtr(v.Address.String())
			} else if v.App != 0 {
				resourceRef.ApplicationId = uint64Ptr(uint64(v.App))
			} else if v.Asset != 0 {
				resourceRef.AssetId = uint64Ptr(uint64(v.Asset))
			} else if v.Holding.Asset != 0 {
				var address sdk.Address
				if v.Holding.Address == 0 {
					// indicates the sender, resolved below
					address = stxn.Txn.Sender
				} else if int(v.Holding.Address-1) < len(stxn.Txn.Access) {
					address = stxn.Txn.Access[v.Holding.Address-1].Address
				}

				var asset sdk.AssetIndex
				// Asset should always be non-zero, but sanity check
				if int(v.Holding.Asset-1) < len(stxn.Txn.Access) {
					asset = stxn.Txn.Access[v.Holding.Asset-1].Asset
				}

				resourceRef.Holding = &generated.HoldingRef{
					Address: address.String(),
					Asset:   uint64(asset),
				}
			} else if v.Locals.Address != 0 || v.Locals.App != 0 {
				var address sdk.Address
				if v.Locals.Address == 0 {
					// indicates the sender, resolved below
					address = stxn.Txn.Sender
				} else if int(v.Locals.Address-1) < len(stxn.Txn.Access) {
					address = stxn.Txn.Access[v.Locals.Address-1].Address
				}

				var app sdk.AppIndex
				if v.Locals.App == 0 {
					app = 0
				} else if int(v.Locals.App-1) < len(stxn.Txn.Access) {
					app = stxn.Txn.Access[v.Locals.App-1].App
				}

				resourceRef.Local = &generated.LocalsRef{
					Address: address.String(),
					App:     uint64(app),
				}
			} else {
				// If all else empty, default to a boxref, because a boxref is the only ResourceRef that should ever be empty
				var appID uint64
				if v.Box.ForeignAppIdx == 0 {
					appID = 0
				} else if int(v.Box.ForeignAppIdx-1) < len(stxn.Txn.Access) {
					// Indexes are 1-based, so we subtract 1
					appID = uint64(stxn.Txn.Access[v.Box.ForeignAppIdx-1].App)
				}
				boxRef := generated.BoxReference{
					App:  appID,
					Name: v.Box.Name,
				}

				resourceRef.Box = &boxRef
			}

			access = append(access, resourceRef)
		}

		a := generated.TransactionApplication{
			Access:            &access,
			Accounts:          &accts,
			ApplicationArgs:   &args,
			ApplicationId:     uint64(stxn.Txn.ApplicationID),
			ApprovalProgram:   byteSliceOmitZeroPtr(stxn.Txn.ApprovalProgram),
			BoxReferences:     &boxRefs,
			ClearStateProgram: byteSliceOmitZeroPtr(stxn.Txn.ClearStateProgram),
			ForeignApps:       &apps,
			ForeignAssets:     &assets,
			GlobalStateSchema: &generated.StateSchema{
				NumByteSlice: stxn.Txn.GlobalStateSchema.NumByteSlice,
				NumUint:      stxn.Txn.GlobalStateSchema.NumUint,
			},
			LocalStateSchema: &generated.StateSchema{
				NumByteSlice: stxn.Txn.LocalStateSchema.NumByteSlice,
				NumUint:      stxn.Txn.LocalStateSchema.NumUint,
			},
			OnCompletion:      onCompletionToTransactionOnCompletion(stxn.Txn.OnCompletion),
			ExtraProgramPages: uint64PtrOrNil(uint64(stxn.Txn.ExtraProgramPages)),
			RejectVersion:     uint64PtrOrNil(stxn.Txn.RejectVersion),
		}

		application = &a
	case sdk.StateProofTx:
		sprf := stxn.Txn.StateProof
		partPath := make([][]byte, len(sprf.PartProofs.Path))
		for idx, part := range sprf.PartProofs.Path {
			digest := make([]byte, len(part))
			copy(digest, part)
			partPath[idx] = digest
		}

		sigProofPath := make([][]byte, len(sprf.SigProofs.Path))
		for idx, sigPart := range sprf.SigProofs.Path {
			digest := make([]byte, len(sigPart))
			copy(digest, sigPart)
			sigProofPath[idx] = digest
		}

		// We need to iterate through these in order, to make sure our responses are deterministic
		keys := make([]uint64, len(sprf.Reveals))
		elems := 0
		for key := range sprf.Reveals {
			keys[elems] = key
			elems++
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		reveals := make([]generated.StateProofReveal, len(sprf.Reveals))
		for i, key := range keys {
			revToConv := sprf.Reveals[key]
			commitment := revToConv.Part.PK.Commitment[:]
			falconSig := []byte(revToConv.SigSlot.Sig.Signature)
			verifyKey := revToConv.SigSlot.Sig.VerifyingKey.PublicKey[:]
			proofPath := make([][]byte, len(revToConv.SigSlot.Sig.Proof.Path))
			for idx, proofPart := range revToConv.SigSlot.Sig.Proof.Path {
				proofPath[idx] = proofPart
			}

			reveals[i] = generated.StateProofReveal{
				Participant: &generated.StateProofParticipant{
					Verifier: &generated.StateProofVerifier{
						Commitment:  &commitment,
						KeyLifetime: uint64Ptr(revToConv.Part.PK.KeyLifetime),
					},
					Weight: uint64Ptr(revToConv.Part.Weight),
				},
				Position: uint64Ptr(key),
				SigSlot: &generated.StateProofSigSlot{
					LowerSigWeight: uint64Ptr(revToConv.SigSlot.L),
					Signature: &generated.StateProofSignature{
						FalconSignature:  &falconSig,
						MerkleArrayIndex: uint64Ptr(revToConv.SigSlot.Sig.VectorCommitmentIndex),
						Proof: &generated.MerkleArrayProof{
							HashFactory: &generated.HashFactory{
								HashType: uint64Ptr(uint64(revToConv.SigSlot.Sig.Proof.HashFactory.HashType)),
							},
							Path:      &proofPath,
							TreeDepth: uint64Ptr(uint64(revToConv.SigSlot.Sig.Proof.TreeDepth)),
						},
						VerifyingKey: &verifyKey,
					},
				},
			}
		}
		proof := generated.StateProofFields{
			PartProofs: &generated.MerkleArrayProof{
				HashFactory: &generated.HashFactory{
					HashType: uint64Ptr(uint64(sprf.PartProofs.HashFactory.HashType)),
				},
				Path:      &partPath,
				TreeDepth: uint64Ptr(uint64(sprf.PartProofs.TreeDepth)),
			},
			Reveals:     &reveals,
			SaltVersion: uint64Ptr(uint64(sprf.MerkleSignatureSaltVersion)),
			SigCommit:   byteSliceOmitZeroPtr(sprf.SigCommit),
			SigProofs: &generated.MerkleArrayProof{
				HashFactory: &generated.HashFactory{
					HashType: uint64Ptr(uint64(sprf.SigProofs.HashFactory.HashType)),
				},
				Path:      &sigProofPath,
				TreeDepth: uint64Ptr(uint64(sprf.SigProofs.TreeDepth)),
			},
			SignedWeight:      uint64Ptr(sprf.SignedWeight),
			PositionsToReveal: &sprf.PositionsToReveal,
		}

		message := generated.IndexerStateProofMessage{
			BlockHeadersCommitment: &stxn.Txn.Message.BlockHeadersCommitment,
			FirstAttestedRound:     uint64Ptr(stxn.Txn.Message.FirstAttestedRound),
			LatestAttestedRound:    uint64Ptr(stxn.Txn.Message.LastAttestedRound),
			LnProvenWeight:         uint64Ptr(stxn.Txn.Message.LnProvenWeight),
			VotersCommitment:       &stxn.Txn.Message.VotersCommitment,
		}

		proofTxn := generated.TransactionStateProof{
			Message:        &message,
			StateProof:     &proof,
			StateProofType: uint64Ptr(uint64(stxn.Txn.StateProofType)),
		}
		stateProof = &proofTxn
	case sdk.HeartbeatTx:
		// HeartbeatTxnFields is embedded as a pointer, so guard against a
		// malformed txn rather than panicking mid-stream.
		hb := stxn.Txn.HeartbeatTxnFields
		if hb == nil {
			break
		}
		hbTxn := generated.TransactionHeartbeat{
			HbAddress:           hb.HbAddress.String(),
			HbChallengeDiscount: boolPtrOrNil(hb.HbChallengeDiscount),
			HbKeyDilution:       hb.HbKeyDilution,
			HbProof: generated.HbProofFields{
				HbPk:     byteSliceOmitZeroPtr(hb.HbProof.PK[:]),
				HbPk1sig: byteSliceOmitZeroPtr(hb.HbProof.PK1Sig[:]),
				HbPk2:    byteSliceOmitZeroPtr(hb.HbProof.PK2[:]),
				HbPk2sig: byteSliceOmitZeroPtr(hb.HbProof.PK2Sig[:]),
				HbSig:    byteSliceOmitZeroPtr(hb.HbProof.Sig[:]),
			},
			HbSeed:   hb.HbSeed[:],
			HbVoteId: hb.HbVoteID[:],
		}
		heartbeat = &hbTxn
	}
	// var localStateDelta *[]AccountStateDelta
	// type tuple struct {
	// 	key     uint64
	// 	address basics.Address
	// }
	// if len(stxn.ApplyData.EvalDelta.LocalDeltas) > 0 {
	// 	keys := make([]tuple, 0)
	// 	for k := range stxn.ApplyData.EvalDelta.LocalDeltas {
	// 		if k == 0 {
	// 			keys = append(keys, tuple{
	// 				key:     0,
	// 				address: stxn.Txn.Sender,
	// 			})
	// 		} else {
	// 			addr := basics.Address{}
	// 			copy(addr[:], stxn.Txn.Accounts[k-1][:])
	// 			keys = append(keys, tuple{
	// 				key:     k,
	// 				address: addr,
	// 			})
	// 		}
	// 	}
	// 	sort.Slice(keys, func(i, j int) bool { return keys[i].key < keys[j].key })
	// 	d := make([]AccountStateDelta, 0)
	// 	for _, k := range keys {
	// 		v := stxn.ApplyData.EvalDelta.LocalDeltas[k.key]
	// 		delta := stateDeltaToStateDelta(v)
	// 		if delta != nil {
	// 			d = append(d, AccountStateDelta{
	// 				Address: k.address.String(),
	// 				Delta:   *delta,
	// 			})
	// 		}
	// 	}
	// 	localStateDelta = &d
	// }

	var logs *[][]byte
	if len(stxn.ApplyData.EvalDelta.Logs) > 0 {
		l := make([][]byte, 0, len(stxn.ApplyData.EvalDelta.Logs))
		for _, v := range stxn.ApplyData.EvalDelta.Logs {
			l = append(l, []byte(v))
		}
		logs = &l
	}

	intra++
	var inners *[]generated.Transaction
	if len(stxn.ApplyData.EvalDelta.InnerTxns) > 0 {
		itxns := make([]generated.Transaction, 0, len(stxn.ApplyData.EvalDelta.InnerTxns))
		for _, t := range stxn.ApplyData.EvalDelta.InnerTxns {
			extra2 := extra
			if t.Txn.Type == sdk.ApplicationCallTx {
				extra2.AssetID = uint64(t.ApplyData.ApplicationID)
			} else if t.Txn.Type == sdk.AssetConfigTx {
				extra2.AssetID = uint64(t.ApplyData.ConfigAsset)
			} else {
				extra2.AssetID = 0
			}
			extra2.AssetCloseAmount = t.ApplyData.AssetClosingAmount

			itxn, nextintra, err := signedTxnWithAdToTransaction(&t, intra, extra2)
			intra = nextintra
			if err != nil {
				return generated.Transaction{}, intra, err
			}
			itxns = append(itxns, itxn)
		}

		inners = &itxns
	}

	txn := generated.Transaction{
		ApplicationTransaction:   application,
		AssetConfigTransaction:   assetConfig,
		AssetFreezeTransaction:   assetFreeze,
		AssetTransferTransaction: assetTransfer,
		PaymentTransaction:       payment,
		KeyregTransaction:        keyreg,
		StateProofTransaction:    stateProof,
		HeartbeatTransaction:     heartbeat,
		ClosingAmount:            uint64Ptr(uint64(stxn.ClosingAmount)),
		ConfirmedRound:           uint64Ptr(extra.Round),
		IntraRoundOffset:         uint64Ptr(uint64(extra.Intra)),
		RoundTime:                uint64Ptr(uint64(extra.RoundTime)),
		Fee:                      uint64(stxn.Txn.Fee),
		FirstValid:               uint64(stxn.Txn.FirstValid),
		GenesisHash:              byteSliceOmitZeroPtr(stxn.SignedTxn.Txn.GenesisHash[:]),
		GenesisId:                strPtr(stxn.SignedTxn.Txn.GenesisID),
		Group:                    byteSliceOmitZeroPtr(stxn.Txn.Group[:]),
		LastValid:                uint64(stxn.Txn.LastValid),
		Lease:                    byteSliceOmitZeroPtr(stxn.Txn.Lease[:]),
		Note:                     byteSliceOmitZeroPtr(stxn.Txn.Note[:]),
		Sender:                   stxn.Txn.Sender.String(),
		ReceiverRewards:          uint64Ptr(uint64(stxn.ReceiverRewards)),
		CloseRewards:             uint64Ptr(uint64(stxn.CloseRewards)),
		SenderRewards:            uint64Ptr(uint64(stxn.SenderRewards)),
		TxType:                   generated.TransactionTxType(stxn.Txn.Type),
		RekeyTo:                  addrPtr(stxn.Txn.RekeyTo),
		GlobalStateDelta:         stateDeltaToStateDelta(stxn.EvalDelta.GlobalDelta),
		//		LocalStateDelta:          localStateDelta,
		Logs:      logs,
		InnerTxns: inners,
		AuthAddr:  addrPtr(stxn.AuthAddr),
	}

	if stxn.Txn.Type == sdk.AssetConfigTx {
		if txn.AssetConfigTransaction != nil && txn.AssetConfigTransaction.AssetId != nil && *txn.AssetConfigTransaction.AssetId == 0 {
			txn.CreatedAssetIndex = uint64Ptr(extra.AssetID)
		}
	}

	if stxn.Txn.Type == sdk.ApplicationCallTx {
		if txn.ApplicationTransaction != nil && txn.ApplicationTransaction.ApplicationId == 0 {
			txn.CreatedApplicationIndex = uint64Ptr(extra.AssetID)
		}
	}

	return txn, intra, nil
}

func transactionAssetID(stxnad *sdk.SignedTxnWithAD, intra uint, block *sdk.Block) (uint64, error) {
	assetid := uint64(0)
	switch stxnad.Txn.Type {
	case sdk.ApplicationCallTx:
		assetid = uint64(stxnad.Txn.ApplicationID)
		if assetid == 0 {
			assetid = uint64(stxnad.ApplyData.ApplicationID)
		}
		if assetid == 0 {
			if block == nil {
				txid := crypto.TransactionIDString(stxnad.Txn)
				return 0, fmt.Errorf("transactionAssetID(): Missing ApplicationID for transaction: %s", txid)
			}
			// pre v30 transactions do not have ApplyData.ConfigAsset or InnerTxns
			// so txn counter + payset pos calculation is OK
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	case sdk.AssetConfigTx:
		assetid = uint64(stxnad.Txn.ConfigAsset)
		if assetid == 0 {
			assetid = uint64(stxnad.ApplyData.ConfigAsset)
		}
		if assetid == 0 {
			if block == nil {
				txid := crypto.TransactionIDString(stxnad.Txn)
				return 0, fmt.Errorf("transactionAssetID(): Missing ConfigAsset for transaction: %s", txid)
			}
			// pre v30 transactions do not have ApplyData.ApplicationID or InnerTxns
			// so txn counter + payset pos calculation is OK
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	case sdk.AssetTransferTx:
		assetid = uint64(stxnad.Txn.XferAsset)
	case sdk.AssetFreezeTx:
		assetid = uint64(stxnad.Txn.FreezeAsset)
	}

	return assetid, nil
}

func sigToTransactionSig(sig sdk.Signature) *[]byte {
	if sig == (sdk.Signature{}) {
		return nil
	}

	tsig := sig[:]
	return &tsig
}

func msigToTransactionMsig(msig sdk.MultisigSig) *generated.TransactionSignatureMultisig {
	if msig.Blank() {
		return nil
	}

	subsigs := make([]generated.TransactionSignatureMultisigSubsignature, 0)
	for _, subsig := range msig.Subsigs {
		subsigs = append(subsigs, generated.TransactionSignatureMultisigSubsignature{
			PublicKey: byteSliceOmitZeroPtr(subsig.Key[:]),
			Signature: sigToTransactionSig(subsig.Sig),
		})
	}

	ret := generated.TransactionSignatureMultisig{
		Subsignature: &subsigs,
		Threshold:    uint64Ptr(uint64(msig.Threshold)),
		Version:      uint64Ptr(uint64(msig.Version)),
	}
	return &ret
}

func lsigToTransactionLsig(lsig sdk.LogicSig) *generated.TransactionSignatureLogicsig {
	// LogicSig.Blank() does not consider PQsig in the current version (it will
	// eventually, of course), but for now we need the extra explicit
	// check. Remove it when sdk updates.
	if lsig.Blank() && lsig.PQsig.Blank() {
		return nil
	}

	args := make([]string, 0)
	for _, arg := range lsig.Args {
		args = append(args, base64.StdEncoding.EncodeToString(arg))
	}

	ret := generated.TransactionSignatureLogicsig{
		Args:                   &args,
		Logic:                  lsig.Logic,
		LogicMultisigSignature: msigToTransactionMsig(lsig.LMsig),
		MultisigSignature:      msigToTransactionMsig(lsig.Msig),
		Pqsig:                  pqsigToTransactionPQsig(lsig.PQsig),
		Signature:              sigToTransactionSig(lsig.Sig),
	}

	return &ret
}

func pqsigToTransactionPQsig(pqsig sdk.PQSig) *generated.TransactionSignaturePQsig {
	if pqsig.Blank() {
		return nil
	}

	ret := generated.TransactionSignaturePQsig{
		Scheme:    string(pqsig.Scheme[:]),
		Salt:      uint64PtrOrNil(uint64(pqsig.Salt)),
		PublicKey: pqsig.PublicKey,
		Signature: pqsig.Signature,
	}
	return &ret
}
