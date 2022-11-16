package rabbitmq

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/algorand/go-codec/codec"
	"github.com/algorand/go-deadlock"
	amqp91 "github.com/rabbitmq/amqp091-go"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-algorand/network"
	"github.com/algorand/go-algorand/protocol"
)

type dumpHandler struct {
	tags map[protocol.Tag]bool
	sink *RmqSink
}

func encode(handle codec.Handle, obj interface{}) ([]byte, error) {
	var output []byte
	enc := codec.NewEncoderBytes(&output, handle)

	err := enc.Encode(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to encode object: %v", err)
	}
	return output, nil
}

func (dh *dumpHandler) Publish(ctx context.Context, hdrs amqp91.Table, obj interface{}) error {

	jTx, err := encode(protocol.JSONStrictHandle, obj)
	if err != nil {
		return err
	}

	msg := amqp91.Publishing{
		ContentType: "application/json",
		Headers:     hdrs,
		Body:        jTx,
	}

	err = dh.sink.tx.ReliablePublishWithContext(ctx, dh.sink.cfg.TxMPStr, "", false, false, msg)

	if err != nil {
		fmt.Fprintf(os.Stderr, "[!ERR][RabbitMQ] %s", err)
		return err
	}
	return nil

}

func (dh *dumpHandler) Handle(msg network.IncomingMessage) network.OutgoingMessage {

	// if dh.tags != nil && !dh.tags[msg.Tag] {
	// 	return network.OutgoingMessage{Action: network.Ignore}
	// }

	switch msg.Tag {
	// case protocol.AgreementVoteTag:
	// 	var v agreement.UnauthenticatedVote
	// 	err := protocol.Decode(msg.Data, &v)
	// 	if err != nil {
	// 		goto print
	// 	}
	//		fmt.Printf("Block: %d Proposer: %s VoteSender: %s\n", v.R.Round, v.R.Proposal.OriginalProposer.String(), v.R.Sender.String())
	// dh.Publish(context.Background(), amqp91.Table{
	// 	"round": int64(v.R.Round),
	// 	"tag":   string(msg.Tag),
	// }, v)

	// case protocol.ProposalPayloadTag:
	// 	var p agreement.TransmittedPayload
	// 	err := protocol.Decode(msg.Data, &p)
	// 	if err != nil {
	// 		goto print
	// 	}
	// 	fmt.Printf("Block: %d Proposer: %s\n", p.BlockHeader.Round, p.OriginalProposer.String())
	// 	dh.Publish(context.Background(), amqp91.Table{
	// 		"round": int64(p.BlockHeader.Round),
	// 		"tag":   string(msg.Tag),
	// 	}, p)

	case protocol.TxnTag:
		dec := protocol.NewMsgpDecoderBytes(msg.Data)
		for {
			var stx transactions.SignedTxn
			err := dec.Decode(&stx)
			if err == io.EOF {
				break
			}
			if err != nil {
				goto print
			}
			txid := stx.Txn.ID().String()
			stx.Lsig.Logic = nil
			stx.Txn.Note = nil
			dh.Publish(context.Background(), amqp91.Table{
				"txtype": string(stx.Txn.Type),
				"tag":    string(msg.Tag),
				"txid":   txid,
			}, stx)

			//fmt.Println(stx.Txn.ID().String())
		}
	}

print:
	//fmt.Printf("Data size: %d bytes\n", len(msg.Data))
	return network.OutgoingMessage{Action: network.Ignore}
}

func setDumpHandlers(n network.GossipNode, sink *RmqSink) {
	dh := dumpHandler{
		sink: sink,
	}

	h := []network.TaggedMessageHandler{
		{Tag: protocol.AgreementVoteTag, MessageHandler: &dh},
		{Tag: protocol.StateProofSigTag, MessageHandler: &dh},
		{Tag: protocol.MsgOfInterestTag, MessageHandler: &dh},
		{Tag: protocol.MsgDigestSkipTag, MessageHandler: &dh},
		{Tag: protocol.NetPrioResponseTag, MessageHandler: &dh},
		// {Tag: protocol.PingTag, MessageHandler: &dh},
		// {Tag: protocol.PingReplyTag, MessageHandler: &dh},
		{Tag: protocol.ProposalPayloadTag, MessageHandler: &dh},
		{Tag: protocol.TopicMsgRespTag, MessageHandler: &dh},
		{Tag: protocol.TxnTag, MessageHandler: &dh},
		{Tag: protocol.UniCatchupReqTag, MessageHandler: &dh},
		{Tag: protocol.UniEnsBlockReqTag, MessageHandler: &dh},
		{Tag: protocol.VoteBundleTag, MessageHandler: &dh},
	}
	n.RegisterHandlers(h)
}

func bootNode(s *RmqSink) {
	log := logging.Base()
	log.SetLevel(logging.Debug)
	log.SetOutput(os.Stderr)

	deadlock.Opts.Disable = true

	conf, _ := config.LoadConfigFromDisk("/dev/null")
	conf.ForceFetchTransactions = true
	conf.GossipFanout = 4
	conf.EnableIncomingMessageFilter = true

	n, _ := network.NewWebsocketGossipNode(log,
		conf,
		[]string{""},
		"mainnet-v1.0",
		protocol.NetworkID("mainnet"))
	setDumpHandlers(n, s)
	n.Start()
}
