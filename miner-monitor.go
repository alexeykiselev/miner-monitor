package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/mr-tron/base58"
	"github.com/wavesplatform/gowaves/pkg/grpc/generated/waves/events"
	bcu "github.com/wavesplatform/gowaves/pkg/grpc/generated/waves/events/grpc"
	"google.golang.org/grpc"
)

func main() {
	var (
		node    string
		storage string
	)
	flag.StringVar(&node, "node", "mainnet-htz-fsn1-6.wavesnodes.com:6881", "Node's gRPC API URL")
	flag.StringVar(&storage, "storage", ".storage", "Node's gRPC API URL")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := grpc.Dial(node, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}
	go func() {
		s, err := newStorage(storage)
		if err != nil {
			log.Fatalf("Failed to open storage: %v", err)
		}
		defer stop()
		defer func() {
			err := s.close()
			if err != nil {
				log.Printf("Failed to close storage: %v", err)
			}
		}()

		err = subscribe(ctx, c, s)
		if err != nil {
			log.Fatalf("Failed to subscribe: %v", err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("DONE")
		os.Exit(0)
	}
}

func subscribe(ctx context.Context, conn *grpc.ClientConn, storage *storage) error {
	c := bcu.NewBlockchainUpdatesApiClient(conn)
	req := &bcu.SubscribeRequest{
		FromHeight: 1,
		ToHeight:   100,
	}
	stream, err := c.Subscribe(ctx, req)
	if err != nil {
		return err
	}
	var event bcu.SubscribeEvent
	for err = stream.RecvMsg(&event); err == nil; err = stream.RecvMsg(&event) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			update := event.GetUpdate()
			height := update.GetHeight()
			switch u := update.Update.(type) {
			case *events.BlockchainUpdated_Append_:
				updates := len(u.Append.StateUpdate.Balances)
				for _, balance := range u.Append.StateUpdate.Balances {
					if len(balance.AmountAfter.AssetId) == 0 {
						log.Printf("[%d] %s: %d", height, base58.Encode(balance.Address), balance.AmountAfter.Amount)
						err := storage.updateAccount(balance.Address, balance.AmountAfter.Amount)
						if err != nil {
							log.Fatalf("Failed to update account: %v", err)
						}
						err = storage.setGenerator(height, balance.Address)
						if err != nil {
							log.Fatalf("Failed to set generator: %v", err)
						}
					}
				}
				updates += len(u.Append.TransactionStateUpdates)
				for _, tx := range u.Append.TransactionStateUpdates {
					for _, balance := range tx.Balances {
						if len(balance.AmountAfter.AssetId) == 0 {
							log.Printf("[%d] %s: %d", height, base58.Encode(balance.Address), balance.AmountAfter.Amount)
							err := storage.updateAccount(balance.Address, balance.AmountAfter.Amount)
							if err != nil {
								log.Fatalf("Failed to update account: %v", err)
							}
						}
					}
				}
				if height%10000 == 0 {
					log.Printf("[%d]", height)
				}
				if updates > 1 {
				}
			case *events.BlockchainUpdated_Rollback_:
				log.Printf("[%d] Rollback", height)
			default:
				log.Printf("[%d] Unknown event", height)
			}
		}
	}
	if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
