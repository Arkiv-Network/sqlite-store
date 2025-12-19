package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/rpciterator"
	sqlitestore "github.com/Arkiv-Network/sqlite-store"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/urfave/cli/v2"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		nodeURL string
		dbPath  string
	}{}

	app := &cli.App{
		Name:  "query",
		Usage: "Load data from a node into a SQLite database",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "node-url",
				Value:       "http://localhost:8545",
				Destination: &cfg.nodeURL,
				EnvVars:     []string{"NODE_URL"},
			},
			&cli.PathFlag{
				Name:        "db-path",
				Value:       "arkiv-data.db",
				Destination: &cfg.dbPath,
				EnvVars:     []string{"DB_PATH"},
			},
		},
		Action: func(c *cli.Context) error {

			store, err := sqlitestore.NewSQLiteStore(logger, cfg.dbPath, 7)
			if err != nil {
				return fmt.Errorf("failed to create SQLite store: %w", err)
			}
			defer store.Close()

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			lastBlock, err := store.GetLastBlock(ctx)
			if err != nil {
				return fmt.Errorf("failed to get last block: %w", err)
			}

			logger.Info("last block", "block", lastBlock)

			rpcClient, err := rpc.DialContext(ctx, cfg.nodeURL)
			if err != nil {
				return fmt.Errorf("failed to dial RPC client: %w", err)
			}
			defer rpcClient.Close()

			iterator := rpciterator.IterateBlocks(ctx, logger, rpcClient, uint64(lastBlock+1))

			err = store.FollowEvents(ctx, arkivevents.BatchIterator(iterator))
			if err != nil {
				return fmt.Errorf("failed to follow events: %w", err)
			}

			return nil

		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
