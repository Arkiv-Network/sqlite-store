package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Arkiv-Network/query-api/query"
	sqlitestore "github.com/Arkiv-Network/sqlite-store"
	"github.com/urfave/cli/v2"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case "sqlQuery", "args", "executionTime", "msg":
				return a
			default:
				return slog.Attr{}
			}
		},
	}))

	cfg := struct {
		dbPath string
	}{}

	app := &cli.App{
		Name:  "load-from-node",
		Usage: "Load data from a node into a SQLite database",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "db-path",
				Value:       "arkiv-data.db",
				Destination: &cfg.dbPath,
				EnvVars:     []string{"DB_PATH"},
			},
		},
		Action: func(c *cli.Context) error {

			q := c.Args().First()

			if q == "" {
				return fmt.Errorf("query is required")
			}

			store, err := sqlitestore.NewSQLiteStore(logger, cfg.dbPath, 7)
			if err != nil {
				return fmt.Errorf("failed to create SQLite store: %w", err)
			}
			defer store.Close()

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			startTime := time.Now()

			response, err := store.QueryEntities(ctx, q, &query.Options{
				IncludeData: &query.IncludeData{
					Key:         true,
					Expiration:  true,
					Owner:       true,
					Payload:     true,
					ContentType: true,
					Attributes:  true,
				},
				ResultsPerPage: 20,
			}, "sqlite")
			if err != nil {
				return fmt.Errorf("failed to query entities: %w", err)
			}

			elapsed := time.Since(startTime)

			logger.Info("query completed", "executionTime", elapsed.Seconds())

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			enc.Encode(response)

			return nil

		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
