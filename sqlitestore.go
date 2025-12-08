package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/sqlite-store/store"
)

type SQLiteStore struct {
	db  *sql.DB
	log *slog.Logger
}

func NewSQLiteStore(
	log *slog.Logger,
	dbPath string,
) (*SQLiteStore, error) {

	err := os.MkdirAll(filepath.Dir(dbPath), 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	err = runMigrations(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &SQLiteStore{db: db, log: log}, nil
}

func runMigrations(db *sql.DB) error {
	sourceDriver, err := iofs.New(store.Migrations, "schema")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	dbDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", dbDriver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) GetLastBlock(ctx context.Context) (int64, error) {
	return store.New(s.db).GetLastBlock(ctx)
}

func (s *SQLiteStore) FollowEvents(ctx context.Context, iterator arkivevents.BatchIterator) error {

	for batch := range iterator {
		if batch.Error != nil {
			return fmt.Errorf("failed to follow events: %w", batch.Error)
		}

		err := func() error {

			tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
				Isolation: sql.LevelSerializable,
				ReadOnly:  false,
			})
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			st := store.New(tx)

			firstBlock := batch.Batch.Blocks[0].Number
			lastBlock := batch.Batch.Blocks[len(batch.Batch.Blocks)-1].Number
			s.log.Info("new batch", "firstBlock", firstBlock, "lastBlock", lastBlock)

			for _, block := range batch.Batch.Blocks {

				// blockNumber := block.Number
				for _, operation := range block.Operations {

					switch {
					case operation.Create != nil:
						// expiresAtBlock := blockNumber + operation.Create.BTL

						stringAttributesBytes, err := json.Marshal(operation.Create.StringAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal string attributes: %w", err)
						}

						numericAttributesBytes, err := json.Marshal(operation.Create.NumericAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal numeric attributes: %w", err)
						}

						err = st.InsertPayload(
							ctx,
							store.InsertPayloadParams{
								EntityKey:         operation.Create.Key.Bytes(),
								FromBlock:         store.Uint64(block.Number),
								ToBlock:           store.Uint64(block.Number + operation.Create.BTL),
								Payload:           operation.Create.Content,
								ContentType:       operation.Create.ContentType,
								StringAttributes:  string(stringAttributesBytes),
								NumericAttributes: string(numericAttributesBytes),
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload %s at block %d txIndex %d opIndex %d: %w", operation.Create.Key.Hex(), block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						for k, v := range operation.Create.StringAttributes {
							err = st.InsertStringAttribute(ctx, store.InsertStringAttributeParams{
								EntityKey: operation.Create.Key.Bytes(),
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(block.Number + operation.Create.BTL),
								Key:       k,
								Value:     v,
							})
							if err != nil {
								return fmt.Errorf("failed to insert string attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}

						for k, v := range operation.Create.NumericAttributes {
							err = st.InsertNumericAttribute(ctx, store.InsertNumericAttributeParams{
								EntityKey: operation.Create.Key.Bytes(),
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(block.Number + operation.Create.BTL),
								Key:       k,
								Value:     store.Uint64(v),
							})
							if err != nil {
								return fmt.Errorf("failed to insert numeric attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}
					}

				}

			}

			err = st.UpsertLastBlock(ctx, int64(lastBlock))
			if err != nil {
				return fmt.Errorf("failed to upsert last block: %w", err)
			}

			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}
