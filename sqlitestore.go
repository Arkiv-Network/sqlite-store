package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/events"
	query "github.com/Arkiv-Network/query-api/query"
	"github.com/Arkiv-Network/query-api/sqlstore"
	"github.com/Arkiv-Network/sqlite-store/store"
)

type SQLiteStore struct {
	writePool *sql.DB
	readPool  *sql.DB
	log       *slog.Logger
}

func NewSQLiteStore(
	log *slog.Logger,
	dbPath string,
	numberOfReadThreads int,
) (*SQLiteStore, error) {

	err := os.MkdirAll(filepath.Dir(dbPath), 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	writeURL := fmt.Sprintf("file:%s?mode=rwc&_busy_timeout=11000&_journal_mode=WAL&_auto_vacuum=incremental&_foreign_keys=true&_txlock=immediate&_cache_size=65536", dbPath)

	writePool, err := sql.Open("sqlite3", writeURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open write pool: %w", err)
	}

	readURL := fmt.Sprintf("file:%s?_query_only=true&_busy_timeout=11000&_journal_mode=WAL&_auto_vacuum=incremental&_foreign_keys=true&_txlock=deferred&_cache_size=65536", dbPath)
	readPool, err := sql.Open("sqlite3", readURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open read pool: %w", err)
	}

	readPool.SetMaxOpenConns(numberOfReadThreads)
	readPool.SetMaxIdleConns(numberOfReadThreads)
	readPool.SetConnMaxLifetime(0)
	readPool.SetConnMaxIdleTime(0)

	err = runMigrations(writePool)
	if err != nil {
		writePool.Close()
		readPool.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &SQLiteStore{writePool: writePool, readPool: readPool, log: log}, nil
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
	return s.writePool.Close()
}

func (s *SQLiteStore) GetLastBlock(ctx context.Context) (int64, error) {
	return store.New(s.writePool).GetLastBlock(ctx)
}

func (s *SQLiteStore) FollowEvents(ctx context.Context, iterator arkivevents.BatchIterator) error {

	for batch := range iterator {
		if batch.Error != nil {
			return fmt.Errorf("failed to follow events: %w", batch.Error)
		}

		err := func() error {

			tx, err := s.writePool.BeginTx(ctx, &sql.TxOptions{
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

			lastBlockFromDB, err := st.GetLastBlock(ctx)
			if err != nil {
				return fmt.Errorf("failed to get last block from database: %w", err)
			}

		mainLoop:
			for _, block := range batch.Batch.Blocks {

				if block.Number <= uint64(lastBlockFromDB) {
					s.log.Info("skipping block", "block", block.Number, "lastBlockFromDB", lastBlockFromDB)
					continue mainLoop
				}

				updatesMap := map[common.Hash][]*events.OPUpdate{}

				for _, operation := range block.Operations {
					if operation.Update != nil {
						currentUpdates := updatesMap[operation.Update.Key]
						currentUpdates = append(currentUpdates, operation.Update)
						updatesMap[operation.Update.Key] = currentUpdates
					}
				}

				// blockNumber := block.Number
				for _, operation := range block.Operations {

					switch {

					case operation.Create != nil:
						// expiresAtBlock := blockNumber + operation.Create.BTL

						key := operation.Create.Key

						stringAttributes := maps.Clone(operation.Create.StringAttributes)

						stringAttributes["$owner"] = strings.ToLower(operation.Create.Owner.Hex())
						stringAttributes["$creator"] = strings.ToLower(operation.Create.Owner.Hex())
						stringAttributes["$key"] = strings.ToLower(key.Hex())

						stringAttributesBytes, err := json.Marshal(stringAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal string attributes: %w", err)
						}

						untilBlock := block.Number + operation.Create.BTL
						numericAttributes := maps.Clone(operation.Create.NumericAttributes)
						numericAttributes["$expiration"] = uint64(untilBlock)
						numericAttributes["$createdAtBlock"] = uint64(block.Number)

						sequence := block.Number<<32 | operation.TxIndex<<16 | operation.OpIndex
						numericAttributes["$sequence"] = sequence
						numericAttributes["$txIndex"] = uint64(operation.TxIndex)
						numericAttributes["$opIndex"] = uint64(operation.OpIndex)

						numericAttributesBytes, err := json.Marshal(numericAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal numeric attributes: %w", err)
						}

						err = st.InsertPayload(
							ctx,
							store.InsertPayloadParams{
								EntityKey:         operation.Create.Key.Bytes(),
								FromBlock:         store.Uint64(block.Number),
								ToBlock:           store.Uint64(untilBlock),
								Payload:           operation.Create.Content,
								ContentType:       operation.Create.ContentType,
								StringAttributes:  string(stringAttributesBytes),
								NumericAttributes: string(numericAttributesBytes),
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload %s at block %d txIndex %d opIndex %d: %w", key.Hex(), block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						for k, v := range stringAttributes {
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

						for k, v := range numericAttributes {
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
					case operation.Update != nil:

						updates := updatesMap[operation.Update.Key]
						lastUpdate := updates[len(updates)-1]

						if operation.Update != lastUpdate {
							continue mainLoop
						}

						key := operation.Update.Key.Bytes()

						s.log.Info("update", "key", common.BytesToHash(key).Hex())

						latestPayload, err := st.GetLatestPayload(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldStringAttributes := map[string]string{}

						err = json.Unmarshal([]byte(latestPayload.StringAttributes), &oldStringAttributes)
						if err != nil {
							return fmt.Errorf("failed to unmarshal string attributes: %w", err)
						}

						oldNumericAttributes := map[string]uint64{}
						err = json.Unmarshal([]byte(latestPayload.NumericAttributes), &oldNumericAttributes)
						if err != nil {
							return fmt.Errorf("failed to unmarshal numeric attributes: %w", err)
						}

						latestFromBlock := latestPayload.FromBlock

						err = st.TerminateNumericAttributesAtBlock(ctx, store.TerminateNumericAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestFromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate numeric attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminateStringAttributesAtBlock(ctx, store.TerminateStringAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestFromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate string attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminatePayloadsAtBlock(ctx, store.TerminatePayloadsAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestFromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate payloads at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						stringAttributes := maps.Clone(operation.Update.StringAttributes)

						stringAttributes["$owner"] = strings.ToLower(operation.Update.Owner.Hex())
						stringAttributes["$creator"] = oldStringAttributes["$creator"]
						stringAttributes["$key"] = strings.ToLower(operation.Update.Key.Hex())

						stringAttributesBytes, err := json.Marshal(stringAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal string attributes: %w", err)
						}

						untilBlock := block.Number + operation.Update.BTL
						numericAttributes := maps.Clone(operation.Update.NumericAttributes)
						numericAttributes["$expiration"] = uint64(untilBlock)
						numericAttributes["$createdAtBlock"] = oldNumericAttributes["$createdAtBlock"]

						numericAttributes["$sequence"] = oldNumericAttributes["$sequence"]
						numericAttributes["$txIndex"] = oldNumericAttributes["$txIndex"]
						numericAttributes["$opIndex"] = oldNumericAttributes["$opIndex"]

						numericAttributesBytes, err := json.Marshal(numericAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal numeric attributes: %w", err)
						}

						err = st.InsertPayload(
							ctx,
							store.InsertPayloadParams{
								EntityKey:         key,
								FromBlock:         store.Uint64(block.Number),
								ToBlock:           store.Uint64(untilBlock),
								Payload:           operation.Update.Content,
								ContentType:       operation.Update.ContentType,
								StringAttributes:  string(stringAttributesBytes),
								NumericAttributes: string(numericAttributesBytes),
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload 0x%x at block %d txIndex %d opIndex %d: %w", key, block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						for k, v := range stringAttributes {
							err = st.InsertStringAttribute(ctx, store.InsertStringAttributeParams{
								EntityKey: key,
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(block.Number + operation.Update.BTL),
								Key:       k,
								Value:     v,
							})
							if err != nil {
								return fmt.Errorf("failed to insert string attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}

						for k, v := range numericAttributes {
							err = st.InsertNumericAttribute(ctx, store.InsertNumericAttributeParams{
								EntityKey: key,
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(block.Number + operation.Update.BTL),
								Key:       k,
								Value:     store.Uint64(v),
							})
							if err != nil {
								return fmt.Errorf("failed to insert numeric attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}

					case operation.Delete != nil || operation.Expire != nil:

						var key []byte
						if operation.Delete != nil {
							key = common.Hash(*operation.Delete).Bytes()
						} else {
							key = common.Hash(*operation.Expire).Bytes()
						}

						s.log.Info("delete or expire", "key", common.BytesToHash(key).Hex())

						latestPayload, err := st.GetLatestPayload(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						err = st.TerminatePayloadsAtBlock(ctx, store.TerminatePayloadsAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate payloads at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminateStringAttributesAtBlock(ctx, store.TerminateStringAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate string attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminateNumericAttributesAtBlock(ctx, store.TerminateNumericAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate numeric attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

					case operation.ExtendBTL != nil:

						key := operation.ExtendBTL.Key.Bytes()

						s.log.Info("extend BTL", "key", common.BytesToHash(key).Hex())

						latestPayload, err := st.GetLatestPayload(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						err = st.TerminatePayloadsAtBlock(ctx, store.TerminatePayloadsAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate payloads at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminateStringAttributesAtBlock(ctx, store.TerminateStringAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate string attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminateNumericAttributesAtBlock(ctx, store.TerminateNumericAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate numeric attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						oldNumericAttributes := map[string]uint64{}
						err = json.Unmarshal([]byte(latestPayload.NumericAttributes), &oldNumericAttributes)
						if err != nil {
							return fmt.Errorf("failed to unmarshal numeric attributes: %w", err)
						}

						newToBlock := block.Number + operation.ExtendBTL.BTL

						numericAttributes := maps.Clone(oldNumericAttributes)
						numericAttributes["$expiration"] = uint64(newToBlock)
						numericAttributesBytes, err := json.Marshal(numericAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal numeric attributes: %w", err)
						}

						err = st.InsertPayload(ctx, store.InsertPayloadParams{
							EntityKey:         key,
							FromBlock:         store.Uint64(block.Number),
							ToBlock:           store.Uint64(newToBlock),
							Payload:           latestPayload.Payload,
							ContentType:       latestPayload.ContentType,
							StringAttributes:  latestPayload.StringAttributes,
							NumericAttributes: string(numericAttributesBytes),
						})
						if err != nil {
							return fmt.Errorf("failed to insert payload at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						for k, v := range numericAttributes {
							err = st.InsertNumericAttribute(ctx, store.InsertNumericAttributeParams{
								EntityKey: key,
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(newToBlock),
								Key:       k,
								Value:     store.Uint64(v),
							})
							if err != nil {
								return fmt.Errorf("failed to insert numeric attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}

						stringAttributes := map[string]string{}
						err = json.Unmarshal([]byte(latestPayload.StringAttributes), &stringAttributes)
						if err != nil {
							return fmt.Errorf("failed to unmarshal string attributes: %w", err)
						}

						for k, v := range stringAttributes {
							err = st.InsertStringAttribute(ctx, store.InsertStringAttributeParams{
								EntityKey: key,
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(newToBlock),
								Key:       k,
								Value:     v,
							})
							if err != nil {
								return fmt.Errorf("failed to insert string attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}

					case operation.ChangeOwner != nil:
						key := operation.ChangeOwner.Key.Bytes()
						s.log.Info("change owner", "key", common.BytesToHash(key).Hex())

						latestPayload, err := st.GetLatestPayload(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						err = st.TerminatePayloadsAtBlock(ctx, store.TerminatePayloadsAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate payloads at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminateStringAttributesAtBlock(ctx, store.TerminateStringAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate string attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						err = st.TerminateNumericAttributesAtBlock(ctx, store.TerminateNumericAttributesAtBlockParams{
							EntityKey: key,
							ToBlock:   store.Uint64(block.Number),
							FromBlock: latestPayload.FromBlock,
						})
						if err != nil {
							return fmt.Errorf("failed to terminate numeric attributes at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						stringAttributes := map[string]string{}
						err = json.Unmarshal([]byte(latestPayload.StringAttributes), &stringAttributes)
						if err != nil {
							return fmt.Errorf("failed to unmarshal string attributes: %w", err)
						}

						stringAttributes["$owner"] = strings.ToLower(operation.ChangeOwner.Owner.Hex())
						stringAttributesBytes, err := json.Marshal(stringAttributes)
						if err != nil {
							return fmt.Errorf("failed to marshal string attributes: %w", err)
						}

						err = st.InsertPayload(ctx, store.InsertPayloadParams{
							EntityKey:         key,
							FromBlock:         store.Uint64(block.Number),
							ToBlock:           store.Uint64(latestPayload.OldToBlock),
							Payload:           latestPayload.Payload,
							ContentType:       latestPayload.ContentType,
							StringAttributes:  string(stringAttributesBytes),
							NumericAttributes: latestPayload.NumericAttributes,
						})
						if err != nil {
							return fmt.Errorf("failed to insert payload at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						for k, v := range stringAttributes {
							err = st.InsertStringAttribute(ctx, store.InsertStringAttributeParams{
								EntityKey: key,
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(latestPayload.OldToBlock),
								Key:       k,
								Value:     v,
							})

							if err != nil {
								return fmt.Errorf("failed to insert string attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}

						numericAttributes := map[string]uint64{}
						err = json.Unmarshal([]byte(latestPayload.NumericAttributes), &numericAttributes)
						if err != nil {
							return fmt.Errorf("failed to unmarshal numeric attributes: %w", err)
						}

						for k, v := range numericAttributes {
							err = st.InsertNumericAttribute(ctx, store.InsertNumericAttributeParams{
								EntityKey: key,
								FromBlock: store.Uint64(block.Number),
								ToBlock:   store.Uint64(latestPayload.OldToBlock),
								Key:       k,
								Value:     store.Uint64(v),
							})
							if err != nil {
								return fmt.Errorf("failed to insert numeric attribute %s at block %d txIndex %d opIndex %d: %w", k, block.Number, operation.TxIndex, operation.OpIndex, err)
							}
						}
					default:
						return fmt.Errorf("unknown operation: %v", operation)
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

func (s *SQLiteStore) QueryEntities(
	ctx context.Context,
	queryStr string,
	options *query.Options,
	sqlDialect string,
) (*query.QueryResponse, error) {
	store := sqlstore.NewSQLStoreFromDB(s.readPool, s.log)

	response, err := store.QueryEntities(
		ctx,
		queryStr,
		options,
		sqlDialect,
	)
	if err != nil {
		return nil, fmt.Errorf("error calling query API: %w", err)
	}

	return response, nil
}
