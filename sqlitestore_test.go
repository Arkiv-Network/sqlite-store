package sqlitestore

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestNewSQLiteStore_RunsMigrations(t *testing.T) {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := NewSQLiteStore(logger, dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.writePool.Close()

	// Verify tables exist by querying them
	tables := []string{"string_attributes", "numeric_attributes", "payloads", "last_block"}
	for _, table := range tables {
		var name string
		err := store.writePool.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			t.Errorf("table %s not found: %v", table, err)
		}
	}

	// Verify last_block has initial row
	var block int64
	err = store.writePool.QueryRow("SELECT block FROM last_block WHERE id = 1").Scan(&block)
	if err != nil {
		t.Fatalf("failed to query last_block: %v", err)
	}
	if block != 0 {
		t.Errorf("expected initial block to be 0, got %d", block)
	}
}

func TestNewSQLiteStore_MigrationsIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// First open
	store1, err := NewSQLiteStore(logger, dbPath)
	if err != nil {
		t.Fatalf("first NewSQLiteStore failed: %v", err)
	}
	store1.writePool.Close()

	// Second open should not fail (migrations already applied)
	store2, err := NewSQLiteStore(logger, dbPath)
	if err != nil {
		t.Fatalf("second NewSQLiteStore failed: %v", err)
	}
	defer store2.writePool.Close()

	// Verify tables still exist
	var count int
	err = store2.writePool.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('string_attributes', 'numeric_attributes', 'payloads', 'last_block')").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count tables: %v", err)
	}
	if count != 4 {
		t.Errorf("expected 4 tables, got %d", count)
	}
}

func TestNewSQLiteStore_InvalidPath(t *testing.T) {
	// Try to create a database in a non-existent directory
	dbPath := "/nonexistent/directory/test.db"
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	_, err := NewSQLiteStore(logger, dbPath)
	if err == nil {
		t.Error("expected error for invalid path, got nil")
	}
}

func TestNewSQLiteStore_FileCreated(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Verify file doesn't exist
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatal("database file should not exist before NewSQLiteStore")
	}

	store, err := NewSQLiteStore(logger, dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore failed: %v", err)
	}
	defer store.writePool.Close()

	// Verify file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("database file should exist after NewSQLiteStore")
	}
}
