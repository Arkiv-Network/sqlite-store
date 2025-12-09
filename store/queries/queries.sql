-- name: InsertStringAttribute :exec
INSERT INTO string_attributes (
    entity_key,
    from_block,
    to_block,
    key,
    value
) VALUES (?, ?, ?, ?, ?);

-- name: InsertNumericAttribute :exec
INSERT INTO numeric_attributes (
    entity_key,
    from_block,
    to_block,
    key,
    value
) VALUES (?, ?, ?, ?, ?);

-- name: InsertPayload :exec
INSERT INTO payloads (
    entity_key,
    from_block,
    to_block,
    payload,
    content_type,
    string_attributes,
    numeric_attributes
) VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: DeleteStringAttributesBeforeBlock :exec
DELETE FROM string_attributes
WHERE from_block < ?;

-- name: DeleteNumericAttributesBeforeBlock :exec
DELETE FROM numeric_attributes
WHERE from_block < ?;

-- name: DeletePayloadsBeforeBlock :exec
DELETE FROM payloads
WHERE from_block < ?;

-- name: UpsertLastBlock :exec
INSERT INTO last_block (id, block)
VALUES (1, ?)
ON CONFLICT (id) DO UPDATE SET block = EXCLUDED.block;

-- name: GetLastBlock :one
SELECT block FROM last_block;

-- name: GetCreator :one
SELECT value FROM string_attributes
WHERE entity_key = ? AND key = '$creator' AND from_block <= ?
ORDER BY from_block DESC
LIMIT 1;

-- TerminateEntityAtBlock is split into 3 separate queries for SQLite compatibility
-- name: TerminatePayloadsAtBlock :exec
UPDATE payloads
SET to_block = sqlc.arg(to_block)
WHERE entity_key = sqlc.arg(entity_key) AND from_block = sqlc.arg(from_block);

-- name: TerminateStringAttributesAtBlock :exec
UPDATE string_attributes
SET to_block = sqlc.arg(to_block)
WHERE entity_key = sqlc.arg(entity_key) AND from_block = sqlc.arg(from_block);

-- name: TerminateNumericAttributesAtBlock :exec
UPDATE numeric_attributes
SET to_block = sqlc.arg(to_block)
WHERE entity_key = sqlc.arg(entity_key) AND from_block = sqlc.arg(from_block);

-- name: GetLatestPayload :one
SELECT from_block, to_block AS old_to_block, payload, content_type, string_attributes, numeric_attributes
FROM payloads
WHERE entity_key = ? ORDER BY from_block DESC LIMIT 1;

-- -- name: GetOldStringAttributes :many
-- SELECT entity_key, to_block AS old_to_block, key, value
-- FROM string_attributes
-- WHERE entity_key = ? AND from_block <= ? AND to_block > ?;

-- -- name: GetOldNumericAttributes :many
-- SELECT entity_key, to_block AS old_to_block, key, value
-- FROM numeric_attributes
-- WHERE entity_key = ? AND from_block <= ? AND to_block > ?;

-- -- ChangeToBlock helper queries (complex operation implemented in Go)
-- -- name: GetOldPayloadsForToBlockChange :many
-- SELECT entity_key, payload, content_type, string_attributes, numeric_attributes
-- FROM payloads
-- WHERE entity_key = ? AND from_block <= ? AND to_block > ?;

-- -- name: GetOldStringAttributesForToBlockChange :many
-- SELECT entity_key, key, value
-- FROM string_attributes
-- WHERE entity_key = ? AND from_block <= ? AND to_block > ?;

-- -- name: GetOldNumericAttributesForToBlockChange :many
-- SELECT entity_key, key, value
-- FROM numeric_attributes
-- WHERE entity_key = ? AND from_block <= ? AND to_block > ?;
