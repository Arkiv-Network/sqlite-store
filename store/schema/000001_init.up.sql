CREATE TABLE string_attributes (
    entity_key BLOB NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (entity_key, key, from_block)
);

CREATE TABLE numeric_attributes (
    entity_key BLOB NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    key TEXT NOT NULL,
    value INTEGER NOT NULL,
    PRIMARY KEY (entity_key, key, from_block)
);

CREATE TABLE payloads (
    entity_key BLOB NOT NULL,
    from_block INTEGER NOT NULL,
    to_block INTEGER NOT NULL,
    payload BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    string_attributes TEXT NOT NULL DEFAULT '{}',
    numeric_attributes TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (entity_key, from_block)
);

CREATE TABLE last_block (
    id INTEGER NOT NULL DEFAULT 1 CHECK (id = 1),
    block INTEGER NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO last_block (id, block) VALUES (1, 0);

-- Indexes for string_attributes
CREATE INDEX string_attributes_entity_key_value_index ON string_attributes (from_block, to_block, key, value);
CREATE INDEX string_attributes_kv_temporal_idx ON string_attributes (key, value, from_block DESC, to_block DESC);
CREATE INDEX string_attributes_entity_key_index ON string_attributes (from_block, to_block, key);
CREATE INDEX string_attributes_delete_index ON string_attributes (to_block);
CREATE INDEX string_attributes_entity_kv_idx ON string_attributes (entity_key, key, from_block DESC);

-- Indexes for numeric_attributes
CREATE INDEX numeric_attributes_entity_key_value_index ON numeric_attributes (from_block, to_block, key, value);
CREATE INDEX numeric_attributes_entity_key_index ON numeric_attributes (from_block, to_block, key);
CREATE INDEX numeric_attributes_kv_temporal_idx ON numeric_attributes (key, value, from_block DESC, to_block DESC);
CREATE INDEX numeric_attributes_delete_index ON numeric_attributes (to_block);

-- Indexes for payloads
CREATE INDEX payloads_entity_key_index ON payloads (entity_key, from_block, to_block);
CREATE INDEX payloads_delete_index ON payloads (to_block);
