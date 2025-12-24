CREATE INDEX numeric_attributes_entity_kv_idx ON numeric_attributes (entity_key, key, from_block DESC);

DROP INDEX IF EXISTS payloads_entity_key_index;
-- This improves overall performance since we avoid needing to do an in-memory
-- sort in our queries. It's not the ideal order for the actual search part of
-- of the query though, so we may want to put entity_key first again in the future.
CREATE INDEX payloads_entity_key_index ON payloads (from_block, entity_key, to_block);
