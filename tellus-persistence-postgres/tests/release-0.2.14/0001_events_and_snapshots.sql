CREATE TABLE IF NOT EXISTS events (
    entity_type    TEXT   NOT NULL,
    entity_id      TEXT   NOT NULL,
    seq_no         BIGINT NOT NULL,
    manifest       TEXT   NOT NULL,
    schema_version INT    NOT NULL,
    payload        BYTEA  NOT NULL,
    PRIMARY KEY (entity_type, entity_id, seq_no)
);

CREATE TABLE IF NOT EXISTS snapshots (
    entity_type    TEXT   NOT NULL,
    entity_id      TEXT   NOT NULL,
    next_seq_no    BIGINT NOT NULL,
    manifest       TEXT   NOT NULL,
    schema_version INT    NOT NULL,
    payload        BYTEA  NOT NULL,
    PRIMARY KEY (entity_type, entity_id)
);
