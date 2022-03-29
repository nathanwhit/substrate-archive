CREATE TABLE IF NOT EXISTS decoded_storage (
    id SERIAL PRIMARY KEY,
    raw_storage_id INTEGER UNIQUE NOT NULL REFERENCES storage(id) ON DELETE CASCADE ON UPDATE CASCADE,
    hash bytea NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
    block_num int check (block_num >= 0 and block_num < 2147483647) NOT NULL,
    module text NOT NULL,
    name text NOT NULL,
    entry jsonb NOT NULL
);

