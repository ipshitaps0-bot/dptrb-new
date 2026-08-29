-- DPTRB persistence layer schema.
-- Executed automatically on first container startup via the
-- postgres image's /docker-entrypoint-initdb.d convention.

CREATE TABLE IF NOT EXISTS stations (
    station_id      VARCHAR(32) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    platform_count  SMALLINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS route_edges (
    id                          SERIAL PRIMARY KEY,
    origin_station_id           VARCHAR(32) NOT NULL REFERENCES stations(station_id),
    destination_station_id      VARCHAR(32) NOT NULL REFERENCES stations(station_id),
    base_weight                 DOUBLE PRECISION NOT NULL,
    capacity                    INTEGER NOT NULL,
    UNIQUE (origin_station_id, destination_station_id)
);

CREATE TABLE IF NOT EXISTS monitored_corridors (
    id                          SERIAL PRIMARY KEY,
    origin_station_id           VARCHAR(32) NOT NULL REFERENCES stations(station_id),
    destination_station_id      VARCHAR(32) NOT NULL REFERENCES stations(station_id),
    UNIQUE (origin_station_id, destination_station_id)
);

CREATE TABLE IF NOT EXISTS ridership_events (
    id                  BIGSERIAL PRIMARY KEY,
    station_id          VARCHAR(32) NOT NULL,
    turnstile_id        VARCHAR(48) NOT NULL,
    direction           VARCHAR(3)  NOT NULL CHECK (direction IN ('IN', 'OUT')),
    passenger_count     INTEGER NOT NULL,
    event_timestamp     TIMESTAMPTZ NOT NULL,
    sequence_id         VARCHAR(24) NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sequence_id)
);

CREATE INDEX IF NOT EXISTS idx_ridership_events_station_ts
    ON ridership_events (station_id, event_timestamp DESC);

-- Seed topology: a minimal four-station corridor used for local
-- development and demonstration purposes.
INSERT INTO stations (station_id, name, platform_count) VALUES
    ('STN-001', 'Central Junction', 2),
    ('STN-002', 'Riverside Interchange', 2),
    ('STN-003', 'Eastgate Terminal', 1),
    ('STN-004', 'Northfield Depot', 1)
ON CONFLICT (station_id) DO NOTHING;

INSERT INTO route_edges (origin_station_id, destination_station_id, base_weight, capacity) VALUES
    ('STN-001', 'STN-002', 4.5, 300),
    ('STN-002', 'STN-001', 4.5, 300),
    ('STN-002', 'STN-003', 6.0, 250),
    ('STN-003', 'STN-002', 6.0, 250),
    ('STN-001', 'STN-004', 3.0, 200),
    ('STN-004', 'STN-001', 3.0, 200),
    ('STN-004', 'STN-003', 7.5, 220),
    ('STN-003', 'STN-004', 7.5, 220)
ON CONFLICT (origin_station_id, destination_station_id) DO NOTHING;

INSERT INTO monitored_corridors (origin_station_id, destination_station_id) VALUES
    ('STN-001', 'STN-003'),
    ('STN-004', 'STN-002')
ON CONFLICT (origin_station_id, destination_station_id) DO NOTHING;
