from __future__ import annotations

import csv
from pathlib import Path

import psycopg

from app.config import DATASET_DIR, get_settings
from app.database import get_admin_conninfo, get_project_conninfo


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS albums (
    album_id TEXT PRIMARY KEY,
    album_name TEXT NOT NULL,
    release_date DATE,
    external_source TEXT,
    external_album_ref TEXT
);

CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT NOT NULL,
    external_source TEXT,
    external_artist_ref TEXT
);

CREATE TABLE IF NOT EXISTS tracks (
    track_id TEXT PRIMARY KEY,
    track_name TEXT NOT NULL,
    album_id TEXT REFERENCES albums(album_id),
    release_date DATE,
    duration_ms INTEGER CHECK (duration_ms IS NULL OR duration_ms > 0),
    popularity INTEGER CHECK (popularity IS NULL OR popularity BETWEEN 0 AND 100),
    external_source TEXT,
    external_track_ref TEXT
);

CREATE TABLE IF NOT EXISTS track_audio_features (
    track_id TEXT PRIMARY KEY REFERENCES tracks(track_id) ON DELETE CASCADE,
    tempo DOUBLE PRECISION CHECK (tempo IS NULL OR tempo > 0),
    energy DOUBLE PRECISION CHECK (energy IS NULL OR energy BETWEEN 0 AND 1),
    valence DOUBLE PRECISION CHECK (valence IS NULL OR valence BETWEEN 0 AND 1),
    danceability DOUBLE PRECISION CHECK (danceability IS NULL OR danceability BETWEEN 0 AND 1),
    acousticness DOUBLE PRECISION CHECK (acousticness IS NULL OR acousticness BETWEEN 0 AND 1),
    speechiness DOUBLE PRECISION CHECK (speechiness IS NULL OR speechiness BETWEEN 0 AND 1),
    liveness DOUBLE PRECISION CHECK (liveness IS NULL OR liveness BETWEEN 0 AND 1),
    loudness DOUBLE PRECISION,
    instrumentalness DOUBLE PRECISION CHECK (instrumentalness IS NULL OR instrumentalness BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS album_artists (
    album_id TEXT REFERENCES albums(album_id) ON DELETE CASCADE,
    artist_id TEXT REFERENCES artists(artist_id) ON DELETE CASCADE,
    artist_role TEXT NOT NULL,
    credit_order INTEGER CHECK (credit_order IS NULL OR credit_order > 0),
    PRIMARY KEY (album_id, artist_id, artist_role)
);

CREATE TABLE IF NOT EXISTS track_artists (
    track_id TEXT REFERENCES tracks(track_id) ON DELETE CASCADE,
    artist_id TEXT REFERENCES artists(artist_id) ON DELETE CASCADE,
    artist_role TEXT NOT NULL,
    credit_order INTEGER CHECK (credit_order IS NULL OR credit_order > 0),
    PRIMARY KEY (track_id, artist_id, artist_role)
);

CREATE TABLE IF NOT EXISTS sampling_relations (
    sampling_relation_id TEXT PRIMARY KEY,
    sampling_track_id TEXT REFERENCES tracks(track_id) ON DELETE CASCADE,
    sampled_track_id TEXT REFERENCES tracks(track_id) ON DELETE CASCADE,
    relation_type TEXT,
    verification_level TEXT,
    note TEXT,
    CONSTRAINT chk_sampling_not_self CHECK (sampling_track_id <> sampled_track_id),
    CONSTRAINT uq_sampling_pair UNIQUE (sampling_track_id, sampled_track_id)
);

CREATE TABLE IF NOT EXISTS cover_relations (
    cover_relation_id TEXT PRIMARY KEY,
    cover_track_id TEXT REFERENCES tracks(track_id) ON DELETE CASCADE,
    original_track_id TEXT REFERENCES tracks(track_id) ON DELETE CASCADE,
    cover_type TEXT,
    verification_level TEXT,
    note TEXT,
    CONSTRAINT chk_cover_not_self CHECK (cover_track_id <> original_track_id),
    CONSTRAINT uq_cover_pair UNIQUE (cover_track_id, original_track_id)
);

CREATE TABLE IF NOT EXISTS source_notes (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    source_type TEXT,
    source_name TEXT,
    source_url_or_ref TEXT,
    verification_note TEXT,
    PRIMARY KEY (entity_type, entity_id, source_type, source_name)
);

CREATE INDEX IF NOT EXISTS idx_tracks_album_id ON tracks(album_id);
CREATE INDEX IF NOT EXISTS idx_track_artists_artist_id ON track_artists(artist_id);
CREATE INDEX IF NOT EXISTS idx_track_artists_track_id ON track_artists(track_id);
CREATE INDEX IF NOT EXISTS idx_sampling_sampling_track ON sampling_relations(sampling_track_id);
CREATE INDEX IF NOT EXISTS idx_sampling_sampled_track ON sampling_relations(sampled_track_id);
CREATE INDEX IF NOT EXISTS idx_cover_cover_track ON cover_relations(cover_track_id);
CREATE INDEX IF NOT EXISTS idx_cover_original_track ON cover_relations(original_track_id);
"""


LOAD_PLAN = [
    (
        "albums.csv",
        "albums",
        ["album_id", "album_name", "release_date", "external_source", "external_album_ref"],
    ),
    (
        "artists.csv",
        "artists",
        ["artist_id", "artist_name", "external_source", "external_artist_ref"],
    ),
    (
        "tracks.csv",
        "tracks",
        [
            "track_id",
            "track_name",
            "album_id",
            "release_date",
            "duration_ms",
            "popularity",
            "external_source",
            "external_track_ref",
        ],
    ),
    (
        "album_artists.csv",
        "album_artists",
        ["album_id", "artist_id", "artist_role", "credit_order"],
    ),
    (
        "track_artists.csv",
        "track_artists",
        ["track_id", "artist_id", "artist_role", "credit_order"],
    ),
    (
        "track_audio_features.csv",
        "track_audio_features",
        [
            "track_id",
            "tempo",
            "energy",
            "valence",
            "danceability",
            "acousticness",
            "speechiness",
            "liveness",
            "loudness",
            "instrumentalness",
        ],
    ),
    (
        "sampling_relations.csv",
        "sampling_relations",
        [
            "sampling_relation_id",
            "sampling_track_id",
            "sampled_track_id",
            "relation_type",
            "verification_level",
            "note",
        ],
    ),
    (
        "cover_relations.csv",
        "cover_relations",
        [
            "cover_relation_id",
            "cover_track_id",
            "original_track_id",
            "cover_type",
            "verification_level",
            "note",
        ],
    ),
    (
        "source_notes.csv",
        "source_notes",
        [
            "entity_type",
            "entity_id",
            "source_type",
            "source_name",
            "source_url_or_ref",
            "verification_note",
        ],
    ),
]


def create_database_if_needed() -> None:
    settings = get_settings()
    project_database = str(settings["project_database"])
    with psycopg.connect(get_admin_conninfo(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %(db_name)s;",
                {"db_name": project_database},
            )
            exists = cur.fetchone()
            if not exists:
                cur.execute(f'CREATE DATABASE "{project_database}"')
                print(f"Created database: {project_database}")
            else:
                print(f"Database already exists: {project_database}")


def reset_schema() -> None:
    with psycopg.connect(get_project_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DROP TABLE IF EXISTS
                    source_notes,
                    cover_relations,
                    sampling_relations,
                    track_audio_features,
                    track_artists,
                    album_artists,
                    tracks,
                    artists,
                    albums
                CASCADE;
                """
            )
            cur.execute(SCHEMA_SQL)
        conn.commit()


def clean_value(value: str):
    if value == "":
        return None
    return value


def load_csv(path: Path, table_name: str, columns: list[str]) -> int:
    placeholders = ", ".join([f"%({column})s" for column in columns])
    column_sql = ", ".join(columns)
    insert_sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [{column: clean_value(row[column]) for column in columns} for row in reader]

    with psycopg.connect(get_project_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()

    return len(rows)


def load_all_data() -> None:
    for filename, table_name, columns in LOAD_PLAN:
        path = DATASET_DIR / filename
        row_count = load_csv(path, table_name, columns)
        print(f"Loaded {row_count:>3} rows into {table_name}")


def main() -> None:
    print("Preparing PostgreSQL database for Music Galaxy...")
    create_database_if_needed()
    reset_schema()
    load_all_data()
    print("Database initialization complete.")


if __name__ == "__main__":
    main()
