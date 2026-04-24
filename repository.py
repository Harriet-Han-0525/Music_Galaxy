from __future__ import annotations

from collections import defaultdict
from math import cos, sin

from psycopg import sql

from app.database import get_connection


MANAGEMENT_HIGHLIGHTS = [
    {
        "title": "3NF entity separation",
        "rationale": "Tracks, artists, albums, and audio features are stored separately so each table describes one theme and avoids transitive redundancy.",
        "implementation": "The schema keeps descriptive entities apart and uses foreign keys instead of duplicated artist or album text inside `tracks`.",
    },
    {
        "title": "Junction tables for many-to-many relationships",
        "rationale": "Artist participation is the core graph structure of the project, so it must be modeled explicitly rather than packed into string columns.",
        "implementation": "`track_artists` and `album_artists` capture role and order while supporting collaboration analysis through joins.",
    },
    {
        "title": "Self-referential relationship tables",
        "rationale": "Sampling and cover lineage are central course requirements and need clean recursive traversal.",
        "implementation": "`sampling_relations` and `cover_relations` link tracks back to tracks, which enables recursive CTEs and descendant queries.",
    },
    {
        "title": "Integrity constraints plus indexes",
        "rationale": "Good database management means protecting data quality and keeping join-heavy demos fast enough to show live.",
        "implementation": "Primary keys, foreign keys, duplicate checks, self-link prevention, and join indexes together support safe writes and responsive demo queries.",
    },
    {
        "title": "Transactional, reproducible project workflow",
        "rationale": "A course demo should be easy to reload, retest, and explain from scratch.",
        "implementation": "The bootstrap script recreates the schema and reloads CSVs deterministically, while write operations validate input before committing a transaction.",
    },
]


SQL_DEMOS = [
    {
        "slug": "sampling-lineage",
        "category": "Recursive queries",
        "title": "Sampling lineage of Gold Digger",
        "description": "Recursive SQL proves that the schema can traverse multi-hop sample ancestry.",
        "sql": """
            WITH RECURSIVE sample_chain AS (
                SELECT
                    1 AS depth,
                    sr.sampling_track_id,
                    sr.sampled_track_id
                FROM sampling_relations sr
                WHERE sr.sampling_track_id = %(track_id)s

                UNION ALL

                SELECT
                    sc.depth + 1,
                    sr.sampling_track_id,
                    sr.sampled_track_id
                FROM sampling_relations sr
                JOIN sample_chain sc
                  ON sr.sampling_track_id = sc.sampled_track_id
            )
            SELECT
                sc.depth,
                sampler.track_name AS sampling_track,
                sampled.track_name AS sampled_track
            FROM sample_chain sc
            JOIN tracks sampler ON sampler.track_id = sc.sampling_track_id
            JOIN tracks sampled ON sampled.track_id = sc.sampled_track_id
            ORDER BY sc.depth;
        """,
        "params": {"track_id": "TRK003"},
    },
    {
        "slug": "sampling-descendants",
        "category": "Recursive queries",
        "title": "Sampling descendants of It Must Be Jesus",
        "description": "Recursive SQL also works forward, tracing later tracks that ultimately descend from an earlier sampled track.",
        "sql": """
            WITH RECURSIVE sample_descendants AS (
                SELECT
                    1 AS depth,
                    sr.sampled_track_id AS root_track_id,
                    sr.sampling_track_id AS descendant_track_id
                FROM sampling_relations sr
                WHERE sr.sampled_track_id = %(track_id)s

                UNION ALL

                SELECT
                    sd.depth + 1,
                    sd.root_track_id,
                    sr.sampling_track_id AS descendant_track_id
                FROM sampling_relations sr
                JOIN sample_descendants sd
                  ON sr.sampled_track_id = sd.descendant_track_id
            )
            SELECT
                sd.depth,
                root.track_name AS root_track,
                descendant.track_name AS descendant_track
            FROM sample_descendants sd
            JOIN tracks root ON root.track_id = sd.root_track_id
            JOIN tracks descendant ON descendant.track_id = sd.descendant_track_id
            ORDER BY sd.depth, descendant.track_name;
        """,
        "params": {"track_id": "TRK001"},
    },
    {
        "slug": "collaboration-network",
        "category": "Complex joins",
        "title": "Direct collaborators of Kanye West",
        "description": "Complex joins show how a many-to-many track-artist table supports network extraction.",
        "sql": """
            SELECT
                collaborator.artist_name AS collaborator,
                COUNT(DISTINCT ta1.track_id) AS shared_tracks
            FROM track_artists ta1
            JOIN track_artists ta2
              ON ta1.track_id = ta2.track_id
             AND ta1.artist_id <> ta2.artist_id
            JOIN artists target ON target.artist_id = ta1.artist_id
            JOIN artists collaborator ON collaborator.artist_id = ta2.artist_id
            WHERE target.artist_name = %(artist_name)s
            GROUP BY collaborator.artist_name
            ORDER BY shared_tracks DESC, collaborator.artist_name
            LIMIT 10;
        """,
        "params": {"artist_name": "Kanye West"},
    },
    {
        "slug": "second-hop-collaborators",
        "category": "Complex joins",
        "title": "Second-hop collaborators of Kanye West",
        "description": "A two-step collaboration query reveals artists indirectly connected through shared intermediaries.",
        "sql": """
            WITH target_artist AS (
                SELECT artist_id
                FROM artists
                WHERE artist_name = %(artist_name)s
            ),
            direct_collaborators AS (
                SELECT DISTINCT ta2.artist_id
                FROM track_artists ta1
                JOIN track_artists ta2
                  ON ta1.track_id = ta2.track_id
                 AND ta1.artist_id <> ta2.artist_id
                WHERE ta1.artist_id = (SELECT artist_id FROM target_artist)
            ),
            second_hop_paths AS (
                SELECT
                    dc.artist_id AS via_artist_id,
                    ta3.artist_id AS second_hop_artist_id
                FROM direct_collaborators dc
                JOIN track_artists ta2
                  ON ta2.artist_id = dc.artist_id
                JOIN track_artists ta3
                  ON ta2.track_id = ta3.track_id
                 AND ta3.artist_id <> ta2.artist_id
                WHERE ta3.artist_id <> (SELECT artist_id FROM target_artist)
                  AND ta3.artist_id NOT IN (SELECT artist_id FROM direct_collaborators)
            )
            SELECT
                a2.artist_name AS second_hop_artist,
                COUNT(DISTINCT shp.via_artist_id) AS connecting_collaborators
            FROM second_hop_paths shp
            JOIN artists a2 ON a2.artist_id = shp.second_hop_artist_id
            GROUP BY a2.artist_name
            ORDER BY connecting_collaborators DESC, a2.artist_name
            LIMIT 15;
        """,
        "params": {"artist_name": "Kanye West"},
    },
    {
        "slug": "most-covered",
        "category": "Aggregation and temporal analysis",
        "title": "Most-covered original songs",
        "description": "Aggregation over self-referential cover links surfaces mother songs with many derivatives.",
        "sql": """
            SELECT
                original.track_name AS original_track,
                COUNT(*) AS number_of_covers
            FROM cover_relations cr
            JOIN tracks original ON original.track_id = cr.original_track_id
            GROUP BY original.track_name
            ORDER BY number_of_covers DESC, original.track_name
            LIMIT 10;
        """,
        "params": {},
    },
    {
        "slug": "cover-distribution-by-decade",
        "category": "Aggregation and temporal analysis",
        "title": "Cover distribution by decade for Hallelujah",
        "description": "Cover links can be combined with release dates to show how a classic song spreads across time.",
        "sql": """
            SELECT
                CONCAT((EXTRACT(YEAR FROM c.release_date)::int / 10) * 10, 's') AS decade,
                COUNT(*) AS cover_count
            FROM cover_relations cr
            JOIN tracks c ON c.track_id = cr.cover_track_id
            WHERE cr.original_track_id = %(track_id)s
              AND c.release_date IS NOT NULL
            GROUP BY decade
            ORDER BY decade;
        """,
        "params": {"track_id": "TRK081"},
    },
    {
        "slug": "feature-similarity",
        "category": "Feature analysis",
        "title": "Tracks closest to Gold Digger in feature space",
        "description": "Numeric audio features support similarity analysis directly inside SQL.",
        "sql": """
            WITH target AS (
                SELECT *
                FROM track_audio_features
                WHERE track_id = %(track_id)s
            )
            SELECT
                t.track_name,
                ROUND(
                    SQRT(
                        POWER((f.tempo - target.tempo) / 50.0, 2) +
                        POWER(f.energy - target.energy, 2) +
                        POWER(f.valence - target.valence, 2) +
                        POWER(f.danceability - target.danceability, 2)
                    )::numeric,
                    3
                ) AS feature_distance
            FROM track_audio_features f
            JOIN target ON TRUE
            JOIN tracks t ON t.track_id = f.track_id
            WHERE f.track_id <> %(track_id)s
            ORDER BY feature_distance ASC, t.track_name
            LIMIT 10;
        """,
        "params": {"track_id": "TRK003"},
    },
    {
        "slug": "extreme-feature-groups",
        "category": "Feature analysis",
        "title": "Extreme feature groups",
        "description": "Tracks with extreme valence and energy values create contrast groups for clustering and visualization.",
        "sql": """
            SELECT
                t.track_name,
                ROUND(f.energy::numeric, 2) AS energy,
                ROUND(f.valence::numeric, 2) AS valence,
                ROUND(f.tempo::numeric, 2) AS tempo,
                CASE
                    WHEN f.energy >= 0.75 AND f.valence >= 0.65 THEN 'high-energy / high-valence'
                    WHEN f.energy <= 0.35 AND f.valence <= 0.35 THEN 'low-energy / low-valence'
                    ELSE 'middle-zone'
                END AS feature_group
            FROM track_audio_features f
            JOIN tracks t ON t.track_id = f.track_id
            WHERE
                (f.energy >= 0.75 AND f.valence >= 0.65)
                OR
                (f.energy <= 0.35 AND f.valence <= 0.35)
            ORDER BY feature_group, t.track_name;
        """,
        "params": {},
    },
    {
        "slug": "most-similar-track-pairs",
        "category": "Feature analysis",
        "title": "Most similar track pairs",
        "description": "Pairwise distance analysis shows which tracks are closest in audio-feature space.",
        "sql": """
            SELECT
                t1.track_name AS track_1,
                t2.track_name AS track_2,
                ROUND(
                    SQRT(
                        POWER((f1.tempo - f2.tempo) / 50.0, 2) +
                        POWER(f1.energy - f2.energy, 2) +
                        POWER(f1.valence - f2.valence, 2) +
                        POWER(f1.danceability - f2.danceability, 2)
                    )::numeric,
                    3
                ) AS feature_distance
            FROM track_audio_features f1
            JOIN track_audio_features f2
              ON f1.track_id < f2.track_id
            JOIN tracks t1 ON t1.track_id = f1.track_id
            JOIN tracks t2 ON t2.track_id = f2.track_id
            ORDER BY feature_distance ASC, track_1, track_2
            LIMIT 10;
        """,
        "params": {},
    },
    {
        "slug": "most-dissimilar-track-pairs",
        "category": "Feature analysis",
        "title": "Most dissimilar track pairs",
        "description": "The same feature model also surfaces songs that occupy opposite ends of the curated audio space.",
        "sql": """
            SELECT
                t1.track_name AS track_1,
                t2.track_name AS track_2,
                ROUND(
                    SQRT(
                        POWER((f1.tempo - f2.tempo) / 50.0, 2) +
                        POWER(f1.energy - f2.energy, 2) +
                        POWER(f1.valence - f2.valence, 2) +
                        POWER(f1.danceability - f2.danceability, 2)
                    )::numeric,
                    3
                ) AS feature_distance
            FROM track_audio_features f1
            JOIN track_audio_features f2
              ON f1.track_id < f2.track_id
            JOIN tracks t1 ON t1.track_id = f1.track_id
            JOIN tracks t2 ON t2.track_id = f2.track_id
            ORDER BY feature_distance DESC, track_1, track_2
            LIMIT 10;
        """,
        "params": {},
    },
    {
        "slug": "top-collaboration-pairs",
        "category": "Complex joins",
        "title": "Most frequent artist collaboration pairs",
        "description": "The normalized junction table supports pairwise network statistics over repeated co-appearances.",
        "sql": """
            SELECT
                a1.artist_name AS artist_1,
                a2.artist_name AS artist_2,
                COUNT(DISTINCT ta1.track_id) AS shared_tracks
            FROM track_artists ta1
            JOIN track_artists ta2
              ON ta1.track_id = ta2.track_id
             AND ta1.artist_id < ta2.artist_id
            JOIN artists a1 ON a1.artist_id = ta1.artist_id
            JOIN artists a2 ON a2.artist_id = ta2.artist_id
            GROUP BY a1.artist_name, a2.artist_name
            ORDER BY shared_tracks DESC, artist_1, artist_2
            LIMIT 15;
        """,
        "params": {},
    },
    {
        "slug": "data-completeness-audit",
        "category": "Design and management",
        "title": "Dataset completeness audit",
        "description": "A compact management-oriented query checks whether the curated dataset remains usable for the demonstration workflow.",
        "sql": """
            SELECT
                (
                    SELECT COUNT(*)
                    FROM tracks t
                    LEFT JOIN track_audio_features f ON f.track_id = t.track_id
                    WHERE f.track_id IS NULL
                ) AS tracks_without_features,
                (
                    SELECT COUNT(*)
                    FROM tracks t
                    LEFT JOIN track_artists ta ON ta.track_id = t.track_id
                    WHERE ta.track_id IS NULL
                ) AS tracks_without_artists,
                (
                    SELECT COUNT(*)
                    FROM artists a
                    LEFT JOIN track_artists ta ON ta.artist_id = a.artist_id
                    WHERE ta.artist_id IS NULL
                ) AS artists_without_tracks,
                (
                    SELECT COUNT(*)
                    FROM tracks
                    WHERE album_id IS NULL
                ) AS tracks_without_album;
        """,
        "params": {},
    },
]


def fetch_overview() -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        counts = {}
        for label, table_name in {
            "tracks": "tracks",
            "artists": "artists",
            "albums": "albums",
            "samples": "sampling_relations",
            "covers": "cover_relations",
        }.items():
            cur.execute(f"SELECT COUNT(*) AS count FROM {table_name}")
            counts[label] = cur.fetchone()["count"]

        cur.execute(
            """
            SELECT
                original.track_id,
                original.track_name,
                COUNT(*) AS cover_count
            FROM cover_relations cr
            JOIN tracks original ON original.track_id = cr.original_track_id
            GROUP BY original.track_id, original.track_name
            ORDER BY cover_count DESC, original.track_name
            LIMIT 5;
            """
        )
        top_covered = cur.fetchall()

        cur.execute(
            """
            SELECT
                sampled.track_id,
                sampled.track_name,
                COUNT(*) AS sampled_count
            FROM sampling_relations sr
            JOIN tracks sampled ON sampled.track_id = sr.sampled_track_id
            GROUP BY sampled.track_id, sampled.track_name
            ORDER BY sampled_count DESC, sampled.track_name
            LIMIT 5;
            """
        )
        top_sampled = cur.fetchall()

        cur.execute(
            """
            SELECT
                a.artist_id,
                a.artist_name,
                COUNT(DISTINCT ta.track_id) AS track_count
            FROM artists a
            JOIN track_artists ta ON ta.artist_id = a.artist_id
            GROUP BY a.artist_id, a.artist_name
            ORDER BY track_count DESC, a.artist_name
            LIMIT 5;
            """
        )
        busiest_artists = cur.fetchall()

        cur.execute(
            """
            SELECT
                ROUND(AVG(tempo)::numeric, 2) AS avg_tempo,
                ROUND(AVG(energy)::numeric, 2) AS avg_energy,
                ROUND(AVG(valence)::numeric, 2) AS avg_valence
            FROM track_audio_features;
            """
        )
        feature_summary = cur.fetchone()

    return {
        "counts": counts,
        "top_covered": top_covered,
        "top_sampled": top_sampled,
        "busiest_artists": busiest_artists,
        "feature_summary": feature_summary,
    }


def list_tracks(search: str = "", limit: int = 50) -> list[dict]:
    pattern = f"%{search.strip()}%"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                t.track_id,
                t.track_name,
                t.release_date,
                t.popularity,
                a.album_name,
                STRING_AGG(ar.artist_name, ', ' ORDER BY ta.credit_order) AS artists
            FROM tracks t
            LEFT JOIN albums a ON a.album_id = t.album_id
            LEFT JOIN track_artists ta ON ta.track_id = t.track_id
            LEFT JOIN artists ar ON ar.artist_id = ta.artist_id
            WHERE %(pattern)s = '%%'
               OR t.track_name ILIKE %(pattern)s
               OR ar.artist_name ILIKE %(pattern)s
            GROUP BY t.track_id, t.track_name, t.release_date, t.popularity, a.album_name
            ORDER BY t.popularity DESC NULLS LAST, t.track_name
            LIMIT %(limit)s;
            """,
            {"pattern": pattern, "limit": limit},
        )
        return cur.fetchall()


def get_track_detail(track_id: str) -> dict | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                t.*,
                a.album_name
            FROM tracks t
            LEFT JOIN albums a ON a.album_id = t.album_id
            WHERE t.track_id = %(track_id)s;
            """,
            {"track_id": track_id},
        )
        track = cur.fetchone()
        if not track:
            return None

        cur.execute(
            """
            SELECT
                ar.artist_id,
                ar.artist_name,
                ta.artist_role,
                ta.credit_order
            FROM track_artists ta
            JOIN artists ar ON ar.artist_id = ta.artist_id
            WHERE ta.track_id = %(track_id)s
            ORDER BY ta.credit_order, ar.artist_name;
            """,
            {"track_id": track_id},
        )
        artists = cur.fetchall()

        cur.execute(
            """
            SELECT *
            FROM track_audio_features
            WHERE track_id = %(track_id)s;
            """,
            {"track_id": track_id},
        )
        features = cur.fetchone()

        cur.execute(
            """
            SELECT
                sr.sampling_relation_id,
                sr.relation_type,
                sr.verification_level,
                sr.note,
                sampled.track_id,
                sampled.track_name
            FROM sampling_relations sr
            JOIN tracks sampled ON sampled.track_id = sr.sampled_track_id
            WHERE sr.sampling_track_id = %(track_id)s
            ORDER BY sampled.track_name;
            """,
            {"track_id": track_id},
        )
        samples_from = cur.fetchall()

        cur.execute(
            """
            SELECT
                sr.sampling_relation_id,
                sr.relation_type,
                sr.verification_level,
                sr.note,
                sampler.track_id,
                sampler.track_name
            FROM sampling_relations sr
            JOIN tracks sampler ON sampler.track_id = sr.sampling_track_id
            WHERE sr.sampled_track_id = %(track_id)s
            ORDER BY sampler.track_name;
            """,
            {"track_id": track_id},
        )
        sampled_by = cur.fetchall()

        cur.execute(
            """
            SELECT
                cr.cover_relation_id,
                cr.cover_type,
                cr.verification_level,
                cr.note,
                original.track_id,
                original.track_name
            FROM cover_relations cr
            JOIN tracks original ON original.track_id = cr.original_track_id
            WHERE cr.cover_track_id = %(track_id)s
            ORDER BY original.track_name;
            """,
            {"track_id": track_id},
        )
        covers = cur.fetchall()

        cur.execute(
            """
            SELECT
                cr.cover_relation_id,
                cr.cover_type,
                cr.verification_level,
                cr.note,
                cover.track_id,
                cover.track_name
            FROM cover_relations cr
            JOIN tracks cover ON cover.track_id = cr.cover_track_id
            WHERE cr.original_track_id = %(track_id)s
            ORDER BY cover.track_name;
            """,
            {"track_id": track_id},
        )
        covered_by = cur.fetchall()

    return {
        "track": track,
        "artists": artists,
        "features": features,
        "samples_from": samples_from,
        "sampled_by": sampled_by,
        "covers": covers,
        "covered_by": covered_by,
    }


def list_artists(search: str = "", limit: int = 50) -> list[dict]:
    pattern = f"%{search.strip()}%"
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.artist_id,
                a.artist_name,
                COUNT(DISTINCT ta.track_id) AS track_count,
                COUNT(DISTINCT CASE WHEN ta.artist_role = 'featured' THEN ta.track_id END) AS featured_count
            FROM artists a
            LEFT JOIN track_artists ta ON ta.artist_id = a.artist_id
            WHERE %(pattern)s = '%%'
               OR a.artist_name ILIKE %(pattern)s
            GROUP BY a.artist_id, a.artist_name
            ORDER BY track_count DESC, a.artist_name
            LIMIT %(limit)s;
            """,
            {"pattern": pattern, "limit": limit},
        )
        return cur.fetchall()


def list_albums(limit: int = 150) -> list[dict]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                album_id,
                album_name,
                release_date
            FROM albums
            ORDER BY album_name, album_id
            LIMIT %(limit)s;
            """,
            {"limit": limit},
        )
        return cur.fetchall()


def get_artist_detail(artist_id: str) -> dict | None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM artists
            WHERE artist_id = %(artist_id)s;
            """,
            {"artist_id": artist_id},
        )
        artist = cur.fetchone()
        if not artist:
            return None

        cur.execute(
            """
            SELECT
                t.track_id,
                t.track_name,
                t.release_date,
                ta.artist_role,
                ta.credit_order
            FROM track_artists ta
            JOIN tracks t ON t.track_id = ta.track_id
            WHERE ta.artist_id = %(artist_id)s
            ORDER BY t.release_date NULLS LAST, t.track_name;
            """,
            {"artist_id": artist_id},
        )
        tracks = cur.fetchall()

        cur.execute(
            """
            SELECT
                collaborator.artist_id,
                collaborator.artist_name,
                COUNT(DISTINCT ta1.track_id) AS shared_tracks
            FROM track_artists ta1
            JOIN track_artists ta2
              ON ta1.track_id = ta2.track_id
             AND ta1.artist_id <> ta2.artist_id
            JOIN artists collaborator ON collaborator.artist_id = ta2.artist_id
            WHERE ta1.artist_id = %(artist_id)s
            GROUP BY collaborator.artist_id, collaborator.artist_name
            ORDER BY shared_tracks DESC, collaborator.artist_name
            LIMIT 15;
            """,
            {"artist_id": artist_id},
        )
        collaborators = cur.fetchall()

    return {
        "artist": artist,
        "tracks": tracks,
        "collaborators": collaborators,
    }


def run_sql_demos() -> list[dict]:
    demos = []
    with get_connection() as conn, conn.cursor() as cur:
        for demo in SQL_DEMOS:
            cur.execute(demo["sql"], demo["params"])
            demos.append(
                {
                    "slug": demo["slug"],
                    "category": demo["category"],
                    "title": demo["title"],
                    "description": demo["description"],
                    "sql": demo["sql"].strip(),
                    "rows": cur.fetchall(),
                }
            )
    return demos


def fetch_reference_data() -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT track_id, track_name
            FROM tracks
            ORDER BY track_name, track_id;
            """
        )
        tracks = cur.fetchall()

        cur.execute(
            """
            SELECT artist_id, artist_name
            FROM artists
            ORDER BY artist_name, artist_id;
            """
        )
        artists = cur.fetchall()

        cur.execute(
            """
            SELECT album_id, album_name
            FROM albums
            ORDER BY album_name, album_id;
            """
        )
        albums = cur.fetchall()

    return {
        "tracks": tracks,
        "artists": artists,
        "albums": albums,
        "management_highlights": MANAGEMENT_HIGHLIGHTS,
    }


def _clean_text(value):
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _has_feature_values(features: dict | None) -> bool:
    if not features:
        return False
    return any(value is not None for value in features.values())


def _next_text_id(cur, table_name: str, id_column: str, prefix: str) -> str:
    cur.execute(
        sql.SQL(
            """
            SELECT COALESCE(MAX(CAST(SUBSTRING({id_column} FROM %s) AS INTEGER)), 0) AS max_id
            FROM {table_name}
            WHERE {id_column} LIKE %s;
            """
        ).format(
            table_name=sql.Identifier(table_name),
            id_column=sql.Identifier(id_column),
        ),
        (rf"{prefix}([0-9]+)", f"{prefix}%"),
    )
    next_id = cur.fetchone()["max_id"] + 1
    return f"{prefix}{next_id:03d}"


def create_sampling_relation(payload: dict) -> dict:
    payload = {
        "sampling_relation_id": _clean_text(payload.get("sampling_relation_id")),
        "sampling_track_id": _clean_text(payload.get("sampling_track_id")),
        "sampled_track_id": _clean_text(payload.get("sampled_track_id")),
        "relation_type": _clean_text(payload.get("relation_type")) or "manual_relation",
        "verification_level": _clean_text(payload.get("verification_level")) or "manual_entry",
        "note": _clean_text(payload.get("note")),
    }

    if not payload["sampling_track_id"] or not payload["sampled_track_id"]:
        raise ValueError("Both track IDs are required.")

    if payload["sampling_track_id"] == payload["sampled_track_id"]:
        raise ValueError("A track cannot sample itself.")

    with get_connection() as conn, conn.cursor() as cur:
        if not payload["sampling_relation_id"]:
            payload["sampling_relation_id"] = _next_text_id(
                cur,
                "sampling_relations",
                "sampling_relation_id",
                "SMP",
            )

        cur.execute(
            """
            SELECT 1
            FROM tracks
            WHERE track_id = %(sampling_track_id)s;
            """,
            {"sampling_track_id": payload["sampling_track_id"]},
        )
        if not cur.fetchone():
            raise ValueError("Sampling track does not exist.")

        cur.execute(
            """
            SELECT 1
            FROM tracks
            WHERE track_id = %(sampled_track_id)s;
            """,
            {"sampled_track_id": payload["sampled_track_id"]},
        )
        if not cur.fetchone():
            raise ValueError("Sampled track does not exist.")

        cur.execute(
            """
            SELECT 1
            FROM sampling_relations
            WHERE sampling_track_id = %(sampling_track_id)s
              AND sampled_track_id = %(sampled_track_id)s;
            """,
            payload,
        )
        if cur.fetchone():
            raise ValueError("This sampling relation already exists.")

        cur.execute(
            """
            INSERT INTO sampling_relations (
                sampling_relation_id,
                sampling_track_id,
                sampled_track_id,
                relation_type,
                verification_level,
                note
            )
            VALUES (
                %(sampling_relation_id)s,
                %(sampling_track_id)s,
                %(sampled_track_id)s,
                %(relation_type)s,
                %(verification_level)s,
                %(note)s
            )
            RETURNING
                sampling_relation_id,
                sampling_track_id,
                sampled_track_id,
                relation_type,
                verification_level,
                note;
            """,
            payload,
        )
        row = cur.fetchone()
        conn.commit()
        return row


def create_track_with_relations(payload: dict) -> dict:
    track_data = {
        "track_id": _clean_text(payload.get("track_id")),
        "track_name": _clean_text(payload.get("track_name")),
        "album_id": _clean_text(payload.get("album_id")),
        "release_date": _clean_text(payload.get("release_date")),
        "duration_ms": payload.get("duration_ms"),
        "popularity": payload.get("popularity"),
        "external_source": _clean_text(payload.get("external_source")),
        "external_track_ref": _clean_text(payload.get("external_track_ref")),
    }

    features = payload.get("features") or {}
    features = {key: features.get(key) for key in [
        "tempo",
        "energy",
        "valence",
        "danceability",
        "acousticness",
        "speechiness",
        "liveness",
        "loudness",
        "instrumentalness",
    ]}

    artists = []
    for index, artist in enumerate(payload.get("artists", []), start=1):
        artists.append(
            {
                "artist_id": _clean_text(artist.get("artist_id")),
                "artist_role": _clean_text(artist.get("artist_role")) or "primary",
                "credit_order": artist.get("credit_order") or index,
            }
        )

    if not artists:
        raise ValueError("At least one artist participation record is required.")

    if not track_data["track_name"]:
        raise ValueError("Track name is required.")

    if not any(artist["artist_role"] == "primary" for artist in artists):
        raise ValueError("At least one artist must have the role 'primary'.")

    artist_keys = {(artist["artist_id"], artist["artist_role"]) for artist in artists}
    if len(artist_keys) != len(artists):
        raise ValueError("Duplicate artist-role pairs are not allowed.")

    with get_connection() as conn, conn.cursor() as cur:
        if not track_data["track_id"]:
            track_data["track_id"] = _next_text_id(cur, "tracks", "track_id", "TRK")

        cur.execute(
            """
            SELECT 1
            FROM tracks
            WHERE track_id = %(track_id)s;
            """,
            {"track_id": track_data["track_id"]},
        )
        if cur.fetchone():
            raise ValueError("Track ID already exists.")

        if track_data["album_id"]:
            cur.execute(
                """
                SELECT 1
                FROM albums
                WHERE album_id = %(album_id)s;
                """,
                {"album_id": track_data["album_id"]},
            )
            if not cur.fetchone():
                raise ValueError("Album ID does not exist.")

        for artist in artists:
            if not artist["artist_id"]:
                raise ValueError("Each artist row must include an artist_id.")
            cur.execute(
                """
                SELECT 1
                FROM artists
                WHERE artist_id = %(artist_id)s;
                """,
                {"artist_id": artist["artist_id"]},
            )
            if not cur.fetchone():
                raise ValueError(f"Artist ID does not exist: {artist['artist_id']}")

        cur.execute(
            """
            INSERT INTO tracks (
                track_id,
                track_name,
                album_id,
                release_date,
                duration_ms,
                popularity,
                external_source,
                external_track_ref
            )
            VALUES (
                %(track_id)s,
                %(track_name)s,
                %(album_id)s,
                %(release_date)s,
                %(duration_ms)s,
                %(popularity)s,
                %(external_source)s,
                %(external_track_ref)s
            );
            """,
            track_data,
        )

        if _has_feature_values(features):
            feature_data = {"track_id": track_data["track_id"], **features}
            cur.execute(
                """
                INSERT INTO track_audio_features (
                    track_id,
                    tempo,
                    energy,
                    valence,
                    danceability,
                    acousticness,
                    speechiness,
                    liveness,
                    loudness,
                    instrumentalness
                )
                VALUES (
                    %(track_id)s,
                    %(tempo)s,
                    %(energy)s,
                    %(valence)s,
                    %(danceability)s,
                    %(acousticness)s,
                    %(speechiness)s,
                    %(liveness)s,
                    %(loudness)s,
                    %(instrumentalness)s
                );
                """,
                feature_data,
            )

        for artist in artists:
            cur.execute(
                """
                INSERT INTO track_artists (
                    track_id,
                    artist_id,
                    artist_role,
                    credit_order
                )
                VALUES (
                    %(track_id)s,
                    %(artist_id)s,
                    %(artist_role)s,
                    %(credit_order)s
                );
                """,
                {
                    "track_id": track_data["track_id"],
                    "artist_id": artist["artist_id"],
                    "artist_role": artist["artist_role"],
                    "credit_order": artist.get("credit_order"),
                },
            )

        conn.commit()

    return {
        "track_id": track_data["track_id"],
        "status": "created",
        "artist_links_created": len(artists),
        "features_created": _has_feature_values(features),
    }

def _require_track(cur, track_id: str) -> dict:
    cur.execute(
        """
        SELECT track_id, track_name
        FROM tracks
        WHERE track_id = %(track_id)s;
        """,
        {"track_id": track_id},
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("Track not found.")
    return row


def _build_track_nodes(
    cur,
    track_ids: list[str],
    *,
    highlight_track_ids: set[str] | None = None,
    relation_roles: dict[str, str] | None = None,
) -> list[dict]:
    if not track_ids:
        return []

    highlight_track_ids = highlight_track_ids or set()
    relation_roles = relation_roles or {}

    cur.execute(
        """
        SELECT
            t.track_id,
            t.track_name,
            COALESCE(t.popularity, 50) AS popularity,
            COALESCE(f.valence, 0.5) AS valence,
            COALESCE(f.energy, 0.5) AS energy,
            COALESCE(f.tempo, 100.0) AS tempo,
            COALESCE(f.danceability, 0.5) AS danceability,
            COALESCE(f.acousticness, 0.5) AS acousticness
        FROM tracks t
        LEFT JOIN track_audio_features f ON f.track_id = t.track_id
        WHERE t.track_id = ANY(%(track_ids)s)
        ORDER BY t.track_id;
        """,
        {"track_ids": track_ids},
    )
    rows = cur.fetchall()
    if not rows:
        return []

    tempo_values = [row["tempo"] for row in rows]
    min_tempo = min(tempo_values)
    max_tempo = max(tempo_values)
    tempo_span = max(max_tempo - min_tempo, 1)

    nodes = []
    for row in rows:
        x = (float(row["valence"]) - 0.5) * 260
        y = (float(row["energy"]) - 0.5) * 260
        z = ((float(row["tempo"]) - min_tempo) / tempo_span - 0.5) * 260

        base_size = 6
        importance_bonus = (float(row.get("popularity", 50)) / 100.0) * 10.0
        final_size = base_size + importance_bonus
        node_color = "#00E5FF"

        if row["track_id"] in highlight_track_ids:
            final_size += 8
            node_color = "#FFFFFF"

        nodes.append(
            {
                "id": row["track_id"],
                "label": row["track_name"],
                "type": "track",
                "x": round(x, 2),
                "y": round(y, 2),
                "z": round(z, 2),
                "color": node_color,
                "size": round(final_size, 2),
                "meta": {
                    "popularity": row["popularity"],
                    "energy": row["energy"],
                    "valence": row["valence"],
                    "tempo": row["tempo"],
                    "acousticness": row["acousticness"],
                    "relation_role": relation_roles.get(row["track_id"]),
                },
            }
        )

    return nodes


def _fetch_sampling_edges(cur, track_ids: list[str]) -> list[dict]:
    if not track_ids:
        return []

    cur.execute(
        """
        SELECT
            sampling_relation_id AS edge_id,
            sampling_track_id AS source_id,
            sampled_track_id AS target_id,
            relation_type AS edge_kind
        FROM sampling_relations
        WHERE sampling_track_id = ANY(%(track_ids)s)
          AND sampled_track_id = ANY(%(track_ids)s)
        ORDER BY sampling_relation_id;
        """,
        {"track_ids": track_ids},
    )
    rows = cur.fetchall()

    return [
        {
            "id": row["edge_id"],
            "source": row["source_id"],
            "target": row["target_id"],
            "type": "sampling",
            "label": row["edge_kind"],
            "color": "#6ee7b7",
        }
        for row in rows
    ]


def _fetch_cover_edges(cur, track_ids: list[str]) -> list[dict]:
    if not track_ids:
        return []

    cur.execute(
        """
        SELECT
            cover_relation_id AS edge_id,
            cover_track_id AS source_id,
            original_track_id AS target_id,
            cover_type AS edge_kind
        FROM cover_relations
        WHERE cover_track_id = ANY(%(track_ids)s)
          AND original_track_id = ANY(%(track_ids)s)
        ORDER BY cover_relation_id;
        """,
        {"track_ids": track_ids},
    )
    rows = cur.fetchall()

    return [
        {
            "id": row["edge_id"],
            "source": row["source_id"],
            "target": row["target_id"],
            "type": "cover",
            "label": row["edge_kind"],
            "color": "#f472b6",
        }
        for row in rows
    ]


def get_sampling_trace_subgraph(track_id: str) -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        focus_track = _require_track(cur, track_id)

        cur.execute(
            """
            WITH RECURSIVE ancestor_chain AS (
                SELECT
                    1 AS depth,
                    sr.sampled_track_id AS ancestor_track_id
                FROM sampling_relations sr
                WHERE sr.sampling_track_id = %(track_id)s

                UNION ALL

                SELECT
                    ac.depth + 1,
                    sr.sampled_track_id AS ancestor_track_id
                FROM sampling_relations sr
                JOIN ancestor_chain ac
                  ON sr.sampling_track_id = ac.ancestor_track_id
            )
            SELECT
                t.track_id,
                t.track_name,
                MIN(ac.depth) AS depth
            FROM ancestor_chain ac
            JOIN tracks t ON t.track_id = ac.ancestor_track_id
            GROUP BY t.track_id, t.track_name
            ORDER BY depth, t.track_name;
            """,
            {"track_id": track_id},
        )
        ancestors = cur.fetchall()

        cur.execute(
            """
            WITH RECURSIVE descendant_chain AS (
                SELECT
                    1 AS depth,
                    sr.sampling_track_id AS descendant_track_id
                FROM sampling_relations sr
                WHERE sr.sampled_track_id = %(track_id)s

                UNION ALL

                SELECT
                    dc.depth + 1,
                    sr.sampling_track_id AS descendant_track_id
                FROM sampling_relations sr
                JOIN descendant_chain dc
                  ON sr.sampled_track_id = dc.descendant_track_id
            )
            SELECT
                t.track_id,
                t.track_name,
                MIN(dc.depth) AS depth
            FROM descendant_chain dc
            JOIN tracks t ON t.track_id = dc.descendant_track_id
            GROUP BY t.track_id, t.track_name
            ORDER BY depth, t.track_name;
            """,
            {"track_id": track_id},
        )
        descendants = cur.fetchall()

        ancestor_ids = [row["track_id"] for row in ancestors]
        descendant_ids = [row["track_id"] for row in descendants]
        lineage_ids = list(dict.fromkeys([track_id, *ancestor_ids, *descendant_ids]))

        relation_roles = {track_id: "focus"}
        for row in ancestors:
            relation_roles[row["track_id"]] = "ancestor"
        for row in descendants:
            relation_roles[row["track_id"]] = "descendant"

        nodes = _build_track_nodes(
            cur,
            lineage_ids,
            highlight_track_ids={track_id},
            relation_roles=relation_roles,
        )
        edges = _fetch_sampling_edges(cur, lineage_ids)

        direct_sources = [row for row in ancestors if row["depth"] == 1]
        direct_descendants = [row for row in descendants if row["depth"] == 1]

        return {
            "mode": "sampling-trace",
            "focus_track": focus_track,
            "nodes": nodes,
            "edges": edges,
            "ancestors": ancestors,
            "descendants": descendants,
            "direct_sources": direct_sources,
            "direct_descendants": direct_descendants,
            "summary": {
                "samples_from_count": len(direct_sources),
                "ancestor_count": len(ancestors),
                "descendant_count": len(descendants),
            },
        }


def get_cover_family_subgraph(track_id: str) -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        selected_track = _require_track(cur, track_id)

        cur.execute(
            """
            WITH RECURSIVE cover_ancestors AS (
                SELECT
                    1 AS depth,
                    cr.original_track_id AS ancestor_track_id
                FROM cover_relations cr
                WHERE cr.cover_track_id = %(track_id)s

                UNION ALL

                SELECT
                    ca.depth + 1,
                    cr.original_track_id AS ancestor_track_id
                FROM cover_relations cr
                JOIN cover_ancestors ca
                  ON cr.cover_track_id = ca.ancestor_track_id
            )
            SELECT ancestor_track_id
            FROM cover_ancestors
            ORDER BY depth DESC
            LIMIT 1;
            """,
            {"track_id": track_id},
        )
        root_row = cur.fetchone()
        root_track_id = root_row["ancestor_track_id"] if root_row else track_id
        root_track = _require_track(cur, root_track_id)

        cur.execute(
            """
            WITH RECURSIVE cover_family AS (
                SELECT
                    1 AS depth,
                    cr.cover_track_id AS family_track_id
                FROM cover_relations cr
                WHERE cr.original_track_id = %(root_track_id)s

                UNION ALL

                SELECT
                    cf.depth + 1,
                    cr.cover_track_id AS family_track_id
                FROM cover_relations cr
                JOIN cover_family cf
                  ON cr.original_track_id = cf.family_track_id
            )
            SELECT
                t.track_id,
                t.track_name,
                MIN(cf.depth) AS depth,
                t.release_date
            FROM cover_family cf
            JOIN tracks t ON t.track_id = cf.family_track_id
            GROUP BY t.track_id, t.track_name, t.release_date
            ORDER BY depth, t.release_date NULLS LAST, t.track_name;
            """,
            {"root_track_id": root_track_id},
        )
        family_members = cur.fetchall()

        family_ids = list(dict.fromkeys([root_track_id, *[row["track_id"] for row in family_members]]))
        relation_roles = {root_track_id: "original"}
        for row in family_members:
            relation_roles[row["track_id"]] = "cover"

        highlight_ids = {root_track_id}
        if track_id != root_track_id:
            highlight_ids.add(track_id)

        nodes = _build_track_nodes(
            cur,
            family_ids,
            highlight_track_ids=highlight_ids,
            relation_roles=relation_roles,
        )
        edges = _fetch_cover_edges(cur, family_ids)
        edge_map = {edge["source"]: edge["target"] for edge in edges}

        cover_ids = [row["track_id"] for row in family_members]

        if cover_ids:
            cur.execute(
                """
                SELECT
                    CONCAT((EXTRACT(YEAR FROM t.release_date)::int / 10) * 10, 's') AS decade,
                    COUNT(*) AS cover_count
                FROM tracks t
                WHERE t.track_id = ANY(%(cover_ids)s)
                  AND t.release_date IS NOT NULL
                GROUP BY decade
                ORDER BY decade;
                """,
                {"cover_ids": cover_ids},
            )
            timeline = cur.fetchall()

            cur.execute(
                """
                WITH root_features AS (
                    SELECT *
                    FROM track_audio_features
                    WHERE track_id = %(root_track_id)s
                )
                SELECT
                    t.track_id,
                    t.track_name,
                    ROUND(
                        SQRT(
                            POWER((f.tempo - root_features.tempo) / 50.0, 2) +
                            POWER(f.energy - root_features.energy, 2) +
                            POWER(f.valence - root_features.valence, 2) +
                            POWER(f.danceability - root_features.danceability, 2)
                        )::numeric,
                        3
                    ) AS feature_distance
                FROM track_audio_features f
                JOIN root_features ON TRUE
                JOIN tracks t ON t.track_id = f.track_id
                WHERE f.track_id = ANY(%(cover_ids)s)
                ORDER BY feature_distance ASC, t.track_name;
                """,
                {"root_track_id": root_track_id, "cover_ids": cover_ids},
            )
            feature_distances = cur.fetchall()
        else:
            timeline = []
            feature_distances = []

        family_tree = []
        for row in family_members:
            parent_track_id = edge_map.get(row["track_id"], root_track_id)
            cur.execute(
                """
                SELECT track_name
                FROM tracks
                WHERE track_id = %(track_id)s;
                """,
                {"track_id": parent_track_id},
            )
            parent_row = cur.fetchone()
            family_tree.append(
                {
                    "track_id": row["track_id"],
                    "track_name": row["track_name"],
                    "parent_track_id": parent_track_id,
                    "parent_track_name": parent_row["track_name"] if parent_row else root_track["track_name"],
                    "depth": row["depth"],
                    "release_date": row["release_date"],
                }
            )

        closest_cover = feature_distances[0] if feature_distances else None
        furthest_cover = feature_distances[-1] if feature_distances else None

        return {
            "mode": "cover-family",
            "selected_track": selected_track,
            "root_track": root_track,
            "nodes": nodes,
            "edges": edges,
            "family_members": family_members,
            "family_tree": family_tree,
            "timeline": timeline,
            "feature_distances": feature_distances,
            "closest_cover": closest_cover,
            "furthest_cover": furthest_cover,
            "summary": {
                "family_size": len(family_members) + 1,
                "timeline_bucket_count": len(timeline),
            },
        }

def build_graph_payload() -> dict:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                t.track_id,
                t.track_name,
                COALESCE(t.popularity, 50) AS popularity,
                COALESCE(f.valence, 0.5) AS valence,
                COALESCE(f.energy, 0.5) AS energy,
                COALESCE(f.tempo, 100.0) AS tempo,
                COALESCE(f.danceability, 0.5) AS danceability,
                COALESCE(f.acousticness, 0.5) AS acousticness
            FROM tracks t
            LEFT JOIN track_audio_features f ON f.track_id = t.track_id
            ORDER BY t.track_id;
            """
        )
        tracks = cur.fetchall()

        cur.execute(
            """
            SELECT
                ar.artist_id,
                ar.artist_name,
                ta.track_id
            FROM artists ar
            JOIN track_artists ta ON ta.artist_id = ar.artist_id
            ORDER BY ar.artist_id, ta.credit_order;
            """
        )
        track_artist_links = cur.fetchall()

        cur.execute(
            """
            SELECT
                sampling_relation_id AS edge_id,
                sampling_track_id AS source_id,
                sampled_track_id AS target_id,
                relation_type AS edge_kind
            FROM sampling_relations;
            """
        )
        sampling_edges = cur.fetchall()

        cur.execute(
            """
            SELECT
                cover_relation_id AS edge_id,
                cover_track_id AS source_id,
                original_track_id AS target_id,
                cover_type AS edge_kind
            FROM cover_relations;
            """
        )
        cover_edges = cur.fetchall()

    tempo_values = [row["tempo"] for row in tracks]
    min_tempo = min(tempo_values)
    max_tempo = max(tempo_values)
    tempo_span = max(max_tempo - min_tempo, 1)

    nodes = []
    track_positions = {}
    track_members = defaultdict(list)

    # 你可以把这里替换成你最终想突出展示的“母曲”ID
    highlighted_track_ids = {"TRK026", "TRK012"}

    for row in tracks:
        x = (float(row["valence"]) - 0.5) * 260
        y = (float(row["energy"]) - 0.5) * 260
        z = ((float(row["tempo"]) - min_tempo) / tempo_span - 0.5) * 260
        track_positions[row["track_id"]] = (x, y, z)

        base_size = 6
        importance_bonus = (float(row.get("popularity", 50)) / 100.0) * 10.0

        if row["track_id"] in highlighted_track_ids:
            final_size = 25
            node_color = "#FFFFFF"
        else:
            final_size = base_size + importance_bonus
            node_color = "#00E5FF"

        nodes.append(
            {
                "id": row["track_id"],
                "label": row["track_name"],
                "type": "track",
                "x": round(x, 2),
                "y": round(y, 2),
                "z": round(z, 2),
                "color": node_color,
                "size": round(final_size, 2),
                "meta": {
                    "popularity": row["popularity"],
                    "energy": row["energy"],
                    "valence": row["valence"],
                    "tempo": row["tempo"],
                    "acousticness": row["acousticness"],
                },
            }
        )

    artist_names = {}
    for row in track_artist_links:
        track_members[row["artist_id"]].append(row["track_id"])
        artist_names[row["artist_id"]] = row["artist_name"]

    for index, (artist_id, member_track_ids) in enumerate(track_members.items()):
        positions = [track_positions[track_id] for track_id in member_track_ids if track_id in track_positions]
        if not positions:
            continue

        xs, ys, zs = zip(*positions)
        base_x = sum(xs) / len(xs)
        base_y = sum(ys) / len(ys)
        base_z = sum(zs) / len(zs)
        offset_x = sin(index * 1.7) * 18
        offset_z = cos(index * 1.3) * 18

        track_count = len(member_track_ids)
        artist_size = round(9 + min(track_count, 12) * 0.9, 2)

        if track_count >= 5:
            artist_color = "#FFD166"
        else:
            artist_color = "#FFB86B"

        nodes.append(
            {
                "id": artist_id,
                "label": artist_names[artist_id],
                "type": "artist",
                "x": round(base_x + offset_x, 2),
                "y": round(base_y + 35, 2),
                "z": round(base_z + offset_z, 2),
                "color": artist_color,
                "size": artist_size,
                "meta": {
                    "track_count": track_count,
                },
            }
        )

    edges = []
    for row in sampling_edges:
        edges.append(
            {
                "id": row["edge_id"],
                "source": row["source_id"],
                "target": row["target_id"],
                "type": "sampling",
                "label": row["edge_kind"],
                "color": "#6ee7b7",
            }
        )

    for row in cover_edges:
        edges.append(
            {
                "id": row["edge_id"],
                "source": row["source_id"],
                "target": row["target_id"],
                "type": "cover",
                "label": row["edge_kind"],
                "color": "#f472b6",
            }
        )

    for link in track_artist_links:
        edges.append(
            {
                "id": f"TA-{link['artist_id']}-{link['track_id']}",
                "source": link["artist_id"],
                "target": link["track_id"],
                "type": "collaboration",
                "label": "track participation",
                "color": "#facc15",
            }
        )

    return {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "track_nodes": sum(1 for node in nodes if node["type"] == "track"),
            "artist_nodes": sum(1 for node in nodes if node["type"] == "artist"),
            "sampling_edges": sum(1 for edge in edges if edge["type"] == "sampling"),
            "cover_edges": sum(1 for edge in edges if edge["type"] == "cover"),
            "collaboration_edges": sum(1 for edge in edges if edge["type"] == "collaboration"),
        },
    }