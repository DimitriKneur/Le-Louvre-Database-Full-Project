SELECT
    *
FROM {{ source('source', 'all_works_of_art_le_louvre_raw') }}
WHERE "title" IS NOT NULL