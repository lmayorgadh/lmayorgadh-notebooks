CREATE TABLE IF NOT EXISTS
    `dh-global-sales-data-dev.leadgen_sf_match_vertex_raw.candidates_PO_FI_geo_non`
(
country_iso STRING,
left_row_id STRING,
left_name STRING,
left_name_local STRING,
left_name_stop STRING,
left_name_stop_phonetic STRING,
left_name_local_transliterated STRING,
left_street STRING,
left_street_stop STRING,
left_street_stop_phonetic STRING,
left_phone_number STRING,
left_lat FLOAT64,
left_lng FLOAT64,
left_registration_number STRING,
right_row_id STRING,
right_name STRING,
right_name_local STRING,
right_name_legal STRING,
right_name_stop STRING,
right_name_stop_phonetic STRING,
right_name_local_transliterated STRING,
right_street STRING,
right_street_stop STRING,
right_street_stop_phonetic STRING,
right_phone_number STRING,
right_lat FLOAT64,
right_lng FLOAT64,
right_registration_number STRING,
haversine FLOAT64,
tokenset_name_stop FLOAT64,
tokenset_street_stop FLOAT64,
tokenset_name_local FLOAT64,
tokenset_name_local_transliterated FLOAT64,
tokenset_name_legal FLOAT64
)
CLUSTER BY
    left_row_id
;

CREATE TEMP TABLE
    left_table
AS
(
    SELECT
        country_iso,
        row_id AS left_row_id,
        name AS left_name,
        name_local AS left_name_local,
        name_stop AS left_name_stop,
        name_stop_phonetic AS left_name_stop_phonetic,
        name_local_transliterated AS left_name_local_transliterated,
        street AS left_street,
        street_stop AS left_street_stop,
        street_stop_phonetic AS left_street_stop_phonetic,
        phone_number AS left_phone_number,
        lat AS left_lat,
        lng AS left_lng,
        city_lat AS left_city_lat,
        city_lng AS left_city_lng,
        registration_number AS left_registration_number,
        bqcarto.h3.LONGLAT_ASH3(city_lng, city_lat, 5) AS h3_idx
    FROM
        `dh-global-sales-data.leadgen_sf_match_vertex_raw.left_processed_PO_FI`
    WHERE
        (lat+lng) IS NOT NULL AND
        mod(abs(FARM_FINGERPRINT(row_id)), 1) = 0
        AND `dh-global-sales-data.achilles.row_id_to_lead_source`(row_id) = 'facebook'
)
;

CREATE TEMP TABLE
    right_table
AS
(
WITH rt AS
(
    SELECT
        country_iso,
        row_id AS right_row_id,
        name AS right_name,
        name_local AS right_name_local,
        name_legal AS right_name_legal,
        name_stop AS right_name_stop,
        name_stop_phonetic AS right_name_stop_phonetic,
        name_local_transliterated AS right_name_local_transliterated,
        street AS right_street,
        street_stop AS right_street_stop,
        street_stop_phonetic AS right_street_stop_phonetic,
        phone_number AS right_phone_number,
        lat AS right_lat,
        lng AS right_lng,
        registration_number AS right_registration_number,
        bqcarto.h3.KRING(bqcarto.h3.LONGLAT_ASH3(city_lng, city_lat,5), 2) AS h3_idx
    FROM
        `dh-global-sales-data.leadgen_sf_match_vertex_raw.right_processed_PO_FI`
    WHERE
        (lat+lng) IS NULL
)

SELECT
    country_iso,
    right_row_id,
    right_name,
    right_name_local,
    right_name_stop,
    right_name_legal,
    right_name_stop_phonetic,
    right_name_local_transliterated,
    right_street,
    right_street_stop,
    right_street_stop_phonetic,
    right_phone_number,
    right_lat,
    right_lng,
    right_registration_number,
    hex AS h3_idx,
FROM
    rt,
    UNNEST(rt.h3_idx) AS hex
)
;

CREATE TEMP TABLE geofilter AS
(
    SELECT DISTINCT
        l.country_iso,
        left_row_id,
        left_name,
        left_name_local,
        left_name_stop,
        left_name_stop_phonetic,
        left_name_local_transliterated,
        left_street,
        left_street_stop,
        left_street_stop_phonetic,
        left_phone_number,
        left_lat,
        left_lng,
        left_registration_number,
        right_row_id,
        right_name,
        right_name_local,
        right_name_stop,
        right_name_legal,
        right_name_stop_phonetic,
        right_name_local_transliterated,
        right_street,
        right_street_stop,
        right_street_stop_phonetic,
        right_phone_number,
        right_lat,
        right_lng,
        right_registration_number,
    FROM
        left_table AS l
    INNER JOIN
        right_table AS r
    ON
        l.h3_idx = r.h3_idx
)
;

INSERT INTO `dh-global-sales-data-dev.leadgen_sf_match_vertex_raw.candidates_PO_FI_geo_non`
(
WITH tsr AS
(
    SELECT
        *,
        `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name_stop, right_name_stop) AS tokenset_name_stop,
        `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name, right_name) AS tokenset_name,
        `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name_local, right_name_local) AS tokenset_name_local,
        `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name_local_transliterated, right_name_local_transliterated) AS tokenset_name_local_transliterated,
        `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name, right_name_legal) AS tokenset_name_legal,
    FROM
        geofilter
)
SELECT
    country_iso,
    left_row_id,
    left_name,
    left_name_local,
    left_name_stop,
    left_name_stop_phonetic,
    left_name_local_transliterated,
    left_street,
    left_street_stop,
    left_street_stop_phonetic,
    left_phone_number,
    left_lat,
    left_lng,
    left_registration_number,
    right_row_id,
    right_name,
    right_name_local,
    right_name_stop,
    right_name_legal,
    right_name_stop_phonetic,
    right_name_local_transliterated,
    right_street,
    right_street_stop,
    right_street_stop_phonetic,
    right_phone_number,
    right_lat,
    right_lng,
    right_registration_number,
    NULL AS haversine,
    tokenset_name_stop,
    NULL AS tokenset_street_stop,
    tokenset_name_local,
    tokenset_name_local_transliterated,
    tokenset_name_legal,
    --'geo_non' as candidate_source
FROM
    tsr
WHERE
    (
        tokenset_name_stop >= 0.5 OR
        tokenset_name >= 0.5 OR
        tokenset_name_local >= 0.5 OR
        tokenset_name_legal >= 0.5 OR
        tokenset_name_local_transliterated >= 0.5 OR
        left_phone_number = right_phone_number
    )
)
;