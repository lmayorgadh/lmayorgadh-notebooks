-- Unique combine all sub-tables.
CREATE TABLE IF NOT EXISTS `dh-global-sales-data-dev.leadgen_sf_match_vertex_raw.candidates_PO_FI`
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
;

INSERT INTO `dh-global-sales-data-dev.leadgen_sf_match_vertex_raw.candidates_PO_FI`
(
    WITH comb AS
    (
        SELECT
            *
        FROM
            `dh-global-sales-data-dev.leadgen_sf_match_vertex_raw.candidates_PO_FI_geo_geo`
        WHERE
            TRUE
            AND `dh-global-sales-data.achilles.row_id_to_lead_source`(left_row_id) = 'facebook'

        UNION DISTINCT

        SELECT
            *,
        FROM
            `dh-global-sales-data-dev.leadgen_sf_match_vertex_raw.candidates_PO_FI_non_all`
        WHERE
            TRUE
            AND `dh-global-sales-data.achilles.row_id_to_lead_source`(left_row_id) = 'facebook'

        UNION DISTINCT

        SELECT
            *,
        FROM
            `dh-global-sales-data-dev.leadgen_sf_match_vertex_raw.candidates_PO_FI_geo_non`
        WHERE
            TRUE
            AND `dh-global-sales-data.achilles.row_id_to_lead_source`(left_row_id) = 'facebook'
    ),
    calc AS
    (
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
            haversine,
            COALESCE(tokenset_name_stop, `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name_stop, right_name_stop)) AS tokenset_name_stop,
            `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_street_stop, right_street_stop) AS tokenset_street_stop,
            COALESCE(tokenset_name_local, `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name_local, right_name_local)) AS tokenset_name_local,
            COALESCE(tokenset_name_local_transliterated, `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name_local_transliterated, right_name_local_transliterated)) AS tokenset_name_local_transliterated,
            COALESCE(tokenset_name_legal, `dh-global-sales-data`.nlp.token_set_ratio_levenshtein(left_name, right_name_legal)) AS tokenset_name_legal,
            --candidate_source  # note that "union distinct" will not work as expected when this line is uncommented.
            -- This can be implemented via "DISTINCT ON"-like clause
        FROM
            comb
    )

    SELECT
        *
    FROM
        calc
)
;