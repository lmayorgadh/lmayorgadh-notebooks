CREATE TABLE `dh-global-sales-data-dev.leadgen_sf_match_vertex_delta_raw.right_sf_PO_FI` AS --output table
(
WITH country_map AS (
    SELECT DISTINCT
        country_code AS country_iso,
        global_entity_id
    FROM
        `fulfillment-dwh-production.curated_data_shared_coredata.global_entities`
    WHERE global_entity_id in ('PO_FI')
)
,

sf_account AS (
    SELECT 
        global_entity_id, grid__c,account_status__c, 
        type as branch_type, recordtypeid, 
        revenue_model__c, special_characteristics__c,
        translated_account_name__c, legal_name__c
    FROM `fulfillment-dwh-production.curated_data_shared_salesforce.account` --no access
    WHERE global_entity_id in ('PO_FI')
 )
,valid_type_accounts AS (
    SELECT r.*, rtype.name as record_type, 
    FROM sf_account as r
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_salesforce.recordtype` as rtype --no access
    ON r.recordtypeid = rtype.id
    ORDER BY 1,2,3
)
,sf_lead AS (
    SELECT 
        global_entity_id, grid__c, 
        recordtypeid, special_characteristics__c
    FROM `fulfillment-dwh-production.curated_data_shared_salesforce.lead` --no access
    WHERE global_entity_id in ('PO_FI')
)
,valid_type_leads AS (
  SELECT r.*, rtype.name as record_type, 
  FROM sf_lead as r
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_salesforce.recordtype` as rtype --no access
  ON r.recordtypeid = rtype.id
  ORDER BY 1,2
)
,joined_input AS (
SELECT
    l.country_iso,
    `dh-global-sales-data.achilles.build_lead_row_id`(cm.global_entity_id,l.lead_provider,l.lead_source,l.lead_id) AS row_id, --create joint row_id through GBQ persistent function
    l.restaurant_name AS name,
    sf_account.translated_account_name__c AS name_local,
    sf_account.legal_name__c AS name_legal,
    CASE
        -- TODO 06.03.23 this is a temporal crutch that should be removed once models have 
        -- more diverse feature set.
        WHEN 
            cm.global_entity_id in ('TB_BH', 'TB_KW', 'FP_SG') 
            AND restaurant_name_ascii IS NOT NULL
            -- to account for names that have just 1-2 ascii symbols in them and the rest is non-ascii
            AND LENGTH(COALESCE(restaurant_name_ascii, '')) > 2
            THEN restaurant_name_ascii
        ELSE restaurant_name_translated
    END AS name_translated,
    restaurant_address AS street,
    restaurant_address_translated AS street_translated,
    phone_number,
    restaurant_lat AS lat,
    restaurant_long AS lng,
    COALESCE(city_lat, restaurant_lat) as city_lat,
    COALESCE(city_long, restaurant_long) AS city_lng,
    sf_vendor_id AS vendor_id,
    lead_uuid,
    report_date,
    registration_number,
    -- Columns for filtering
    sf_account.branch_type,
    sf_account.revenue_model__c,
    COALESCE (sf_account.record_type, sf_lead.record_type) AS record_type,
    COALESCE (sf_account.special_characteristics__c,sf_lead.special_characteristics__c) AS special_characteristics__c
FROM
    `dh-global-sales-data.leadgen_cl.vendor_complete` AS l --who creates this table? what does it contain?
INNER JOIN
    country_map AS cm
ON
    l.country_iso=cm.country_iso
LEFT JOIN  valid_type_accounts as sf_account
ON
    l.lead_id = sf_account.grid__c
LEFT JOIN  valid_type_leads as sf_lead
ON
    l.lead_id = sf_lead.grid__c
-- TODO Once rejected leads are part of saleslayer, this should be removed
LEFT JOIN `dh-global-sales-data.cl_lead_publish.fct_manual_rejected` AS m_rejected -- ask about this
ON
    l.lead_id = m_rejected.lead_id

WHERE
    -- Do not consider brand-specific cloud kitchens.
    (l.is_dark_kitchen IS NULL OR l.is_dark_kitchen = FALSE) AND
    --Do not consider deleted salesforce objects.
    (l.isdeleted IS NULL OR l.isdeleted = FALSE) AND
    -- Only consider Main-Branch, virtual, kitchen and leads
    -- (l.sf_type IS NULL OR l.sf_type in ('Branch - Main', 'Branch - Virtual Restaurant', 'Branch - Kitchen Restaurant', 'lead')) AND
    -- Require location information.
    (restaurant_address IS NOT NULL OR (restaurant_lat + restaurant_long IS NOT NULL)) AND
    -- Ideally, we would require city location for candidate retrieval.
    -- However, sometimes lead providers give us only vendor coordinates.
    -- In this case we do a simple substitution (hence COALESCE statement for city_lat and city_long above)
    -- with a vendor coordinates. Corollary: vendors without both vendor and city coordinates
    -- should be discarded.
    ((city_lat + city_long IS NOT NULL) OR (restaurant_lat + restaurant_long IS NOT NULL)) AND
    -- Do not include manually rejected leads
    m_rejected.lead_id IS NULL AND
    -- Exclude leads with problems with lats/longs (see GSD-5197 ticket for more context)
    (l.is_correct_lat_long is NULL or l.is_correct_lat_long = True)
    -- Filter to specific sources.
    AND l.lead_source='salesforce'
    AND DATE(l.last_updated) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

SELECT * EXCEPT (branch_type, record_type, revenue_model__c, special_characteristics__c)
FROM joined_input
WHERE 
-- Only platforms that are accepting orders to be considered
(revenue_model__c is null or revenue_model__c like ("%Ordering Platform%")) 
--  All records that are not Corporates, to be considered
AND (NOT(record_type like ("%Corporate%")) or record_type is null) 
AND (branch_type is null or branch_type in ("Branch - Main","Branch - Virtual Restaurant","Branch - Kitchen Restaurant","Branch - Virtual Shop"))
AND (special_characteristics__c is null OR special_characteristics__c not like ("%0/0 Partner%")) 
)
;