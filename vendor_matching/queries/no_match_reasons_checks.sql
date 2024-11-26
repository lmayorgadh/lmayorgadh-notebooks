## CHECK GRIDIDs IN VENDOR MATCHING OUTPUT

SELECT *
FROM `dh-global-sales-data.cl_saleslayer.leadgen_sf_match` 
WHERE global_entity_id in ("DJ_CZ") 
and lead_source in ("wolt")
and sf_grid_id in ('45TQLT', 'HZADFO', '4CR1F2', '4CGW4S', '45T276', '45TXOH', 'HADOP8', 'HWQR4X', 'HRDIVV', '4CJ0FH', '45TQOP', '4FMMM9', '4CZMS5', '4F5A6K', '45T2K4', 'HR3WEQ', '4C4JYL', '4CNRPA', '4F253Z', '45TX6C', '4C7JO8', 'HZ331P', 'HAN4JL', '4C89TH', '4CH58I', '4C4DFE', '4CO80N', '45TXUF', '45T24T', 'HRDIVI', 'HRDIV2', 'HW25G7', '4C7J0O', 'HW8H2A', 'HWB1IR', 'HRDIXB', 'HRDIX3', '45TVDU', '4FXELS', '45C2VT', 'HRDIX4', '45N2KQ', 'HZZC7H', '45T2ER', '45T2EZ', 'HRDIXD', '45TVDY', 'HRDIXG', 'HW67EK', 'HZ1YSJ', '45TQZM', 'HWV3TP')

###########################################################################

## CHECK IF MATCHED WITH LEAD_ID AND/OR SF_GRID_ID
## IF THEY ARE NOT IN MATCHED OUTPUT: CANDGEN ERROR - GEO OR NON-GEO
## IF THEY ARE MATCHED AND HAVE 0 LABEL: MODEL ERROR
## IF MULTIPLE MATCHES ARE HAVING PREDICTED LABEL AS 1: PROBABLY POSTPROCESS ERROR
 
##NOTE: PROBABILITY IS FOR THE SUBSEQUENT LABELS.
########## IF LABEL IS 0 AND PROB IS 0.99, IT MEANS THAT THE ACTUAL PROB IS 0.01.
########## IF LABEL IS 1 AND PROB IS 0.99, IT MEANS THAT THE ACTUAL PROB IS 0.99.
 
WITH matched AS (
    select
    m.left_row_id, right_row_id,
    predicted_label, m.model_name,
    prob AS predicted_prob,
    from `dh-global-sales-data.leadgen_sf_match_vertex_raw.matched_DJ_CZ` AS m
)
select
-- *,
`dh-global-sales-data.achilles.row_id_to_lead_source`(c.left_row_id) as lead_source,
`dh-global-sales-data.achilles.row_id_to_lead_id`(c.left_row_id) as lead_id,
`dh-global-sales-data.achilles.row_id_to_lead_id`(c.right_row_id) as sf_grid_id,
-- c.left_row_id, c.right_row_id,
matched.predicted_label, matched.predicted_prob,
c.left_name,c.right_name, c.left_street,c.right_street,
feat.haversine, feat.tokenset_name_stop, feat.tokenset_street_stop, feat.jaro_winkler_name,
c.left_name_stop,c.right_name_stop, c.left_street_stop,c.right_street_stop,  matched.model_name,
from matched
right join `dh-global-sales-data.leadgen_sf_match_vertex_raw.candidates_DJ_CZ` AS c
USING (left_row_id,right_row_id)
FULL JOIN
`dh-global-sales-data.leadgen_sf_match_vertex_raw.features_DJ_CZ` as feat
USING (left_row_id,right_row_id)
where 
`dh-global-sales-data.achilles.row_id_to_lead_id`(c.right_row_id) in ("45N2KQ")  and
`dh-global-sales-data.achilles.row_id_to_lead_source`(c.left_row_id) in ('wolt')
 and  `dh-global-sales-data.achilles.row_id_to_lead_id`(c.left_row_id) in ('55173143-f3db-4f6f-82ab-d00e0fc16b30')
-- and matched.predicted_label = 1
order by right_row_id,  matched.predicted_label DESC, matched.predicted_prob DESC;
 
###########################################################################
### CANDIDATE GENERATION CHECKS

select * 
from `dh-global-sales-data.leadgen_sf_match_vertex_raw.candidates_DJ_CZ` a
where `dh-global-sales-data.achilles.row_id_to_lead_id`(a.left_row_id) in ('27163983-de44-444e-a6d9-fd6c836d260e')
and `dh-global-sales-data.achilles.row_id_to_lead_id`(a.right_row_id) in ("45TXUF");

select 'right', * 
from `dh-global-sales-data.leadgen_sf_match_vertex_raw.right_processed_DJ_CZ` a
where `dh-global-sales-data.achilles.row_id_to_lead_id`(a.row_id) in ('4FXELS')

union all

select 'left', * 
from `dh-global-sales-data.leadgen_sf_match_vertex_raw.left_processed_DJ_CZ` a
where `dh-global-sales-data.achilles.row_id_to_lead_id`(a.row_id) in ('523342aa-82b3-484f-87d9-f3d0cecaf02e');
 

###########################################################################
 
###### POSTPROCESS ERROR ######
## CHECK PRIORITY FOR A GIVEN SF ACCOUNT IN POSTPROCESS WORKING (LOWER VALUE IS BETTER)
 
## CHECK WITH SF_GRID_ID: IF SOME OTHER LEAD IS GIVEN LOWER PRIORITY THAN OUR DESIRED LEAD-> POSTPROCESS ERROR
## CHECK WITH LEAD_ID: IF SOME OTHER GRID_ID EX. SF_LEAD IS GIVEN LOWER PRIORITY THEN WE HAVE THAT AS SF_STATUS IN MATCHING OUTPUT.
########### LOCAL TEAMS DONT LIKE THIS. THEY WANT TO MATCH AGAINST SF_ACTIVE_ACCOUNT ALWAYS
select
post.sf_status, post.sf_grid_id, post.lead_source, post.lead_id, post.confidence, post.match_priority,
c.left_name,c.right_name, c.left_street,c.right_street,
c.haversine, c.tokenset_name_stop, c.tokenset_street_stop,
from `dh-global-sales-data.leadgen_sf_match_vertex_raw.postprocess_working` as post
right join
`dh-global-sales-data.leadgen_sf_match_vertex_raw.candidates_DJ_CZ` AS c
ON
`dh-global-sales-data.achilles.row_id_to_lead_id`(c.left_row_id) = post.lead_id
and `dh-global-sales-data.achilles.row_id_to_lead_id`(c.right_row_id) = post.sf_grid_id
where
post.global_entity_id in ("DJ_CZ") and
 `dh-global-sales-data.achilles.row_id_to_lead_source`(c.left_row_id) in ('wolt')
 and sf_grid_id in ('HAABOB')
-- where lead_id like ('b51c91b5-71b8-488c-939b-f39722f294bc')
order by post.match_priority DESC;
 
###########################################################################
 
# CHECK FINAL OUTPUT
select *
from `dh-global-sales-data.cl_saleslayer.leadgen_sf_match`
where
    global_entity_id like ("DJ_CZ") and
    sf_grid_id in ('HAABOB')
    and lead_id in ("4ce29765-65b7-4e87-b634-2dfb5de69af7");
 
 
###########################################################################
 
select global_entity_id, count(lead_id), count(sf_grid_id)
from `dh-global-sales-data.leadgen_sf_match_vertex_raw.postprocess_working`
where
    global_entity_id in ("DJ_CZ") AND
    lead_source in ("wolt")
GROUP BY ALL
ORDER BY 1,2
;