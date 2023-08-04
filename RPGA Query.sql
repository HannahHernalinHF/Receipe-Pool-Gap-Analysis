--CPS All Countries Recipe Pool Query


-- we need to double check GB query, i think is wrong due to greenchef
/* Imporatant to know:
0 When adding new countries, check all the where statement filters from the following CTE's:
1 _all_countries CTEs are queries for all countries without exeptions
*/

--*************************************************  1 SCORES  *************************************************
--########### BENELUX ADDED AS country 'BNLF'
/*
NOTES TO CONSIDER:
0 This section needs to be modified for adding new countries
1 Spain is Missing in materialized_views.gamp_recipe_scores
2 When adding new countries, it is important to select the distribution center accordingly
*/
--------------------------------------------------------------------------------------------------------------
--URGENT******************
  -- Missing ES Spain in materialized_views.gamp_recipe_scores
  -- Check logic for BNLF
    -- Check if region BNLF is fine, or do we need countries as well
--CHANGES
  --Added BNLF as region into country


WITH recipe_scores_countries_simple AS
(
  SELECT *
  FROM
    (
      SELECT
            CASE
              WHEN region = 'DK' OR region = 'SE'
                  THEN "DKSE"
              WHEN region='NO'
                  THEN 'NO'
              WHEN country IN ('BE','NL','LU')
                  THEN 'BNL'
              ELSE country
            END AS country,
            mainrecipecode,
            score,
            rating_count,
            score_wscm,
            rating_count_wscm,
            hellofresh_week,
            DENSE_RANK() OVER (PARTITION BY mainrecipecode, country ORDER BY hellofresh_week DESC) AS Max_Rank
      FROM  materialized_views.gamp_recipe_scores
      WHERE (country IN ('IT','IE') AND score>0 AND rating_count>0) -- Add new countries countraints here
        OR  (region ='NO' AND score>0 AND rating_count>10)
        OR  (region IN ('DK','SE') AND score > 0 AND rating_count > 50)
        OR  (region ='BNLF' AND score>0 AND rating_count>0)
      ) AS T
  WHERE Max_Rank = 1
) --- this CTE includes markets: BNL, DKSE, FR, IE, IT, NO


--------------------------------------------------------------------------------------------------------------
, recipe_scores_countries_complex AS
(
  SELECT *
  FROM
    (
      SELECT
            country,
            mainrecipecode,
            score,
            rating_count,
            score_wscm,
            rating_count_wscm,
            hellofresh_week,
            DENSE_RANK() OVER (PARTITION BY  mainrecipecode, region, CASE WHEN RIGHT(uniquerecipecode,2) IN ('FR','CH','DK') THEN RIGHT(uniquerecipecode,2) ELSE 'X' END ORDER BY hellofresh_week DESC) AS Max_Rank
      FROM  materialized_views.gamp_recipe_scores
      WHERE region='GB' AND score>0 AND rating_count>100 -- Add new countries countraints here
    ) AS T
  WHERE Max_Rank = 1
) --- this CTE includes market: GB



--------------------------------------------------------------------------------------------------------------
, recipe_scores_all_countries AS
(
  SELECT *
  FROM recipe_scores_countries_simple
    UNION ALL
  SELECT *
  FROM recipe_scores_countries_complex
)
--------------------------------------------------------------------------------------------------------------
, scores_all_countries AS
(
  SELECT
        country,
        mainrecipecode,
        SUM(score*rating_count)/SUM(rating_count) as scorewoscm,
        SUM(score_wscm*rating_count_wscm)/SUM(rating_count_wscm) AS scorescm
  FROM  recipe_scores_all_countries
  GROUP BY 1,2
)

--*************************************************  2 NUTRITION/USAGE  *************************************************
--########### BENELUX ADDED AS market benelux
/*
NOTES TO CONSIDER:
0 This section needs to be modified for adding new countries
1 Spain is Missing here
2 When adding new countries, it is important to select the distribution center accordingly
3 Seasonality not needed replaced with Uzzys DM https://tableau.hellofresh.io/#/views/MVPSeasonalityRiskView/SeasonalityRiskView?:iid=2
*/
--------------------------------------------------------------------------------------------------------------
-- changes
  -- added benelux in WHERE (market IN ('it','ie','benelux','es') AND region_code IS NOT NULL)
  --deleted jp

, recipe_usage_all_countries AS
(
  SELECT *,
        CASE
          WHEN market = 'dkse' AND region_code = 'se'
            THEN 'dkse'
          WHEN market = 'dkse' AND region_code = 'no'
            THEN 'no'
          ELSE market
        END AS country_final_id --In order to join with all_recipes and unique code
  FROM  materialized_views.isa_services_recipe_usage
  WHERE (market IN ('it','ie','benelux','es','fr') AND region_code IS NOT NULL)
    OR  market = 'gb'
    OR  region_code <> 'dk'
)
--------------------------------------------------------------------------------------------------------------
-- changes
  --deleted jp

, nutrition_all_countries AS

(
  SELECT *,
        CASE
          WHEN market = 'dkse' AND segment = 'SE'
            THEN 'dkse'
          WHEN market = 'dkse' AND segment = 'NO'
            THEN 'no'
          ELSE market
        END AS country_final_id --In order to join with all_recipes and unique code
  FROM  materialized_views.culinary_services_recipe_segment_nutrition
  WHERE segment <> 'DK' AND segment <> 'JP' AND country <> 'ES'
    OR  country = 'GB' AND market = 'gb'
)

--*************************************************  3 COSTS/SKU COSTS  *************************************************
--########### BENELUX ADDED AS distribution_center = 'DH'
-------URGENT Missing ES Spain in materialized_views.culinary_services_recipe_static_price
/*
NOTES TO CONSIDER:
0 This section needs to be modified for adding new countries
1 Spain is Missing here
2 When adding new countries, it is important to select the distribution center accordingly
3 It is important to select the range of weeks for costing the skus properly
4 Seasonality not needed replaced with Uzzys DM https://tableau.hellofresh.io/#/views/MVPSeasonalityRiskView/SeasonalityRiskView?:iid=2
*/
--------------------------------------------------------------------------------------------------------------
-- changes
  --deleted jp
  -- added BNLF and spain
  -- FIXED QUERY TO INCLUDE ALL SEGMENTS

, cost_all_countries AS
(
  SELECT
        CASE
          WHEN segment = 'GR'
            THEN 'GB'
          WHEN segment = 'SE'
            THEN 'DKSE'
          WHEN segment IN ('BE','NL','LU') AND distribution_center = 'DH'
            THEN 'BNL'
          ELSE segment
        END AS segment,
        recipe_id,
        size,
        AVG(price) AS cost
  FROM  materialized_views.culinary_services_recipe_static_price
  WHERE hellofresh_week >= '2023-W30' AND hellofresh_week <= '2023-W43'AND -- Here is filter for weeks
        segment IN ('IT','IE')
    OR  distribution_center = 'DH' -- for adding benelux
    OR  segment = 'SE' AND distribution_center = 'SK'
    OR  segment='GR' AND distribution_center='GR'
    OR  segment = 'NO' AND distribution_center = 'MO'
  GROUP BY 1,2,3
)

------------NEW PRICING METHOD ALL EXCEPT NO------------
-- changes
  --deleted jp
  -- added BNLF and spain

, current_running_quarter AS -- calculate current running quarter (this code is built this way, in case that user needs monthly, weekly, etc calculations.
(
  SELECT MAX(running_quarter) AS rqm
  FROM dimensions.date_dimension
  WHERE  date_string_backwards = DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy-MM-dd')
)

, current_quarter_avg_sku_price_except_NO AS -- calculate avg price of current quarter
(
  SELECT
        CASE
            WHEN sku.market = 'beneluxfr'
            THEN 'benelux'
            ELSE sku.market
        END AS market,
        sku.code,
        sp.currency,
        AVG(sp.price) AS current_quarter_avg_sku_price
  FROM  materialized_views.procurement_services_staticprices AS sp
  LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
    ON  sku.id = sp.culinary_sku_id
  LEFT JOIN dimensions.date_dimension AS hfd
    ON sp.hellofresh_week = hfd.hellofresh_week
  WHERE hfd.running_quarter =
        (
          SELECT rqm
          FROM current_running_quarter
        )
    OR  sku.market='dkse'AND sp.distribution_center = 'SK'
    OR  sku.market='gb' AND sp.distribution_center='GR'
    OR  sku.market IN ('it','ie','beneluxfr','es')
  GROUP BY 1,2,3
)


, next_quarter_avg_sku_price_except_NO AS -- calculate avg price of following quarter
(
    SELECT
        CASE
            WHEN sku.market = 'beneluxfr'
            THEN 'benelux'
            ELSE sku.market
        END AS market,
        sku.code,
        AVG(sp.price) AS next_quarter_avg_sku_price
  FROM  materialized_views.procurement_services_staticprices AS sp
  LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
    ON  sku.id = sp.culinary_sku_id
  LEFT JOIN dimensions.date_dimension AS hfd
    ON sp.hellofresh_week = hfd.hellofresh_week
  WHERE hfd.running_quarter =
        (
          SELECT rqm + 1
          FROM current_running_quarter
        )
    OR  sku.market='dkse'AND sp.distribution_center = 'SK'
    OR  sku.market='gb' AND sp.distribution_center='GR'
    OR  sku.market IN ('it','ie','beneluxfr','es')
  GROUP BY 1,2
)

, sku_cost_all_except_NO AS
(
  SELECT
        cq.market,
        cq.code,
        cq.currency,
        cq.current_quarter_avg_sku_price,
        nq.next_quarter_avg_sku_price
  FROM current_quarter_avg_sku_price_except_NO AS cq
  LEFT JOIN next_quarter_avg_sku_price_except_NO AS nq
  ON cq.market = nq.market AND cq.code = nq.code
    GROUP BY 1,2,3,4,5
)
------------NEW PRICING METHOD ALL ONLY NO------------

, current_quarter_avg_sku_price_NO AS -- calculate avg price of current quarter
(
  SELECT
        'no' AS market,
        sku.code,
        sp.currency,
        AVG(sp.price) AS current_quarter_avg_sku_price
  FROM  materialized_views.procurement_services_staticprices AS sp
  LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
    ON  sku.id = sp.culinary_sku_id
  LEFT JOIN dimensions.date_dimension AS hfd
    ON sp.hellofresh_week = hfd.hellofresh_week
  WHERE hfd.running_quarter =
        (
          SELECT rqm
          FROM current_running_quarter
        )
    AND sku.market='dkse'AND sp.distribution_center = 'MO'
  GROUP BY 1,2,3
)

, next_quarter_avg_sku_price_NO AS -- calculate avg price of following quarter
(
    SELECT
        'no' AS market,
        sku.code,
        AVG(sp.price) AS next_quarter_avg_sku_price
  FROM  materialized_views.procurement_services_staticprices AS sp
  LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
    ON  sku.id = sp.culinary_sku_id
  LEFT JOIN dimensions.date_dimension AS hfd
    ON sp.hellofresh_week = hfd.hellofresh_week
  WHERE hfd.running_quarter =
        (
          SELECT rqm + 1
          FROM current_running_quarter
        )
  AND sku.market='dkse'AND sp.distribution_center = 'MO'
  GROUP BY 1,2
)

, sku_cost_NO AS
(
  SELECT
        cq.market,
        cq.code,
        cq.currency,
        cq.current_quarter_avg_sku_price,
        nq.next_quarter_avg_sku_price
  FROM current_quarter_avg_sku_price_NO AS cq
  LEFT JOIN next_quarter_avg_sku_price_NO AS nq
  ON cq.market = nq.market AND cq.code = nq.code
)

--*************************************************  4 PICKLISTS  *************************************************
-- changes
  --deleted jp

/*
NOTES TO CONSIDER:
00 This section needs to be modified for adding new countries
0 Spain is Missing in cost_all_countries only
*/
--------------------------------------------------------------------------------------------------------------
--########### BENELUX ADDED AS market = 'benelux'

, isa_services_recipe_consolidated_simple AS
(
  SELECT *
  FROM
    (
      SELECT
            id,
            market,
            unique_recipe_code,
            recipe_code,
            version,
            status,
            title,
            is_default,
            primary_protein,
            main_protein,
            protein_cut,
            primary_starch,
            main_starch,
            primary_vegetable,
            main_vegetable,
            cuisine,
            dish_type,
            hands_on_time,
            hands_on_time_max,
            hands_off_time,
            hands_off_time_max,
            difficulty,
            tags,
            target_preferences,
            target_products,
            recipe_type,
            created_at,
            updated_at,
            DENSE_RANK() OVER (PARTITION BY recipe_code, market ORDER BY version  DESC) AS last_version
      FROM  materialized_views.isa_services_recipe_consolidated
      WHERE  -- Added in isa_services_recipe_consolidated_all_countries
        (market NOT IN ('jp','es'))
        AND ((market IN ('dkse','ie') AND recipe_type <> 'Modularity' AND recipe_type <> 'Surcharge' AND LOWER(status) IN ('ready for menu planning'))
        OR  (market = 'gb' AND recipe_type <> 'Modularity' AND recipe_type <> 'Surcharge' AND LOWER(status) IN ('ready for menu planning','in development', 'final cook', 'external testing') AND is_default = 1)
        OR  (market = 'it' AND recipe_type <> 'Modularity' AND recipe_type <> 'Surcharge' AND LOWER(status) IN ('ready for menu planning','in development'))
        OR  (market = 'benelux' AND recipe_type <> 'Modularity' AND recipe_type <> 'Surcharge' AND LOWER(status) IN ('ready for menu planning','under improvement'))
        OR  (market = 'fr' AND recipe_type <> 'Modularity' AND recipe_type <> 'Surcharge' AND LOWER(status) IN ('ready for menu planning','planned')))
    ) AS temp
  WHERE (market NOT IN ('ie','it','es') AND last_version = 1)
    OR  (market IN ('ie','it') AND is_default = 1 AND recipe_type <> 'Modularity' AND recipe_type <> 'Surcharge')
)


--Markets ie, es, gb, it, dkse, benelux, jp
--------------------------------------------------------------------------------------------------------------
, isa_services_recipe_consolidated_NO AS
(
  SELECT
        id,
        'no' as market,
        unique_recipe_code,
        recipe_code,
        version,
        status,
        title,
        is_default,
        primary_protein,
        main_protein,
        protein_cut,
        primary_starch,
        main_starch,
        primary_vegetable,
        main_vegetable,
        cuisine,
        dish_type,
        hands_on_time,
        hands_on_time_max,
        hands_off_time,
        hands_off_time_max,
        difficulty,
        tags,
        target_preferences,
        target_products,
        recipe_type,
        created_at,
        updated_at,
        1 AS last_version
  FROM isa_services_recipe_consolidated_simple
  WHERE market = 'dkse' AND recipe_type <> 'Modularity' AND recipe_type <> 'Surcharge' AND LOWER(status) = 'ready for menu planning'
)
--------------------------------------------------------------------------------------------------------------
, isa_services_recipe_consolidated_all_countries AS
(
  SELECT *
  FROM
      (SELECT *
      FROM isa_services_recipe_consolidated_simple
        UNION ALL
      SELECT *
      FROM isa_services_recipe_consolidated_NO) AS ISR
  WHERE target_products NOT LIKE 'add-ons%' -- Filtering last modularity and add-ons recipes, there is no established logic, so this is the best way
    AND target_products NOT LIKE 'addon%'
    AND target_products NOT LIKE 'gc-%'
    AND target_products NOT LIKE '%gc-%'
    AND target_products <> 'modularity'
    AND target_products <> '1.2-nl-only'
    AND recipe_type NOT LIKE 'addon%'
    AND recipe_type NOT LIKE 'Add on%'
    AND recipe_type NOT LIKE 'Add-on%'
    AND recipe_type NOT LIKE 'Add-On%'
    AND dish_type <> '123'
)

--------------------------------------------------------------------------------------------------------------
--########### BENELUX ADDED AS market = 'benelux'

, picklists_simple AS
(
  SELECT
        unique_recipe_code,
        market,
        currency,
        CONCAT_WS(" | ", COLLECT_LIST(code)) AS skucode,
        CONCAT_WS(" | ", COLLECT_LIST(name)) AS skuname,
        NULL AS boxitem, --NO and DKSE have this column; it is added here for a proper UNION ALL
        COUNT(DISTINCT code) AS skucount,
        SUM(cost2p_current) AS cost2p_current,
        SUM(cost2p_next) AS cost2p_next,
        CONCAT_WS(" | ", COLLECT_LIST(price_missing)) AS pricemissingskus
  FROM
    (
      SELECT
            r.unique_recipe_code,
            p.code,
            r.market,
            currency,
            REGEXP_REPLACE(p.name, '\t|\n', '') AS name,
            CASE WHEN current_quarter_avg_sku_price IS NULL or current_quarter_avg_sku_price=0 THEN p.code END AS price_missing,
            SUM(CASE WHEN size = 2 THEN pick_count * current_quarter_avg_sku_price ELSE 0 END) AS cost2p_current,
            SUM(CASE WHEN size = 2 THEN pick_count * next_quarter_avg_sku_price ELSE 0 END) AS cost2p_next
      FROM  isa_services_recipe_consolidated_simple r
      JOIN  materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
        ON  r.id = p.recipe_id AND r.market = p.market
      JOIN  (
              SELECT *,
                  CASE WHEN market = 'beneluxfr'
                      THEN 'benelux'
                      ELSE market
                      END AS market_id
              FROM materialized_views.procurement_services_culinarysku
            ) AS pk
        ON  p.code = pk.code AND p.market = pk.market_id --- p.market = benelux and pk.market = beneluxfr
      LEFT JOIN sku_cost_all_except_NO AS c
        ON  c.code = p.code AND c.market = p.market
      WHERE (r.market = 'dkse' AND p.segment_name = 'SE')
        OR  (r.market = 'gb' AND p.segment_name = 'GR')
        OR  (r.market IN ('ie','it','benelux','fr'))
      --r.market IN ('ie','it','gb','jp','dkse',) --r.market filter not really needed but segment_name is needed for dkse and gb
        --AND p.segment_name IN ('IE','IT','GR','JP','SE')
      GROUP BY 1,2,3,4,5,6
    ) AS t
  GROUP BY 1,2,3
)
--------------------------------------------------------------------------------------------------------------

, picklists_NO AS
(
  SELECT
        unique_recipe_code,
        'no' AS market,
        currency,
        CONCAT_WS(" | ", COLLECT_LIST(code)) AS skucode,
        CONCAT_WS(" | ", COLLECT_LIST(name)) AS skuname,
        SUM(COALESCE(boxitem,0)) AS boxitem,
        COUNT(DISTINCT code) AS skucount,
        SUM(cost2p_current) AS cost2p_current,
        SUM(cost2p_next) AS cost2p_next,
        CONCAT_WS(" | ", COLLECT_LIST(price_missing)) AS pricemissingskus
  FROM
    (
      SELECT
            r.unique_recipe_code,
            p.code,
            r.market,
            REGEXP_REPLACE(p.name, '\t|\n', '') AS name,
            boxitem,
            currency,
            CASE WHEN current_quarter_avg_sku_price IS NULL or current_quarter_avg_sku_price=0 THEN p.code END AS price_missing,
            SUM(CASE WHEN size = 2 THEN pick_count * current_quarter_avg_sku_price ELSE 0 END) AS cost2p_current,
            SUM(CASE WHEN size = 4 THEN pick_count * next_quarter_avg_sku_price ELSE 0 END) AS cost2p_next
      FROM  isa_services_recipe_consolidated_NO r
      JOIN  materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
        ON  r.id = p.recipe_id --AND r.market = p.market->not needed since codes are filtered only for NO
      LEFT JOIN sku_cost_NO AS c
        ON  c.code = p.code --AND c.market = p.market->not needed since codes are filtered only for NO
      LEFT JOIN uploads.gamp_dkse_boxitems b
        ON  b.code= p.code
      WHERE p.segment_name = 'NO'
      GROUP BY 1,2,3,4,5,6,7
    ) AS t
  GROUP BY 1,2,3
)
--------------------------------------------------------------------------------------------------------------
, picklists_all_countries AS
(
  SELECT *
  FROM picklists_simple
    UNION ALL
  SELECT *
  FROM picklists_NO
)

--*************************************************  5 ALL RECIPES  *************************************************

/*
NOTES TO CONSIDER:
00 This section needs to be modified for adding new countries
0 Spain is Missing in cost_all_countries only
1 Newlydded created_at and updated_at from created_at materialized_views.isa_services_recipe_consolidated r
2 Added all surcharges and modularity filters here instead of ISA_SERVICES, in case this needs to be changed after and better control
*/
--------------------------------------------------------------------------------------------------------------
, all_recipes_all_countries AS
(
  SELECT
        r.id AS recipe_id,
        UPPER(r.market) AS country,
        r.unique_recipe_code AS uniquerecipecode,
        r.recipe_code AS code,
        r.version,
        r.status,
        REGEXP_REPLACE(r.title, '\t|\n', '') AS title,
        NULL AS slot_number, -- added to match columns with export table.
        NULL AS menu_status, -- added to match columns with export table.
        u.last_used AS lastused,
        u.next_used AS nextused,
        CASE WHEN u.absolute_last_used IS NULL THEN '' ELSE u.absolute_last_used END AS absolutelastused,
        COALESCE(CAST(u.is_newrecipe AS INTEGER),1) AS isnewrecipe,
        COALESCE(CAST(u.is_newscheduled AS INTEGER),0) AS isnewscheduled,
        r.is_default AS isdefault,
        r.primary_protein AS primaryprotein,
        r.main_protein AS mainprotein,
        r.protein_cut AS proteincut,
        r.primary_starch AS primarystarch,
        r.main_starch AS mainstarch,
        COALESCE(r.primary_vegetable, 'none') AS primaryvegetable,
        r.main_vegetable AS mainvegetable,
        p.currency,
        CASE
          WHEN n.energy = 0 OR n.energy IS NULL
            THEN 999
          ELSE n.energy
        END AS calories,
        CASE
          WHEN n.carbs = 0 OR n.carbs IS NULL
            THEN 999
          ELSE n.carbs
        END AS carbohydrates,
        r.cuisine,
        r.dish_type AS dishtype,
        CASE
          WHEN r.market IN ('jp', 'it') -- NEW COUNTRIES MAY ENTER HERE
            THEN
              (CASE
                  WHEN r.hands_on_time_max = "" OR r.hands_on_time_max IS NULL
                    THEN CAST(99 AS FLOAT)
                  ELSE CAST(r.hands_on_time_max AS FLOAT)
              END
               +
              CASE
                  WHEN r.hands_off_time_max = "" OR r.hands_off_time_max IS NULL
                    THEN CAST(99 AS FLOAT)
                  ELSE CAST(r.hands_off_time_max AS FLOAT)
              END)
          WHEN r.market NOT IN ('jp', 'it')
            THEN
              (CASE
                  WHEN r.hands_on_time = "" OR r.hands_on_time IS NULL
                    THEN CAST(99 AS FLOAT)
                  ELSE CAST(r.hands_on_time AS FLOAT)
              END
              +
              CASE
                  WHEN r.hands_off_time = "" OR r.hands_off_time IS NULL
                    THEN CAST(99 AS FLOAT)
                  ELSE CAST(r.hands_off_time AS FLOAT)
              END)
        END AS totaltime,
        r.difficulty,
        r.tags AS tag,
        r.target_preferences AS preference,
        r.target_products AS producttype,
        r.recipe_type AS recipetype,
        r.created_at, -- Newly added by Pedro
        r.updated_at, -- Newly added by Pedro
        ROUND(p.cost2p_current, 2) AS cost2p_current,
        ROUND(p.cost2p_next, 2) AS cost2p_next,
        CASE
          WHEN s.scorescm IS NOT NULL
            THEN s.scorescm
          WHEN AVG(s.scorescm) OVER (PARTITION BY r.primary_protein) IS NOT NULL
            THEN AVG(s.scorescm) OVER (PARTITION BY r.primary_protein)
          WHEN AVG(s.scorescm) OVER (PARTITION BY r.main_protein) IS NOT NULL
            THEN AVG(s.scorescm) OVER (PARTITION BY r.main_protein)
          ELSE 3.4
        END AS scorescm,
        CASE
          WHEN s.scorewoscm IS NOT NULL
              THEN s.scorewoscm
          WHEN AVG(s.scorewoscm) over (PARTITION BY r.primary_protein) IS NOT NULL
              THEN AVG(s.scorewoscm) over (PARTITION BY r.primary_protein)
          WHEN AVG(s.scorewoscm) over (PARTITION BY r.main_protein) IS NOT NULL
              THEN AVG(s.scorewoscm) over (PARTITION BY r.main_protein)
          ELSE 3.4
        END AS scorewoscm,
        CASE
          WHEN s.scorewoscm IS NULL
            THEN 1
          ELSE 0
        END AS isscorereplace,
        p.skucode,
        p.skuname,
        p.skucount,
        'Recipe Pool' AS source,
        NULL AS hellofresh_week
  FROM  isa_services_recipe_consolidated_all_countries AS r --Working for all_countries
  LEFT JOIN recipe_usage_all_countries AS u --Working for all_countries
    ON  u.recipe_code = r.recipe_code
    AND u.country_final_id =  r.market
  LEFT JOIN nutrition_all_countries AS n --Working for all_countries
    ON  n.recipe_id = r.id
    AND n.country_final_id =  r.market
  --LEFT JOIN (SELECT* FROM cost_all_countries WHERE size = 2) AS rc_2 -- Includes all_countries except Spain (ES)
    --ON  rc_2.recipe_id = r.id
    --AND LOWER(rc_2.segment) = r.market
  --LEFT JOIN (SELECT * FROM cost_all_countries WHERE size = 4) AS rc_4 -- Includes all_countries except Spain (ES)
    --ON  rc_4.recipe_id = r.id
    --AND LOWER(rc_4.segment) = r.market
  LEFT JOIN (SELECT * FROM scores_all_countries) AS s -- Includes all_countries except Spain (ES)
    ON  s.mainrecipecode = r.recipe_code
    AND LOWER(s.country) =  r.market
  LEFT JOIN picklists_all_countries AS p
    ON  p.unique_recipe_code = r.unique_recipe_code
    AND p.market = r.market
  WHERE
        --LOWER(r.recipe_type) <> 'modularity' -- Added in isa_services_recipe_consolidated_all_countries
        --AND LOWER(r.recipe_type) <> 'surcharge' -- Added in isa_services_recipe_consolidated_all_countries
        r.market NOT IN('jp','benelux')
    AND cost2p_current > 0
    AND
        (r.market ='ie'
        AND r.primary_protein <>'N/A'
        AND LENGTH(r.primary_protein)>0
        AND p.cost2p_current >0
        --AND p.cost4p >0
        )
    OR
        (r.market = 'it'
        AND p.cost2p_current > 0
        )
      --AND r.is_default = true
    OR
        (r.market = 'es'
        AND p.cost2p_current > 0
        )
    OR
        (r.market='gb'
        AND p.cost2p_current >1.5
        --AND p.cost4p >0
        --AND LOWER(r.recipe_type) <> 'surcharge' -- Added all surcharges and modularity filters here instead of ISA_SERVICES
        AND LOWER(r.title) NOT LIKE '%not use%'
        AND LOWER(r.title) NOT LIKE '%wrong%'
        AND LOWER(r.title) NOT LIKE '%test%'
        AND LOWER(r.title) NOT LIKE '%brexit%'
        AND LENGTH(r.primary_protein)>0
        AND r.primary_protein <>'White Fish - Coley'
        AND r.primary_protein <>'N/A'
        AND r.unique_recipe_code NOT LIKE '%MOD%'
        AND r.unique_recipe_code NOT LIKE '%ASD%'
        AND r.unique_recipe_code NOT LIKE 'GC%'
        AND r.unique_recipe_code NOT LIKE 'A%'
        AND r.unique_recipe_code NOT LIKE 'X%'
        AND r.target_products NOT IN ('add-on', 'Baking kits','Breakfast', 'Sides', 'Dessert', 'Bread','Brunch','Cheese', 'Desserts', 'Modularity', 'Ready Meals','Speedy lunch', 'Speedy Lunch' ,'Soup')
        )
    OR
        (r.market IN ('dkse','no')
        AND LENGTH (r.primary_protein)>0
        AND r.primary_protein <>'N/A'
        AND p.cost2p_current > 0
        )
        --AND LOWER(r.recipe_type) <> 'surcharge' -- Added all surcharges and modularity filters IN ISA_SERVICES)
)

--*************************************************  6 CROSS-PREFERENCES  *************************************************
-- CHECK COSTS2P = NULL, OR CARBS = 999, OR ANY OTHER MISISNG LOGIC (SPECIALLY ITALY AND GB)


/*
NOTES TO CONSIDER:
00 This section needs to be modified for adding new countries
0 Spain is Missing in cost_all_countries only
1 Newlydded created_at and updated_at from created_at materialized_views.isa_services_recipe_consolidated r
*/
--------------------------------------------------------------------------------------------------------------
, all_recipes_with_columns AS
(
  SELECT *,
          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%CALORIE_SMART%')
              OR LOWER(rp.preference) LIKE LOWER('%CS%')
              OR LOWER(rp.preference) LIKE LOWER('%CALORIE SMART%')
              OR LOWER(rp.preference) LIKE LOWER('%unter 650 Kalorien%')
              OR LOWER(rp.preference) LIKE LOWER('%CONTA CALORIE%')
              OR LOWER(rp.preference) LIKE LOWER('%Calorie Limited%')
              OR LOWER(rp.preference) LIKE LOWER('%Healthy%')
            THEN 'Calorie Smart'
        END AS Calorie_Smart,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%LOW CARB%')
              OR LOWER(rp.preference) LIKE LOWER('%CARB SMART%')
              OR LOWER(rp.preference) LIKE LOWER('%here%')
            THEN 'Carb Smart'
        END AS Carb_Smart,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%C3_APPROPRIATE%')
              OR LOWER(rp.preference) LIKE LOWER('%CLASSIC%')
              OR LOWER(rp.preference) LIKE LOWER('%Fleisch & Gemüse%')
              OR LOWER(rp.preference) LIKE LOWER('%Fleisch & GemÃ¼se%')
              OR LOWER(rp.preference) LIKE LOWER('%CARNE & PESCE%')
              OR LOWER(rp.preference) LIKE LOWER("%Chef's choice%")
              OR LOWER(rp.preference) LIKE LOWER("%CL%")
              OR LOWER(rp.preference) LIKE LOWER("%Fish Free%") --Newly added, all countries, not only GB
              OR LOWER(rp.preference) LIKE LOWER("%Pork Free%")
              OR LOWER(rp.preference) LIKE LOWER("%Keto%")
              OR LOWER(rp.preference) LIKE LOWER("%Gluten Free%")
              OR LOWER(rp.preference) = 'CL'
              OR rp.preference IS NULL
              OR rp.preference = ''
            THEN 'Classic'
        END AS Classic,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%DISCOVERY%')
            THEN 'Discovery'
        END AS Discovery,

          CASE --CHECK FOR BENELUX
            WHEN LOWER(rp.preference) LIKE LOWER('%FAMILY FRIENDLY%')
              OR LOWER(rp.preference) LIKE LOWER('%FAMILY%')
              OR LOWER(rp.preference) LIKE LOWER('%Familienfreundlich%')
              OR LOWER(rp.preference) LIKE LOWER('%FAMIGLIA%')
              OR (rp.country = 'BNL'
                AND LOWER(rp.preference) LIKE LOWER('%F,')
                OR  LOWER(rp.preference) = 'F')
            THEN 'Family'
        END AS Family,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%PESCATARIAN%')
              OR rp.mainprotein IN ('Fish','White Fish','Seafood','Salmon','Shellfish')
            THEN 'Pescatarian'
        END AS Pescatarian,
          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%QUICK%')
              OR LOWER(rp.preference) LIKE LOWER('%Quick & Easy%')
              OR LOWER(rp.preference) LIKE LOWER('%RAPID%')
              OR LOWER(rp.preference) LIKE LOWER('%EASY%')
              OR (rp.country = 'BNL'
                  AND LOWER(rp.preference) LIKE LOWER('%Q,')
                  OR  LOWER(rp.preference) = 'Q')
            THEN 'Quick and Easy'
        END AS Quick_and_Easy,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%VEGAN%')
            THEN 'Vegan'
        END AS Vegan,
          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%VEGETARIAN%')
              OR LOWER(rp.preference) LIKE LOWER('%VEGGIE%')
              OR LOWER(rp.preference) LIKE LOWER('%Vegetarisch%')
              OR LOWER(rp.preference) LIKE LOWER('%VEGETARIANO%')
              OR (rp.country = 'BNL'
                AND LOWER(rp.preference) LIKE LOWER('%V,')
                OR  LOWER(rp.preference) = 'V')
            THEN 'Veggie'
        END AS Veggie
  FROM all_recipes_all_countries AS rp
)

--------------------------------------------------------------------------------------------------------------
, recipe_pool_crosspreferences AS
(
  SELECT *,
          CASE
            WHEN rc.preference IS NULL
              OR rc.preference = ''
            THEN "EMPTY_PREFERENCE"
            WHEN    rc.preference IS NULL
                AND rc.Calorie_Smart IS NULL
                AND rc.Carb_Smart IS NULL
                AND rc.Classic IS NULL
                AND rc.Discovery IS NULL
                AND rc.Family IS NULL
                AND rc.Pescatarian IS NULL
                AND rc.Quick_and_Easy IS NULL
                AND rc.Vegan IS NULL
                AND rc.Veggie IS NULL
            THEN 'NOT_IDENTIFIED'
            ELSE 'AT_LEAST_1'
        END AS Category_Check,
          CASE
            WHEN rc.preference IS NULL
              OR rc.preference = ''
            THEN "EMPTY_PREFERENCE"
            ELSE
            CONCAT_WS(
                      ', ',
                      IF(rc.Calorie_Smart IS NOT NULL,'Calorie Smart',NULL),
                      IF(rc.Carb_Smart IS NOT NULL,'Carb Smart',NULL),
                      IF(rc.Classic IS NOT NULL,'Classic',NULL),
                      IF(rc.Discovery IS NOT NULL,'Discovery',NULL),
                      IF(rc.Family IS NOT NULL,'Family',NULL),
                      IF(rc.Pescatarian IS NOT NULL,'Pescatarian',NULL),
                      IF(rc.Quick_and_Easy IS NOT NULL,'Quick and Easy',NULL),
                      IF(rc.Vegan IS NOT NULL,'Vegan',NULL),
                      IF(rc.Veggie IS NOT NULL,'Veggie',NULL)
                    )
        END AS cross_preferences
        --NULL AS preference_slot -- added to match columns with export table.
  FROM all_recipes_with_columns rc
)

--------------------------------------------------------------------------------------------------------------

-- we are filtering out dk region, so only se is here. Check it out later on

--*************************** SERVICES NEW DEMAND ***************
-- Correct
, current_running_week AS
(
  SELECT MAX(running_week) AS current_week
  FROM dimensions.date_dimension
  WHERE date_string_backwards = DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy-MM-dd')
)

----------------------------
-- Correct
, minus_12_weeks AS
(
  SELECT current_week -12 AS running_week_minus_12
  FROM current_running_week
)


----------------------------
-- Correct
,services_menu AS
(
  SELECT
        CASE
           WHEN market='ca'
              AND ((slot_number > 50 AND slot_number < 1000)
              OR  slot_number > 4000)  THEN 'ck'
           WHEN region_code='se' THEN 'dkse'
           WHEN region_code='no' THEN 'no'
           WHEN market = 'gb' AND brand_name = 'HelloFresh' THEN 'gb'
           WHEN market = 'gb' AND brand_name = 'Green Chef' THEN 'gn'
           ELSE market
        END AS market,
        region_code,
        slot_number,
        sp.hellofresh_week,
        status AS menu_status,
        id AS id_menu,
        recipe_id,
        unique_recipe_code,
        MAX(hfd.running_week)
  FROM materialized_views.isa_services_menu AS sp
  LEFT JOIN dimensions.date_dimension AS hfd
  ON sp.hellofresh_week = hfd.hellofresh_week
  WHERE item_type <> 'addon'
    AND brand_name <> 'Green Chef'
    AND region_code <> 'dk'
    AND status <> 'draft'
    AND hfd.running_week >= (SELECT running_week_minus_12 FROM minus_12_weeks) -- filter from last 12 weeks demand
    AND market NOT IN ( 'ie', 'es', 'jp')
    GROUP BY 1,2,3,4,5,6,7,8
)

----------------------------

, services_recipes_all_except_NO AS
(
  SELECT
        id,
        market AS market_recipes,
        unique_recipe_code AS unique_recipe_code_recipes,
        recipe_code,
        version,
        status,
        title,
        is_default,
        primary_protein,
        main_protein,
        protein_cut,
        primary_starch,
        main_starch,
        primary_vegetable,
        main_vegetable,
        cuisine,
        dish_type,
        hands_on_time,
        hands_on_time_max,
        hands_off_time,
        hands_off_time_max,
        difficulty,
        tags,
        target_preferences,
        target_products,
        recipe_type,
        created_at,
        updated_at
  FROM  materialized_views.isa_services_recipe_consolidated
)


----------------------------


, services_recipes_NO AS
(
  SELECT
        id,
        'no' as market_recipes,
        unique_recipe_code_recipes,
        recipe_code,
        version,
        status,
        title,
        is_default,
        primary_protein,
        main_protein,
        protein_cut,
        primary_starch,
        main_starch,
        primary_vegetable,
        main_vegetable,
        cuisine,
        dish_type,
        hands_on_time,
        hands_on_time_max,
        hands_off_time,
        hands_off_time_max,
        difficulty,
        tags,
        target_preferences,
        target_products,
        recipe_type,
        created_at,
        updated_at
  FROM  services_recipes_all_except_NO
  WHERE market_recipes = 'dkse'
)

----------------------------

, services_recipes_all_countries AS
(
SELECT *
FROM services_recipes_all_except_NO
UNION ALL
SELECT *
FROM services_recipes_NO
)

----------------------------

, joined_services_demand AS
(
  SELECT *
  FROM services_menu sm
  LEFT JOIN services_recipes_all_countries rc
        ON  sm.unique_recipe_code = rc.unique_recipe_code_recipes
        AND sm.market = rc.market_recipes
)

----------------------------



--*************************** SLOT PREFERENCE QUERY ***************


,preferences_slot AS
(
  SELECT *
  FROM
  (
    SELECT
          recipe_preference AS preference,
          country,
          slot,
          hellofresh_week,
          year_quarter,
          DENSE_RANK() OVER (PARTITION BY country ORDER BY hellofresh_week DESC) AS last_week
    FROM  uploads.isa_preference_mapping
    WHERE LEFT(year_quarter,4) > '2021'
    ) AS p
WHERE p.last_week = 1
)

----------------------------

, preferences_slot_with_columns AS

(
    SELECT
          preference,
          country,
          slot,
          hellofresh_week,
          year_quarter,

  ------------ Calorie_Smart
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%CALORIE_SMART%')
                OR LOWER(rp.preference) LIKE LOWER('%CS%')
                OR LOWER(rp.preference) LIKE LOWER('%CALORIE SMART%')
                 OR LOWER(rp.preference) LIKE LOWER('%unter 650 Kalorien%')
                OR LOWER(rp.preference) LIKE LOWER('%CONTA CALORIE%')
                OR LOWER(rp.preference) LIKE LOWER('%Calorie Limited%')
      --------NEW
                OR LOWER(rp.preference) LIKE LOWER('%Balanced%')
                OR LOWER(rp.preference) LIKE LOWER('%Healthy%')
              THEN 'Calorie Smart'
          END AS Calorie_Smart,

  ------------ Carb_Smart
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%LOW CARB%')
                OR LOWER(rp.preference) LIKE LOWER('%CARB SMART%')
                OR LOWER(rp.preference) LIKE LOWER('%Carb Limited%')
                OR LOWER(rp.preference) LIKE LOWER('%here%')
              THEN 'Carb Smart'
          END AS Carb_Smart,

  ------------ Classic
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%C3_APPROPRIATE%')
                OR LOWER(rp.preference) LIKE LOWER('%CLASSIC%')
                OR LOWER(rp.preference) LIKE LOWER('%Fleisch & Gemüse%')
                OR LOWER(rp.preference) LIKE LOWER('%CARNE & PESCE%')
                OR LOWER(rp.preference) LIKE LOWER("%Chef's choice%")
                OR LOWER(rp.preference) LIKE LOWER("%CL%")
                OR LOWER(rp.preference) LIKE LOWER("%Fish Free%") --Newly added, all countries, not only GB
                OR LOWER(rp.preference) LIKE LOWER("%Pork Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Keto%")
                OR LOWER(rp.preference) LIKE LOWER("%Gluten Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Chef Choice%")
      --------NEW
                OR LOWER(rp.preference) LIKE LOWER("%Beef Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Everyday Favorites%")
                OR LOWER(rp.preference) LIKE LOWER("%Fish Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Flexible%")
                OR LOWER(rp.preference) LIKE LOWER("%Flexitarian%")
                OR LOWER(rp.preference) LIKE LOWER("%Gluten Free%")
                OR LOWER(rp.preference) LIKE LOWER("%HelloExtra%")
                OR LOWER(rp.preference) LIKE LOWER("%High Protein%")
                OR LOWER(rp.preference) LIKE LOWER("%Meat & Veggies%")
                OR LOWER(rp.preference) LIKE LOWER("%Mix%")
                OR LOWER(rp.preference) LIKE LOWER("%Pork & Fish Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Pork Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Protein Rich%")
                OR LOWER(rp.preference) LIKE LOWER("%Protein increase%")
                OR LOWER(rp.preference) LIKE LOWER("%Seafood Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Shellfish Free%")
                OR LOWER(rp.preference) LIKE LOWER("%Static%")
                OR rp.preference IS NULL
                OR rp.preference = ''
              THEN 'Classic'
          END AS Classic,

  ------------ Discovery
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%DISCOVERY%')
              THEN 'Discovery'
          END AS Discovery,

  ------------ Family
            CASE --CHECK FOR BENELUX
              WHEN LOWER(rp.preference) LIKE LOWER('%FAMILY FRIENDLY%')
                OR LOWER(rp.preference) LIKE LOWER('%FAMILY%')
                OR LOWER(rp.preference) LIKE LOWER('%Familienfreundlich%')
                OR LOWER(rp.preference) LIKE LOWER('%FAMIGLIA%')
                OR (rp.country = 'BNL'
                  AND LOWER(rp.preference) LIKE LOWER('%F,')
                  OR  LOWER(rp.preference) = 'F')
              THEN 'Family'
          END AS Family,

  ------------ Pescatarian
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%PESCATARIAN%')
                OR LOWER(rp.preference) LIKE LOWER('%Pescaterian%')
              THEN 'Pescatarian'
          END AS Pescatarian,

  ------------ Quick_and_Easy
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%QUICK%')
                OR LOWER(rp.preference) LIKE LOWER('%Quick & Easy%')
                OR LOWER(rp.preference) LIKE LOWER('%RAPID%')
                OR LOWER(rp.preference) LIKE LOWER('%EASY%')
                OR (rp.country = 'BNL'
                    AND LOWER(rp.preference) LIKE LOWER('%Q,')
                    OR  LOWER(rp.preference) = 'Q')
              THEN 'Quick and Easy'
          END AS Quick_and_Easy,

  ------------ Vegan
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%VEGAN%')
              THEN 'Vegan'
          END AS Vegan,

  ------------ Veggie
            CASE
              WHEN LOWER(rp.preference) LIKE LOWER('%VEGETARIAN%')
                OR LOWER(rp.preference) LIKE LOWER('%VEGGIE%')
                OR LOWER(rp.preference) LIKE LOWER('%Vegetarisch%')
                OR LOWER(rp.preference) LIKE LOWER('%VEGETARIANO%')
                OR (rp.country = 'BNL'
                  AND LOWER(rp.preference) LIKE LOWER('%V,')
                  OR  LOWER(rp.preference) = 'V')
              THEN 'Veggie'
          END AS Veggie

    FROM preferences_slot AS rp
    WHERE rp.preference NOT IN ('Premium', 'Premium Upcharge - Chef Choice', 'Premium Upcharge - Keto', 'Specials', 'Surcharges')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
----------------------------

, preferences_slot_with_columns_crosspreferences AS
(
  SELECT *,
          CASE
            WHEN rc.preference IS NULL
              OR rc.preference = ''
            THEN "EMPTY_PREFERENCE"
            WHEN    rc.preference IS NULL
                AND rc.Calorie_Smart IS NULL
                AND rc.Carb_Smart IS NULL
                AND rc.Classic IS NULL
                AND rc.Discovery IS NULL
                AND rc.Family IS NULL
                AND rc.Pescatarian IS NULL
                AND rc.Quick_and_Easy IS NULL
                AND rc.Vegan IS NULL
                AND rc.Veggie IS NULL
            THEN 'NOT_IDENTIFIED'
            ELSE 'AT_LEAST_1'
        END AS Category_Check,
          CASE
            WHEN rc.preference IS NULL
              OR rc.preference = ''
            THEN "EMPTY_PREFERENCE"
            ELSE
            CONCAT_WS(
                      ', ',
                      IF(rc.Calorie_Smart IS NOT NULL,'Calorie Smart',NULL),
                      IF(rc.Carb_Smart IS NOT NULL,'Carb Smart',NULL),
                      IF(rc.Classic IS NOT NULL,'Classic',NULL),
                      IF(rc.Discovery IS NOT NULL,'Discovery',NULL),
                      IF(rc.Family IS NOT NULL,'Family',NULL),
                      IF(rc.Pescatarian IS NOT NULL,'Pescatarian',NULL),
                      IF(rc.Quick_and_Easy IS NOT NULL,'Quick and Easy',NULL),
                      IF(rc.Vegan IS NOT NULL,'Vegan',NULL),
                      IF(rc.Veggie IS NOT NULL,'Veggie',NULL)
                    )
        END AS cross_preferences
  FROM preferences_slot_with_columns rc
)

, final_preference_slots AS
(
  SELECT
        CASE
            WHEN country = 'DK'
                THEN 'dkse'
            WHEN country = 'BE'
                THEN 'benelux'
            WHEN country = 'DK'
                THEN 'dk'
            ELSE LOWER(country)
        END AS country,
        slot,
        hellofresh_week,
        year_quarter,
        cross_preferences AS preference_slot
  FROM preferences_slot_with_columns_crosspreferences
  WHERE SLOT IS NOT NULL
  GROUP BY 1,2,3,4,5
)



, final_recipes_demand AS
(
  SELECT
        id AS recipe_id,
        UPPER(market) AS country,
        unique_recipe_code AS uniquerecipecode,
        recipe_code AS code,
        version,
        status,
        title,
        slot_number, -- newly added for slot preference join
        menu_status, -- newly added for slot preference join
        NULL AS lastused,
        NULL AS nextused,
        NULL AS absolutelastused,
        NULL AS isnewrecipe,
        NULL AS isnewscheduled,
        NULL AS isdefault,
        primary_protein AS primaryprotein,
        main_protein AS mainprotein,
        protein_cut AS proteincut,
        primary_starch AS primarystarch,
        main_starch AS mainstarch,
        primary_vegetable AS primaryvegetable,
        main_vegetable AS mainvegetable,
        NULL AS currency,
        NULL AS calories,
        NULL AS carbohydrates,
        cuisine AS cuisine,
        dish_type AS dishtype,
        NULL AS totaltime,
        NULL AS difficulty,
        tags AS tag,
        ps.preference_slot AS preference,
        target_products AS producttype,
        recipe_type AS recipetype,
        created_at AS created_at,
        updated_at AS updated_at,
        NULL AS cost2p_current,
        NULL AS cost2p_next,
        NULL AS scorescm,
        NULL AS scorewoscm,
        NULL AS isscorereplace,
        NULL AS skucode,
        NULL AS skuname,
        NULL AS skucount,
        'Demand' AS source,
        dr.hellofresh_week
  FROM joined_services_demand dr
  LEFT JOIN final_preference_slots AS ps
  ON dr.slot_number = ps.slot AND dr.market = ps.country
  WHERE dr.market NOT IN ('es','ie', 'jp')
  AND preference_slot IS NOT NULL
)


--*************************************************  6 CROSS-PREFERENCES  *************************************************
-- CHECK COSTS2P = NULL, OR CARBS = 999, OR ANY OTHER MISISNG LOGIC (SPECIALLY ITALY AND GB)


/*
NOTES TO CONSIDER:
00 This section needs to be modified for adding new countries
0 Spain is Missing in cost_all_countries only
1 Newlydded created_at and updated_at from created_at materialized_views.isa_services_recipe_consolidated r
*/

----------------------------

, all_demand_recipes_with_columns AS
(
  SELECT *,
          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%CALORIE_SMART%')
              OR LOWER(rp.preference) LIKE LOWER('%CS%')
              OR LOWER(rp.preference) LIKE LOWER('%CALORIE SMART%')
              OR LOWER(rp.preference) LIKE LOWER('%unter 650 Kalorien%')
              OR LOWER(rp.preference) LIKE LOWER('%CONTA CALORIE%')
              OR LOWER(rp.preference) LIKE LOWER('%Calorie Limited%')
              OR LOWER(rp.preference) LIKE LOWER('%Healthy%')
            THEN 'Calorie Smart'
        END AS Calorie_Smart,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%LOW CARB%')
              OR LOWER(rp.preference) LIKE LOWER('%CARB SMART%')
              OR LOWER(rp.preference) LIKE LOWER('%here%')
            THEN 'Carb Smart'
        END AS Carb_Smart,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%C3_APPROPRIATE%')
              OR LOWER(rp.preference) LIKE LOWER('%CLASSIC%')
              OR LOWER(rp.preference) LIKE LOWER('%Fleisch & Gemüse%')
              OR LOWER(rp.preference) LIKE LOWER('%CARNE & PESCE%')
              OR LOWER(rp.preference) LIKE LOWER("%Chef's choice%")
              OR LOWER(rp.preference) LIKE LOWER("%CL%")
              OR LOWER(rp.preference) LIKE LOWER("%Fish Free%") --Newly added, all countries, not only GB
              OR LOWER(rp.preference) LIKE LOWER("%Pork Free%")
              OR LOWER(rp.preference) LIKE LOWER("%Keto%")
              OR LOWER(rp.preference) LIKE LOWER("%Gluten Free%")
              OR rp.preference IS NULL
              OR rp.preference = ''
            THEN 'Classic'
        END AS Classic,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%DISCOVERY%')
            THEN 'Discovery'
        END AS Discovery,

          CASE --CHECK FOR BENELUX
            WHEN LOWER(rp.preference) LIKE LOWER('%FAMILY FRIENDLY%')
              OR LOWER(rp.preference) LIKE LOWER('%FAMILY%')
              OR LOWER(rp.preference) LIKE LOWER('%Familienfreundlich%')
              OR LOWER(rp.preference) LIKE LOWER('%FAMIGLIA%')
              OR (rp.country = 'BNL'
                AND LOWER(rp.preference) LIKE LOWER('%F,')
                OR  LOWER(rp.preference) = 'F')
            THEN 'Family'
        END AS Family,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%PESCATARIAN%')
            THEN 'Pescatarian'
        END AS Pescatarian,
          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%QUICK%')
              OR LOWER(rp.preference) LIKE LOWER('%Quick & Easy%')
              OR LOWER(rp.preference) LIKE LOWER('%RAPID%')
              OR LOWER(rp.preference) LIKE LOWER('%EASY%')
              OR (rp.country = 'BNL'
                  AND LOWER(rp.preference) LIKE LOWER('%Q,')
                  OR  LOWER(rp.preference) = 'Q')
            THEN 'Quick and Easy'
        END AS Quick_and_Easy,

          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%VEGAN%')
            THEN 'Vegan'
        END AS Vegan,
          CASE
            WHEN LOWER(rp.preference) LIKE LOWER('%VEGETARIAN%')
              OR LOWER(rp.preference) LIKE LOWER('%VEGGIE%')
              OR LOWER(rp.preference) LIKE LOWER('%Vegetarisch%')
              OR LOWER(rp.preference) LIKE LOWER('%VEGETARIANO%')
              OR (rp.country = 'BNL'
                AND LOWER(rp.preference) LIKE LOWER('%V,')
                OR  LOWER(rp.preference) = 'V')
            THEN 'Veggie'
        END AS Veggie
  FROM final_recipes_demand AS rp
)

----------------------------

, final_table AS
(
  SELECT *,
          CASE
            WHEN rc.preference IS NULL
              OR rc.preference = ''
            THEN "EMPTY_PREFERENCE"
            WHEN    rc.preference IS NULL
                AND rc.Calorie_Smart IS NULL
                AND rc.Carb_Smart IS NULL
                AND rc.Classic IS NULL
                AND rc.Discovery IS NULL
                AND rc.Family IS NULL
                AND rc.Pescatarian IS NULL
                AND rc.Quick_and_Easy IS NULL
                AND rc.Vegan IS NULL
                AND rc.Veggie IS NULL
            THEN 'NOT_IDENTIFIED'
            ELSE 'AT_LEAST_1'
        END AS Category_Check,
          CASE
            WHEN rc.preference IS NULL
              OR rc.preference = ''
            THEN "EMPTY_PREFERENCE"
            ELSE
            CONCAT_WS(
                      ', ',
                      IF(rc.Calorie_Smart IS NOT NULL,'Calorie Smart',NULL),
                      IF(rc.Carb_Smart IS NOT NULL,'Carb Smart',NULL),
                      IF(rc.Classic IS NOT NULL,'Classic',NULL),
                      IF(rc.Discovery IS NOT NULL,'Discovery',NULL),
                      IF(rc.Family IS NOT NULL,'Family',NULL),
                      IF(rc.Pescatarian IS NOT NULL,'Pescatarian',NULL),
                      IF(rc.Quick_and_Easy IS NOT NULL,'Quick and Easy',NULL),
                      IF(rc.Vegan IS NOT NULL,'Vegan',NULL),
                      IF(rc.Veggie IS NOT NULL,'Veggie',NULL)
                    )
        END AS cross_preferences
  FROM all_demand_recipes_with_columns rc
)


, final_cte AS (
SELECT *
FROM recipe_pool_crosspreferences
UNION ALL
SELECT *
FROM final_table

    )


SELECT *
FROM final_cte
WHERE (UPPER(country) IN ("DKSE","NO","IE") AND LOWER(status)='ready for menu planning')
    OR (UPPER(country)="GB" AND LOWER(status) IN ('ready for menu planning','in development', 'final cook', 'external testing'))
    OR (UPPER(country)="IT" AND LOWER(status) IN ('ready for menu planning','in development'))
    OR (UPPER(country)="BENELUX" AND LOWER(status) IN ('ready for menu planning','under improvement'))
    OR (UPPER(country)="FR" AND LOWER(status) IN ('ready for menu planning','planned'))
ORDER BY 2,3





/*

FILTERED COUNTRIES
benelux NOT
es NOT
ie NOT
jp NOT

ONLY COUNTRIES
dkse
gb
it
no



SELECT *
FROM all_demand_recipes_crosspreferences AS dr
LEFT JOIN final_preference_slots AS ps
ON dr.slot_number = ps.slot AND dr.country = ps.country

SELECT distinct country
FROM final_preference_slots

AT
AU
BE
CA
CH
DE
DK
FJ
FR
GB
GB-GC
IT
JP
LU
NL
NO
NZ
SE

SELECT distinct country
FROM all_demand_recipes_crosspreferences

country

benelux
dkse
es
gb
ie
it
jp
no

*/


