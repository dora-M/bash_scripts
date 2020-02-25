----------------------------------------------- interlinks
WITH interlinks 
AS 
( 
SELECT incidents.id, incidents.node_id, incidents.link, link_name,
  CASE WHEN incidents.incident_number LIKE '2826' THEN incidents.date ELSE NULL END AS date_down,
  CASE WHEN incidents.incident_number LIKE '2826' AND LEAD(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id) LIKE '2827'
         THEN LEAD(incidents.date) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id)
       WHEN incidents.incident_number LIKE '2827' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id) LIKE '2827'
         THEN incidents.date
       WHEN incidents.incident_number LIKE '2827' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id) IS NULL
         THEN incidents.date
       ELSE NULL
    END AS date_up
FROM incidents LEFT JOIN link ON incidents.node_id=node_number AND incidents.link=link.link
WHERE (incidents.incident_number LIKE '2826' OR incidents.incident_number LIKE '2827')
  AND CAST(incidents.node_id AS text) LIKE substr(incidents.link,1,2)
) 
SELECT node_id, link, link_name, date_down, date_up, date_up - date_down AS duration
FROM interlinks
WHERE (date_down IS NOT NULL OR date_up IS NOT NULL) 
  AND (date_down > '2020-02-01 00:00:00' OR date_up > '2020-02-01 00:00:00')
ORDER BY node_id, link, id
;
----------------------------------------------- external_alarm
WITH external_alarm 
AS 
( 
SELECT incidents.id, incidents.node_id, incidents.cry, incidents.cpl, incidents.term,
  CASE WHEN incidents.incident_number LIKE '1100' THEN incidents.date ELSE NULL END AS date_down,
  CASE WHEN incidents.incident_number LIKE '1100' AND LEAD(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term, incidents.id) 
              LIKE '1099' 
         THEN LEAD(incidents.date) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id)
       WHEN incidents.incident_number LIKE '1099' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id) 
              LIKE '1099'
         THEN incidents.date
       WHEN incidents.incident_number LIKE '1099' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id) 
              IS NULL
         THEN incidents.date
       ELSE NULL
    END AS date_up
FROM incidents
WHERE incidents.incident_number LIKE '1100' OR incidents.incident_number LIKE '1099'
) 
SELECT last_name||'-'||first_name AS name, 
  external_alarm.node_id, 
  external_alarm.cry||'-'||external_alarm.cpl||'-'||external_alarm.term AS cry_cpl_term, 
  date_down AS data_loss, 
  date_up AS data_in_service, 
  date_up - date_down AS duration
FROM external_alarm 
  LEFT JOIN listerm USING (node_id, cry, cpl, term)
  LEFT JOIN directory ON dir_nb=directory_number
WHERE (date_down IS NOT NULL OR date_up IS NOT NULL) 
  AND (date_down > '2020-02-01 00:00:00' OR date_up > '2020-02-01 00:00:00')
ORDER BY external_alarm.node_id, external_alarm.cry, external_alarm.cpl, external_alarm.term ,external_alarm.id
;
----------------------------------------------- link_T2
WITH link_T2 
AS 
( 
SELECT incidents.id, incidents.node_id, incidents.cry, incidents.cpl, link_name,
  CASE WHEN incidents.incident_number LIKE '0310' THEN incidents.date ELSE NULL END AS date_down,
  CASE WHEN incidents.incident_number LIKE '0310' AND LEAD(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term, incidents.id)
              LIKE '0311' 
         THEN LEAD(incidents.date) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id)
       WHEN incidents.incident_number LIKE '0311' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id) 
              LIKE '0311'
         THEN incidents.date
       WHEN incidents.incident_number LIKE '0311' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id) 
              IS NULL
         THEN incidents.date
       ELSE NULL 
    END AS date_up
FROM incidents LEFT JOIN isdn_shelf_aditional 
    ON incidents.node_id=node_number AND incidents.cry=shelf_address AND incidents.cpl=board_address
WHERE (incidents.incident_number LIKE '0310' OR incidents.incident_number LIKE '0311')
  AND trk NOT LIKE 'link'
) 
SELECT node_id, 
  cry||'-'||cpl AS cry_cpl, 
  link_name,
  date_down AS data_released, 
  date_up AS data_established, 
  date_up - date_down AS duration
FROM link_T2
WHERE (date_down IS NOT NULL OR date_up IS NOT NULL) 
  AND (date_down > '2020-02-01 00:00:00' OR date_up > '2020-02-01 00:00:00')
ORDER BY node_id, cry, cpl, id
;
----------------------------------------------- kfs8_status
WITH kfs8_status 
AS 
( 
SELECT incidents.id, incidents.node_id, incidents.cry, incidents.cpl,
  CASE WHEN substr(incidents.incident_detail,36,1) LIKE '1' THEN incidents.date ELSE NULL END AS date_down,
  CASE WHEN substr(incidents.incident_detail,36,1) LIKE '1' AND substr(LEAD(incidents.incident_detail) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term, incidents.id),36,1)
              LIKE '0'
         THEN LEAD(incidents.date) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id)
       WHEN substr(incidents.incident_detail,36,1) LIKE '0' AND substr(LAG(incidents.incident_detail) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id),36,1)
              LIKE '0'
         THEN incidents.date
       WHEN substr(incidents.incident_detail,36,1) LIKE '0' AND LAG(incidents.incident_detail) 
             OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term 
             ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id) 
              IS NULL
         THEN incidents.date
       ELSE NULL 
    END AS date_up
FROM incidents
WHERE incidents.incident_number LIKE '3660' 
) 
SELECT node_id, 
  cry||'-'||cpl AS cry_cpl, 
  date_down AS date_ko, 
  date_up AS date_ok, 
  date_up - date_down AS duration
FROM kfs8_status
WHERE date_down IS NOT NULL OR date_up IS NOT NULL
ORDER BY node_id, cry, cpl, id 
;
----------------------------------------------- ua_term
WITH ua_term 
AS 
( 
SELECT incidents.id, node_id, incidents.cry, incidents.cpl, incidents.term,
  CASE WHEN incidents.incident_number LIKE '2050' THEN incidents.date ELSE NULL END AS date_down,
  CASE WHEN incidents.incident_number LIKE '2050' AND LEAD(incidents.incident_number) 
         OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term, incidents.id) LIKE '2053' 
         THEN LEAD(incidents.date) 
         OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id)
       WHEN incidents.incident_number LIKE '2053' AND LAG(incidents.incident_number) 
         OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id) LIKE '2053'
         THEN incidents.date
       WHEN incidents.incident_number LIKE '2053' AND LAG(incidents.incident_number) 
         OVER (PARTITION BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ORDER BY incidents.node_id, incidents.cry, incidents.cpl, incidents.term ,incidents.id) IS NULL
         THEN incidents.date
       ELSE NULL 
    END AS date_up
FROM incidents
WHERE (incidents.incident_number LIKE '2050' OR incidents.incident_number LIKE '2053')
) 
SELECT dir_nb, 
  CASE WHEN typ_term LIKE 'AUTPOS'     THEN NULL
       WHEN typ_term LIKE '4010(VLE_3' THEN '4010'
       WHEN typ_term LIKE '4020(LE_3G' THEN '4020'
       WHEN typ_term LIKE '4035(MR2_3' THEN '4035T'
       WHEN typ_term LIKE 'OP 4035'    THEN '4035T'
       WHEN typ_term LIKE 'TYUSGS0'    THEN 'S0 Set'
       ELSE typ_term 
    END AS typ_term,
  last_name||'-'||first_name AS name, 
  ua_term.node_id, 
  ua_term.cry||'-'||ua_term.cpl||'-'||ua_term.term AS cry_cpl_term, 
  date_down AS data_loss, 
  date_up AS data_in_service, 
  date_up - date_down AS duration
FROM ua_term 
  LEFT JOIN listerm USING (node_id, cry, cpl, term)
  LEFT JOIN directory ON dir_nb=directory_number
WHERE (date_down IS NOT NULL OR date_up IS NOT NULL) 
  AND (date_down > '2020-02-01 00:00:00' OR date_up > '2020-02-01 00:00:00')
ORDER BY ua_term.node_id, ua_term.cry, ua_term.cpl, ua_term.term, ua_term.id 
;
----------------------------------------------- loop
SELECT incidents.date,
  incidents.node_id,
  CAST(incidents.cry AS TEXT) || '-' || CAST(incidents.cpl AS TEXT) AS cry_cpl,
  CASE WHEN incidents.count_inc IS NULL THEN incidents.incident_detail
       ELSE incidents.incident_detail || ' x ' || CAST(incidents.count_inc AS TEXT) 
    END AS incident_detail
FROM incidents
WHERE incidents.incident_number LIKE '3692'
ORDER BY incidents.date, incidents.id, incidents.node_id
;
----------------------------------------------- inc
SELECT incidents.date, 
  incidents.node_id||'-'||incidents.cpu AS node, 
  COALESCE(CAST(incidents.cry AS text),'')||'-'||COALESCE(CAST(incidents.cpl AS text),'')||'-'||COALESCE(CAST(incidents.ac AS text),'')||'-'||COALESCE(CAST(incidents.term AS text),'') AS cry_cpl_ac_term,
  incidents.link, 
  CAST(incidents.severity AS text), 
  incidents.incident_number||'-'||COALESCE(CAST(incidents.count_inc AS text),'') AS incident, 
  incidents.incident_detail 
FROM incidents
WHERE date > '2020-02-01 00:00:00' 
  AND incidents.incident_number NOT IN ('2050','2053','2826','2827','1100','1099','2862','2867','0310','0311','3660','3692','0275','0276','1125','1554','2750','2813','2864','2865','2866','3812','4109','5068','1680','0283','0274','1546','1721','2766','2780','2830','2841','2873','2101','2825','2839','2840','2080','2085','2112','2113','3580','2102','2836','2838')
ORDER BY severity, incident_number, node_id, cry, cpl, term, ac, date
;
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------- inc***********
SELECT incidents.date, 
  incidents.node_id||'-'||incidents.cpu AS node, 
  COALESCE(CAST(incidents.cry AS text),'')||'-'||COALESCE(CAST(incidents.cpl AS text),'')||'-'||COALESCE(CAST(incidents.ac AS text),'')||'-'||COALESCE(CAST(incidents.term AS text),'') AS cry_cpl_ac_term,
  incidents.link, 
  CAST(incidents.severity AS text), 
  incidents.incident_number||'-'||COALESCE(CAST(incidents.count_inc AS text),'') AS incident, 
  incidents.incident_detail 
FROM incidents
WHERE date > '2020-02-01 00:00:00' 
  AND incidents.incident_number IN ('0275','0276','1125','1554','2750','2813','1680','0283','0274','1546','2101','2825','2839','2840','2080','2085','2112','2113','3580','2102','2836','2838')
ORDER BY severity, incident_number, node_id, cry, cpl, term, ac, date
;
----------------------------------------------- inc***********hybrid
SELECT incidents.date, 
  incidents.node_id||'-'||incidents.cpu AS node, 
  COALESCE(CAST(incidents.cry AS text),'')||'-'||COALESCE(CAST(incidents.cpl AS text),'')||'-'||COALESCE(CAST(incidents.ac AS text),'')||'-'||COALESCE(CAST(incidents.term AS text),'') AS cry_cpl_ac_term,
  incidents.link, 
  CAST(incidents.severity AS text), 
  incidents.incident_number||'-'||COALESCE(CAST(incidents.count_inc AS text),'') AS incident, 
  incidents.incident_detail 
FROM incidents
WHERE date > '2020-02-01 00:00:00' 
  AND incidents.incident_number IN ('2864','2865','2866','3812','4109','5068','1721','2766','2780','2830','2841','2873')
ORDER BY severity, incident_number, node_id, cry, cpl, term, ac, date
;
----------------------------------------------- interlinks_satelit
WITH interlinks_satelit 
AS 
( 
SELECT incidents.id, incidents.node_id, incidents.link, link_name,
  CASE WHEN incidents.incident_number LIKE '2826' THEN incidents.date ELSE NULL END AS date_down,
  CASE WHEN incidents.incident_number LIKE '2826' AND LEAD(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id) LIKE '2827'
         THEN LEAD(incidents.date) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id)
       WHEN incidents.incident_number LIKE '2827' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id) LIKE '2827'
         THEN incidents.date
       WHEN incidents.incident_number LIKE '2827' AND LAG(incidents.incident_number) 
             OVER (PARTITION BY incidents.node_id, incidents.link 
             ORDER BY incidents.node_id, incidents.link ,incidents.id) IS NULL
         THEN incidents.date
       ELSE NULL
    END AS date_up
FROM incidents LEFT JOIN link ON incidents.node_id=node_number AND incidents.link=link.link
WHERE (incidents.incident_number LIKE '2826' OR incidents.incident_number LIKE '2827')
  AND CAST(incidents.node_id AS text) LIKE substr(incidents.link,4,2)
) 
SELECT node_id, link, link_name, date_down, date_up, date_up - date_down AS duration
FROM interlinks_satelit
WHERE (date_down IS NOT NULL OR date_up IS NOT NULL) 
  AND (date_down > '2020-02-01 00:00:00' OR date_up > '2020-02-01 00:00:00')
ORDER BY node_id, link, id
;
----------------------------------------------- link_hybrid_cs
WITH link_hybrid_cs 
AS 
(
SELECT id, node_id, 
  cry||'-'||cpl||'-'||term AS cry_cpl_term,
  CASE WHEN incident_number LIKE '2867' THEN date ELSE NULL END AS date_down,
  CASE WHEN incident_number LIKE '2867' AND LEAD(incident_number) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term, id) LIKE '2862' 
         THEN LEAD(date) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term ,id)
       WHEN incident_number LIKE '2862' AND LAG(incident_number) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term ,id) LIKE '2862'
         THEN date
       WHEN incident_number LIKE '2862' AND LAG(incident_number) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term ,id) IS NULL
         THEN date
       ELSE NULL 
  END AS date_up
FROM incidents
WHERE (incident_number LIKE '2867' OR incident_number LIKE '2862') AND node_id=40
)
SELECT node_id, cry_cpl_term, date_down, date_up, 
  date_up - date_down AS duration
FROM link_hybrid_cs
WHERE (date_down IS NOT NULL OR date_up IS NOT NULL) 
  AND (date_down > '2020-02-01 00:00:00' OR date_up > '2020-02-01 00:00:00')
ORDER BY node_id, cry_cpl_term ,id 
;
----------------------------------------------- link_hybrid_orsova
WITH link_hybrid_orsova 
AS 
(
SELECT id, node_id, 
  cry||'-'||cpl||'-'||term AS cry_cpl_term,
  CASE WHEN incident_number LIKE '2867' THEN date ELSE NULL END AS date_down,
  CASE WHEN incident_number LIKE '2867' AND LEAD(incident_number) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term, id) LIKE '2862' 
         THEN LEAD(date) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term ,id)
       WHEN incident_number LIKE '2862' AND LAG(incident_number) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term ,id) LIKE '2862'
         THEN date
       WHEN incident_number LIKE '2862' AND LAG(incident_number) 
             OVER (PARTITION BY node_id, cry, cpl, term 
             ORDER BY node_id, cry, cpl, term ,id) IS NULL
         THEN date
       ELSE NULL 
  END AS date_up
FROM incidents
WHERE (incident_number LIKE '2867' OR incident_number LIKE '2862') AND node_id=58
)
SELECT node_id, cry_cpl_term, date_down, date_up, 
  date_up - date_down AS duration
FROM link_hybrid_orsova
WHERE (date_down IS NOT NULL OR date_up IS NOT NULL) 
  AND (date_down > '2020-02-01 00:00:00' OR date_up > '2020-02-01 00:00:00')
ORDER BY node_id, cry_cpl_term ,id 
;






