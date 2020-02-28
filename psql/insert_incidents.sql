DO
$do$
BEGIN

DELETE FROM incidents_temp;
INSERT INTO incidents_temp
   SELECT id,
          to_timestamp(date, 'DD/MM/YY HH24:MI:SS'),
          CAST(network_id AS INTEGER),
          CAST(node_id AS INTEGER),
          cpu,
          CASE WHEN adress LIKE '%/%' AND adress LIKE '--%' THEN NULL
               WHEN adress LIKE '%/%' THEN CAST(substr(adress,1,2) AS INTEGER)
               ELSE NULL
            END AS cry,
          CASE WHEN adress LIKE '%/%' AND adress LIKE '___--%' THEN NULL
               WHEN adress LIKE '%/%' THEN CAST(substr(adress,4,2) AS INTEGER)
               ELSE NULL
            END AS cpl,
          CASE WHEN adress LIKE '%/%' AND adress LIKE '%-____' THEN NULL
               WHEN adress LIKE '%/%' THEN CAST(substr(adress,7,1) AS INTEGER)
               ELSE NULL 
            END AS ac, 
          CASE WHEN adress LIKE '%/%' AND adress LIKE '%---' THEN NULL
               WHEN adress LIKE '%/%' THEN CAST(substr(adress,9,3) AS INTEGER)
               ELSE NULL 
            END AS term,
          CASE WHEN adress LIKE '__-%' THEN substr(adress,1,5)
               ELSE NULL 
            END AS link,
          severity,
          substr(incident_number,1,4) AS incident_number,
          CASE WHEN incident_number like '%x%' THEN CAST(substr(incident_number,6,3) AS INTEGER)
               ELSE NULL 
            END AS count,
          TRIM(incident_detail)
   FROM incidents_import;
DELETE FROM incidents_temp WHERE date < '2020-01-01 00:00:00';
----------------------------------------------- interlinks
IF 
   (SELECT count(incidents_temp.id)
    FROM incidents_temp LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
    WHERE (incidents_temp.incident_number LIKE '2826' OR incidents_temp.incident_number LIKE '2827')
      AND CAST(incidents_temp.node_id AS text) LIKE substr(incidents_temp.link,1,2) AND incidents.id IS NULL) > 0
THEN
COPY 
( 
WITH interlinks 
AS 
( 
SELECT incidents_temp.id, incidents_temp.node_id, incidents_temp.link, link_name,
  CASE WHEN incidents_temp.incident_number LIKE '2826' THEN incidents_temp.date ELSE NULL END AS date_down,
  CASE WHEN incidents_temp.incident_number LIKE '2826' AND LEAD(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.link 
             ORDER BY incidents_temp.node_id, incidents_temp.link ,incidents_temp.id) LIKE '2827'
         THEN LEAD(incidents_temp.date) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.link 
             ORDER BY incidents_temp.node_id, incidents_temp.link ,incidents_temp.id)
       WHEN incidents_temp.incident_number LIKE '2827' AND LAG(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.link 
             ORDER BY incidents_temp.node_id, incidents_temp.link ,incidents_temp.id) LIKE '2827'
         THEN incidents_temp.date
       WHEN incidents_temp.incident_number LIKE '2827' AND LAG(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.link 
             ORDER BY incidents_temp.node_id, incidents_temp.link ,incidents_temp.id) IS NULL
         THEN incidents_temp.date
       ELSE NULL
    END AS date_up
FROM incidents_temp
  LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
  LEFT JOIN link ON incidents_temp.node_id=node_number AND incidents_temp.link=link.link
WHERE (incidents_temp.incident_number LIKE '2826' OR incidents_temp.incident_number LIKE '2827')
  AND CAST(incidents_temp.node_id AS text) LIKE substr(incidents_temp.link,1,2) AND incidents.id IS NULL
) 
SELECT node_id, link, link_name, date_down, date_up, date_up - date_down AS duration
FROM interlinks
WHERE date_down IS NOT NULL OR date_up IS NOT NULL
ORDER BY node_id, link, id 
) 
TO '/var/lib/pgsql/reports/mail_interlinks'  DELIMITER E'\t' CSV HEADER;
END IF;
----------------------------------------------- external_alarm
IF 
   (SELECT count(incidents_temp.id)
    FROM incidents_temp LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
    WHERE (incidents_temp.incident_number LIKE '1100' OR incidents_temp.incident_number LIKE '1099')
      AND incidents.id IS NULL) > 0
THEN
COPY 
( 
WITH external_alarm 
AS 
( 
SELECT incidents_temp.id, incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term,
  CASE WHEN incidents_temp.incident_number LIKE '1100' THEN incidents_temp.date ELSE NULL END AS date_down,
  CASE WHEN incidents_temp.incident_number LIKE '1100' AND LEAD(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term, incidents_temp.id) 
              LIKE '1099' 
         THEN LEAD(incidents_temp.date) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id)
       WHEN incidents_temp.incident_number LIKE '1099' AND LAG(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id) 
              LIKE '1099'
         THEN incidents_temp.date
       WHEN incidents_temp.incident_number LIKE '1099' AND LAG(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id) 
              IS NULL
         THEN incidents_temp.date
       ELSE NULL
    END AS date_up
FROM incidents_temp
  LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
WHERE (incidents_temp.incident_number LIKE '1100' OR incidents_temp.incident_number LIKE '1099') AND incidents.id IS NULL
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
WHERE date_down IS NOT NULL OR date_up IS NOT NULL
ORDER BY external_alarm.node_id, external_alarm.cry, external_alarm.cpl, external_alarm.term ,external_alarm.id 
) 
TO '/var/lib/pgsql/reports/mail_external_alarm'  DELIMITER E'\t' CSV HEADER;
END IF;
----------------------------------------------- link_T2
IF 
   (SELECT count(incidents_temp.id)
    FROM incidents_temp 
       LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
       LEFT JOIN isdn_shelf_aditional 
         ON incidents_temp.node_id=node_number AND incidents_temp.cry=shelf_address AND incidents_temp.cpl=board_address
    WHERE (incidents_temp.incident_number LIKE '0310' OR incidents_temp.incident_number LIKE '0311')
      AND trk NOT LIKE 'link' AND incidents.id IS NULL) > 0
THEN
COPY 
( 
WITH link_T2 
AS 
( 
SELECT incidents_temp.id, incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, link_name,
  CASE WHEN incidents_temp.incident_number LIKE '0310' THEN incidents_temp.date ELSE NULL END AS date_down,
  CASE WHEN incidents_temp.incident_number LIKE '0310' AND LEAD(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term, incidents_temp.id)
              LIKE '0311' 
         THEN LEAD(incidents_temp.date) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id)
       WHEN incidents_temp.incident_number LIKE '0311' AND LAG(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id) 
              LIKE '0311'
         THEN incidents_temp.date
       WHEN incidents_temp.incident_number LIKE '0311' AND LAG(incidents_temp.incident_number) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id) 
              IS NULL
         THEN incidents_temp.date
       ELSE NULL 
    END AS date_up
FROM incidents_temp
  LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
  LEFT JOIN isdn_shelf_aditional 
    ON incidents_temp.node_id=node_number AND incidents_temp.cry=shelf_address AND incidents_temp.cpl=board_address
WHERE (incidents_temp.incident_number LIKE '0310' OR incidents_temp.incident_number LIKE '0311')
  AND trk NOT LIKE 'link' AND incidents.id IS NULL
) 
SELECT node_id, 
  cry||'-'||cpl AS cry_cpl, 
  link_name,
  date_down AS data_released, 
  date_up AS data_established, 
  date_up - date_down AS duration
FROM link_T2
WHERE date_down IS NOT NULL OR date_up IS NOT NULL
ORDER BY node_id, cry, cpl, id 
) 
TO '/var/lib/pgsql/reports/mail_link_T2'  DELIMITER E'\t' CSV HEADER;
END IF;
----------------------------------------------- kfs8_status
IF 
   (SELECT count(incidents_temp.id)
    FROM incidents_temp LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
    WHERE incidents_temp.incident_number LIKE '3660' AND incidents.id IS NULL) > 0
THEN
COPY 
( 
WITH kfs8_status 
AS 
( 
SELECT incidents_temp.id, incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl,
  CASE WHEN substr(incidents_temp.incident_detail,36,1) LIKE '1' THEN incidents_temp.date ELSE NULL END AS date_down,
  CASE WHEN substr(incidents_temp.incident_detail,36,1) LIKE '1' AND substr(LEAD(incidents_temp.incident_detail) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term, incidents_temp.id),36,1)
              LIKE '0'
         THEN LEAD(incidents_temp.date) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id)
       WHEN substr(incidents_temp.incident_detail,36,1) LIKE '0' AND substr(LAG(incidents_temp.incident_detail) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id),36,1)
              LIKE '0'
         THEN incidents_temp.date
       WHEN substr(incidents_temp.incident_detail,36,1) LIKE '0' AND LAG(incidents_temp.incident_detail) 
             OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term 
             ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id) 
              IS NULL
         THEN incidents_temp.date
       ELSE NULL 
    END AS date_up
FROM incidents_temp
  LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
WHERE incidents_temp.incident_number LIKE '3660' AND incidents.id IS NULL 
) 
SELECT node_id, 
  cry||'-'||cpl AS cry_cpl, 
  date_down AS date_ko, 
  date_up AS date_ok, 
  date_up - date_down AS duration
FROM kfs8_status
WHERE date_down IS NOT NULL OR date_up IS NOT NULL
ORDER BY node_id, cry, cpl, id 
) 
TO '/var/lib/pgsql/reports/mail_kfs8_status'  DELIMITER E'\t' CSV HEADER;
END IF;
----------------------------------------------- ua_term
IF 
   (SELECT count(incidents_temp.id)
    FROM incidents_temp LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
    WHERE (incidents_temp.incident_number LIKE '2050' OR incidents_temp.incident_number LIKE '2053')
      AND incidents.id IS NULL) > 0
THEN
COPY 
(
WITH ua_term 
AS 
( 
SELECT incidents_temp.id, node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term,
  CASE WHEN incidents_temp.incident_number LIKE '2050' THEN incidents_temp.date ELSE NULL END AS date_down,
  CASE WHEN incidents_temp.incident_number LIKE '2050' AND LEAD(incidents_temp.incident_number) 
         OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term, incidents_temp.id) LIKE '2053' 
         THEN LEAD(incidents_temp.date) 
         OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id)
       WHEN incidents_temp.incident_number LIKE '2053' AND LAG(incidents_temp.incident_number) 
         OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id) LIKE '2053'
         THEN incidents_temp.date
       WHEN incidents_temp.incident_number LIKE '2053' AND LAG(incidents_temp.incident_number) 
         OVER (PARTITION BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ORDER BY incidents_temp.node_id, incidents_temp.cry, incidents_temp.cpl, incidents_temp.term ,incidents_temp.id) IS NULL
         THEN incidents_temp.date
       ELSE NULL 
    END AS date_up
FROM incidents_temp
  LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
WHERE (incidents_temp.incident_number LIKE '2050' OR incidents_temp.incident_number LIKE '2053')
  AND incidents.id IS NULL
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
WHERE date_down IS NOT NULL OR date_up IS NOT NULL
ORDER BY ua_term.node_id, ua_term.cry, ua_term.cpl, ua_term.term, ua_term.id 
) 
TO '/var/lib/pgsql/reports/mail_ua_term'  DELIMITER E'\t' CSV HEADER;
END IF;
----------------------------------------------- loop
IF 
   (SELECT count(incidents_temp.id)
    FROM incidents_temp LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
    WHERE incidents_temp.incident_number LIKE '3692' AND incidents.id IS NULL) > 0
THEN
COPY 
( 
SELECT incidents_temp.date,
  incidents_temp.node_id,
  CAST(incidents_temp.cry AS TEXT) || '-' || CAST(incidents_temp.cpl AS TEXT) AS cry_cpl,
  CASE WHEN incidents_temp.count_inc IS NULL THEN incidents_temp.incident_detail
       ELSE incidents_temp.incident_detail || ' x ' || CAST(incidents_temp.count_inc AS TEXT) 
    END AS incident_detail
FROM incidents_temp
   LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
WHERE incidents_temp.incident_number LIKE '3692' AND incidents.id IS NULL
ORDER BY incidents_temp.date, incidents_temp.id, incidents_temp.node_id
) 
TO '/var/lib/pgsql/reports/mail_loop'  DELIMITER E'\t' CSV HEADER;
END IF;
----------------------------------------------- inc
IF 
   (SELECT count(incidents_temp.id)
    FROM incidents_temp LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
    WHERE incidents_temp.incident_number NOT IN ('2050','2053','2826','2827','1100','1099','2862','2867','0310','0311','3660','3692','0275','0276','1125','1554','2750','2813','2864','2865','2866','3812','4109','5068','1680','0283','0274','1546','1721','2766','2780','2830','2841','2873','2101','2825','2839','2840','2080','2085','2112','2113','3580','2102','2836','2838')
      AND incidents.id IS NULL) > 0
THEN
COPY 
( 
SELECT incidents_temp.date, 
  incidents_temp.node_id||'-'||incidents_temp.cpu AS node, 
  COALESCE(CAST(incidents_temp.cry AS text),'')||'-'||COALESCE(CAST(incidents_temp.cpl AS text),'')||'-'||COALESCE(CAST(incidents_temp.ac AS text),'')||'-'||COALESCE(CAST(incidents_temp.term AS text),'') AS cry_cpl_ac_term,
  incidents_temp.link, 
  CAST(incidents_temp.severity AS text), 
  incidents_temp.incident_number||'-'||COALESCE(CAST(incidents_temp.count_inc AS text),'') AS incident, 
  incidents_temp.incident_detail 
FROM incidents_temp
  LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
WHERE incidents_temp.incident_number NOT IN ('2050','2053','2826','2827','1100','1099','2862','2867','0310','0311','3660','3692','0275','0276','1125','1554','2750','2813','2864','2865','2866','3812','4109','5068','1680','0283','0274','1546','1721','2766','2780','2830','2841','2873','2101','2825','2839','2840','2080','2085','2112','2113','3580','2102','2836','2838')
  AND incidents.id IS NULL
ORDER BY 
  incidents_temp.severity, 
  incidents_temp.incident_number, 
  incidents_temp.node_id, 
  incidents_temp.cry, 
  incidents_temp.cpl, 
  incidents_temp.term, 
  incidents_temp.ac, 
  incidents_temp.date
) 
TO '/var/lib/pgsql/reports/mail_inc'  DELIMITER E'\t' CSV HEADER;
END IF;
--------------------------------------------------------------------------
INSERT INTO incidents
  SELECT incidents_temp.* 
  FROM incidents_temp
    LEFT OUTER JOIN incidents USING (id, date, network_id, node_id)
  WHERE incidents.id IS NULL;

END
$do$
