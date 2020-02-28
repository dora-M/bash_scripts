CREATE FUNCTION public.d_alarme_externe() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
var_network_id TEXT;
var_node_id TEXT;
var_cry TEXT;
var_cpl TEXT;
var_term TEXT;
var_al_ext TEXT;
var_incident_number TEXT;
var_incident_id TEXT;
var_incident_date timestamp without time zone;
var_incident_date_START timestamp without time zone;
var_incident_date_END timestamp without time zone;
var_duration TEXT;
var_START TEXT DEFAULT '1100';
var_END TEXT DEFAULT '1099';
i TEXT;
cursor_distinct CURSOR FOR SELECT DISTINCT network_id, node_id, cry, cpl, term FROM incidents
	WHERE incidents.incident_number=var_END OR incidents.incident_number=var_START;
cursor_alarme_externe CURSOR (c_network_id TEXT, c_node_id TEXT, c_cry TEXT, c_cpl TEXT, c_term TEXT) FOR 
	SELECT SUBSTRING(incident_detail from position('**' in incident_detail)+2), incident_number, incident_id, incident_date
	FROM incidents
	WHERE (incidents.incident_number=var_END OR incidents.incident_number=var_START)
		AND  network_id=c_network_id
		AND  node_id=c_node_id
		AND  cry=c_cry
		AND  cpl=c_cpl
		AND  term=c_term
	ORDER BY incident_date, incident_id;
BEGIN
DELETE FROM d_alarme_externe;
   OPEN cursor_distinct; 
   LOOP FETCH cursor_distinct INTO var_network_id, var_node_id, var_cry, var_cpl, var_term;
      EXIT WHEN NOT FOUND;
i='START';
OPEN cursor_alarme_externe(var_network_id, var_node_id, var_cry, var_cpl, var_term); 
   LOOP FETCH cursor_alarme_externe INTO var_al_ext, var_incident_number, var_incident_id, var_incident_date;
      EXIT WHEN NOT FOUND;
CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'START' THEN 
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_alarme_externe
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_al_ext, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_END AND i LIKE 'END' THEN 
		var_incident_date_END=var_incident_date;
		var_duration =TO_CHAR(var_incident_date_END- var_incident_date_START, 'DD HH24:MI:SS');	
		i='START';
		INSERT INTO d_alarme_externe
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_al_ext, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
	WHEN var_incident_number LIKE var_END AND i LIKE 'START' THEN
		var_incident_date_END=var_incident_date;
		var_duration ='xx xx:xx:xx';	
		i='START';
		INSERT INTO d_alarme_externe
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_al_ext, var_incident_id, var_incident_date,
			'xxxx-xx-xx xx:xx:xx',
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
END CASE;
  END LOOP;
FETCH LAST FROM cursor_alarme_externe INTO var_al_ext, var_incident_number, var_incident_id, var_incident_date;
	CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_alarme_externe
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_al_ext, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
	ELSE i='START';
	END CASE;
CLOSE cursor_alarme_externe;
  END LOOP;
CLOSE cursor_distinct;
  RETURN;
END;
$$;

--
-- Name: d_hybrid(); Type: FUNCTION; 
--

CREATE FUNCTION public.d_hybrid() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
var_network_id TEXT;
var_node_id TEXT;
var_term TEXT;
var_incident_number TEXT;
var_incident_id TEXT;
var_incident_date timestamp without time zone;
var_incident_date_START timestamp without time zone;
var_incident_date_END timestamp without time zone;
var_duration TEXT;
var_START TEXT DEFAULT '2867';
var_END TEXT DEFAULT '2862';
i TEXT;
cursor_distinct CURSOR FOR SELECT DISTINCT  network_id, node_id, term FROM incidents
	WHERE incidents.incident_number=var_START OR incidents.incident_number=var_END;
cursor_hybrid CURSOR (c_network_id TEXT, c_node_id TEXT, c_term TEXT) FOR SELECT incident_number, incident_id, incident_date FROM incidents
	WHERE (incidents.incident_number=var_START OR incidents.incident_number=var_END)
		AND  network_id=c_network_id
		AND  node_id=c_node_id
		AND  term=c_term
		ORDER BY incident_date, incident_id;
BEGIN
DELETE FROM d_hybrid;
   OPEN cursor_distinct; 
   LOOP FETCH cursor_distinct INTO var_network_id, var_node_id, var_term;
      EXIT WHEN NOT FOUND;
i='START';
OPEN cursor_hybrid(var_network_id, var_node_id, var_term); 
   LOOP FETCH cursor_hybrid INTO var_incident_number, var_incident_id, var_incident_date;
      EXIT WHEN NOT FOUND;
CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'START' THEN 
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_hybrid
			VALUES (var_network_id, var_node_id, var_term, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_END AND i LIKE 'END' THEN 
		var_incident_date_END=var_incident_date;
		var_duration =TO_CHAR(var_incident_date_END- var_incident_date_START, 'DD HH24:MI:SS');	
		i='START';
		INSERT INTO d_hybrid
			VALUES (var_network_id, var_node_id, var_term, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
	WHEN var_incident_number LIKE var_END AND i LIKE 'START' THEN
		var_incident_date_END=var_incident_date;
		var_duration ='xx xx:xx:xx';	
		i='START';
		INSERT INTO d_hybrid
			VALUES (var_network_id, var_node_id, var_term, var_incident_id, var_incident_date,
			'xxxx-xx-xx xx:xx:xx',
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
END CASE;
  END LOOP;
FETCH LAST FROM cursor_hybrid INTO var_incident_number, var_incident_id, var_incident_date;
	CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_hybrid
			VALUES (var_network_id, var_node_id, var_term, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
	ELSE i='START';
	END CASE;
CLOSE cursor_hybrid;
  END LOOP;
CLOSE cursor_distinct;
  RETURN;
END;
$$;

--
-- Name: d_link(); Type: FUNCTION; 
--

CREATE FUNCTION public.d_link() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
var_network_id TEXT;
var_cry TEXT;
var_cpl TEXT;
var_incident_number TEXT;
var_incident_id TEXT;
var_incident_date timestamp without time zone;
var_incident_date_START timestamp without time zone;
var_incident_date_END timestamp without time zone;
var_duration TEXT;
var_START TEXT DEFAULT '2826';
var_END TEXT DEFAULT '2827';
i TEXT;
cursor_distinct CURSOR FOR SELECT DISTINCT network_id, cry, cpl FROM incidents
	WHERE (incidents.incident_number=var_START OR incidents.incident_number=var_END) AND node_id LIKE cry;
cursor_link CURSOR (c_network_id TEXT, c_cry TEXT, c_cpl TEXT) FOR SELECT incident_number, incident_id, incident_date FROM incidents
	WHERE (incidents.incident_number=var_START OR incidents.incident_number=var_END) AND node_id LIKE cry
		AND  network_id=c_network_id
		AND  cry=c_cry
		AND  cpl=c_cpl
		ORDER BY incident_date, incident_id;
BEGIN
DELETE FROM d_link;
   OPEN cursor_distinct; 
   LOOP FETCH cursor_distinct INTO var_network_id, var_cry, var_cpl;
      EXIT WHEN NOT FOUND;
i='START';
OPEN cursor_link(var_network_id, var_cry, var_cpl); 
   LOOP FETCH cursor_link INTO var_incident_number, var_incident_id, var_incident_date;
      EXIT WHEN NOT FOUND;
CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'START' THEN 
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_link
			VALUES (var_network_id, var_cry, var_cpl, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_END AND i LIKE 'END' THEN 
		var_incident_date_END=var_incident_date;
		var_duration =TO_CHAR(var_incident_date_END- var_incident_date_START, 'DD HH24:MI:SS');	
		i='START';
		INSERT INTO d_link
			VALUES (var_network_id, var_cry, var_cpl, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
	WHEN var_incident_number LIKE var_END AND i LIKE 'START' THEN
		var_incident_date_END=var_incident_date;
		var_duration ='xx xx:xx:xx';	
		i='START';
		INSERT INTO d_link
			VALUES (var_network_id, var_cry, var_cpl, var_incident_id, var_incident_date,
			'xxxx-xx-xx xx:xx:xx',
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
END CASE;
  END LOOP;
FETCH LAST FROM cursor_link INTO var_incident_number, var_incident_id, var_incident_date;
	CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_link
			VALUES (var_network_id, var_cry, var_cpl, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
	ELSE i='START';
	END CASE;
CLOSE cursor_link;
  END LOOP;
CLOSE cursor_distinct;
  RETURN;
END;
$$;

--
-- Name: d_t2(); Type: FUNCTION; 
--

CREATE FUNCTION public.d_t2() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
var_network_id TEXT;
var_node_id TEXT;
var_cry TEXT;
var_cpl TEXT;
var_trk_name TEXT;
var_incident_number TEXT;
var_incident_id TEXT;
var_incident_date timestamp without time zone;
var_incident_date_START timestamp without time zone;
var_incident_date_END timestamp without time zone;
var_duration TEXT;
var_START TEXT DEFAULT '0310';
var_END TEXT DEFAULT '0311';
i TEXT;
cursor_distinct CURSOR FOR SELECT DISTINCT incidents.network_id, incidents.node_id, incidents.cry, incidents.cpl, isdn_shelf_aditional.name
	FROM incidents LEFT JOIN isdn_shelf_aditional 
		ON  CAST(incidents.node_id AS INTEGER)=isdn_shelf_aditional.node_number
		AND CAST(incidents.cry AS INTEGER)= isdn_shelf_aditional.shelf_address
		AND CAST(incidents.cpl AS INTEGER)=isdn_shelf_aditional.board_address
	WHERE (incidents.incident_number=var_START OR incidents.incident_number=var_END)
		AND isdn_shelf_aditional.trk LIKE 'trunkgroup' AND isdn_shelf_aditional.type LIKE 'T2';
cursor_T2 CURSOR (c_network_id TEXT, c_node_id TEXT, c_cry TEXT, c_cpl TEXT) FOR SELECT incident_number, incident_id, incident_date FROM incidents
	WHERE (incidents.incident_number=var_START OR incidents.incident_number=var_END) 
		AND  network_id=c_network_id
		AND  node_id=c_node_id
		AND  cry=c_cry
		AND  cpl=c_cpl
		ORDER BY incident_date, incident_id;
BEGIN
DELETE FROM d_T2;
   OPEN cursor_distinct; 
   LOOP FETCH cursor_distinct INTO var_network_id, var_node_id, var_cry, var_cpl, var_trk_name;
      EXIT WHEN NOT FOUND;
i='START';
OPEN cursor_T2(var_network_id, var_node_id, var_cry, var_cpl); 
   LOOP FETCH cursor_T2 INTO var_incident_number, var_incident_id, var_incident_date;
      EXIT WHEN NOT FOUND;
CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'START' THEN 
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_T2
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_trk_name, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_END AND i LIKE 'END' THEN 
		var_incident_date_END=var_incident_date;
		var_duration =TO_CHAR(var_incident_date_END- var_incident_date_START, 'DD HH24:MI:SS');	
		i='START';
		INSERT INTO d_T2
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_trk_name, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
	WHEN var_incident_number LIKE var_END AND i LIKE 'START' THEN
		var_incident_date_END=var_incident_date;
		var_duration ='xx xx:xx:xx';	
		i='START';
		INSERT INTO d_T2
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_trk_name, var_incident_id, var_incident_date,
			'xxxx-xx-xx xx:xx:xx',
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
END CASE;
  END LOOP;
FETCH LAST FROM cursor_T2 INTO var_incident_number, var_incident_id, var_incident_date;
	CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_T2
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_trk_name, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
	ELSE i='START';
	END CASE;
CLOSE cursor_T2;
  END LOOP;
CLOSE cursor_distinct;
  RETURN;
END;
$$;

--
-- Name: d_ua(); Type: FUNCTION; 
--

CREATE FUNCTION public.d_ua() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
var_network_id TEXT;
var_node_id TEXT;
var_cry TEXT;
var_cpl TEXT;
var_term TEXT;
var_UA TEXT;
var_typ_term TEXT;
var_incident_number TEXT;
var_incident_id TEXT;
var_incident_date timestamp without time zone;
var_incident_date_START timestamp without time zone;
var_incident_date_END timestamp without time zone;
var_duration TEXT;
var_START TEXT DEFAULT '2050';
var_END TEXT DEFAULT '2053';
i TEXT;
cursor_distinct CURSOR FOR SELECT DISTINCT incidents.network_id, incidents.node_id, incidents.cry, incidents.cpl, incidents.term,
					   listerm.dir_nb || '-'|| directory.last_name || '-'|| directory.first_name,
  					   TRIM(listerm.typ_term) AS typ_term
	FROM incidents LEFT JOIN listerm 
				ON  CAST(incidents.node_id AS integer) = listerm.node_id 
				AND CAST(incidents.cry AS integer) = listerm.cry 
				AND CAST(incidents.cpl AS integer) = listerm.cpl 
				AND CAST(incidents.term AS integer) = listerm.term
  		       LEFT JOIN directory ON listerm.dir_nb = directory.directory_number
	WHERE incidents.incident_number=var_END OR incidents.incident_number=var_START;
cursor_UA CURSOR (c_network_id TEXT, c_node_id TEXT, c_cry TEXT, c_cpl TEXT, c_term TEXT, c_UA TEXT, c_typ_term TEXT) FOR 
	SELECT incident_number, incident_id, incident_date
	FROM incidents
	WHERE (incidents.incident_number=var_END OR incidents.incident_number=var_START)
		AND  network_id=c_network_id
		AND  node_id=c_node_id
		AND  cry=c_cry
		AND  cpl=c_cpl
		AND  term=c_term
	ORDER BY incident_date, incident_id;
BEGIN
DELETE FROM d_UA;
   OPEN cursor_distinct; 
   LOOP FETCH cursor_distinct INTO var_network_id, var_node_id, var_cry, var_cpl, var_term, var_UA, var_typ_term;
      EXIT WHEN NOT FOUND;
i='START';
OPEN cursor_UA(var_network_id, var_node_id, var_cry, var_cpl, var_term, var_UA, var_typ_term); 
   LOOP FETCH cursor_UA INTO var_incident_number, var_incident_id, var_incident_date;
      EXIT WHEN NOT FOUND;
CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'START' THEN 
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_UA
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_UA, var_typ_term, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
		var_incident_date_START=var_incident_date;
		i='END';
	WHEN var_incident_number LIKE var_END AND i LIKE 'END' THEN 
		var_incident_date_END=var_incident_date;
		var_duration =TO_CHAR(var_incident_date_END- var_incident_date_START, 'DD HH24:MI:SS');	
		i='START';
		INSERT INTO d_UA
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_UA, var_typ_term, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
	WHEN var_incident_number LIKE var_END AND i LIKE 'START' THEN
		var_incident_date_END=var_incident_date;
		var_duration ='xx xx:xx:xx';	
		i='START';
		INSERT INTO d_UA
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_UA, var_typ_term, var_incident_id, var_incident_date,
			'xxxx-xx-xx xx:xx:xx',
			TO_CHAR(var_incident_date_END,'YYYY-MM-DD HH24:MI:SS'), var_duration);
END CASE;
  END LOOP;
FETCH LAST FROM cursor_UA INTO var_incident_number, var_incident_id, var_incident_date;
	CASE     
	WHEN var_incident_number LIKE var_START AND i LIKE 'END' THEN
		var_duration ='xx xx:xx:xx';
		INSERT INTO d_UA
			VALUES (var_network_id, var_node_id, var_cry, var_cpl, var_term, var_UA, var_typ_term, var_incident_id, var_incident_date,
			TO_CHAR(var_incident_date_START,'YYYY-MM-DD HH24:MI:SS'),
			'xxxx-xx-xx xx:xx:xx', var_duration);
	ELSE i='START';
	END CASE;
CLOSE cursor_UA;
  END LOOP;
CLOSE cursor_distinct;
  RETURN;
END;
$$;
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
SELECT d_alarme_externe();
SELECT d_hybrid();
SELECT d_link();
SELECT d_t2();
SELECT d_ua();
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------- inc
SELECT 
  incidents.incident_date, 
  incidents.node_id||'-'||
  incidents.cpu ||'/'||
  incidents.cry ||'/'||
  incidents.cpl ||'/'||
  incidents.ac ||'/'||
  incidents.term AS adress, 
  incidents.severity||'-'||
  incidents.incident_number||'-'||
  incidents.count_inc AS severity_countInc, 
  incidents.incident_detail
FROM incidents
WHERE  incidents.incident_date > '2015-01-01 00:00:00' 
  AND incidents.incident_number NOT LIKE '0275'
  AND incidents.incident_number NOT LIKE '0276'
  AND incidents.incident_number NOT LIKE '0283'
  AND incidents.incident_number NOT LIKE '1099'
  AND incidents.incident_number NOT LIKE '1100'
  AND incidents.incident_number NOT LIKE '1125'
  AND incidents.incident_number NOT LIKE '1546'
-- hybrid
  AND incidents.incident_number NOT LIKE '2860'
  AND incidents.incident_number NOT LIKE '2862'
  AND incidents.incident_number NOT LIKE '2864' 
  AND incidents.incident_number NOT LIKE '2865'
  AND incidents.incident_number NOT LIKE '2866'
  AND incidents.incident_number NOT LIKE '2867'  
  AND incidents.incident_number NOT LIKE '3812'
  AND incidents.incident_number NOT LIKE '4109'  
  AND incidents.incident_number NOT LIKE '5068'  
-- loop
  AND incidents.incident_number NOT LIKE '3692' 
-- syncro
  AND incidents.incident_number NOT LIKE '2873'  
-- T2
  AND incidents.incident_number NOT LIKE '0310'
  AND incidents.incident_number NOT LIKE '0311'
  AND incidents.incident_number NOT LIKE '2080'
  AND incidents.incident_number NOT LIKE '2085'
  AND incidents.incident_number NOT LIKE '2101'
  AND incidents.incident_number NOT LIKE '2102'
  AND incidents.incident_number NOT LIKE '2112'
  AND incidents.incident_number NOT LIKE '2113'
  AND incidents.incident_number NOT LIKE '2825'
  AND incidents.incident_number NOT LIKE '2826'
  AND incidents.incident_number NOT LIKE '2827'
  AND incidents.incident_number NOT LIKE '2830'   
  AND incidents.incident_number NOT LIKE '2836'
  AND incidents.incident_number NOT LIKE '2838'
  AND incidents.incident_number NOT LIKE '2839'
  AND incidents.incident_number NOT LIKE '2840'
  AND incidents.incident_number NOT LIKE '2841'
  AND incidents.incident_number NOT LIKE '3580'  
-- UA
  AND incidents.incident_number NOT LIKE '3754'
-- UA term
  AND incidents.incident_number NOT LIKE '2050'
  AND incidents.incident_number NOT LIKE '2053'
  AND TRIM(incidents.incident_number) NOT LIKE ''
ORDER BY
  incidents.severity ASC, 
  incidents.incident_number ASC, 
  incidents.node_id ASC, 
  incidents.cry ASC, 
  incidents.cpl ASC, 
  incidents.ac ASC, 
  incidents.term ASC, 
  incidents.incident_date ASC;
----------------------------------------------- external_alarm
SELECT al_ext, incident_date_default, incident_date_end, duration AS "DD HH:MM:SS"
FROM d_alarme_externe
WHERE  incident_date > '2015-01-01 00:00:00' 
ORDER BY al_ext, incident_date, incident_id;
----------------------------------------------- hybrid
SELECT node_id, term, incident_date_down, incident_date_up, duration
FROM d_hybrid
WHERE node_id LIKE '00' AND incident_date > '2015-01-01 00:00:00' 
ORDER BY node_id, term, incident_date, incident_id;
----------------------------------------------- loop
SELECT*FROM incidents
WHERE  incidents.incident_date > '2015-01-01 00:00:00' AND incidents.incident_number LIKE '3692'   
ORDER BY incidents.incident_date ASC;
----------------------------------------------- syncro
SELECT*FROM incidents
WHERE incidents.incident_date > '2015-01-01 00:00:00' AND incidents.incident_number LIKE '2873'   
ORDER BY incidents.incident_date ASC;
----------------------------------------------- link_T2
SELECT node_id, trk_name, incident_date_down, incident_date_up, duration
FROM d_t2
WHERE  incident_date > '2015-01-01 00:00:00' 
ORDER BY incident_date, incident_id;
----------------------------------------------- interlinks
SELECT cry||'-'||cpl AS link, 
  incident_date_down, 
  incident_date_up, 
  duration AS "DD HH:MM:SS"
FROM d_link 
WHERE  incident_date > '2015-01-01 00:00:00' 
ORDER BY cry, cpl, incident_date, incident_id;
----------------------------------------------- ua_term
SELECT node_id, ua, typ_term, incident_date_loss, incident_date_in, duration
FROM d_ua
WHERE incident_date > '2015-01-01 00:00:00'
ORDER BY node_id, cry, cpl, term, incident_date, incident_id;
