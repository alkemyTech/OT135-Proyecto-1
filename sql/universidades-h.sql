SELECT
  universities     AS university,
  careers          AS career,
  names            AS student_full_name,
  sexo             AS gender,
  CASE 
    WHEN TO_DATE(birth_dates,'DD-MM-YYYY') >= CURRENT_DATE - INTERVAL '16 years'
      THEN (TO_DATE(birth_dates,'DD-MM-YYYY') - INTERVAL '100 years')::date
    ELSE TO_DATE(birth_dates,'DD-MM-YYYY')::date
  END             AS birth_date,
  NULL            AS postal_code,
  locations       AS location,
  emails          AS email
FROM lat_sociales_cine
WHERE universities = 'UNIVERSIDAD-DEL-CINE'
  AND TO_DATE(inscription_dates,'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION ALL
SELECT
  universidades    AS university,
  carreras         AS career,
  nombres          AS student_full_name,
  sexo             AS gender,
  CASE
   WHEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') >= CURRENT_DATE - INTERVAL '16 years'
     THEN (TO_DATE(fechas_nacimiento,'YY-Mon-DD') - INTERVAL '100 years')::date
   ELSE TO_DATE(fechas_nacimiento,'YY-Mon-DD')::date
  END              AS birth_date,
  codigos_postales AS postal_code,
  NULL             AS location,
  emails           AS email
FROM uba_kenedy
WHERE universidades = 'universidad-de-buenos-aires'
  AND TO_DATE(fechas_de_inscripcion,'YY-Mon-DD') BETWEEN '2020-09-01' AND '2021-02-01'