SELECT 
  university            AS university,
  career                AS career,
  nombre                AS full_name, 
  sexo                  AS gender,
  NULL                  AS postal_code,
  location              AS location,
  email                 AS email,
  CASE
    WHEN TO_DATE(birth_date, 'YYYY/MM/DD') >= CURRENT_DATE - INTERVAL '16 years'
      THEN (TO_DATE(birth_date, 'YYYY/MM/DD') - INTERVAL '100 years')::date
    ELSE TO_DATE(birth_date, 'YYY/MM/DD')::date
  END                   AS birth_date
FROM jujuy_utn
WHERE (university='universidad nacional de jujuy')
  AND (inscription_date BETWEEN '2020/09/01' AND '2021/02/01')
UNION
SELECT
  universidad           AS university,
  careers               AS career,
  names                 AS full_name,
  sexo                  AS gender,
  codigo_postal         AS postal_code,
  NULL                  AS location,
  correos_electronicos  AS email,
  CASE
    WHEN TO_DATE(birth_dates, 'DD/Mon/YY') >= CURRENT_DATE - INTERVAL '16 years'
      THEN (TO_DATE(birth_dates, 'DD/Mon/YY') - INTERVAL '100 years')::date
    ELSE TO_DATE(birth_dates, 'DD/Mon/YY')::date
  END                   AS birth_date
FROM palermo_tres_de_febrero
WHERE (universidad= '_universidad_de_palermo')
  AND (TO_DATE(fecha_de_inscripcion, 'DD/Mon/YY') BETWEEN '2020/09/01' AND '2021/02/01');