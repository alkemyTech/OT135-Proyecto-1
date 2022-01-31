SELECT
  university as university,
  career as career,
  nombre as full_name,
  sexo as gender,
  TO_DATE(birth_date,'YYYY-MM-DD') as age,
  'NULL' as postal_code,
  location as location,
   email as email
FROM jujuy_utn
WHERE (university = 'universidad tecnológica nacional')
AND (inscription_date BETWEEN '2020/09/01' AND '2021/02/01')
UNION
SELECT 
  universidad as university,
  careers as career,
  names as full_name,
  sexo as gender,
  TO_DATE(birth_dates,'DD/Mon/YY') as age,
  codigo_postal as postal_code,
  'NULL' as location,
  correos_electronicos as email
FROM palermo_tres_de_febrero
WHERE (universidad = 'universidad_nacional_de_tres_de_febrero')
AND (TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '2020/09/01' AND '2021/02/01');
