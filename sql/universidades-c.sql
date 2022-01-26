SELECT university, career, nombre AS full_name, sexo AS gender, birth_date, NULL AS postal_code, location, email 
FROM jujuy_utn
WHERE (university = 'universidad nacional de jujuy')
AND (inscription_date BETWEEN '2020/09/01' AND '2021/02/01')
UNION
SELECT universidad AS university, careers AS career, names AS full_name, sexo AS gender, birth_dates AS birth_date, codigo_postal AS postal_code, NULL AS location, correos_electronicos AS email 
FROM palermo_tres_de_febrero 
WHERE (university = '_universidad_de_palermo')
AND (TO_DATE(fecha_de_inscripcion, 'DD/Mon/YY') BETWEEN '2020/09/01' AND '2021/02/01');