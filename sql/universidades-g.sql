SELECT universities AS universidades, careers AS carreras, names AS nombres, sexo, TO_DATE(birth_dates,'DD/MM/YYYY') AS fechas_nacimiento, null AS codigos_postales, locations AS direcciones, emails, TO_DATE(inscription_dates,'DD/MM/YYYY') AS fechas_de_inscripcion
FROM lat_sociales_cine
WHERE universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
AND inscription_dates BETWEEN '01-09-2020' AND '02-01-2021'
UNION
SELECT universidades, carreras, nombres, sexo, 
CASE
 WHEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') >= CURRENT_DATE - INTERVAL '16 years' THEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') - INTERVAL '100 years'
 ELSE TO_DATE(fechas_nacimiento,'YY-Mon-DD')
END AS fechas_nacimiento,
codigos_postales, direcciones, emails, TO_DATE(fechas_de_inscripcion,'YY/Mon/DD')
FROM uba_kenedy
WHERE universidades ='universidad-j.-f.-kennedy'
AND fechas_de_inscripcion BETWEEN '20-Sep-09' AND '21-Ene-02';