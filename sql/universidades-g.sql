SELECT 
  universities AS university, 
  careers AS career, 
  names AS full_name, 
  sexo AS gender, 
  TO_DATE(birth_dates,'DD/MM/YYYY') AS birth_date, 
  null AS zipcode, 
  locations AS adress, 
  emails AS email, 
  TO_DATE(inscription_dates,'DD/MM/YYYY') AS inscription_date
FROM lat_sociales_cine
WHERE universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
  AND inscription_dates BETWEEN '01-09-2020' AND '02-01-2021'
UNION
SELECT 
  universidades AS university, 
  carreras AS career, 
  nombres AS full_name, 
  sexo AS gender, 
  CASE
     WHEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') >= CURRENT_DATE - INTERVAL '16 years' THEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') - INTERVAL '100 years'
     ELSE TO_DATE(fechas_nacimiento,'YY-Mon-DD')
  END AS birth_date,
  codigos_postales AS zipcode, 
  direcciones AS adress,
  emails AS email, 
  TO_DATE(fechas_de_inscripcion,'YY/Mon/DD') AS inscription_date
FROM uba_kenedy
WHERE universidades ='universidad-j.-f.-kennedy'
  AND fechas_de_inscripcion BETWEEN '20-Sep-09' AND '21-Ene-02';