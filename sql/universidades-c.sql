SELECT university, career, nombre, sexo, birth_date, location, email
FROM jujuy_utn
WHERE (university = 'Universidad Nacional de Jujuy')
AND (inscription_date BETWEEN '2020/09/01' AND '2021/02/01')
UNION
SELECT universidad, careers, names, sexo, birth_dates, codigo_postal, correos_electronicos
FROM palermo_tres_de_febrero 
WHERE (universidad = 'Universidad De Palermo')
AND (fecha_de_inscripcion BETWEEN '2020/09/01' AND '2021/02/01');