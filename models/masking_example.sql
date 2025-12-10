{{ config(materialized="table") }}

select *
from (
    values
        (1, 'Julie Roberts',  'julier86@yahoo.com',  670107295, '1986-03-03'::date),
        (2, 'Angela Scott',   'ascott@gmail.com',    750212384, '1975-07-12'::date),
        (3, 'Tyler Shaw DDS', 'tylershaw@gmail.com', 820405672, '1982-11-05'::date),
        (4, 'Shane Hayes',    'hayes-s@outlook.com', 900623145, '1990-06-23'::date),
        (5, 'Amanda Golden',  'golden99@yahoo.com',  730918256, '1973-09-18'::date)
    as v(id, name, email, ssn, dob)
)
