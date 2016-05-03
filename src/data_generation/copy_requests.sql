CREATE TABLE traces.first_week_of_march AS
SELECT
    *
FROM
    wmf.webrequest
WHERE
    year = 2016
    AND month = 3
    AND day in (1,2,3,4,5,6,7,8)
    AND webrequest_source = 'text';
