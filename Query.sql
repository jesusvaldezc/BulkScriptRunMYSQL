
UPDATE testpython.test_cad od
SET SOURCE = LOWER(SOURCE)
WHERE 1=1
;

-- DELETE FROM testpython.test_cad;

DROP TABLE IF EXISTS testpython.rawr;
CREATE TEMPORARY TABLE testpython.rawr
(
	SELECT * FROM testpython.wtpartmaster WHERE OBJ_NUMBER = 'TEST'


)
;

UPDATE wtpartmaster dos
JOIN testpython.rawr ra ON dos.OBJ_NUMBER = ra.OBJ_NUMBER
SET dos.OBJ_NUMBER = 'LOL'
WHERE 1=1
;

		
SELECT * FROM testpython.wtpart;