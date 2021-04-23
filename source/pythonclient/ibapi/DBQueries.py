"""


--Query 1
--Select everything raised more than 10%
SELECT * FROM bar
WHERE time_span='1 day' AND (_close-_open)/_open>=0.1
ORDER BY time desc, symbol;


--Query 2
--Create a new temporary column "prev_close" in the current row and
--copy the _close value from the previous row
SELECT
	*,
	lag(_close,1) OVER (
			PARTITION BY symbol
			ORDER BY time)
		AS prev_close
FROM
	bar;


--Query 3: Continue Query 2
--Create a new temporary table "preclose" and select those rows that has gap% >= 10%
WITH preclose AS(
SELECT
	*,
	lag(_close,1) OVER (
			PARTITION BY symbol
			ORDER BY time)
		AS prev_close
FROM
	bar)
SELECT * FROM preclose
WHERE (_open-prev_close)/prev_close>=0.1;

--Query 4:
--Delete contract and bar data for particular tickers, and return those deleted rows.
DELETE FROM bar
WHERE symbol='CSR' OR symbol='ORA' OR symbol='CTD'
RETURNING *;

DELETE FROM contract
WHERE symbol='CSR' OR symbol='ORA' OR symbol='CTD'
RETURNING *;


--Query 5:
--Select all rows for particular tickers
SELECT * FROM contract
WHERE symbol='CSR' OR symbol='ORA' OR symbol='CTD';

SELECT * FROM bar
WHERE symbol='CSR' OR symbol='ORA' OR symbol='CTD'
ORDER BY time;











"""
