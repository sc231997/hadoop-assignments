-- beeline -u jdbc:hive2://localhost:10000/default  -n hadoop

-- Creating STAGING tables to load the data from any source
-- this helps in preserving the original state of data before any
-- transformation is applied for any data analysis.

-- sqoop import --connect jdbc:mysql://pccoe-hive.clm70ijmdtd1.us-east-1.rds.amazonaws.com:3306/BDHS_PROJECT --username admin --password password123 --table STOCK_PRICES --hive-import --hive-table STOCK_PRICES -m 1
-- sqoop import --connect jdbc:mysql://pccoe-hive.clm70ijmdtd1.us-east-1.rds.amazonaws.com:3306/BDHS_PROJECT --username admin --password password123 --table STOCK_COMPANIES --hive-import --create-hive-table --hive-table STOCK_COMPANIES -m 1
SELECT * FROM STOCK_PRICES LIMIT 5;
SELECT * FROM STOCK_COMPANIES LIMIT 5;

CREATE TABLE IF NOT EXISTS STOCK_DATA (
    Trading_year INT,
    Trading_month INT,
    Symbol STRING,
    Company_name STRING,
    State STRING,
    Sector STRING,
    Sub_industry STRING,
    Open DOUBLE,
    Close DOUBLE,
    Low DOUBLE,
    High DOUBLE,
    Volume INT 
);

INSERT INTO STOCK_DATA
SELECT 
    YEAR(Trading_date) AS Trading_year,
    MONTH(Trading_date) AS Trading_month,
    C.Symbol AS Symbol,
    Company_Name,
    SPLIT(Headquater,';')[0] AS State,
    Sector,
    Sub_industry,
    AVG(Open) AS Open,
    AVG(Close) AS Close,
    AVG(Low) AS Low,
    AVG(High) AS High,
    AVG(Volume) AS Volume
FROM STOCK_PRICES P
INNER JOIN STOCK_COMPANIES C ON C.Symbol = P.SYMBOL
GROUP BY 
    MONTH(Trading_date), 
    YEAR(Trading_date),
    C.Symbol,
    Company_Name,
    Headquater,
    Sector,
    Sub_industry;

select * from STOCK_DATA limit 3;

-- 3) Find the top five companies that are good for investment
SELECT SYMBOL, COMPANY_NAME, AVG(HIGH) AS AVG_HIGH 
FROM STOCK_DATA
GROUP BY SYMBOL, COMPANY_NAME
ORDER BY AVG_HIGH DESC
LIMIT 5;
-- 4) Show the best-growing industry by each state, having at least two or more industries mapped.

-- 5) For each sector find the following.
--      •	Worst year
--      •	Best year
--      •	Stable year
WITH CTE AS
(
    SELECT Sector, Trading_year, AVG(LOW) AS AVG_LOW 
    FROM STOCK_DATA
    GROUP BY Sector, Trading_year
)

SELECT CTE.* FROM CTE
INNER JOIN (
    SELECT Sector, MIN(AVG_LOW) as min_low FROM CTE GROUP BY Sector
) A ON A.Sector = cte.Sector and min_low = avg_low;