-- wget https://pccoe-hadoop.s3.amazonaws.com/Stockcompanies.csv
-- wget https://pccoe-hadoop.s3.amazonaws.com/StockPrices.csv
-- mysql -h pccoe-hive.clm70ijmdtd1.us-east-1.rds.amazonaws.com -u admin -p
-- Create Database
CREATE DATABASE BDHS_PROJECT;
USE BDHS_PROJECT;

-- Create Tables
CREATE TABLE IF NOT EXISTS STOCK_PRICES (
    Trading_date DATE,
    Symbol VARCHAR(255),
    Open DOUBLE,
    Close DOUBLE,
    Low DOUBLE,
    High DOUBLE,
    Volume INT 
);
CREATE TABLE IF NOT EXISTS STOCK_COMPANIES (
    Symbol VARCHAR(255),
    Company_name VARCHAR(255),
    Sector VARCHAR(255),
    Sub_industry VARCHAR(255),
    Headquater VARCHAR(255)
);

-- Load data
-- In case load data fails try the following cmd
-- SET GLOBAL local_infile=1;
LOAD DATA LOCAL INFILE 'StockPrices.csv' 
INTO TABLE STOCK_PRICES
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'Stockcompanies.csv' 
INTO TABLE STOCK_COMPANIES
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Verifying
SELECT * FROM STOCK_PRICES LIMIT 5;
SELECT * FROM STOCK_COMPANIES LIMIT 5;