-- create database betterpurchase;
use betterpurchase;
-- select count(*) from amazon;
-- DROP TABLE apple_exchange_rate;
-- create table apple_exchange_rate(
-- Date_currently date,
-- Currency_CodeA varchar(40),
-- X decimal(7,5)
-- );
 -- drop table walmartdata;

-- describe amazon;
-- CREATE TABLE Walmart_data
--  (
--  Uniq_Id varchar(150),
--  Crawl_Timestamp varchar(200),
--  Product_Url varchar(1000),
--  Product_Name varchar(500),
--  Sale_Price float,
--  Brand varchar(500),
--  Item_Number int,
--  Gtin varchar(100),
--  Product_inovation varchar(30),
--  Category varchar(500),
--  Available varchar(15)
--  );
-- select * from walmartdata;
-- drop table amazon;

----- creating amazon table ----------------
-- create table amazon(
-- Uniq_Id varchar(70),
-- Product_Name varchar(200),
-- Brand_Name varchar(200),
-- Category varchar(200),	
-- Selling_Price varchar(200),
-- Model_Number varchar(100),
-- About_Product varchar(200),
-- Product_Specification varchar(200),
-- Shipping_Weight varchar(300),
-- Variants varchar(300),
-- Product_Url varchar(200),
-- Is_Amazon_Seller varchar(250)
-- );
-- drop table apple_exchange_rate;
-- create table apple_exchange_rate(
-- Date varchar(30),
-- CurrencyCodeA varchar(40),
-- X DECIMAL(8,5)
-- );
-- select * from amazon;
-- select * from amazon;

-- CREATE TABLE walmartdata(Uniq_Id varchar(150),Crawl_Timestamp varchar(200),Product_Url varchar(1000),Product_Name varchar(500),Sale_Price DECIMAL(10,5),Brand varchar(500),Item_Number int,Gtin varchar(100),Product_inovation varchar(30),Category varchar(500), Available varchar(50));

-- select * from apple_exchange_rate;
 -- select * from walmartdata;
-- select * FROM walmartdata group by Uniq_Id having count(Uniq_Id) > 1;
-- select * from amazon;
-- drop table apple_product_price;
-- drop table walmartdata;

-- create table ebay(
-- prod_name varchar(200),
-- category varchar(200),
-- price decimal(5,5),
-- rating int
-- );

-- create table barens(
-- prod_name varchar(30),
-- category varchar(30),
-- Price decimal(5,5)
-- );

-- select * from amazon

-- use case to explain 
-- select * from barnes b INNER JOIN ebay e ON b.prod_name = e.prod_name;
-- select * from barens b LEFT JOIN amazon a ON b.prod_name = product_name 
-- select * from ebay;
-- select * from apple_product_price group by pid, region having count(pid) > 1 and count(region) > 1
-- drop table barnes;
-- drop table walmartdata;
-- create table trig(product_name varchar(30),inserted_time timestamp);
-- drop table bestbuy;
-- select * from apple_product_price;
-- --------- All Tables ------
-- describe  walmartdata;
-- drop table bestbuy;
-- select * from walmartdata;
-- select * from bestbuy;
-- select * from  amazon;
-- select * from barnes;
-- select * from bestbuy
-- drop table apple_product_price;
-- select * from amazon;
-- select  *   from walmartdata w inner join apple_product_price ap on w.Product_name = ap.model;
-- drop table apple_product_price

--  select * from barnes b  OUTER JOIN ebay e on b.prod_name = e.prod_name;
-- select *from apple_product_price
-- w.product_name,w.Sale_Price as walmart_price ,e.price as ebay_price ,a.Selling_Price as amazon_price from walmartdata w
-- select * from walmartdata w inner join amazon a on w.Product_name = a.Product_name 
-- inner join  ebay e on  w.Product_name = e.prod_name where w.Category = "Office Supplies | Paper | All Paper & Printable Media"
-- select e.prod_name,e.price,w.Sale_Price from ebay e inner join walmartdata w on e.prod_name = w.Product_name order by e.price asc 

-- select * from amazon


select e.Product_name,e.Selling_Price,w.Sale_Price from amazon e inner join walmartdata w on e.Product_name = w.Product_name order by e.price asc;