-- update mother_table with lover case
UPDATE mother_table
SET CategoryName = LOWER(CategoryName),
     ItemDescription = LOWER(ItemDescription),
	 StoreName = LOWER(StoreName),
	 StoreLocation = LOWER(StoreLocation),
	 address = LOWER(address),
	 county = LOWER(county),
	 VendorName = LOWER(VendorName),
	 city = LOWER(city)
WHERE 1 = 1;
-- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--changing the format of string Date column to standard format. It is somehow casting
update mother_table
set date = substr(date, 7, 4) || '-' || substr(date, 1, 2) || '-' || substr(date, 4, 2) ;
-- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE mother_table
SET city = "ottumwa"
WHERE city =  "ottuwma";
UPDATE mother_table
SET city = "guttenberg"
WHERE city =  "guttenburg";
UPDATE mother_table
SET city = "shueyville"
WHERE city =  "swisher";

-- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Create product table with a primary key:CREATE TABLE products (
  CREATE TABLE product (
  product_id INT NOT NULL PRIMARY KEY  , -- Specify primary key explicitly
  description TEXT, 
  category_id INTEGER,
  pack INTEGER,
 bottle_volume_ml INTEGER 
);

with occurrence_finding as (
SELECT ItemNumber as product_id, ItemDescription as description, Category, Pack, "BottleVolume(ml)" as bottle_volume_ml, 
count(*) over(PARTITION by ItemNumber, Category) as ctg,
count(*) over(PARTITION by ItemNumber, ItemDescription) as itd,
count(*) over(PARTITION by ItemNumber, Pack) as pc,
count(*) over(PARTITION by ItemNumber, "BottleVolume(ml)") as volume	
from 
mother_table
where Category is not NULL or ItemDescription is not NULL or Pack is not NULL or "BottleVolume(ml)" is not NULL 
),
rank_finding as (
SELECT product_id, description, Category, Pack, bottle_volume_ml, 
ctg,
row_number() OVER( PARTITION by product_id ORDER by ctg DESC) as rank_catg, itd,
row_number() OVER( PARTITION by product_id ORDER by itd DESC) as rank_itd, pc,
row_number() OVER( PARTITION by product_id ORDER by pc DESC) as rank_pc, volume,
row_number() OVER( PARTITION by product_id ORDER by volume DESC)  as rank_volume
FROM
occurrence_finding
ORDER by product_id
)
SELECT product_id, 
max(case when rank_itd = 1 THEN description END) as description,
max(case when rank_catg = 1 THEN Category END) as Category, 
max(case when rank_pc = 1 THEN Pack END) as Pack, 
max(case when rank_volume = 1 THEN bottle_volume_ml END) as bottle_volume_ml
from rank_finding
GROUP by product_id
-- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Create vendors with a primary key:
CREATE TABLE vendor (
  vendor_id INT NOT NULL PRIMARY KEY, -- Specify primary key explicitly
  vendor_name TEXT
);

WITH count_VendorName AS (
    SELECT VendorNumber, VendorName, COUNT(*) AS count_VendorName
    FROM mother_table
    WHERE VendorName IS NOT NULL
    GROUP BY VendorNumber, VendorName
	UNION
	SELECT VendorNumber, NULL as VendorName, 1 AS count_VendorName
    FROM mother_table
    WHERE VendorName IS NOT NULL
    GROUP BY VendorNumber
	having  VendorName is null and count(DISTINCT VendorName) < 1 and VendorNumber is not NULL
)

insert into vendor
SELECT vendor_id, vendor_name
from (
SELECT VendorNumber as vendor_id, 
		FIRST_VALUE(VendorName ) OVER (PARTITION BY VendorNumber ORDER BY count_VendorName DESC) AS vendor_name,
		row_number() over(PARTITION BY VendorNumber) as rankk
FROM count_VendorName
)
WHERE rankk = 1;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Create sales with a primary key:
CREATE TABLE sales (
  sales_id  INT NOT NULL PRIMARY KEY, -- Specify primary key explicitly
  date TEXT,
  store_id INTEGER,
  product_id INTEGER,
  category_id INTEGER,
  vendor_id INTEGER,
  bottle_sold INTEGER,
  sold_dollars REAL,
  sold_liters REAL
);
INSERT INTO sales
SELECT "Invoice/ItemNumber" as sales_id, Date as date, StoreNumber as store_id, ItemNumber as product_id, category as category_id, VendorNumber as vendor_id, BottlesSold as bottle_sold, "Sale(Dollars)" as sold_dollars, "VolumeSold(liters)" as sold_liters
FROM mother_table
ORDER by Date;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Create state_retail_cost with a primary key:
CREATE TABLE state_retail_cost(
 product_id INTEGER NOT NULL,
 date TEXT NOT NULL,
state_bottle_cost REAL,
state_bottle_retail REAL,
PRIMARY KEY(product_id, date)
);
  
INSERT INTO state_retail_cost
SELECT ItemNumber as product_id, Date as date, StateBottleCost as state_bottle_cost, StateBottleRetail as state_bottle_retail
FROM mother_table
group by ItemNumber, date
ORDER by Date;
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--- Create Categories with a primary key:
CREATE TABLE category (
  category_id INT NOT NULL PRIMARY KEY, -- Specify primary key explicitly
  category_name TEXT
);

WITH count_CategoryName AS (
    SELECT Category, CategoryName, COUNT(*) AS count_CategoryName
    FROM mother_table
    WHERE CategoryName IS NOT NULL
    GROUP BY Category, CategoryName
	UNION
	SELECT Category, null as CategoryName, 1 AS count_CategoryName
    FROM mother_table
    GROUP BY Category
	having  CategoryName is null and count(DISTINCT CategoryName) < 1 and Category is not NULL
)

insert into category
SELECT category_id, category_name
from (
SELECT Category as category_id, 
		FIRST_VALUE(CategoryName ) OVER (PARTITION BY Category ORDER BY count_CategoryName DESC) AS category_name,
		row_number() over(PARTITION BY Category) as rankk
FROM count_CategoryName
)
WHERE rankk = 1;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Create stores with a primary key:
CREATE TABLE store (
  store_id INT NOT NULL PRIMARY KEY, -- Specify primary key explicitly
  store_name TEXT,
  address TEXT,
  city TEXT,
  zip_code INTEGER,
  store_location TEXT,
 county_num INTEGER
);

WITH count_city AS (
    SELECT StoreNumber, city, COUNT(*) AS count_city
    FROM mother_table
    WHERE city IS NOT NULL
    GROUP BY StoreNumber, city
	UNION
	SELECT StoreNumber, null as city, 1 AS count_city
    FROM mother_table
    GROUP BY StoreNumber
	having  city is null and count(DISTINCT city) < 1 and StoreNumber is not NULL
),
count_address AS (
    SELECT StoreNumber, Address, COUNT(*) AS count_address
    FROM mother_table
    WHERE Address IS NOT NULL
    GROUP BY StoreNumber, Address
	UNION
	SELECT StoreNumber, null as address, 1 AS count_address
    FROM mother_table
    GROUP BY StoreNumber
	having  address is null and count(DISTINCT address) < 1 and StoreNumber is not NULL
),

count_ZipCode AS (
    SELECT StoreNumber, ZipCode, COUNT(*) AS count_ZipCode
    FROM mother_table
    WHERE ZipCode IS NOT NULL
    GROUP BY StoreNumber, ZipCode
	UNION
	SELECT StoreNumber, null as ZipCode, 1 AS count_ZipCode
    FROM mother_table
    GROUP BY StoreNumber
	having  ZipCode is null and count(DISTINCT ZipCode) < 1 and StoreNumber is not NULL
),
count_storeName AS (
    SELECT StoreNumber, StoreName, COUNT(*) AS count_storeName
    FROM mother_table
    WHERE StoreName IS NOT NULL
    GROUP BY StoreNumber, StoreName
	UNION
	SELECT StoreNumber, null as StoreName, 1 AS count_storeName
    FROM mother_table
    GROUP BY StoreNumber
	having  StoreName is null and count(DISTINCT StoreName) < 1 and StoreNumber is not NULL
),
count_StoreLocation AS (
    SELECT StoreNumber, StoreLocation, COUNT(*) AS count_StoreLocation
    FROM mother_table
    WHERE StoreLocation IS NOT NULL
    GROUP BY StoreNumber, StoreLocation
	UNION
	SELECT StoreNumber, null as StoreLocation, 1 AS count_StoreLocation
    FROM mother_table
    GROUP BY StoreNumber
	having  StoreLocation is null and count(DISTINCT StoreLocation) < 1 and StoreNumber is not NULL
),

count_CountyNumber AS (
    SELECT StoreNumber, CountyNumber, COUNT(*) AS count_CountyNumber
    FROM mother_table
    WHERE CountyNumber IS NOT NULL
    GROUP BY StoreNumber, CountyNumber
	UNION
	SELECT StoreNumber, null as CountyNumber, 1 AS count_CountyNumber
    FROM mother_table
    GROUP BY StoreNumber
	having  CountyNumber is null and count(DISTINCT CountyNumber) < 1 and StoreNumber is not NULL
)

insert into store
SELECT store_id, store_name, Address, city, zip_code, store_location, county_num
from (
SELECT ca.StoreNumber as store_id, 
		FIRST_VALUE(cc.city ) OVER (PARTITION BY cc.StoreNumber ORDER BY cc.count_city DESC) AS city,
        FIRST_VALUE(ca.address) OVER (PARTITION BY ca.StoreNumber ORDER BY ca.count_address DESC) AS address,
		FIRST_VALUE(cz.ZipCode) OVER (PARTITION BY cz.StoreNumber ORDER BY cz.count_ZipCode DESC) AS zip_code,
		FIRST_VALUE(cn.StoreName) OVER (PARTITION BY cn.StoreNumber ORDER BY cn.count_storeName DESC) AS store_name,
		FIRST_VALUE(cll.StoreLocation) OVER (PARTITION BY cll.StoreNumber ORDER BY cll.count_StoreLocation DESC) AS store_location,
		FIRST_VALUE(ccn.CountyNumber) OVER (PARTITION BY ccn.StoreNumber ORDER BY ccn.count_CountyNumber DESC) AS county_num,
		row_number() over(PARTITION BY ca.StoreNumber) as rankk

FROM count_address AS ca
left JOIN count_city AS cc ON ca.StoreNumber = cc.StoreNumber
left join count_ZipCode as cz on ca.StoreNumber = cz.StoreNumber
LEFT join count_storeName as cn on cn.StoreNumber = cc.StoreNumber
left join count_StoreLocation as cll on cll.StoreNumber = cc.StoreNumber
left join count_CountyNumber as ccn on ccn.StoreNumber = cc.StoreNumber

)
WHERE rankk = 1;
