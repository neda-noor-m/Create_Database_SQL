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

--changing the format of string Date column to standard format. It is somehow casting
update mother_table
set date = substr(date, 7, 4) || '-' || substr(date, 1, 2) || '-' || substr(date, 4, 2) ;
