CREATE TABLE houses (
  account_id INT,
  account_name VARCHAR(MAX),
  ad_id INT,
  address VARCHAR(MAX),
  area INT,
  area_name VARCHAR(MAX),
  area_v2 INT,
  body VARCHAR(MAX),
  category INT,
  category_name VARCHAR(MAX),
  date VARCHAR(MAX),
  image VARCHAR(MAX),
  latitude DOUBLE PRECISION,
  length DOUBLE PRECISION,
  list_id INT,
  list_time BIGINT,
  location VARCHAR(MAX),
  longitude DOUBLE PRECISION,
  price VARCHAR(MAX),
  region_name VARCHAR(MAX),
  size DOUBLE PRECISION,
  street_name VARCHAR(MAX),
  subject VARCHAR(MAX),
  type VARCHAR(MAX),
  ward_name VARCHAR(MAX),
  width DOUBLE PRECISION,
  house_type INT,
  living_size DOUBLE PRECISION,
  rooms INT,
  toilets INT,
  direction INT,
  floors INT,
  street_number VARCHAR(MAX),
  detail_address VARCHAR(MAX)
);


COPY nhatot.public.houses (account_id, account_name, ad_id, address, area, area_name, area_v2, body, category, category_name, date, image, latitude, length, list_id, list_time, location, longitude, price, region_name, size, street_name, subject, type, ward_name, width, house_type, living_size, rooms, toilets, direction, floors, street_number, detail_address) 
FROM 's3://tigerlake/clean_houses.json' IAM_ROLE 'arn:aws:iam::762330586788:role/myspectrum_role' FORMAT AS JSON 'auto' REGION AS 'ap-southeast-1'