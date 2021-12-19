-- Create Dimension table
-- Province table
create table if not exists dim_province_table(
	province_id int not null,
	province_name varchar(50)
);

-- District table
create table if not exists dim_district_table (
	district_id int not null,
	province_id int,
	district_name varchar(50)
);

-- Case table
create table if not exists dim_case_table (
	id SERIAL,
	status_name varchar(50),
	status_detail varchar(50)
);

-- Create Fact table
-- fact province daily table
create table if not exists fact_province_daily (
	id SERIAL,
	province_id int,
	case_id int,
	"date" text,
	total int
);

-- fact province monthly table
create table if not exists fact_province_monthly (
	id SERIAL,
	province_id int,
	case_id int,
	"month" text,
	total int
);

-- fact province yearly table
create table if not exists fact_province_yearly (
	id SERIAL,
	province_id int,
	case_id int,
	"year" text,
	total int
);


-- fact district monthly table
create table if not exists fact_district_monthly (
	id SERIAL,
	district_id int,
	case_id int,
	"month" text,
	total int
);

-- fact district yearly table
create table if not exists fact_district_yearly (
	id SERIAL,
	district_id int,
	case_id int,
	"year" text,
	total int
);


