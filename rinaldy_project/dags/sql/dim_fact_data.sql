-- truncate and create dim province
truncate dim_province_table;
insert into dim_province_table
select distinct kode_prov::int, nama_prov from data_covid;

-- truncate and create dim district
truncate dim_district_table;
insert into dim_district_table 
select DISTINCT
kode_kab::int, kode_prov::int, nama_kab
FROM data_covid;

-- truncate and create fact province daily
truncate fact_province_daily restart identity;
insert into fact_province_daily(province_id, case_id, "date", total)
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'suspect' and status_detail ='diisolasi')as case_id,
	tanggal,
	sum (suspect_diisolasi) as total
from data_covid
group by tanggal 
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'suspect' and status_detail ='discarded')as case_id,
	tanggal,
	sum (suspect_discarded) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'suspect' and status_detail ='meninggal')as case_id,
	tanggal,
	sum (suspect_meninggal) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'closecontact' and status_detail ='dikarantina')as case_id,
	tanggal,
	sum (closecontact_dikarantina) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'closecontact' and status_detail ='discarded')as case_id,
	tanggal,
	sum (closecontact_discarded) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'closecontact' and status_detail ='meninggal')as case_id,
	tanggal,
	sum (closecontact_meninggal) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'probable' and status_detail ='diisolasi')as case_id,
	tanggal,
	sum (probable_diisolasi) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'probable' and status_detail ='discarded')as case_id,
	tanggal,
	sum (probable_discarded) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'probable' and status_detail ='meninggal')as case_id,
	tanggal,
	sum (probable_meninggal) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'confirmation' and status_detail ='sembuh')as case_id,
	tanggal,
	sum (confirmation_sembuh) as total
from data_covid
group by tanggal
union 
select (select province_id from dim_province_table) as province_id , 
	(select id from dim_case_table where status_nama = 'confirmation' and status_detail ='meninggal')as case_id,
	tanggal,
	sum (confirmation_meninggal) as total
from data_covid
group by tanggal;

-- truncate and create fact province monthly
truncate fact_province_monthly restart identity;
insert into fact_province_monthly(province_id, case_id, "month", total) 
select province_id , case_id, to_char("date"::date ,'YYYY-MM' )as "month", sum (total) as total
from fact_province_daily
group by province_id , case_id ,to_char("date"::date,'YYYY-MM');

-- truncate and create fact province yearly
truncate fact_province_yearly restart identity;
insert into fact_province_yearly(province_id, case_id, "year", total)
select province_id , case_id, left("month" ,'4' ) as "year" , sum (total) as total
from fact_province_monthly 
group by province_id , case_id , left("month" ,'4' );

-- truncate and create fact district monthly
truncate fact_district_monthly restart identity;
insert into fact_district_monthly(district_id, case_id, "month", total) 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'suspect' and status_detail ='diisolasi')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (suspect_diisolasi) as total
from data_covid
group by district_id , case_id, "month"
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'suspect' and status_detail ='discarded')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (suspect_discarded) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'suspect' and status_detail ='meninggal')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (suspect_meninggal) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'closecontact' and status_detail ='dikarantina')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (closecontact_dikarantina) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id , 
	(select id from dim_case_table where status_nama = 'closecontact' and status_detail ='discarded')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (closecontact_discarded) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'closecontact' and status_detail ='meninggal')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (closecontact_meninggal) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'probable' and status_detail ='diisolasi')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (probable_diisolasi) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'probable' and status_detail ='discarded')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (probable_discarded) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id , 
	(select id from dim_case_table where status_nama = 'probable' and status_detail ='meninggal')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (probable_meninggal) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'confirmation' and status_detail ='sembuh')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (confirmation_sembuh) as total
from data_covid
group by district_id , case_id, "month" 
union 
select kode_kab::int as district_id, 
	(select id from dim_case_table where status_nama = 'confirmation' and status_detail ='meninggal')as case_id,
	to_char(tanggal::date,'YYYY-MM')as "month",
	sum (confirmation_meninggal) as total
from data_covid
group by district_id , case_id, "month";

-- truncate and create fact district yearly
truncate fact_district_yearly restart identity;
insert into fact_district_yearly(district_id, case_id, "year", total)
select district_id, case_id, left("month" ,'4') as "year" , sum (total) as total
from fact_district_monthly
group by district_id, case_id , "year";

