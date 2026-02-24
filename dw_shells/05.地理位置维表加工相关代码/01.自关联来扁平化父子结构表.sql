CREATE TABLE doit50_gps_ck
SELECT
    ck.BD09_LAT,
    ck.BD09_LNG,
    province.AREANAME as province ,
    city.AREANAME as city,
    region.AREANAME as region

from t_md_areas ck join t_md_areas region on ck.PARENTID = region.id and ck.`LEVEL`=4
                   join t_md_areas city   on region.PARENTID = city.id
                   join t_md_areas province on city.PARENTID = province.id