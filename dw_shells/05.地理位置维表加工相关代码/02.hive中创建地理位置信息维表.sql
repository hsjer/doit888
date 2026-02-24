create table dim.geohash_area(

    geohash string,
    province string,
    city string,
    region string

)
stored as orc
tblproperties (
    'orc.compress' = 'snappy'
    )
