package top.doe.hive.dataware;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Gps2Geohash extends UDF {

    public String evaluate(Double lat, Double lng) {

        return GeoHash.geoHashStringWithCharacterPrecision(lat,lng,6);
    }

}
