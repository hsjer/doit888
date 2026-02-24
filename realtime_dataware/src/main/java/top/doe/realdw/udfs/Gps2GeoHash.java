package top.doe.realdw.udfs;

import ch.hsr.geohash.GeoHash;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;

public class Gps2GeoHash extends ScalarFunction implements Serializable {

    public String eval(Double lat, Double lng) {

        try {
            String geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6);
            return geohash;
        }catch (Exception e) {
            return null;
        }
    }

}
