package cn.doitedu.mr;

import top.doe.mr.map.Mapper;

public class NiuXMapper implements Mapper {

    @Override
    public String map(String data) {

        return data.toUpperCase();
    }
}
