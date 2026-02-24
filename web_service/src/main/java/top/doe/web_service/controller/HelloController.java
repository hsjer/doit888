package top.doe.web_service.controller;


import org.apache.commons.lang3.RandomUtils;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import top.doe.web_service.pojo.Event;

import java.util.Random;

@RestController
public class HelloController {


    RoaringBitmap bm = RoaringBitmap.bitmapOf();

    @RequestMapping(value="/api/get_event",method = RequestMethod.GET)
    public Event getEventById(String event_id){
        return new Event(event_id, RandomUtils.nextInt(1,100));
    }


    // http://some:8080/api/xxxx/yyyy/some
    @RequestMapping("/api/{db}/{tb}/some")
    public String someFun(@PathVariable String db, @PathVariable String tb){

        return null;
    }


    @RequestMapping("/api/bit/add")
    public String bitAdd(int idx){
        bm.add(idx);

        return "ok";
    }

    @RequestMapping("/api/bit/delete")
    public String bitDelete(int idx){
        bm.remove(idx);

        return "ok";
    }

}
