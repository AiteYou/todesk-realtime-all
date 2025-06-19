package com.todesk.bigdata.realtime.utils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Author : Liuchao
 * Date : 2022/11/2 16:15
 * Desc : Comet中国省份判断
 */
public class ProvinceCompareUtil {
    public static boolean compare (String province){
        List<String> provinces = Stream.of("广东省","江苏省","上海市","浙江省","福建省","山东省","河南省","四川省","北京市","河北省","辽宁省","吉林省","湖南省","安徽省","湖北省","江西省","广西壮族自治区","黑龙江省","陕西省","山西省","重庆市","贵州省","天津市","云南省","内蒙古自治区","新疆维吾尔自治区","甘肃省","中国香港","海南省","宁夏回族自治区","中国台湾","青海省","西藏自治区","澳门特别行政区")
                .collect(Collectors.toList());

        return provinces.contains(province);
    }

    public static void main(String[] args) {
        System.out.println(compare(null));
    }
}
