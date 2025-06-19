package com.todesk.bigdata.realtime.factor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Author : Liuchao
 * Date : 2022/10/12 11:08
 * Desc :
 */
public class IdTest {
    public static void main(String[] args) {
        Double totalPercentage = 100.0;
        Double curPecentage = 0.1;
        try {
            BufferedReader in = new BufferedReader(new FileReader("G:\\IDEAProjects\\todesk-realtime-all\\todesk-realtime\\src\\main\\java\\com\\todesk\\bigdata\\realtime\\factor\\id.csv"));
            String str;
            while ((str = in.readLine()) != null) {
                if ("电信".equals(str) || "移动".equals(str) || "联通".equals(str) || "鹏博士".equals(str)){
                    System.out.println(str);
                }else if(str.endsWith("广电")){
                    System.out.println(str+"->广电");
                }else if(str.contains("/")){
                    System.out.println(str+"->"+str.split("/")[0]);
                }else System.out.println(str+"->其他");
                System.out.println("cice");
            }
            //System.out.println(str);
        } catch (IOException e) {
        }
    }

    public static Long getHash(String id){
        long h =0;
        if ( id != null ){
            char val[] = id.toCharArray();
            System.out.println(val);
            int len = id.length();

            for (int i = 0; i < len; i++) {
                h = 31 * h + val[i];
            }
        }
        return Math.abs(h);
    }
}
