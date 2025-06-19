package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/5/31 12:36
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BackendDetailBean {
    /*
    " `new_connid` STRING," +
                " `new_serv` STRING," +
                " `new_logtime`  BIGINT," +
                " `new_pt` TIMESTAMP_LTZ(3)," +
                " `hand_su_connid` STRING," +
                " `hand_su_logtime` BIGINT," +
                " `hand_su_pt` TIMESTAMP_LTZ(3)," +
                " `ts` TIMESTAMP_LTZ(3)," +*/
    String new_serv;
    String hand_su_connid;
    String new_logtime;
    String new_pt;
    String hand_su_logtime;
    String hand_su_pt;
    String new_connid;
    String ts;
}
