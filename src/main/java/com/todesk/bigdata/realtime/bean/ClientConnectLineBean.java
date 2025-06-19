package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/11/2 19:42
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientConnectLineBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    String line;
    String province;
    // 登陆数
    Long clientConnectCount;
    // 时间戳
    Long ts;
}
