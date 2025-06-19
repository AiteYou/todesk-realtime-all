package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/6/5 18:34
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BackendClientConnectBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 登陆数
    Long clientConnectCount;
    // 时间戳
    Long ts;
}
