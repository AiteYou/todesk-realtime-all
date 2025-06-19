package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/6/15 20:55
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BackendClientBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    String suClient;
    String duClient;
    // 发起连接数
    Long newConnectCount;
    // 握手成功数
    Long handshakeSuccessCount;
    // 握手成功率
    Double handshakeSuccessRate;
    // 时间戳
    Long ts;
}
