package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/5/24 16:54
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BackendServBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 服务器名称
    String serv;
    // 发起连接数
    Long newConnectCount;
    // 握手成功数
    Long handshakeSuccessCount;
    // 握手成功率
    Double handshakeSuccessRate;
    // 时间戳
    Long ts;
}
