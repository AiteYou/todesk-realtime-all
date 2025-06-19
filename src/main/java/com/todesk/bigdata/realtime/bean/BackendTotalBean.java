package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/5/9 19:41
 * Desc : 连通率汇总bean
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BackendTotalBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 发起连接数
    Long newConnectCount;
    // 握手成功数
    Long handshakeSuccessCount;
    // 握手成功率
    Double handshakeSuccessRate;
    // 时间戳
    Long ts;
}
