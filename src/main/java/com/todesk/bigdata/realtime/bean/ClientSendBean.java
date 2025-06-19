package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/6/26 18:03
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientSendBean {
    String stt;

    String edt;

    String sendType;

    Long sendTotalCount;

    Long sendFailedCount;

    Long ts;
}
