package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2024-03-13 20:01
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BackendHandSuccessBean {
    String stt;
    String edt;
    String serv;
    String suClient;
    String duClient;
    String onServer;
    Long newConnectCount;
    Long handshakeSuccessCount;
    Double handshakeSuccessRate;
    Long ts;
}
