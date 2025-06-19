package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2023/8/21 17:48
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DorisAlertBean {
    String dt;

    String stt;

    String edt;

    String flag;

    Long cnt;

    Long ts;
}
