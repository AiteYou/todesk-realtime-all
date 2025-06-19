package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/8/11 15:31
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AndroidForwardBean {
    String stt;

    String edt;

    String platform;

    String version;

    String type;

    Long successCount;

    Long totalCount;

    Double successRate;

    Long ts;
}
