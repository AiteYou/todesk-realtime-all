package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/7/10 17:21
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AndroidLoginBean {
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
