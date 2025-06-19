package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/7/10 16:12
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AndroidSettingBean {
    String stt;

    String edt;

    String platform;

    String version;

    Long successCount;

    Long totalCount;

    Double successRate;

    Long ts;
}
