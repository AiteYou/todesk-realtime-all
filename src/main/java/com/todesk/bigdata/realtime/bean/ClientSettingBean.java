package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/6/26 17:42
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientSettingBean {
    String stt;

    String edt;

    String action;

    Long actionCount;

    Long ts;
}
