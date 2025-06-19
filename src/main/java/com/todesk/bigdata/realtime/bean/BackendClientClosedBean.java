package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/6/28 12:31
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BackendClientClosedBean {
    String stt;

    String edt;

    Long clientClosedCount;

    Long ts;
}
