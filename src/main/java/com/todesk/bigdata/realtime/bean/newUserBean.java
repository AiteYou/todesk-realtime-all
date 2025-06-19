package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/11/23 19:02
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class newUserBean {
    String stt;

    String edt;

    String platform;

    Long count;

    Long ts;
}
