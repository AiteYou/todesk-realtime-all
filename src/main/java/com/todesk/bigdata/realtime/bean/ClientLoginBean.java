package com.todesk.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author : Liuchao
 * Date : 2022/6/16 18:25
 * Desc :
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientLoginBean {

    String stt;

    String edt;

    String loginType;

    Long loginFailedCount;

    Long loginTotalCount;

    Long ts;
}
