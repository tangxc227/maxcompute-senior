package com.tangxc.maxcompute.udf;

import com.aliyun.odps.udf.UDF;
import com.tangxc.maxcompute.util.MD5Util;

import java.text.ParseException;

/**
 * @author Xicheng.Tang
 */
public class MD5Enc extends UDF {

    /**
     * 功能描述: <br>
     * 返回指定数据的Base64加密值
     *
     * @param inputval
     * @throws ParseException
     */
    public String  evaluate(String inputval) throws ParseException {
        return MD5Util.MD5(inputval) ;
    }

}
