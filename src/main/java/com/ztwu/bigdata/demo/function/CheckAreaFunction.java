package com.ztwu.bigdata.demo.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 检测是否存在于指定区域
 */
public class CheckAreaFunction extends ScalarFunction {
    public boolean eval(float lon, float lat, String area) {
        // return District.isInArea(lon, lat, area);
        return false;
    }
}