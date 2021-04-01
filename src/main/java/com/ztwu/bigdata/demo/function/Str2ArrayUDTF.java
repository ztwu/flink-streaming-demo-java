package com.ztwu.bigdata.demo.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * create function str2Arr as 'demo.function.Str2ArrayUDTF';
 */
@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class Str2ArrayUDTF extends TableFunction<Row> {

    public void eval(String s) {
        String[] results = s.split(",");
        for (String result : results) {
            collect(Row.of(result));
        }
    }
}