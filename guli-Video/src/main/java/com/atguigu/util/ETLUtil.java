package com.atguigu.util;

import org.junit.Test;

public class ETLUtil {

    public static String etlStr(String oriStr){
        StringBuffer sb = new StringBuffer();
        //分割字段
        String[] fields = oriStr.split("\t");
        //过滤字段小于九的数据
        if (fields.length < 9) return null;
        //去掉类别字段中的空格
        fields[3] = fields[3].replaceAll(" ", "");
        //修改相关视频ID字段的分隔符，由‘\t’替换为'&'
        for (int i = 0; i < fields.length; i++) {
            if (i < 9) {
                if (i == fields.length - 1 ) {
                    sb.append(fields[i]);
                }else {
                    sb.append(fields[i]).append("\t");
                }
            }else {
                if (i == fields.length - 1 ) {
                    sb.append(fields[i]);
                }else {
                    sb.append(fields[i]).append("&");
                }
            }
        }
        return sb.toString();
    }
    @Test
    public void test(){
        System.out.println(ETLUtil.etlStr("RX24KLBhwMI\tlemonette\t697\tPeople & Blogs\t512\t24149\t4.22\t315\t474\tt60tW0WevkE\tWZgoejVDZlo\tXa_op4MhSkg\tMwynZ8qTwXA\tsfG2rtAkAcg\tj72VLPwzd_c\t24Qfs69Al3U\tEGWutOjVx4M\tKVkseZR5coU\tR6OaRcsfnY4\tdGM3k_4cNhE\tai-cSq6APLQ\t73M0y-iD9WE\t3uKOSjE79YA\t9BBu5N0iFBg\t7f9zwx52xgA\tncEV0tSC7xM\tH-J8Kbx9o68\ts8xf4QX1UvA\t2cKd9ERh5-8"));
    }
}
