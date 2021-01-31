package com.canal.canal_clinet;

/**
 * @author 大数据老哥
 * @version V1.0
 * @Package com.canal.canal_clinet
 * @File ：Entrance.java
 * @date 2021/1/31 14:35
 * 程序入库
 */
public class Entrance {
    public static void main(String[] args) {
        CanalClient canalClient = new CanalClient();
        canalClient.start();
    }
}
