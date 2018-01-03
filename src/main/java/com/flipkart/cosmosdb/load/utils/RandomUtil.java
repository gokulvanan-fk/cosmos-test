package com.flipkart.cosmosdb.load.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;


public enum RandomUtil {
    INSTANCE;

    private final char USABLE_SET[] = new char[] { '0', '1', '2', '3', '4',
            '5', '6', '7', '8', '9' };
    private static Random random;
    private static String host= "";

    static {
        try {
            random = new Random(InetAddress.getLocalHost().getHostAddress()
                    .hashCode());
            //uncomment if running from multiple host
            host = InetAddress.getLocalHost().getHostAddress().replace('.', '0');
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public String randomKey() {
        String val = RandomStringUtils.random(4, 0, 9, false, true,
                USABLE_SET, random);
        return new StringBuilder("OD").append(val).append(host)
                .append(System.nanoTime())
                .append(Thread.currentThread().getId()).toString();
    }
}
