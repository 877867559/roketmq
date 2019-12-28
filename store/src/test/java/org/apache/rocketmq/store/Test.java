package org.apache.rocketmq.store;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Test {

    public static void main(String[] args) {
        TimeUnit timeUnit = TimeUnit.HOURS;
        long deadline = System.currentTimeMillis() + timeUnit.toMillis(3);
        System.out.println(""+deadline);
        System.out.println(new Date(deadline).toString());
    }
}
