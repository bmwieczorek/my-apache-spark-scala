package com.bawi.spark.myjava;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

public class TestVector {
    public static void main(String[] args) {
//        int times = 3;
//        int minInc = 10;
//        int maxExc = 13;
//        int size = times * (maxExc - minInc);
//        int[] array = new int[size];
//        for (int idx = 0, i = 0; i < times; i++) {
//            for (int j = minInc; j < maxExc; j++) {
//                array[idx] = j;
//                idx++;
//            }
//        }
//        System.out.println(array);
        int timesOuter = 4;
        int timesInner = 5;
        int minInc = 10;
        int maxExc = 13;
        int size = timesOuter * timesInner * (maxExc - minInc);
        int[] array = new int[size];
        for (int idx = 0, i = 0; i < timesOuter; i++) {
            for (int j = minInc; j < maxExc; j++) {
                for (int k = 0; k < timesInner; k++) {
                    array[idx++] = j;
                }
            }
        }
        //System.out.println(Arrays.stream(array).boxed().collect(Collectors.toList()));

        timesOuter = 4;
        timesInner = (2 + 1) * (3 + 1);
        int[] hour = new int[timesOuter * timesInner];
        for (int idx = 0, i = 0; i < timesOuter; i++) {
            for (int j = 0; j < timesInner; j++) {
                hour[idx++] = i;
            }
        }
        System.out.println(Arrays.stream(hour).boxed().collect(Collectors.toList()));

    }
}
