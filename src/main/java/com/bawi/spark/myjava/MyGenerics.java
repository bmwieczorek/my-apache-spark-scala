package com.bawi.spark.myjava;

import java.util.Arrays;
import java.util.List;

public class MyGenerics {
    static class SuperClass {}
    static class SubClass extends SuperClass {}

    public static void main(String[] args) {
// get1:
        List<SuperClass> superClasses = get1(Arrays.asList(new SuperClass()));

        // Error: java: incompatible types: List<MyGenerics.SuperClass> cannot be converted to List<MyGenerics.SubClass>
        //List<SubClass> subClasses = get1(Arrays.asList(new SubClass()));

// get2 (none compiles):
        // Error: java: incompatible types: List<capture#1 of ? extends MyGenerics.SuperClass> cannot be converted to List<MyGenerics.SuperClass>
        //List<SuperClass> superClasses2 = get2(Arrays.asList(new SuperClass()));

        // Error: java: incompatible types: List<capture#1 of ? extends MyGenerics.SuperClass> cannot be converted to List<MyGenerics.SubClass>
        //List<SubClass> subClasses2 = get2(Arrays.asList(new SubClass()));

// get3: (all compiles):
        List<SuperClass> superClasses3 = get3(Arrays.asList(new SuperClass()));
        List<SubClass> subClasses3 = get3(Arrays.asList(new SubClass()));
        List<SuperClass> subClasses3_1 = get3(Arrays.asList(new SubClass()));
        List<SuperClass> subClasses3_2 = get3(Arrays.asList(new SubClass(), new SuperClass()));
    }

    static List<SuperClass> get1(List<SuperClass> input) { return null; }

    static List<? extends SuperClass> get2(List<? extends SuperClass> input) { return null; }

    static <T extends SuperClass> List<T> get3(List<T> input) { return null; }
}
