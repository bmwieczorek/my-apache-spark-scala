package com.bawi.spark;

class Super {
    String superA = getSuperA();
    static String superStaticA = getSuperStaticA();

    Super() {
        System.out.println("Super");
    }
    String getSuperA() {
        System.out.println("superA");
        return "superA";
    }
    static String getSuperStaticA() {
        System.out.println("superStaticA");
        return "superStaticA";
    }
}

public class Sub extends Super {
    String subA = getSubA();
    static String subStaticA = getSubStaticA();

    Sub() {
        System.out.println("Sub");
    }
    public static void main(String[] args) {
        new Sub();
    }
    String getSubA() {
        System.out.println("subA");
        return "subA";
    }
    static String getSubStaticA() {
        System.out.println("subStaticA");
        return "subStaticA";
    }
}
