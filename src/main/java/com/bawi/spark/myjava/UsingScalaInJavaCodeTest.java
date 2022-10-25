package com.bawi.spark.myjava;

import com.bawi.spark.myscala.*;
import scala.collection.Seq;

public class UsingScalaInJavaCodeTest {
    public static void main(String[] args) {
        MyScalaCaseClass myScalaCaseClass = new MyScalaCaseClass(12);
        myScalaCaseClass.age();
        System.out.println(myScalaCaseClass);
        MyScalaClass myScalaClass = new MyScalaClass();
        Seq<String> sequence = myScalaClass.getSequence(1);
        System.out.println(myScalaClass.myField());
        //MyScalaObject myScalaObject = new MyScalaObject();
        System.out.println(MyScalaObject.myObjectMethod(1));
        System.out.println(MyScalaObject$.MODULE$.myObjectMethod(2));
        
        SpCtx spCtx = new SpCtx();
//        spCtx.myimplicits().mySqlMethod();
    }
}
