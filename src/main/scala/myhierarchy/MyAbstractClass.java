package myhierarchy;

public abstract class MyAbstractClass implements MyIface {

    public abstract Number doInGet();

    @Override
    public Integer getNumericValue() throws IllegalArgumentException { // OK
        System.out.println("MyAbstractClass: in getNumericValue");
        doInGet();
        return null;
    }

    /*@Override
    public Long getNumericValue() { // OK (neither RuntimeException nor Exception does not need to included in overriding method signature)
        return null;
    }*/

    /*@Override
    public Number getNumericValue() throws Exception {  //Error:(7, 20) java: getNumericValue() in myhierarchy.MyAbstractClass cannot implement getNumericValue() in myhierarchy.MyIface overridden method does not throw java.lang.Exception
        return null;
    }*/

    /*@Override
    public Object getNumericValue() throws RuntimeException {  //Error:(17, 19) java: getNumericValue() in myhierarchy.MyAbstractClass cannot implement getNumericValue() in myhierarchy.MyIface         return type java.lang.Object is not compatible with java.lang.Number
        return null;
    }*/

}
