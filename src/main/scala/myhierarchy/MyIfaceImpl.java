package myhierarchy;

public class MyIfaceImpl implements MyIface {
    @Override
    public Number getNumericValue() throws RuntimeException {
        System.out.println("MyIfaceImpl: in getNumericValue");
        return null;
    }
}
