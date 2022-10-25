package myhierarchy;

public class AllCaller {
    public static void main(String[] args) {
        MyIface myIface = new MyIfaceImpl();
        myIface.getNumericValue();
        MyAbstractClass myAbstractClass = new MyAbstractClassImpl();
        myAbstractClass.getNumericValue();
    }
}
