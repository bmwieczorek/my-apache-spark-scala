package myhierarchy;

public class MyAbstractClassImpl extends MyAbstractClass {
    @Override
    public Number doInGet() {
        System.out.println("MyImpl: in doInGet");
        return null;
    }
}
