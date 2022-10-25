package myhierarchy;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

public class RandomCallerApp {

    public static void main(String[] args) {
        Supplier<MyIface> myIfaceSupplier = get();
        System.out.println("supplier created, value lazy");
        myIfaceSupplier.get().getNumericValue();
    }

    private static Supplier<MyIface> get() {
        List<MyIface> myIfaces = Arrays.asList(new MyIfaceImpl(), new MyAbstractClassImpl());
        int i = new Random().nextInt(myIfaces.size());
        return () -> myIfaces.get(i);
    }
}
