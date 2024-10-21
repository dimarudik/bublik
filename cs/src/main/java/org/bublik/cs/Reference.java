package org.bublik.cs;

public class Reference {
    public static void main(String[] args) {
        A a = new A();
        System.out.println(a.hashCode());
        A a1 = getA(a);
        System.out.println(a1.hashCode());
        int c = 1;
        int b = 2;
        int e = Math.max(c, b);

    }

    public static A getA(A a) {
        return a;
    }
}

class A {
    private int a;

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }
}
