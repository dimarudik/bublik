package org.bublik.cs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MyReflection {
    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, IllegalAccessException, InstantiationException {
        Class<?> obj = Class.forName("java.lang.String");
        System.out.println(obj.getName());
        System.out.println(obj.isAssignableFrom(Object.class));

        String s = (String) obj.getDeclaredConstructor(obj).newInstance("sss");
        Method method = s.getClass().getMethod("length");
        int l = (int) method.invoke(s);
        System.out.println(l);
/*
        X obj = new X();
        Method method = obj.getClass().getMethod("test", null);
        method.invoke(obj, null);
*/
    }
}

/*
class X {
    public void test(){
        System.out.println("method call");
    }
}

interface Cyber<E, D, K, X, Y> {
    E encrypt(K k, X x);
    D decrypt(K k, Y y);
}
*/
