package org.bublik.core.ioc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConstructorAnnotatedOnlyTest {
    @Test
    public void constructorAnnotatedOnly() {
        BeanFactory beanFactory = new BeanFactory();
        beanFactory.register(ConstructorAnnotatedOnly.class);
        ConstructorAnnotatedOnly constructorAnnotatedOnly = beanFactory.getBean(ConstructorAnnotatedOnly.class);
        constructorAnnotatedOnly.call();
        assertEquals(1, beanFactory.getBeanContainer().size());

    }
}

@BublikBean
class ConstructorAnnotatedOnly {
    @BublikInject
    public ConstructorAnnotatedOnly(){
    }
    public void call() {
        System.out.println("MainBean calling call");
    }
}