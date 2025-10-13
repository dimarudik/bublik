package org.bublik.core.ioc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldAnnotatedDefaultConstructorTest {
    @Test
    public void FieldAnnotatedDefaultConstructor() {
        BeanFactory beanFactory = new BeanFactory();
        beanFactory.register(FieldAnnotatedDefaultConstructor.class);
        FieldAnnotatedDefaultConstructor fieldAnnotatedDefaultConstructor = beanFactory.getBean(FieldAnnotatedDefaultConstructor.class);
        fieldAnnotatedDefaultConstructor.call();
        assertEquals(2, beanFactory.getBeanContainer().size());
    }
}

@BublikBean
class FieldAnnotatedDefaultConstructor {
    @BublikInject
    private InnerBean innerBean;
    public FieldAnnotatedDefaultConstructor(){}
    public void call() {
        System.out.println("MainBean calling call");
        innerBean.doWork();
    }
}
