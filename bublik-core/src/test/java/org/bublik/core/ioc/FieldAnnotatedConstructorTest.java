package org.bublik.core.ioc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldAnnotatedConstructorTest {
    @Test
    public void FieldAnnotatedConstructor() {
        BeanFactory beanFactory = new BeanFactory();
        beanFactory.register(FieldAnnotatedWithConstructor.class);
        FieldAnnotatedWithConstructor fieldAnnotatedConstructor = beanFactory.getBean(FieldAnnotatedWithConstructor.class);
        fieldAnnotatedConstructor.call();
        assertEquals(2, beanFactory.getBeanContainer().size());
    }
}

@BublikBean
class FieldAnnotatedWithConstructor {
    @BublikInject
    private InnerBean innerBean;
    public FieldAnnotatedWithConstructor(){}
    public FieldAnnotatedWithConstructor(InnerBean innerBean) {
        this.innerBean = innerBean;
    }
    public void call() {
        System.out.println("MainBean calling call");
        innerBean.doWork();
    }
}

