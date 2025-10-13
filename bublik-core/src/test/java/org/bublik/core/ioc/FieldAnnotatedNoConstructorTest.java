package org.bublik.core.ioc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldAnnotatedNoConstructorTest {
    @Test
    public void FieldAnnotatedNoConstructor() {
        BeanFactory beanFactory = new BeanFactory();
        beanFactory.register(FieldAnnotatedNoConstructor.class);
        FieldAnnotatedNoConstructor fieldAnnotatedNoConstructor = beanFactory.getBean(FieldAnnotatedNoConstructor.class);
        fieldAnnotatedNoConstructor.call();
        assertEquals(2, beanFactory.getBeanContainer().size());
    }
}

@BublikBean
class FieldAnnotatedNoConstructor {
    @BublikInject
    private InnerBean innerBean;
    public void call() {
        System.out.println("MainBean calling call");
        innerBean.doWork();
    }
}
