package org.bublik.core.ioc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldAndConstructorAnnotatedTest {
    @Test
    public void fieldAndConstructorAnnotated() {
        BeanFactory beanFactory = new BeanFactory();
        beanFactory.register(FieldAndConstructorAnnotated.class);
        FieldAndConstructorAnnotated fieldAndConstructorAnnotated = beanFactory.getBean(FieldAndConstructorAnnotated.class);
        fieldAndConstructorAnnotated.call();
        assertEquals(2, beanFactory.getBeanContainer().size());
    }
}

@BublikBean
class FieldAndConstructorAnnotated {
    @BublikInject
    private final InnerBean innerBean;
    @BublikInject
    public FieldAndConstructorAnnotated(InnerBean innerBean){
        this.innerBean = innerBean;
    }
    public void call() {
        System.out.println("MainBean calling call");
        innerBean.doWork();
    }
}

