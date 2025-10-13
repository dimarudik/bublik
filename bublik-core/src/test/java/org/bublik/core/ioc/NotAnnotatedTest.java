package org.bublik.core.ioc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NotAnnotatedTest {
    @Test
    public void notAnnotated() {
        BeanFactory beanFactory = new BeanFactory();
        beanFactory.register(NotAnnotated.class);
        Assertions.assertThrows(NullPointerException.class, () -> {
            beanFactory.getBean(NotAnnotated.class);
        });
    }
}

class NotAnnotated {
    @BublikInject
    private InnerBean innerBean;
    public void call() {
        System.out.println("MainBean calling call");
        innerBean.doWork();
    }
}

