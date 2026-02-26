package dev.bublik.core.ioc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OnlyBeanTest {
    @Test
    public void constructorAnnotatedField() {
        BeanFactory beanFactory = new BeanFactory();
        beanFactory.register(OnlyBean.class);
        OnlyBean onlyBean = beanFactory.getBean(OnlyBean.class);
        onlyBean.call();
        assertEquals(1, beanFactory.getBeanContainer().size());
    }
}

@BublikBean
class OnlyBean {
    private InnerBean innerBean;
    public void call() {
        System.out.println("MainBean calling call");
    }
}
