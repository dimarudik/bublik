package dev.bublik.core.ioc;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class BeanFactory {

    private final Map<Class<?>, Object> container = new HashMap<>();

    public synchronized void register(Class<?> type) {
        if (type.isAnnotationPresent(BublikBean.class)) {

            for (Constructor<?> constructor : type.getConstructors()) {
                if (constructor.isAnnotationPresent(BublikInject.class)) {
                    Class<?>[] constructorParameterTypes = constructor.getParameterTypes();
                    for (Class<?> aClass : constructorParameterTypes) {
                        Object bean = container.get(aClass);
                        if (bean == null) {
                            Object tempBean = create(aClass);
                            container.put(aClass, tempBean);
                        }
                    }
                }
            }

            Object mainBean = create(type);
            container.put(type, mainBean);

            for (Field field : type.getDeclaredFields()) {
                if (field.isAnnotationPresent(BublikInject.class)) {
                    Object bean = container.get(field.getType());
                    if (bean == null) {
                        Object tempBean = create(field.getType());
                        container.put(field.getType(), tempBean);
                    }
                    bean = container.get(field.getType());
                    setField(field, mainBean, bean);
                }
            }

        }
    }

    public synchronized <T> T getBean(Class<T> type) {
        Object bean = container.get(type);
        if (bean == null) throw new NullPointerException();
        try {
            return (T) bean;
        } catch (ClassCastException e) {
            throw new RuntimeException(e);
        }
    }

    private Object create(Class<?> type) {
        try {
            Object instance = null;
            for (Constructor<?> constructor: type.getConstructors()) {
                if (constructor.isAnnotationPresent(BublikInject.class)) {
                    Class<?>[] constructorParameterTypes = constructor.getParameterTypes();
                    Object[] parameters = new Object[constructorParameterTypes.length];
                    for (int i = 0; i < constructorParameterTypes.length; i++) {
                        parameters[i] = container.get(constructorParameterTypes[i]);
                    }
                    instance = constructor.newInstance(parameters);
                    break;
                }
            }
            if (instance == null) {
                Constructor<?> constructor = type.getDeclaredConstructor();
                constructor.setAccessible(true);
                instance = constructor.newInstance();
            }
            return instance;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private void setField(Field field, Object mainBean, Object bean) {
        try {
            field.setAccessible(true);
            field.set(mainBean, bean);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void printBeans() {
        container.forEach((key, value) -> System.out.println(key.getName() + " : " + value));
    }

    public synchronized Map<Class<?>, Object> getBeanContainer() {
        return container;
    }
}
