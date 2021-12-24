package org.apache.dolphinscheduler.plugin.datasource.api.utils;

public class ThreadContextClassLoader {
    private ClassLoader threadContextClassLoader;

    public ThreadContextClassLoader(ClassLoader newThreadContextClassLoader) {
        this.threadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
    }

    public void close() {
        Thread.currentThread().setContextClassLoader(threadContextClassLoader);
    }
}
