package org.apache.dolphinscheduler.plugin.datasource.api.utils;

import java.io.Closeable;

public class ThreadContextClassLoader
        implements Closeable {
    private final ClassLoader threadContextClassLoader;

    public ThreadContextClassLoader(ClassLoader newThreadContextClassLoader) {
        this.threadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
    }

    @Override
    public void close() {
        Thread.currentThread().setContextClassLoader(threadContextClassLoader);
    }
}
