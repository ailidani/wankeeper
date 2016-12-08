package org.wankeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public abstract class Configuration {

    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

    private long nextCheckTime = 0;
    private long lastLoadTime = 0;
    private Properties properties = new Properties();
    private final File configFile;

    protected Configuration(String filename) {
        this.configFile = new File(filename);
    }

    public String getProperty(String propertyName, String defaultValue) {
        String result = getProperty(propertyName);
        if (result == null) {
            LOG.trace("Missing property {} in {}", propertyName, properties.keySet());
            return defaultValue;
        }
        return result;
    }

    public String getRequiredProperty(String propertyName) {
        String result = getProperty(propertyName);
        if (result == null) {
            throw new RuntimeException("Missing property " + propertyName);
        }
        return result;
    }

    public boolean getFlag(String property, boolean defaultValue) {
        return Boolean.parseBoolean(getProperty(property, String.valueOf(defaultValue)));
    }

    public int getIntProperty(String propertyName, int defaultValue) {
        return Integer.parseInt(getProperty(propertyName, String.valueOf(defaultValue)));
    }

    public Properties getProperties() {
        ensureConfigurationIsFresh();
        return properties;
    }

    private String getProperty(String propertyName) {
        if (System.getProperty(propertyName) != null) {
            LOG.trace("Reading {} from system properties", propertyName);
            return System.getProperty(propertyName);
        }
        if (System.getenv(propertyName.replace('.', '_')) != null) {
            LOG.trace("Reading {} from environment", propertyName);
            return System.getenv(propertyName.replace('.', '_'));
        }

        ensureConfigurationIsFresh();
        return properties.getProperty(propertyName);
    }

    private synchronized void ensureConfigurationIsFresh() {
        if (System.currentTimeMillis() < nextCheckTime) return;
        nextCheckTime = System.currentTimeMillis() + 10000;
        LOG.trace("Rechecking {}", configFile);

        if (!configFile.exists()) {
            LOG.error("Missing configuration file {}", configFile);
        }

        if (lastLoadTime >= configFile.lastModified()) return;
        lastLoadTime = configFile.lastModified();
        LOG.debug("Reloading {}", configFile);

        try (FileInputStream inputStream = new FileInputStream(configFile)) {
            properties.clear();
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + configFile, e);
        }
    }

}
