package org.wankeeper.server;

import org.wankeeper.token.LeaseType;

import java.util.Properties;

public class Config extends Configuration {

    private static Config instance = new Config();

    private Config() {
        super(System.getProperty("config", "wankeeper.properties"));
    }

    public static Config instance() {
        return instance;
    }

    public static Properties get() {
        return instance.getProperties();
    }

    public int getClusterID() {
        return getIntProperty("cid", 0);
    }

    public boolean isMaster() {
        return getFlag("master", false);
    }

    public String getMasterAddress() {
        return getProperty("master.address", null);
    }

    public int getMasterPort() { return getIntProperty("master.port", 1925); }

    public boolean ssl() { return getFlag("ssl", false); }

    public LeaseType getLeaseType() {
        LeaseType type;
        switch (getIntProperty("leasetype", 2)) {
            case 0:
                type = LeaseType.PERCENTAGE;
                break;
            case 2:
                type = LeaseType.TWO_CONSECUTIVE;
                break;
            case 3:
                type = LeaseType.THREE_CONSECUTIVE;
                break;
            case 4:
                type = LeaseType.FOUR_CONSECUTIVE;
                break;
            case 5:
                type = LeaseType.FIVE_CONSECUTIVE;
                break;
            default:
                type = LeaseType.TWO_CONSECUTIVE;
                break;
        }
        return type;
    }

    public int getTickTime() {
        return getIntProperty("WanTickTime", 5000);
    }

    // Used by worker sync remote request
    public boolean isSync() {
        return getFlag("sync", false);
    }

    public boolean isRWToken() {
        return getFlag("rwtoken", false);
    }

}
