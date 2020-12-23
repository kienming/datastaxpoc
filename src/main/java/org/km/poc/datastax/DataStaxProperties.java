package org.km.poc.datastax;

import java.util.Properties;

public class DataStaxProperties {
    private String secureConnectBundlePath;
    private String username;
    private String password;
    private String keyspaceName;
    private int insertCount;

    public DataStaxProperties(Properties properties) {
        this.secureConnectBundlePath = properties.getProperty("datastax.secure.connect.bundle.path");
        this.username = properties.getProperty("datastax.username");
        this.password = properties.getProperty("datastax.password");
        this.keyspaceName = properties.getProperty("datastax.keyspace.name");

        String insertCountString = properties.getProperty("datastax.insert.count", "1");

        try {
            this.insertCount = Integer.parseInt(insertCountString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Insert count should be string", e);
        }

        if(this.secureConnectBundlePath == null || this.secureConnectBundlePath.isEmpty()) {
            throw new IllegalArgumentException("Properties [datastax.secure.connect.bundle.path] is empty");
        }

        if(this.username == null || this.username.isEmpty()) {
            throw new IllegalArgumentException("Properties [datastax.username] is empty");
        }

        if(this.password == null || this.password.isEmpty()) {
            throw new IllegalArgumentException("Properties [datastax.password] is empty");
        }

        if(this.keyspaceName == null || this.keyspaceName.isEmpty()) {
            throw new IllegalArgumentException("Properties [datastax.keyspace.name] is empty");
        }
    }

    public String getSecureConnectBundlePath() {
        return secureConnectBundlePath;
    }

    public void setSecureConnectBundlePath(String secureConnectBundlePath) {
        this.secureConnectBundlePath = secureConnectBundlePath;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getInsertCount() {
        return insertCount;
    }

    public void setInsertCount(int insertCount) {
        this.insertCount = insertCount;
    }
}
