package org.cukesalad.cssndra.support;

import static org.cukesalad.cssndra.support.CassandraSaladContext.cssndracluster;
import static org.cukesalad.cssndra.support.CassandraSaladContext.cssndraenv;
import static org.cukesalad.cssndra.support.CassandraSaladContext.cssndraprop;
import static org.cukesalad.cssndra.support.CassandraSaladContext.cssndrasession;
import static org.cukesalad.cssndra.support.CassandraSaladContext.cssndratearDownFiles;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PlainTextAuthProvider;
//import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ProtocolVersion;

import cucumber.api.java.After;
import cucumber.api.java.Before;

public class CassandraSaladHook {
  static final Logger LOG = LoggerFactory.getLogger(CassandraSaladHook.class);
  
  public static void refresh() {
    cssndratearDownFiles = new ArrayList<>();
    cssndraprop = new Properties();
    cssndraenv = System.getProperty(CassandraSaladConstants.ENV);
  }

  @Before()
  public void beforeHook() {
    loadProperties();
    setupCassandraConnection();
  }

  public static void setupCassandraConnection() {
    String[] cassandraNodes = cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_CONTACTPOINTS).split(",");
    Integer port = Integer.valueOf(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_PORT));
    //ProtocolVersion version = ProtocolVersion.valueOf(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_PROTOCOL));
    
    Builder builder = Cluster.builder().addContactPoints(cassandraNodes).withPort(port);
    
    if(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_USERNAME)!=null && cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_PWD)!=null){
      builder.withAuthProvider(new PlainTextAuthProvider(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_USERNAME), cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_PWD)));
    }
    if(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_PROTOCOL)!=null){
      builder.withProtocolVersion(ProtocolVersion.fromInt(Integer.valueOf(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_PROTOCOL))));
    }
    if(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_SSL_ENABLED)!=null && Boolean.valueOf(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_SSL_ENABLED))){
      builder.withSSL();
    }
    
    cssndracluster = Cluster.builder().addContactPoints(cassandraNodes).withPort(port)
        .withAuthProvider(new PlainTextAuthProvider(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_USERNAME), cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_PWD)))
        .build();
    cssndrasession = cssndracluster.connect(cssndraprop.getProperty(CassandraSaladConstants.CASSANDRA_KEYSPACE));
  }

  @After
  public void afterHook() throws IOException {
    tearDownCassandraData();
    closeCassandraConnection();
    refresh();
  }

  public static void tearDownCassandraData() throws IOException {
    for (DynamicCQLQuery tearDownFile : cssndratearDownFiles) {
      String batchQuery = tearDownFile.getCqlQuery();
      cssndrasession.execute(batchQuery);
    }
  }

  public static void closeCassandraConnection() {
    cssndrasession.close();
    cssndracluster.close();
  }

  public static void loadProperties() {
    try {
      LOG.debug("loading cassandra salad Properties");
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      InputStream stream = loader.getResourceAsStream("cassandrasalad.properties");
      cssndraprop.load(stream);
      if (cssndraenv != null) {
        stream = loader.getResourceAsStream("cassandrasalad.{env}.properties".replace("{env}", cssndraenv));
        if (stream != null) {
          cssndraprop.load(stream);
        }
      }
      cssndraprop.putAll(System.getProperties());
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
