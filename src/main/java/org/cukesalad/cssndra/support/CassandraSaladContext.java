package org.cukesalad.cssndra.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraSaladContext {
  public static List<DynamicCQLQuery> cssndratearDownFiles = new ArrayList<>();
  public static Session               cssndrasession;
  public static Cluster               cssndracluster;
  public static Properties            cssndraprop          = new Properties();
  public static String                cssndraenv = System.getProperty(CassandraSaladConstants.ENV);
}
