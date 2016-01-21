package org.cukesalad.cssndra.support;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.xml.ClassPathXmlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;

public class CassandraHookTest {

  // @Rule
  // public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new
  // ClassPathCQLDataSet("cqls/createTable.cql","cassandra_unit_keyspace"));

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9171");
    dataLoader.load(new ClassPathXmlDataSet("cassandra_unit_keyspace.xml"));
  }

  @After
  public void tearDown() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void testRefresh() {
    CassandraSaladHook.refresh();
    assertTrue(CassandraSaladContext.cssndratearDownFiles.isEmpty());
    assertTrue(CassandraSaladContext.cssndraprop.isEmpty());
  }

  @Test
  public void testBeforeHook() {
    CassandraSaladHook.refresh();

    CassandraSaladHook hook = new CassandraSaladHook();
    hook.beforeHook();
    // check if the connection is made and the props are loaded
    assertTrue(CassandraSaladContext.cssndracluster.connect() != null);
    assertTrue(CassandraSaladContext.cssndrasession != null);
    assertTrue(!CassandraSaladContext.cssndraprop.isEmpty());

    CassandraSaladHook.refresh();
  }

  @Test
  public void testSetupCassandraConnection() {
    // as pre condition refresh and load properties
    CassandraSaladHook.refresh();
    CassandraSaladHook.loadProperties();
    // make connection
    CassandraSaladHook.setupCassandraConnection();
    // assert connection
    assertTrue(CassandraSaladContext.cssndracluster.connect() != null);
    assertTrue(CassandraSaladContext.cssndrasession != null);
    CassandraSaladHook.refresh();
  }

  @Test
  public void testAfterHook() throws IOException {
    // refresh and setup connection as a pre condition
    CassandraSaladHook.refresh();
    CassandraSaladHook hook = new CassandraSaladHook();
    hook.beforeHook();
    // setup table to insert and delete data
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
    dynamicCQLQuery.setCqlFileName("createTable.cql");
    // insert data to delete at teardown
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
    dynamicCQLQuery.setCqlFileName("insertUser.cql");
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("userid", "userid1");
    parameterMap.put("fname", "fname1");
    parameterMap.put("lname", "lname1");
    parameterMap.put("email", "email1");
    dynamicCQLQuery.setParameterMap(parameterMap);
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());

    //add delete script as part of teardown list
    dynamicCQLQuery.setCqlFileName("deleteUser.cql");
    CassandraSaladContext.cssndratearDownFiles.add(dynamicCQLQuery);

    // teardown using the list of tear down files
    hook.afterHook();
    
    // assert the connections are closed
    assertTrue(CassandraSaladContext.cssndracluster.isClosed());
    assertTrue(CassandraSaladContext.cssndrasession.isClosed());
    // select the deleted data to verify
    CassandraSaladHook.loadProperties();
    CassandraSaladHook.setupCassandraConnection();
    dynamicCQLQuery.setCqlFileName("selectUser.cql");
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
    assertTrue(resultSet.all().isEmpty());
    CassandraSaladHook.closeCassandraConnection();
  }

  @Test
  public void testTearDownCassandraData() throws IOException {
    // refresh and setup connection as a pre condition
    CassandraSaladHook.refresh();
    CassandraSaladHook hook = new CassandraSaladHook();
    hook.beforeHook();
    // setup table to insert and delete data
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
    dynamicCQLQuery.setCqlFileName("createTable.cql");
    // insert data to delete at teardown
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
    dynamicCQLQuery.setCqlFileName("insertUser.cql");
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("userid", "userid1");
    parameterMap.put("fname", "fname1");
    parameterMap.put("lname", "lname1");
    parameterMap.put("email", "email1");
    dynamicCQLQuery.setParameterMap(parameterMap);
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());

    //add delete script as part of teardown list
    dynamicCQLQuery.setCqlFileName("deleteUser.cql");
    CassandraSaladContext.cssndratearDownFiles.add(dynamicCQLQuery);

    // teardown using the list of tear down files
    CassandraSaladHook.tearDownCassandraData();
    
    // select the deleted data to verify
    dynamicCQLQuery.setCqlFileName("selectUser.cql");
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
    assertTrue(resultSet.all().isEmpty());
  }

  @Test
  public void testCloseCassandraConnection() {
    // as pre condition refresh and load properties
    CassandraSaladHook.refresh();
    CassandraSaladHook.loadProperties();
    // setup connection
    CassandraSaladHook.setupCassandraConnection();
    assertTrue(CassandraSaladContext.cssndracluster.connect() != null);
    assertTrue(CassandraSaladContext.cssndrasession != null);
    CassandraSaladHook.closeCassandraConnection();
    assertTrue(CassandraSaladContext.cssndracluster.isClosed());
    assertTrue(CassandraSaladContext.cssndrasession.isClosed());
    CassandraSaladHook.refresh();
  }

  @Test
  public void testLoadProperties() {
    CassandraSaladHook.loadProperties();
    assertTrue(!CassandraSaladContext.cssndraprop.isEmpty());
    assertTrue(
        CassandraSaladContext.cssndraprop.get(CassandraSaladConstants.CASSANDRA_CONTACTPOINTS).equals("localhost"));

    CassandraSaladHook.refresh();
    CassandraSaladContext.cssndraenv = "dev";
    CassandraSaladHook.loadProperties();
    assertTrue(!CassandraSaladContext.cssndraprop.isEmpty());
    assertTrue(
        CassandraSaladContext.cssndraprop.get(CassandraSaladConstants.CASSANDRA_CONTACTPOINTS).equals("localhost-dev"));

    CassandraSaladHook.refresh();
    System.setProperty(CassandraSaladConstants.CASSANDRA_CONTACTPOINTS, "localhost-sys");
    CassandraSaladHook.loadProperties();
    assertTrue(!CassandraSaladContext.cssndraprop.isEmpty());
    assertTrue(
        CassandraSaladContext.cssndraprop.get(CassandraSaladConstants.CASSANDRA_CONTACTPOINTS).equals("localhost-sys"));

    CassandraSaladHook.refresh();
    System.clearProperty(CassandraSaladConstants.CASSANDRA_CONTACTPOINTS);
  }

}
