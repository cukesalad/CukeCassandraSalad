package org.cukesalad.cssndra.step;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.xml.ClassPathXmlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.cukesalad.cssndra.support.CassandraSaladContext;
import org.cukesalad.cssndra.support.CassandraSaladHook;
import org.cukesalad.cssndra.support.DynamicCQLQuery;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import cucumber.api.DataTable;

public class CassandraSaladStepsTest {

  CassandraSaladSteps cassandraSaladSteps = new CassandraSaladSteps();
  static CassandraSaladHook cassandraSaladHook = new CassandraSaladHook();
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9171");
    dataLoader.load(new ClassPathXmlDataSet("cassandra_unit_keyspace.xml"));
    cassandraSaladHook.beforeHook();
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("createTable.cql");
    CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }
  @Before
  public void setUp() throws Exception {
    cassandraSaladHook.beforeHook();
  }
  @After
  public void tearDown() throws Exception {
    cassandraSaladHook.afterHook();
  }
  

  @Test
  public void testI_set_up_data_in_cassandra_using_and_rollback_test_data_using_at_the_end_of_test_case() throws Throwable {
    cassandraSaladSteps.i_set_up_data_in_cassandra_using_and_rollback_test_data_using_at_the_end_of_test_case("insertUser.cql", "deleteUser.cql");
    
    // select the set up data and assert results
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("selectUser.cql");
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    List<Row> rows = resultSet.all();
    assertTrue("Expecting 1 result, but some result returned",rows.size()==1);
    assertEquals("$userid", rows.get(0).getString("userid"));
    
    // tear down that data and test if the teardown worked
    CassandraSaladHook.tearDownCassandraData();
    resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    rows = resultSet.all();
    assertTrue("Expecting no result, but some result returned",rows.isEmpty());
  }

  @Test
  public void testCollectTearDownFiles() {
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("$userid", "user1");
    CassandraSaladSteps.collectTearDownFiles("deleteUser.cql", parameterMap);
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery("deleteUser.cql", parameterMap);
    assertEquals(dynamicCQLQuery, CassandraSaladContext.cssndratearDownFiles.get(0));
  }

  @Test
  public void testI_set_up_data_in_cassandra_using() throws Throwable {
    cassandraSaladSteps.i_set_up_data_in_cassandra_using("insertUser.cql");
    
    // select the set up data and assert results
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("selectUser.cql");
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    List<Row> rows = resultSet.all();
    assertTrue("Expecting 1 result, but some result returned",rows.size()==1);
    assertEquals("$userid", rows.get(0).getString("userid"));
    
    // clean the test data
    cqlQuery.setCqlFileName("deleteUser.cql");
    CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
  }

  @Test
  public void testI_teardown_data_using() throws Throwable {
    cassandraSaladSteps.i_set_up_data_in_cassandra_using("insertUser.cql");
    
    // select the set up data and assert results
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("selectUser.cql");
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    List<Row> rows = resultSet.all();
    assertTrue("Expecting no result, but some result returned",rows.size()==1);
    assertEquals("$userid", rows.get(0).getString("userid"));
    
    // tear down that data and test if ther teardown worked
    cassandraSaladSteps.i_teardown_data_in_cassandra_using("deleteUser.cql");
    resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    rows = resultSet.all();
    assertTrue("Expecting no result, but some result returned",rows.isEmpty());
  }

  @Test
  public void testI_teardown_data_in_cassandra_using_and_below_parameters() throws Throwable {
    List<List<String>> raw = new ArrayList<List<String>>();
    raw.add(Arrays.asList("key","value"));
    raw.add(Arrays.asList("userid","userid1"));
    raw.add(Arrays.asList("fname","fname1"));
    raw.add(Arrays.asList("lname","lname1"));
    raw.add(Arrays.asList("email","email1"));
    DataTable parameters = DataTable.create(raw );
    cassandraSaladSteps.i_set_up_data_in_cassandra_using_and_below_parameters("insertUser.cql", parameters);
    
    // select the set up data and assert results
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("selectUser.cql");
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("userid", "userid1");
    cqlQuery.setParameterMap(parameterMap );
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    List<Row> rows = resultSet.all();
    assertTrue("Expecting no result, but some result returned",rows.size()==1);
    assertEquals("userid1", rows.get(0).getString("userid"));
    
    // tear down that data and test if ther teardown worked
    cassandraSaladSteps.i_teardown_data_in_cassandra_using_and_below_parameters("deleteUser.cql", parameters);
    resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    rows = resultSet.all();
    assertTrue("Expecting no result, but some result returned",rows.isEmpty());
  }

  @Test
  public void testI_set_up_data_in_cassandra_using_and_below_parameters() throws Throwable {
    List<List<String>> raw = new ArrayList<List<String>>();
    raw.add(Arrays.asList("key","value"));
    raw.add(Arrays.asList("userid","userid1"));
    raw.add(Arrays.asList("fname","fname1"));
    raw.add(Arrays.asList("lname","lname1"));
    raw.add(Arrays.asList("email","email1"));
    DataTable parameters = DataTable.create(raw );
    cassandraSaladSteps.i_set_up_data_in_cassandra_using_and_below_parameters("insertUser.cql", parameters);
    
    // select the set up data and assert results
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("selectUser.cql");
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("userid", "userid1");
    cqlQuery.setParameterMap(parameterMap );
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    List<Row> rows = resultSet.all();
    assertTrue("Expecting no result, but some result returned",rows.size()==1);
    assertEquals("userid1", rows.get(0).getString("userid"));
    
    // clean the test data
    cqlQuery.setCqlFileName("deleteUser.cql");
    CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
  }

  @Test
  public void testI_setup_up_data_in_cassandra_using_and_rollback_test_data_at_the_end_using_with_below_parameters() throws Throwable {
    List<List<String>> raw = new ArrayList<List<String>>();
    raw.add(Arrays.asList("key","value"));
    raw.add(Arrays.asList("userid","userid1"));
    raw.add(Arrays.asList("fname","fname1"));
    raw.add(Arrays.asList("lname","lname1"));
    raw.add(Arrays.asList("email","email1"));
    DataTable parameters = DataTable.create(raw );
    cassandraSaladSteps.i_setup_up_data_in_cassandra_using_and_rollback_test_data_at_the_end_using_with_below_parameters("insertUser.cql", "deleteUser.cql", parameters);
    
    // select the set up data and assert results
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("selectUser.cql");
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("userid", "userid1");
    cqlQuery.setParameterMap(parameterMap );
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    List<Row> rows = resultSet.all();
    assertTrue("Expecting 1 result, but some result returned",rows.size()==1);
    assertEquals("userid1", rows.get(0).getString("userid"));
    
    // tear down that data and test if the teardown worked
    CassandraSaladHook.tearDownCassandraData();
    resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    rows = resultSet.all();
    assertTrue("Expecting no result, but some result returned",rows.isEmpty());
  }

  @Test
  public void testI_set_up_data_using_the_cql_file_for_the_below_data() throws Throwable {
    List<List<String>> raw = new ArrayList<List<String>>();
    raw.add(Arrays.asList("userid","fname","lname","email"));
    raw.add(Arrays.asList("userid1","fname1","lname1","email1"));
    DataTable parameters = DataTable.create(raw );
    cassandraSaladSteps.i_set_up_data_using_the_cql_file_for_the_below_data("insertUser.cql", parameters);
    
    // select the set up data and assert results
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("selectUser.cql");
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("userid", "userid1");
    cqlQuery.setParameterMap(parameterMap );
    ResultSet resultSet = CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
    List<Row> rows = resultSet.all();
    assertTrue("Expecting 1 result, but some result returned",rows.size()==1);
    assertEquals("userid1", rows.get(0).getString("userid"));
    
    // clean the test data
    cqlQuery.setCqlFileName("deleteUser.cql");
    CassandraSaladContext.cssndrasession.execute(cqlQuery.getCqlQuery());
  }

  @Test
  public void testThe_result_of_the_cql_is() throws Throwable {
    // setup a row of data
    cassandraSaladSteps.i_set_up_data_in_cassandra_using("insertUser.cql");
    
    // assert using the method
    List<List<String>> raw = new ArrayList<List<String>>();
    raw.add(Arrays.asList("userid","fname","lname","email","some"));
    raw.add(Arrays.asList("$userid","$fname","$lname","$email","1"));
    DataTable expectedResults = DataTable.create(raw );
    cassandraSaladSteps.the_result_of_the_cql_is("selectUser.cql", expectedResults);
  }

  @Test
  public void testThe_result_of_the_cql_has_rows() throws Throwable {
    // setup a row of data
    cassandraSaladSteps.i_set_up_data_in_cassandra_using("insertUser.cql");
    
    // assert using the method
    cassandraSaladSteps.the_result_of_the_cql_has_rows("selectUser.cql",1);
  }
  
  @Test
  public void testThe_result_of_the_cql_is_empty() throws Throwable {
    // remove the row of data
    cassandraSaladSteps.i_set_up_data_in_cassandra_using("deleteUser.cql");
    
    // assert using the method
    cassandraSaladSteps.the_result_of_the_cql_is_empty("selectUser.cql");
  }

  @Test
  public void testCreateParamMap() {
    List<List<String>> raw = new ArrayList<List<String>>();
    raw.add(Arrays.asList("key","value"));
    raw.add(Arrays.asList("id1","1"));
    raw.add(Arrays.asList("id2","2"));
    raw.add(Arrays.asList("id3","3"));
    DataTable parameters = DataTable.create(raw );
    assertEquals(cassandraSaladSteps.createParamMap(parameters).size(), 3);
    assertTrue(!cassandraSaladSteps.createParamMap(parameters).containsKey("key"));
  }

}
