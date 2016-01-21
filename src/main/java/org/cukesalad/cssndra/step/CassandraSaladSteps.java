package org.cukesalad.cssndra.step;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cukesalad.cssndra.support.CassandraSaladContext;
import org.cukesalad.cssndra.support.CassandraSaladConstants;
import org.cukesalad.cssndra.support.DynamicCQLQuery;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import cucumber.api.DataTable;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

public class CassandraSaladSteps {

  @When("^I set up data in cassandra using \"([^\"]*)\", and rollback test data using \"([^\"]*)\" at the end of test case$")
  public void i_set_up_data_in_cassandra_using_and_rollback_test_data_using_at_the_end_of_test_case(String setupFile, String tearDownFile)
      throws Throwable {
    // first clean dirty data
    i_teardown_data_using(tearDownFile);
    // then setup data
    i_set_up_data_in_cassandra_using(setupFile);
    collectTearDownFiles(tearDownFile,null);
  }
  public static void collectTearDownFiles(String tearDownFile, Map<String, String> parameterMap) {
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
    dynamicCQLQuery.setCqlFileName(tearDownFile);
    dynamicCQLQuery.setParameterMap(parameterMap);
    CassandraSaladContext.cssndratearDownFiles.add(dynamicCQLQuery);
  }

  @When("^I set up data in cassandra using \"([^\"]*)\"$")
  public void i_set_up_data_in_cassandra_using(String setupFile) throws Throwable {
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
    dynamicCQLQuery.setCqlFileName(setupFile);
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
  }

  @When("^I teardown data using \"([^\"]*)\"$")
  public void i_teardown_data_using(String tearDownFile) throws Throwable {
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
    dynamicCQLQuery.setCqlFileName(tearDownFile);
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
  }

  @Given("^I teardown data in cassandra using \"([^\"]*)\" and below parameters:$")
  public void i_teardown_data_in_cassandra_using_and_below_parameters(String cqlFileName, DataTable parameters) throws Throwable {
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
    dynamicCQLQuery.setCqlFileName(cqlFileName);
    Map<String, String> parameterMap = createParamMap(parameters);
    dynamicCQLQuery.setParameterMap(parameterMap);
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
  }
  @Given("^I set up data in cassandra using \"([^\"]*)\" and below parameters:$")
  public void i_set_up_data_in_cassandra_using_and_below_parameters(String cqlFileName, DataTable parameters) throws Throwable {
    DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
    dynamicCQLQuery.setCqlFileName(cqlFileName);
    Map<String, String> parameterMap = createParamMap(parameters);
    dynamicCQLQuery.setParameterMap(parameterMap);
    CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
  }

 
  @Given("^I set up data in cassandra using \"([^\"]*)\", which will rollback at the end using \"([^\"]*)\" and below parameters:$")
  public void i_setup_up_data_in_cassandra_using_which_will_rollback_at_the_end_using_and_below_parameters(String setupFileName, String tearDownFileName, DataTable parameters) throws Throwable {
 // first clean dirty data
    i_teardown_data_in_cassandra_using_and_below_parameters(tearDownFileName, parameters);
    // then setup data
    i_set_up_data_in_cassandra_using_and_below_parameters(setupFileName, parameters);
    collectTearDownFiles(tearDownFileName, createParamMap(parameters));
  }
  
  @Given("^I set up data using the cql file \"([^\"]*)\", for the below data:$")
  public void i_set_up_data_using_the_cql_file_for_the_below_data(String setupFileName, DataTable parameters) throws Throwable {
    List<Map<String, String>> paramMapList = parameters.asMaps(String.class, String.class);
    for (Map<String, String> paramMap : paramMapList) {
      DynamicCQLQuery dynamicCQLQuery = new DynamicCQLQuery();
      dynamicCQLQuery.setCqlFileName(setupFileName);
      dynamicCQLQuery.setParameterMap(paramMap);
      CassandraSaladContext.cssndrasession.execute(dynamicCQLQuery.getCqlQuery());
    }
  }
  
  @Then("^the result of the cql \"([^\"]*)\", is:$")
  public void the_result_of_the_cql_is(String setupFileName, DataTable expectedResults) throws Throwable {
    List<Map<String, String>> expectedResultsMaps = expectedResults.asMaps(String.class, String.class);
    DynamicCQLQuery dynamicSQLQuery = new DynamicCQLQuery();
    dynamicSQLQuery.setCqlFileName(setupFileName);
    
    ResultSet result = CassandraSaladContext.cssndrasession.execute(dynamicSQLQuery.getCqlQuery());
    
    List<Row> rows = result.all();
    List<String> columnNames = expectedResults.topCells();
    List<Map<String, String>> actualResultsMaps = new ArrayList<Map<String,String>>();
    for (Row row : rows) {
      Map<String, String> actualRow = new HashMap<String, String>();
      for (String columnName : columnNames) {
        actualRow.put(columnName, row.getString(columnName));
      }
      actualResultsMaps.add(actualRow);
    }
    assertTrue(actualResultsMaps.containsAll(expectedResultsMaps));
  }

  public Map<String, String> createParamMap(DataTable parameters) {
    Map<String, String> parameterMap = new HashMap<String, String>(parameters.asMap(String.class, String.class));
    parameterMap.remove(CassandraSaladConstants.PARAM_MAP_KEY);
    return parameterMap;
  }
}
