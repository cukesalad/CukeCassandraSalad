package org.cukesalad.cssndra.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class DynamicCQLQueryTest {

  @Test
  public void testGetCqlQuery() throws IOException {
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    cqlQuery.setCqlFileName("deleteUser.cql");

    // test without param
    String expectedfileContent = "delete from user where userid ='$userid';";
    assertEquals(expectedfileContent, cqlQuery.getCqlQuery());
    
    // test with param
    expectedfileContent = "delete from user where userid ='userid1';";
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("userid", "userid1");
    cqlQuery.setParameterMap(parameterMap);
    assertEquals(expectedfileContent, cqlQuery.getCqlQuery());
    
  }

  @Test
  public void testGetInputStreamFromFile() throws IOException {
    DynamicCQLQuery cqlQuery = new DynamicCQLQuery();
    InputStream stream = cqlQuery.getInputStreamFromFile(CassandraSaladConstants.CQL_DIR + "deleteUser.cql");
    StringWriter writer = new StringWriter();
    IOUtils.copy(stream, writer);
    String actualfileContent = writer.toString();
    String expectedfileContent = "delete from user where userid ='$userid';";
    assertEquals(expectedfileContent, actualfileContent);
  }

}
