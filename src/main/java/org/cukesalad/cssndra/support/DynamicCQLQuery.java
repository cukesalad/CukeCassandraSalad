package org.cukesalad.cssndra.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.cukesalad.cssndra.support.CassandraSaladConstants;

public class DynamicCQLQuery {

  private String              cqlFileName;
  private Map<String, String> parameterMap;

  public DynamicCQLQuery() {
  }

  public DynamicCQLQuery(String cqlFileName, Map<String, String> parameterMap) {
    super();
    this.cqlFileName = cqlFileName;
    this.parameterMap = parameterMap;
  }

  public String getCqlFileName() {
    return cqlFileName;
  }

  public void setCqlFileName(String cqlFileName) {
    this.cqlFileName = cqlFileName;
  }

  public Map<String, String> getParameterMap() {
    return parameterMap;
  }

  public void setParameterMap(Map<String, String> parameterMap) {
    this.parameterMap = parameterMap;
  }


  public String getCqlQuery() throws IOException {
    InputStream stream = getInputStreamFromFile(CassandraSaladConstants.CQL_DIR + cqlFileName);
    StringWriter writer = new StringWriter();
    IOUtils.copy(stream, writer);
    String rawCql = writer.toString();
    String batchQuery = manipulateBatchQuery(rawCql, parameterMap);
    return batchQuery;
  }
  public static InputStream getInputStreamFromFile(String fileName){
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream stream = loader.getResourceAsStream(fileName);
    return stream;
  }
  private static String manipulateBatchQuery(String rawCql, Map<String, String> parameterMap) {
    String batchQuery = new String(rawCql);
    if (parameterMap != null) {
      for (Entry<String, String> entry : parameterMap.entrySet()) {
        batchQuery = batchQuery.replaceAll(CassandraSaladConstants.PARAM_PREFIX+entry.getKey(), entry.getValue());
      }
    }
    return batchQuery;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((cqlFileName == null) ? 0 : cqlFileName.hashCode());
    result = prime * result + ((parameterMap == null) ? 0 : parameterMap.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DynamicCQLQuery other = (DynamicCQLQuery) obj;
    if (cqlFileName == null) {
      if (other.cqlFileName != null)
        return false;
    } else if (!cqlFileName.equals(other.cqlFileName))
      return false;
    if (parameterMap == null) {
      if (other.parameterMap != null)
        return false;
    } else if (!parameterMap.equals(other.parameterMap))
      return false;
    return true;
  }
  
}
