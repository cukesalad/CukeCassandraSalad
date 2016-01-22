# CukeCassandraSalad
##Who can use this?

This module is intended to help in testing any application that has Cassandra interactions. It gives you an intuitive way of applying changed to Cassandra and validating data in Cassandra in cucumber steps. 

##Pre Requisites

JDK8/JDK7

##Getting Started

Lets say you want to test an application with Cassandra interaction as a black box.

Create the project you want to use for testing. This will host all you feature files.
Add the below dependancy similar to - SampleCukeDBTest :
```gradle
  compile('org.cukesalad:CukeCassandraSalad:1.0.0')
```
Create a Cassandra connection details file - "cassandrasalad.properties" with below details:
```properties
cassandra.contactpoints=localhost
cassandra.port=9142
cassandra.keyspace=cassandra_unit_keyspace
cassandra.pwd.env=N
cassandra.ssl.env=N
cassandra.username=testuser
cassandra.protocol=1
cassandra.ssl.enabled=false
```
Create feature files inside your project under src/main/resources/feature
Run the below commands for linux/mac:
```shell
> cd <git project root>
> sh gradlew clean build
> unzip build/distributions/<your project name>-1.0.0.zip -d build/distributions/
> sh build/distributions/<your project name>-1.0.0/bin/<your project name> org.cukesalad.cssndra.runner.Runner
```
Sample feature file:
```gherkin
Feature: A feature to demonstrate Cassandra cucumber util to setup/teardown/validate data in Cassandra

  Scenario: testing setup/teardown/validate data in Cassandra
  
  Given I set up data in cassandra using "insertUser.cql"
  And I teardown data in cassandra using "deleteUser.cql"
  Given I set up data in cassandra using "insertUser.cql" and below parameters:
  | key    | value |
  | userid | 1     |
  And I teardown data in cassandra using "deleteUser.cql" and below parameters:
  | key    | value |
  | userid | 1     |
  Given I set up data in cassandra using "insertUser.cql", and rollback test data using "deleteUser.cql" at the end of test case
  Given I set up data in cassandra using "insertUser.cql", and rollback test data at the end using "deleteUser.cql" with below  parameters:
  | key    | value |
  | userid | 1     |
  Given I set up data using the cql file "insertUser.cql", for the below data:
  | userid | fname    | lname     | email              |
  | 1      | Ned      | Stark     | ned@gmail.com      |
  | 2      | Tyrion   | Lannister | tyrion@yahoo.com   |
  | 3      | Daenerys | Targeryan | daenerys@gmail.com |
  Then the result of the cql "selectUser.cql", is:
  | userid | fname    | lname     | email              |
  | 1      | Ned      | Stark     | ned@gmail.com      |
  | 2      | Tyrion   | Lannister | tyrion@yahoo.com   |
  | 3      | Daenerys | Targeryan | daenerys@gmail.com |

```
## What if i have different DB instances/schemas for different environments?
Add files like ```cassandrasalad.dev.properties``` and pass a run time jvm arg like ```-Denv=dev```. The Cassandra details of ```cassandrasalad.dev.properties``` will override ```cassandrasalad.properties``` which is the default

##Latest release:

Release 1.0.0

##How to contribute

These are just a few steps I could think of. If there are any other feature that you wish for, please go ahead and create the same in the [issue tracker](https://github.com/cukesalad/CukeCassandraSalad/issues). I will make best efforts to add them ASAP. If you wish contribute by coding, please fork the repository and raise a pull request.
