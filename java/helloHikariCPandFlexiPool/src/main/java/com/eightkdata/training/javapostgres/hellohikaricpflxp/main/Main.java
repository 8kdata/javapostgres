/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellohikaricpflxp.main;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.eightkdata.training.javapostgres.hellohikaricpflxp.dao.CountriesLanguageDAO;
import com.eightkdata.training.javapostgres.hellohikaricpflxp.model.CountriesLanguage;
import com.vladmihalcea.flexypool.FlexyPoolDataSource;
import com.vladmihalcea.flexypool.adaptor.HikariCPPoolAdapter;
import com.vladmihalcea.flexypool.config.Configuration;
import com.vladmihalcea.flexypool.strategy.RetryConnectionAcquiringStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import javax.sql.DataSource;


public class Main {

  private static final int POOL_THREADS_NUMBER = 1000;
  private static final int THREADS_NUMBER = 16000;
  private static HikariDataSource embeddableDataSource;
  private static FlexyPoolDataSource<HikariDataSource> dataSource;
  private static MetricRegistry metric;

  public static void main(String[] args) {

    metric = new MetricRegistry();
    embeddableDataSource = createPooledDataSource(metric);
    dataSource = wrapDataSource(embeddableDataSource, metric);
    dataSource.start();

    printMetrics(metric);

    ExecutorService pool = Executors.newFixedThreadPool(POOL_THREADS_NUMBER);
    List<CompletableFuture<?>> futures = new ArrayList<>();

    IntStream.range(0, THREADS_NUMBER).forEach(fe -> {
      futures.add(CompletableFuture.runAsync(() -> {
        execQuery();
      }, pool));
    });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])).join();
    pool.shutdown();

    dataSource.stop();
    embeddableDataSource.close();
  }


  /**
   * Create the DataSource for Hikari
   * 
   * @param metric
   * @return
   */
  private static HikariDataSource createPooledDataSource(MetricRegistry metric) {

    HikariConfig hkconfig = new HikariConfig("/db.hikari.properties");

    //In order to use JMX, you must set true the RegisterMBeans
    hkconfig.setRegisterMbeans(true);
    // Set PoolName for Metrics
    hkconfig.setPoolName("javaPgPool");
    hkconfig.setMetricRegistry(metric);

    return new HikariDataSource(hkconfig);

  }

  /**
   * Create Flexypool dataSource
   * 
   * @param dataSource
   * @return
   */
  private static FlexyPoolDataSource<HikariDataSource> wrapDataSource(HikariDataSource dataSource,
      MetricRegistry metric) {

    final int MAX_RETRY_ATTEMPS = 5;

    Configuration<HikariDataSource> hikariConfiguration =
        createPooledObservableDataSourceConfiguration(dataSource, metric);

    return new FlexyPoolDataSource<HikariDataSource>(hikariConfiguration,
        new RetryConnectionAcquiringStrategy.Factory<HikariDataSource>(MAX_RETRY_ATTEMPS));

  }

  /**
   * Configuration for FlexiPool
   * 
   * @param poolingDataSource
   * @return
   */
  private static Configuration<HikariDataSource> createPooledObservableDataSourceConfiguration(
      HikariDataSource poolingDataSource, MetricRegistry metric) {
    return new Configuration.Builder<>(poolingDataSource.getPoolName(), poolingDataSource,
        HikariCPPoolAdapter.FACTORY).build();
  }

  /**
   * Get a valid Connection from DataSource
   * 
   * @param ds
   * @return
   * @throws SQLException
   */
  private static Connection getConnection(DataSource ds) throws SQLException {

    try {
      Connection conn = ds.getConnection();
      if (!conn.isValid(500)) {
        throw new RuntimeException("DB connection is not valid");
      }
      conn.setAutoCommit(false);

      return conn;
    } catch (SQLException ex) {
      throw new SQLException(ex);
    }
  }

  /**
   * Print metrics every 10 seconds
   * 
   * @param metric
   */
  private static void printMetrics(MetricRegistry metric) {
    ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metric)
        .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
    consoleReporter.start(10, TimeUnit.SECONDS);
  }


  private static void execQuery() {
    try (Connection connection = getConnection(dataSource)) {

      CountriesLanguageDAO countriesLanguageDAO = new CountriesLanguageDAO(connection);
      List<CountriesLanguage> countriesLanguages = countriesLanguageDAO.getCountriesLanguages(0);

      if (countriesLanguages != null) {
        // Print query results
        countriesLanguages.forEach(r -> System.out
            .println(r.getCountries() + "," + r.getLanguage() + "," + r.getAveragePercentage()));
      }

    } catch (SQLException e) {
      System.err.println("Error connecting to, querying or disconnecting from the database");
      e.printStackTrace();
    }
  }

}
