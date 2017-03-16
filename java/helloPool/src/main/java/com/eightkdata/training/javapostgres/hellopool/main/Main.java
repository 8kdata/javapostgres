/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellopool.main;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.MetricRegistry;
import com.eightkdata.training.javapostgres.hellopool.dao.CountriesLanguageDAO;
import com.vladmihalcea.flexypool.FlexyPoolDataSource;
import com.vladmihalcea.flexypool.adaptor.HikariCPPoolAdapter;
import com.vladmihalcea.flexypool.common.ConfigurationProperties;
import com.vladmihalcea.flexypool.config.Configuration;
import com.vladmihalcea.flexypool.metric.Metrics;
import com.vladmihalcea.flexypool.metric.codahale.CodahaleMetrics;
import com.vladmihalcea.flexypool.strategy.IncrementPoolOnTimeoutConnectionAcquiringStrategy;
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

  // HikariCP parameters
  private static final int MAXIMUM_POOL_SIZE = 8;
  private static final int MINIMUM_IDLE_SIZE = 0;
  private static final long IDLE_TIMEOUT_MILLIS = MINUTES.toMillis(5);
  private static final long CONNECTION_TIMEOUT_MILLIS = 250;

  // FlexyPool parameters
  private static final int MAX_OVERFLOW_POOL_SIZE = 16;
  private static final int MAX_RETRY_ATTEMPS = 10;
  
  // Threads Pool parameters
  private static final int POOL_THREADS_NUMBER = 64;
  private static final int LOOP_NUMBER = 160_000;
  
  public static void main(String[] args) {

    MetricRegistry metric = new MetricRegistry();
    HikariDataSource embeddableDataSource = createPooledDataSource(metric);
    FlexyPoolDataSource<HikariDataSource> dataSource = wrapDataSource(embeddableDataSource, metric);
    dataSource.start();

    printMetrics(metric);

    ExecutorService pool = Executors.newFixedThreadPool(POOL_THREADS_NUMBER);
    List<CompletableFuture<?>> futures = new ArrayList<>();

    IntStream.range(0, LOOP_NUMBER).forEach(fe -> {
      futures.add(CompletableFuture.runAsync(() -> {
        execQuery(dataSource);
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
    
    // Set Pool config
    hkconfig.setMaximumPoolSize(MAXIMUM_POOL_SIZE);
    hkconfig.setMinimumIdle(MINIMUM_IDLE_SIZE);
    hkconfig.setIdleTimeout(IDLE_TIMEOUT_MILLIS);
    hkconfig.setConnectionTimeout(CONNECTION_TIMEOUT_MILLIS);

    return new HikariDataSource(hkconfig);

  }

  /**
   * Create Flexypool dataSource
   * 
   * @param dataSource
   * @return
   */
  @SuppressWarnings("unchecked")
  private static FlexyPoolDataSource<HikariDataSource> wrapDataSource(HikariDataSource dataSource,
      MetricRegistry metric) {

    Configuration<HikariDataSource> hikariConfiguration =
        createPooledObservableDataSourceConfiguration(dataSource, metric);

    return new FlexyPoolDataSource<HikariDataSource>(hikariConfiguration,
        new IncrementPoolOnTimeoutConnectionAcquiringStrategy.Factory<HikariDataSource>(MAX_OVERFLOW_POOL_SIZE),
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
        HikariCPPoolAdapter.FACTORY)
        .setMetricsFactory(c -> createMetrics(c, metric))
        .build();
  }
  
  private static Metrics createMetrics(ConfigurationProperties<?, ?, ?> c, MetricRegistry metric) {
    return new CodahaleMetrics(c, metric, (metricClass, metricName) -> new ExponentiallyDecayingReservoir());
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
    consoleReporter.start(1, TimeUnit.SECONDS);
  }


  private static void execQuery(DataSource ds) {
    try (Connection connection = getConnection(ds)) {
      // Executing the query and throwing result since we are just generating traffic to see how pool behave
      CountriesLanguageDAO countriesLanguageDAO = new CountriesLanguageDAO(connection);
      countriesLanguageDAO.getCountriesLanguages(0);
      
    } catch (SQLException e) {
      System.err.println("Error connecting to, querying or disconnecting from the database");
      e.printStackTrace();
    }
  }

}
