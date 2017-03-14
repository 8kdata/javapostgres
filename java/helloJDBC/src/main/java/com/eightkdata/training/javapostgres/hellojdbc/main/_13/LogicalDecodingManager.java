/*
 * BSD 3-Clause License
 * 
 * Copyright (c) 2017, Dave Cramer
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * * Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * Adapted from https://github.com/davecramer/LogicalDecode
 * 
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._13;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class LogicalDecodingManager implements AutoCloseable {

  private final String slotName;
  private final String outputPlugin;
  private final Connection connection;
  private final Connection replicationConnection;
  private final PGReplicationStream stream;

  @FunctionalInterface
  public interface ConnectionProvider {
    public Connection getConnection(Optional<Properties> properties) throws SQLException;

    default public Connection getConnection() throws SQLException {
      return getConnection(Optional.empty());
    }
    
    default public Connection getConnection(Properties properties) throws SQLException {
      return getConnection(Optional.of(properties));
    }
  }

  public LogicalDecodingManager(String slotName, String outputPlugin,
      ConnectionProvider connectionProvider) throws InterruptedException, SQLException, TimeoutException {
    super();
    this.slotName = slotName;
    this.outputPlugin = outputPlugin;
    this.connection = connectionProvider.getConnection();

    this.replicationConnection = createReplicationConnection(connectionProvider);

    LogSequenceNumber lsn = createSlot(slotName, outputPlugin);
    this.stream = createReplicationStream(lsn);
  }

  private Connection createReplicationConnection(ConnectionProvider connectionProvider)
      throws SQLException {
    Properties properties = new Properties();
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
    PGProperty.REPLICATION.set(properties, "database");
    PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
    return connectionProvider.getConnection(properties);
  }

  private LogSequenceNumber createSlot(String slotName, String outputPlugin)
      throws SQLException, InterruptedException, TimeoutException {
    // drop previous slot
    dropReplicationSlot();

    LogSequenceNumber lsn;
    try (PreparedStatement preparedStatement = connection.prepareStatement(

        "SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {

      preparedStatement.setString(1, slotName);
      preparedStatement.setString(2, outputPlugin);
      try (ResultSet rs = preparedStatement.executeQuery()) {
        if (!rs.next()) {
          throw new IllegalStateException("Can not read slot LSN");
        }
        lsn = LogSequenceNumber.valueOf(rs.getString(2));
      }
    }
    
    return lsn;
  }

  private PGReplicationStream createReplicationStream(LogSequenceNumber lsn)
      throws SQLException, InterruptedException, TimeoutException {
    PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);
    
    return pgConnection.getReplicationAPI()
        .replicationStream()
        .logical()
        .withSlotName(slotName)
        .withStartPosition(lsn)
        .withStatusInterval(20, TimeUnit.SECONDS).start();
  }

  public Connection getConnection() {
    return connection;
  }

  public String getSlotName() {
    return slotName;
  }

  public String getOutputPlugin() {
    return outputPlugin;
  }

  private void dropReplicationSlot()
      throws SQLException, InterruptedException, TimeoutException {
    try (PreparedStatement preparedStatement = connection
        .prepareStatement("select pg_terminate_backend(active_pid) from pg_replication_slots "
            + "where active = true and slot_name = ?")) {
      preparedStatement.setString(1, slotName);
      preparedStatement.execute();
    }

    waitStopReplicationSlot();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement("select pg_drop_replication_slot(slot_name) "
            + "from pg_replication_slots where slot_name = ?")) {
      preparedStatement.setString(1, slotName);
      preparedStatement.execute();
    }
  }

  public LogSequenceNumber getCurrentLSN() throws SQLException {
    try (Statement st = connection.createStatement()) {
      try (ResultSet rs = st.executeQuery(
          "select " + (((BaseConnection) connection).haveMinimumServerVersion(ServerVersion.v10)
              ? "pg_current_wal_location()" : "pg_current_xlog_location()"))) {

        if (rs.next()) {
          String lsn = rs.getString(1);
          return LogSequenceNumber.valueOf(lsn);
        } else {
          return LogSequenceNumber.INVALID_LSN;
        }
      }
    }
  }

  public boolean isReplicationSlotActive()
      throws SQLException {

    try (PreparedStatement preparedStatement = connection
        .prepareStatement("select active from pg_replication_slots where slot_name = ?")) {
      preparedStatement.setString(1, slotName);
      try (ResultSet rs = preparedStatement.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  private void waitStopReplicationSlot()
      throws InterruptedException, TimeoutException, SQLException {
    long startWaitTime = System.currentTimeMillis();
    boolean stillActive;
    long timeInWait = 0;

    do {
      stillActive = isReplicationSlotActive();
      if (stillActive) {
        TimeUnit.MILLISECONDS.sleep(100L);
        timeInWait = System.currentTimeMillis() - startWaitTime;
      }
    } while (stillActive && timeInWait <= 30000);

    if (stillActive) {
      throw new TimeoutException("Wait stop replication slot " + timeInWait + " timeout occurs");
    }
  }

  public void receiveNextChangesTo(LogSequenceNumber lsn, OutputStream out) throws SQLException, InterruptedException, IOException {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    
    try (
      // Open a channel to write ByteBuffer to and close it automagically
      WritableByteChannel channel = Channels.newChannel(outBuffer);
    ) { 
      ByteBuffer buffer;
      boolean pending = true;
      while (true) {
        buffer = stream.readPending();
        if (buffer == null) {
          TimeUnit.MILLISECONDS.sleep(10L);
          
          if (pending) {
            continue;
          }
          
          break;
        } else {
          pending = false;
        }
        
        channel.write(buffer);
        channel.write(ByteBuffer.wrap("\n".getBytes()));
        
        // feedback
        stream.setAppliedLSN(stream.getLastReceiveLSN());
        stream.setFlushedLSN(stream.getLastReceiveLSN());
      }
    }
    
    out.write(outBuffer.toByteArray());
  }

  @Override
  public void close() throws SQLException, InterruptedException, TimeoutException {
    dropReplicationSlot();
    
    connection.close();
    replicationConnection.close();
  }
}
