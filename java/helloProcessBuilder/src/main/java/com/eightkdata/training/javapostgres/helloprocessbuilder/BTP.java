/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.helloprocessbuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.ProcessBuilder.Redirect;
import java.util.Date;

/**
 * This class provides a trivial example of how to use ProcessBuilder to launch a new process, external to the JVM,
 * which runs a PostgreSQL client program (such as psql) and feeds commands to that program via stdin.
 *
 * This example is kept intentionally simple, having some hardcoded variables.
 *
 * This example is related to the
 * <a href="http://www.slideshare.net/nosys/billion-tables-project-nycpug-2013">Billion Tables Project</a>,
 * aiming to create 1B+ tables in a single postgres database.
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class BTP {

    private static final String PSQL = "/usr/bin/psql";
    private static final String CONN_INFO = "dbname=world host=localhost port=5432 user=worlduser password=HahodOid3";
    private static final int N_TABLES = 10 * 1000;

    public static void main(String[] args) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(PSQL, "-e", CONN_INFO)
              .redirectOutput(Redirect.INHERIT)
              .redirectError(Redirect.INHERIT);
        Date start = new Date();
        Process process = processBuilder.start();

        try(
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()))
        ) {
          // Remove tables and schema if exists
          writer.write("BEGIN;");
          for(int i = 0; i < N_TABLES; i++) {
            writer.write("DROP TABLE IF EXISTS btp.t_" + String.format("%09d", i) + ";");
            if (i % 1000 == 0) {
              // prevent overflowing max_locks_per_transaction
              writer.write("COMMIT;");
              writer.write("BEGIN;");
            }
          }
          writer.write("COMMIT;");
          writer.write("DROP SCHEMA IF EXISTS btp;");
          
          // Create schema and tables
          writer.write("CREATE SCHEMA btp;");
          writer.write("BEGIN;");
          for(int i = 0; i < N_TABLES; i++) {
            writer.write("CREATE TABLE btp.t_" + String.format("%09d", i) + " (i integer);");
            if (i % 1000 == 0) {
              // prevent overflowing max_locks_per_transaction
              writer.write("COMMIT;");
              writer.write("BEGIN;");
            }
          }
          writer.write("COMMIT;");
          writer.write("\\q");
        }

        process.waitFor();

        System.out.println(new Date().getTime() - start.getTime());
    }
}
