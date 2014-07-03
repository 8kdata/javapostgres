/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.helloprocessbuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
 * To run the code, create before an empty database called "btp".
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class BTP {
    private static final String PSQL = "/usr/bin/psql";
    private static final String DATABASE_NAME = "btp";
    private static final int N_TABLES = 10 * 1000;

    public static void main(String[] args) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(PSQL, "-p 5432", "-q", DATABASE_NAME);
        Date start = new Date();
        Process process = processBuilder.start();

        try(
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()))
        ) {
            for(int i = 0; i < N_TABLES; i++) {
                writer.write("CREATE TABLE t_" + String.format("%09d", i) + " (i integer);");
            }
            writer.write("\\q");
        }

        process.waitFor();

        System.out.println(new Date().getTime() - start.getTime());
    }
}
