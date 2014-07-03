PostgreSQL and Java training
==========================

This repository contains the source code of the examples and code demos included in some of the "PostgreSQL and Java" trainings that [8Kdata][1] delivers.

Java source code
----------------

Included in the `java` directory are some maven-ized Java projects:

* `helloJDBC`: an iterative approach to JDBC, where a simple example is improved across several executable programs, adding better JDBC constructs and Java best-practices
* `hellojOOQ`: a simple project to show how to take back control of your SQL with [jOOQ][2]
* `helloMyBatis`: a simple project so show how to use the [MyBatis][3] mapper
* `helloProcessBuilder`: connect to PostgreSQL via the stdin


Example database
----------------

Included in the `db` directory is an example database used by the projects in the `java` folder. This database is derived from the [PgFoundry Sample Databases][4] world database.


[1]: http://www.8kdata.com
[2]: http://www.jooq.org/
[3]: http://blog.mybatis.org/
[4]: http://pgfoundry.org/projects/dbsamples/
