# PL/Java Demo

## Build PL/Java extension

Make sure you have installed PostgreSQL 9.4 or 9.5 server and include files.

Let's build version 1.5.0:

    git clone https://github.com/tada/pljava
    cd pljava
    git checkout V1_5_0
    mvn clean install
    sudo java -jar ./pljava-packaging/target/pljava-*.jar

## Install PL/Java extension

You can install the extension in the database with:

    psql -c 'CREATE EXTENSION pljava';

If you see an error like:

    WARNING:  Java virtual machine not yet loaded
    DETAIL:  libjvm: cannot open shared object file: No such file or directory
    HINT:  SET pljava.libjvm_location TO the correct path to the jvm library (libjvm.so or jvm.dll, etc.)
    ERROR:  cannot use PL/Java before successfully completing its setup
    HINT:  Check the log for messages closely preceding this one, detailing what step of setup failed and what will be needed, probably setting one of the "pljava." configuration variables, to complete the setup. If there is not enough help in the log, try again with different settings for "log_min_messages" or "log_error_verbosity".

Just SET pljava.libjvm_location to the correct location of libjvm.so. You may find correct location with command:

    find /usr/lib -name libjvm.so

And then:

    psql -c 'SET pljava.libjvm_location TO '<path to libjvm.so>'; CREATE EXTENSION pljava';

## Install the demo

    mvn clean package
    psql -c "SELECT sqlj.install_jar('file://`pwd`/target/hello-pljava-1.0.jar','hellopljava',true);"

## Try the demo

    SELECT sqlj.set_classpath('public', 'hellopljava');
    SELECT * FROM getcountrieslanguagescsv(95);