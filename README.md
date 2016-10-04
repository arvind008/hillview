# Hiero
Big data spreadsheet


# Installing spark and HDFS

To get a local spark and HDFS installation running, execute the following steps.

First, download a JDK for Linux x64 from here: http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

Make sure to download the tarball version of the JDK.

Unpack the JDK, and set your JAVA_HOME environment variable to point to the unpacked folder (e.g, <fully qualified path to>/jdk/jdk1.8.0_101). To set your JAVA_HOME environment variable, add the following to your ~/.bashrc or ~/.zshrc.

export JAVA_HOME="<path-to-jdk-folder>"


Next, download Spark and Hadoop. It will ask for your password.

$: ./install.sh


Once that succeeds, configure your Hadoop installation:

$: ./configure.sh

The above script formats your HDFS directory. It will prompt you if an existing HDFS installation exists.

Next, test whether you can SSH without a password to your local machine because Hadoop needs that to transfer files around.

$: ssh localhost

If the above step asked for your password, run the following script to setup an SSH key just for hadoop to use. This is a one time step:

$: ./keygen.sh


Lastly, start your new development Hadoop and Spark cluster:

$: ./services.sh start
