# GridFTP/HDFS

This provides a HDFS plugin for the Globus GridFTP server, allowing the Globus GridFTP server to read and
write directly into HDFS using the default `libhdfs` client.

This uses Globus GridFTP's DSI interface to implement the plugin, and utilizes the default HDFS configuration on the host.

Note this package contains the plugin code as well as the configuration and packaging scripts, targeting RedHat Enterprise Linux
and variants.  Particularly, the plugin requires `$CLASSPATH` to be configured correctly for the Globus GridFTP process: each
OS and Hadoop packaging variant may need the bootstrap scripts to be customized to get HDFS working.
