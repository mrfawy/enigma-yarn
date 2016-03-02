simple-yarn-app
===============

Simple YARN application to run n copies of a unix command - deliberately kept simple (with minimal error handling etc.)

Usage:
======

#copy target jar to cluster
ssh mabdelra@silver-edge.db.aexp.com rm -f /axp/gcp/cpsetlh/dev/enigma-yarn-app-1.1.0.jar && \
scp /cygdrive/c/Users/mabdelra/.m2/repository/com/home/enigma-yarn-app/1.1.0/enigma-yarn-app-1.1.0.jar mabdelra@silver-edge.db.aexp.com:/axp/gcp/cpsetlh/dev



### run

 yarn jar /axp/gcp/cpsetlh/dev/enigma-yarn-app-1.1.0.jar  -jar /axp/gcp/cpsetlh/dev/enigma-yarn-app-1.1.0.jar
 -queue "cpsetlh"

  
    
