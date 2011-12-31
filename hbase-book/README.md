### HBase is a moving train

My rant about HBase: it is a moving train! And moving fast.

*HBase: The Definitive Guide* has a sample application called `hush`. But it can't run as is. As this writing, the latest so-called stable release of HBase is 0.90.5, which lacks certain features the book needs, such as coprocessor. So `hush` even can't compile against 0.90.5 HBase jar. It requires HBase 0.91.0-SNAPSHOT. Revision 1130916, to be precise, according to [the book's website](http://www.hbasebook.com/). Lars George, the author, kindly provides such a distribution in [his apache site](http://people.apache.org/~larsgeorge/repo/org/apache/hbase/hbase/0.91.0-SNAPSHOT/). But `hush` still need some minor tweaks to function properly as advertised.

HBase is approaching 0.92. So I also tried to run sample code with that. No luck either. Method signatures change here and there, classes move to another package, etc.

That's a lot of rough edges. The book should've done better in regard to explain how to run its own sample application. Or it might not be the book's fault. I read somewhere that a team picked another NoSQL database over HBase because they felt HBase has 'too many moving parts'. According to my experience with HBase so far, this is still very true. Since Hadoop reaches 1.0 several days ago, HBase should be able to stabilize its API now. Please do so as soon as possible.

In the meantime, I try to install latest HBase in a truly distributed fashion. The goal is to successfully run `hush` against said HBase cluster. Hope this will save time for someone who attempts the same.

#### [Install Hadoop](http://hadoop.apache.org/common/docs/r1.0.0/) on illumos

``` bash
$ ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
$ cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
$ wget -c http://mirror.atlanticmetro.net/apache//hadoop/core/hadoop-1.0.0/hadoop-1.0.0-bin.tar.gz
$ tar xzf hadoop-1.0.0-bin.tar.gz
$ cd hadoop-1.0.0
$ mkdir conf
$ cp etc/hadoop/hadoop-env.sh conf
$ vim conf/hadoop-env.sh
export JAVA_HOME=/usr/java
$ vim conf/core-site.xml
<configuration>
     <property>
         <name>fs.default.name</name>
         <value>hdfs://192.168.1.6:9000</value>
     </property>
</configuration>
$ mkdir name_dir
$ mkdir data_dir
$ vim conf/hdfs-site.xml
<configuration>
     <property>
         <name>dfs.replication</name>
         <value>1</value>
     </property>
     <property>
        <name>dfs.name.dir</name>
        <value>/home/edward/ws/sandbox/hadoop-1.0.0/name_dir</value>
     </property>
     <property>
        <name>dfs.data.dir</name>
        <value>/home/edward/ws/sandbox/hadoop-1.0.0/data_dir</value>
     </property>
</configuration>
$ vim conf/masters
192.168.1.6
$ vim conf/slaves
192.168.1.6
$ bin/hadoop namenode -format
$ chmod +x sbin/*.sh
$ sbin/start-dfs.sh
$ bin/hadoop dfsadmin -safemode leave (name node starts in safe mode, because dfs.replication => 1?)
$ bin/hadoop fs -put ...
$ bin/hadoop fs -get ...
$ sbin/stop-dfs.sh
```

#### Install HBase 0.92

``` bash
$ git clone git://git.apache.org/hbase.git hbase
$ cd hbase
$ git checkout 0.92.0rc2
$ mvn package -DskipTests=true
```
