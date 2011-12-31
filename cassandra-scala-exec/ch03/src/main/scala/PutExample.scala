import me.prettyprint.hector.api._
import me.prettyprint.hector.api.factory._
import me.prettyprint.cassandra.serializers._
import me.prettyprint.cassandra.service.template._

import org.apache.log4j.Logger

object PutExample {
  def main(args : Array[String]) : Unit = {
    val LOG = Logger.getLogger("PutExample")

    // fixit: cassandra hosts configuration should be externalized
    val cluster = HFactory.getOrCreateCluster("test_cluster", "192.168.1.6:9160")
    
    // cassandra-scala-exec is invalid keyspace name
    // interesting, but can't find any document what is valid
    // probably constrained by Thrift. find that out
    val keyspaceName = "cassandra_scala_exec"
    if (cluster.describeKeyspace(keyspaceName) != null) {
      cluster.dropKeyspace(keyspaceName)
    }
    val keyspaceDef = HFactory.createKeyspaceDefinition(keyspaceName)
    var ret = cluster.addKeyspace(keyspaceDef, true)
    LOG.info("****** Added keyspace: " + ret)

    // set up schema (think of RDBMS table)
    val colFamilyName = "test_table"
    val colFamily = HFactory.createColumnFamilyDefinition(keyspaceName, colFamilyName)
    ret = cluster.addColumnFamily(colFamily, true)
    LOG.info("****** Added column family: " + ret)

    // create
    val keyspace = HFactory.createKeyspace(keyspaceName, cluster)
    val template = new ThriftColumnFamilyTemplate(keyspace, colFamilyName,
                                                  StringSerializer.get(), StringSerializer.get())
    val updater = template.createUpdater("row1")
    updater.setString("qual1", "val1")
    updater.setString("qual2", "val2")
    template.update(updater)

    HFactory.shutdownCluster(cluster)
  }
}
