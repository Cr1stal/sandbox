import me.prettyprint.hector.api._
import me.prettyprint.hector.api.factory._
import me.prettyprint.cassandra.serializers._
import me.prettyprint.cassandra.service.template._

import org.apache.log4j.Logger

object GetExample {
  def main(args : Array[String]) : Unit = {
    val LOG = Logger.getLogger("GetExample")

    // fixit: cassandra hosts configuration should be externalized
    val cluster = HFactory.getOrCreateCluster("test_cluster", "192.168.1.6:9160")
    
    // cassandra-scala-exec is invalid keyspace name
    // interesting, but can't find any document what is valid
    // probably constrained by Thrift. find that out
    val keyspace = HFactory.createKeyspace("cassandra_scala_exec", cluster)
    val colFamilyName = "test_table"

    // read
    val template = new ThriftColumnFamilyTemplate(keyspace, colFamilyName,
                                                  StringSerializer.get(), StringSerializer.get())
    val result = template.queryColumns("row1")
    LOG.info("****** Value: " + result.getString("qual1"))
    LOG.info("****** Value: " + result.getString("qual2"))

    HFactory.shutdownCluster(cluster)
  }
}
