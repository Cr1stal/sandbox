import util.CassandraHelper

import me.prettyprint.hector.api._
import me.prettyprint.hector.api.factory._
import me.prettyprint.cassandra.serializers._
import me.prettyprint.cassandra.service._
import me.prettyprint.cassandra.service.template._
import org.apache.log4j.Logger
import scalaz._
import Scalaz._

object GetExample extends App {
    val LOG = Logger.getLogger("GetExample")

    // fixit: cassandra hosts configuration should be externalized
    val conf = new CassandraHostConfigurator("192.168.1.6:9160")

    withResource (
        new CassandraHelper(conf),
        (helper : CassandraHelper) => {
        // cassandra-scala-exec is invalid keyspace name
        // interesting, but can't find any document what is valid
        // probably constrained by Thrift. find that out
        val keyspace = helper.createKeyspace("cassandra_scala_exec")
        val colFamilyName = "test_table"

        // read
        val template = new ThriftColumnFamilyTemplate(keyspace, colFamilyName,
                                                      StringSerializer.get(),
                                                      StringSerializer.get())
        val result = template.queryColumns("row1")
        LOG.info("****** Value: " + result.getString("qual1"))
        LOG.info("****** Value: " + result.getString("qual2"))
    })
}
