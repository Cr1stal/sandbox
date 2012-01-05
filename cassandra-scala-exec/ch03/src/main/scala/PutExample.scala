import util.CassandraHelper

import me.prettyprint.hector.api._
import me.prettyprint.hector.api.factory._
import me.prettyprint.cassandra.serializers._
import me.prettyprint.cassandra.service._
import me.prettyprint.cassandra.service.template._
import org.apache.log4j.Logger
import scalaz._
import Scalaz._

object PutExample extends App {
    val LOG = Logger.getLogger("PutExample")

    // fixit: cassandra hosts configuration should be externalized
    val conf = new CassandraHostConfigurator("192.168.1.5:9160")
    
    withResource (
        new CassandraHelper(conf),
        (helper : CassandraHelper) => {
        // cassandra-scala-exec is invalid keyspace name
        // interesting, but can't find any document what is valid
        // probably constrained by Thrift. find that out
        val keyspaceName = "cassandra_scala_exec"
        helper.dropKeyspace(keyspaceName)
        var ret = helper.addKeyspace(keyspaceName)
        LOG.info("****** Added keyspace: " + ret)

        // set up schema (think of RDBMS table)
        val colFamilyName = "test_table"
        ret = helper.addColumnFamily(keyspaceName, colFamilyName)
        LOG.info("****** Added column family: " + ret)

        // create
        val keyspace = helper.createKeyspace(keyspaceName)
        val template = new ThriftColumnFamilyTemplate(keyspace, colFamilyName,
                                                      StringSerializer.get(),
                                                      StringSerializer.get())
        val updater = template.createUpdater("row1")
        updater.setString("qual1", "val1")
        updater.setString("qual2", "val2")
        template.update(updater)
    })
}
