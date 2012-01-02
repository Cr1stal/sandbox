import util.CassandraHelper

import me.prettyprint.hector.api._
import me.prettyprint.hector.api.factory._
import me.prettyprint.cassandra.serializers._
import me.prettyprint.cassandra.service._
import me.prettyprint.cassandra.service.template._
import org.apache.log4j.Logger
import scalaz._
import Scalaz._

object DeleteExample extends App {
    val LOG = Logger.getLogger("DeleteExample")

    // fixit: cassandra hosts configuration should be externalized
    val conf = new CassandraHostConfigurator("192.168.1.6:9160")

    withResource (
        new CassandraHelper(conf),
        (helper : CassandraHelper) => {

        val kspName = "cassandra_scala_exec"
        val colFName = "test_table"
        helper.dropKeyspace(kspName)
        helper.addKeyspace(kspName)
        helper.addColumnFamily(kspName, colFName)
        helper.batchPut(kspName, colFName, List("row1"),
                        List(("qual1", 1, "val1"),
                             ("qual1", 2, "val2"),
                             ("qual2", 3, "val3"),
                             ("qual2", 4, "val4"),
                             ("qual3", 5, "val5"),
                             ("qual3", 6, "val6")))
        LOG.info("****** Before delete call...")
        helper.dump(kspName, colFName, List("row1"))

        // delete
        val keyspace = helper.createKeyspace(kspName)
        val template = new ThriftColumnFamilyTemplate(keyspace, colFName,
                                                      StringSerializer.get(),
                                                      StringSerializer.get())
        val mutator = template.createMutator()
        mutator.addDeletion("row1", colFName, "qual1", StringSerializer.get(), 1)
        mutator.addDeletion("row1", colFName, "qual3", StringSerializer.get(), 5)

        mutator.addDeletion("row1", colFName, "qual1", StringSerializer.get())
        mutator.addDeletion("row1", colFName, "qual3", StringSerializer.get())

        mutator.addDeletion("row1", colFName)
        mutator.addDeletion("row1", colFName, 3)

        mutator.execute()
        LOG.info("****** After delete call...")
        helper.dump(kspName, colFName, List("row1"))
    })
}
