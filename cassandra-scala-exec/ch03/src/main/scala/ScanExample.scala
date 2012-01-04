import util.CassandraHelper

import me.prettyprint.hector.api._
import me.prettyprint.hector.api.beans._
import me.prettyprint.hector.api.factory._
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.cassandra.serializers._
import me.prettyprint.cassandra.service._
import me.prettyprint.cassandra.service.template._
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import scalaz._
import Scalaz._

object ScanExample extends App {
    val LOG = Logger.getLogger("ScanExample")

    // fixit: cassandra hosts configuration should be externalized
    val conf = new CassandraHostConfigurator("192.168.1.6:9160")

    withResource (
        new CassandraHelper(conf),
        (helper : CassandraHelper) => {

        val kspName = "cassandra_scala_exec"
        val colFName = "test_table"
        helper.dropKeyspace(kspName)
        helper.addKeyspace(kspName)
        helper.addColumnFamily(kspName, colFName, ComparatorType.COMPOSITETYPE)
        helper.fillTable(kspName, colFName, 1, 100, 10, "compcol1", "compcol2")

        LOG.info("****** Scanning table #1...")
        for (row <- helper.scan(kspName, colFName, 1, 100).get()) {
            LOG.info("******\t" + row.getKey() + " -> " + helper.row2String(row))
        }

        LOG.info("****** Scanning table #2...")
        for (row <- helper.scan(kspName, colFName, 20, 21).get()) {
            LOG.info("******\t" + row.getKey() + " -> " + helper.row2String(row))
        }

        LOG.info("****** Scanning table #3...")
        for (row <- helper.scan(kspName, colFName, 70, 80,
                List(("compcol1", 6), ("compcol1", 7), ("compcol2", 5))).get()) {
            LOG.info("******\t" + row.getKey() + " -> " + helper.row2String(row))
        }
    })
}
