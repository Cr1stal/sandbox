package util

import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.HColumnImpl
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate
import org.apache.log4j.Logger
import scalaz._
import Scalaz._

class CassandraHelper(clusterName : String, conf : CassandraHostConfigurator) {

    private val cluster = HFactory.getOrCreateCluster(clusterName, conf)
    private val LOG = Logger.getLogger("CassandraHelper")

    def this(conf : CassandraHostConfigurator) = this("test_cluster", conf)

    // The choice of name create/drop/addKeyspace is a little unfortunate.
    // They are not semantically symmetric, although resemble literally.
    // add/dropKeyspace are members of interface Cluster and they do add or
    // destroy keyspace in cluster, whereas createKeyspace
    // is a member of HFactory and it merely 'create' pointer representing
    // an existing keyspace.
    // Fixit: come up with some better names.
    def createKeyspace(kspName : String) : Keyspace = {
        return HFactory.createKeyspace(kspName, cluster)
    }

    // a safer version of Cluster.dropKeyspace, which will throw an
    // exception if given keyspace doesn't exist
    def dropKeyspace(kspName : String) : Unit = {
        if (cluster.describeKeyspace(kspName) != null) {
          cluster.dropKeyspace(kspName)
        }
    }

    // Fixit: define an implicit conversion and eliminate this function definition
    def addKeyspace(kspName : String) : String = {
        val kspDef = HFactory.createKeyspaceDefinition(kspName)
        return cluster.addKeyspace(kspDef, true)
    }

    def addColumnFamily(kspName : String, colFamilyName : String) : String = {
        val colFamily = HFactory.createColumnFamilyDefinition(kspName, colFamilyName)
        return cluster.addColumnFamily(colFamily, true)
    }

    def batchPut(kspName : String, colFName : String, rowKeys : List[String],
                cols : List[(String, Long, String)]) : Unit = {

        val keyspace = createKeyspace(kspName)
        val template = new ThriftColumnFamilyTemplate(keyspace, colFName,
                                                      StringSerializer.get(),
                                                      StringSerializer.get())
        for (rowKey <- rowKeys) {
            val updater = template.createUpdater(rowKey)
            for ((colName, timestamp, colValue) <- cols) {
                val col = new HColumnImpl(colName, colValue, timestamp)
                updater.setColumn(col)
            }

            template.update(updater)
        }
    }

    // Fixit: also dump all versions of data, not just latest one
    // Fixit: use String.join or the like instead of ugly string concatenation
    def dump(kspName : String, colFName : String, rowKeys : List[String]) : Unit = {
        val keyspace = createKeyspace(kspName)
        val template = new ThriftColumnFamilyTemplate(keyspace, colFName,
                                                      StringSerializer.get(),
                                                      StringSerializer.get())
        for (rowKey <- rowKeys) {
            LOG.info("****** Row: " + rowKey)
            val res = template.queryColumns(rowKey)
            var line = "******\t "
            for (colName <- res.getColumnNames())
                line += colName + " -> " + res.getString(colName) + ", "
            LOG.info(line)
        }
    }

    def shutdown() : Unit = {
        HFactory.shutdownCluster(cluster)
    }
}

// scaladoc: http://scalaz.github.com/scalaz/scalaz-2.9.1-6.0.2/doc/index.html#scalaz.Resources
object CassandraHelper {
    implicit val helperResource: Resource[CassandraHelper] = resource(h => h.shutdown)
}
