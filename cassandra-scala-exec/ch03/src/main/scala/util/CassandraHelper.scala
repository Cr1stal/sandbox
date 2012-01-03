package util

import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.cassandra.model.HColumnImpl
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import scalaz._
import Scalaz._

class CassandraHelper(clusterName : String, conf : CassandraHostConfigurator) {

    import CassandraHelper._

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

    def addKeyspace = cluster.addKeyspace(_ : String, true)

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

    // Jonathan Ellis: "No, we're not planning to add support for retrieving old versions."
    // Also see CASSANDRA-580, CASSANDRA-1070 and CASSANDRA-1072
    def dump(kspName : String, colFName : String, rowKeys : List[String]) : Unit = {
        val keyspace = createKeyspace(kspName)
        val template = new ThriftColumnFamilyTemplate(keyspace, colFName,
                                                      StringSerializer.get(),
                                                      StringSerializer.get())
        for (rowKey <- rowKeys) {
            LOG.info("****** Row: " + rowKey)
            val res = template.queryColumns(rowKey)
            val line = res.getColumnNames().map {(c) => c + "@" + res.getColumn(c).getClock() + " -> " + res.getString(c)}
            LOG.info(line.mkString("******\t ", ", ", ""))
        }
    }

    def shutdown() : Unit = {
        HFactory.shutdownCluster(cluster)
    }
}

object CassandraHelper {
    // scaladoc: http://scalaz.github.com/scalaz/scalaz-2.9.1-6.0.2/doc/index.html#scalaz.Resources
    implicit def CassandraResource : Resource[CassandraHelper] = resource(c => c.shutdown)

    // 1. As a rule of thumb, always define explicit result type for an implicit conversion
    // Martin Odersky: "An implicit conversion without explicit result type is visible only in the text following its own definition. That way, we avoid the cyclic reference errors."
    // also, http://stackoverflow.com/questions/2731185/why-does-this-explicit-call-of-a-scala-method-allow-it-to-be-implicitly-resolved
    // 2. Martin Odersky: "It's fair to say that point-free style is not idiomatic Scala." See http://scala-lang.org/node/9371
    implicit def string2keyspacedefinition(k : String) : KeyspaceDefinition = HFactory.createKeyspaceDefinition(k)
}
