package util

import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.beans._
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.hector.api.ddl.ComparatorType._
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.hector.api.query._
import me.prettyprint.cassandra.model.HColumnImpl
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.CompositeSerializer
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
          cluster.dropKeyspace(kspName, true)
        }
    }

    def addKeyspace = cluster.addKeyspace(_ : String, true)

    def addColumnFamily(kspName : String, colFamilyName : String) : String = {
        val colFamily = HFactory.createColumnFamilyDefinition(kspName, colFamilyName)
        return cluster.addColumnFamily(colFamily, true)
    }

    // It's said compisite column is favored over super column
    def addColumnFamily(kspName: String, colFamilyName: String,
                        comparatorType: ComparatorType): String = {

        val cfd = HFactory createColumnFamilyDefinition
                        (kspName, colFamilyName, comparatorType)
        // Fragile API: UTF8Type not UTF8TYPE, IntegerType not INTEGERTYPE
        // as what ComparatorType defines.
        // Use "(UTF8TYPE, INTEGERTYPE)" will fail with exception:
        //  InvalidRequestException(why:Invalid definition for comparator org.apache.cassandra.db.marshal.CompositeType.)
        cfd.setComparatorTypeAlias("(UTF8Type, IntegerType)")
        cfd.setKeyValidationClass(UTF8TYPE.getClassName())
        cfd.setDefaultValidationClass(UTF8TYPE.getClassName())
        return cluster.addColumnFamily(cfd, true)
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

    def fillTable(kspName: String, colFName: String,
                    startRow: Int, endRow: Int,
                    numCols: Int, compCols: String*): Unit = {
        
        val mutator = HFactory.createMutator(createKeyspace(kspName), StringSerializer.get())
        for (row <- startRow to endRow) {
            val rowKey = "row" + row
            for (col <- 1 to numCols)
                compCols.foreach(colHeader => {
                    val colKey = new Composite()
                    colKey.addComponent(colHeader, StringSerializer.get())
                    // Scala Int and java.lang.Integer are not the same thing
                    // also works:
                    //  colKey.addComponent(int2Integer(col), IntegerSerializer.get())
                    // int2Integer is defined in Prelude
                    colKey.addComponent(col:java.lang.Integer, IntegerSerializer.get())
                    mutator.addInsertion(rowKey, colFName,
                            HFactory.createColumn(colKey, "row-" + row + "-" + colHeader + "-" + col,
                                    new CompositeSerializer(), StringSerializer.get()))
                })
        }

        // Fixit: the test cluster has two nodes. Following instrument will cause
        //  me.prettyprint.hector.api.exceptions.HUnavailableException: : May not be enough replicas present to handle consistency level.
        // when shutting down if there's only one node available. But other three examples, Put/Get/Delete,
        // are all fine under same circumstance. Find out why.
        mutator.execute()
    }

    def scan(kspName: String, colFName: String, startRow: Int, endRow: Int) = {
        
        val sliceQuery = HFactory.createMultigetSliceQuery(createKeyspace(kspName), StringSerializer.get(),
                new CompositeSerializer(), StringSerializer.get())
        val start = new Composite()
        val finish = new Composite()
        sliceQuery.setColumnFamily(colFName).setKeys((startRow to endRow).map("row"+_))
        start.addComponent("a", StringSerializer.get())
             .addComponent(1:java.lang.Integer, IntegerSerializer.get())
        finish.addComponent(Char.MaxValue.toString(), StringSerializer.get())
              .addComponent(Int.MaxValue:java.lang.Integer, IntegerSerializer.get())
        sliceQuery.setRange(start, finish, false, 100)
                  .execute()
    }

    def scan(kspName: String, colFName: String, startRow: Int, endRow: Int, cols: List[(String, Int)]) = {
        
        val sliceQuery = HFactory.createMultigetSliceQuery(createKeyspace(kspName), StringSerializer.get(),
                new CompositeSerializer(), StringSerializer.get())
        sliceQuery.setColumnFamily(colFName).setKeys((startRow to endRow).map("row"+_))
        // Hmm, MultigetSliceQuery doesn't have setColumnNames(Collection<SN> columnNames)
        // only setColumnNames(SN... columnNames)
        // AbstractSliceQuery have both. WTF?!
        // Also note the way pass a Scala collection to Java varargs:
        //      collection.toArray._*
        // not pretty...
        // Third, the method chaining of AbstractComposite.addComponent is broken too,
        // (new Composite()).addComponent will give back an AbstractComposite
        // instead of Composite!
        sliceQuery.setColumnNames(cols.map(c => {
                val comp = new Composite()
                comp.addComponent(c._1, StringSerializer.get())
                    .addComponent(c._2:java.lang.Integer, IntegerSerializer.get())
                comp}).toArray:_*)

        sliceQuery.execute()
    }

    def row2String(row: Row[String, Composite, String]): String = {
        
        val cells = row.getColumnSlice().getColumns().map(c => {
            val compCols = c.getName().getComponents()
            val compCol1 = StringSerializer.get().fromByteBuffer(compCols(0).getBytes())
            val compCol2 = IntegerSerializer.get().fromByteBuffer(compCols(1).getBytes())
            compCol1 + ", " + compCol2 + ": " + c.getValue()
        })

        cells.mkString("|", "|", "|")
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
    implicit def mkKeyspaceDefinition(k : String) : KeyspaceDefinition = HFactory.createKeyspaceDefinition(k)
}
