import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheMode._
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.IgniteCache
import scala.io.Source
import java.io._


@SerialVersionUID(1L)
case class Product(company: String, name: String, qtt: Int)


object Test {


	lazy val companies = Array("comp1", "comp2", "comp3", "comp4")

    /** Cache name. */
    private val defaultCacheName = "MyCache"

    private lazy val ignite = Ignition.ignite;

	private lazy val igCfg = new IgniteConfiguration()
    
    private lazy val cacheCfg = {
    	val config = new CacheConfiguration[String, Product]
		config.setName(defaultCacheName)
        config.setCacheMode(PARTITIONED)
        config.setIndexedTypes(classOf[String], classOf[Product])
        config
    }

	private lazy val cache =  ignite.getOrCreateCache(cacheCfg)	

	def createTeantnCache(tenant: String): IgniteCache[String, Product] = {
        val config = new CacheConfiguration[String, Product]
		config.setName(tenant)
		config.setCacheMode(PARTITIONED)
		config.setIndexedTypes(classOf[String], classOf[Product])
    	ignite.getOrCreateCache(config)
    }


	val debug : Boolean = true

	val maxProducts = 200000

    def main(args: Array[String]) {
    	createFile
		println("File ok")    	
    	Ignition.start(igCfg)
	    try {
	    	var timePut: Long = 0
	    	var timeGet: Long = 0
	    	var timeCreateCache: Long = 0

	    	// Change this option to test with tenant per cache (true) or single cache (false)
	    	val tenant: Boolean = true	

	    	if (tenant) {	    		
	    		println(s"Runing ${maxProducts} tenant cache")
	    		val times  = concurrentTenantCachePut
	    		timePut = times._1
	    		timeCreateCache = times._2
	    		println(s"Create tenant cache TimeMillis ${timeCreateCache}")
	    	} else {
	    		println(s"Runing ${maxProducts} single cache")
	    		val startCreateCache = java.lang.System.currentTimeMillis()
    			cache
    			timeCreateCache = java.lang.System.currentTimeMillis() - startCreateCache
				println(s"Create single cache TimeMillis ${timeCreateCache}")

	    		timePut = concurrentSingleCachePut	    		
	    	}	
	    	println(s"Time put ${timePut}")
	    	for (i <- 1 to 5) {
	    		if (debug) println(s"Starting ${i}")

		    	if (tenant) {
		    		timeGet = timeGet + concurrentTenantCacheGet
		    	} else {
		    		timeGet = timeGet + concurrentSingleCacheGet
		    	}	
				if (debug) println(s"Finished ${i}")
			}

			println(s"Time get ${timeGet}")
			println(s"Time total ${timeGet + timePut}")
			println(s"Time total - timeCreateCache ${timeGet + timePut - timeCreateCache}")
	    } finally {
	    	cache.destroy()
	    	Ignition.stop(true)
	    }
    }

    def createFile {
    	for (company <- companies) {
    		val file = new File(company + ".txt")
			val bw = new BufferedWriter(new FileWriter(file))
    		for (p <- 1 to maxProducts) {
	    		val product = "product" + p
				val qtt = p
				val text = s"${company},${product},${qtt}\n"
				bw.write(text)
	    	}
	    	bw.close()
	    }			
    }

    def concurrentSingleCachePut: Long = {
    	val threads:Array[Thread] = new Array[Thread](companies.size)
    	var count = 0
    	val start = java.lang.System.currentTimeMillis()
    	for (company <- companies) {
			val thread = new Thread {
				override def run {
			    	for (line <- Source.fromFile(company + ".txt").getLines()) {
			    		val record = line.split(",")
						val p = Product(record(0), record(1), record(2).toInt)
						cache.put(p.company + "_" + p.name, p)			    							
				  	}
			    }
			}
			thread.start
			threads(count) = thread
			count = count + 1
	    }
	    for (t <- threads) {
	    	t.join
	    }
	    var time = java.lang.System.currentTimeMillis() - start
	    if (debug) println(s"ConcurrentSingleCachePut TimeMillis ${time}")
	    time
    }


    def concurrentTenantCachePut: (Long, Long) = {
    	val threads:Array[Thread] = new Array[Thread](companies.size)
    	var count = 0
    	val start = java.lang.System.currentTimeMillis()    	
    	var totalCacheTime: Long = 0
    	for (company <- companies) {
    		val startCreateCache = java.lang.System.currentTimeMillis()
    		val tenantCache = createTeantnCache(company)
    		var timeCreateCache = java.lang.System.currentTimeMillis() - startCreateCache
    		totalCacheTime = totalCacheTime + timeCreateCache
			val thread = new Thread {
				override def run {
					for (line <- Source.fromFile(company + ".txt").getLines()) {
						val record = line.split(",")
						val p = Product(record(0), record(1), record(2).toInt)
						tenantCache.put(p.company + "_" + p.name, p)			    	
					}
				}
			}
	    	thread.start
			threads(count) = thread
			count = count + 1			
	    }
	    for (t <- threads) {
	    	t.join
	    }
	    var time = java.lang.System.currentTimeMillis() - start
	    if (debug) println(s"ConcurrentTenantCachePut TimeMillis ${time}")
	    (time, totalCacheTime)
    }


    def concurrentSingleCacheGet: Long = {
    	val threads:Array[Thread] = new Array[Thread](companies.size)
    	var count = 0
    	val start = java.lang.System.currentTimeMillis()    	
    	for (company <- companies) {
    		val thread = new Thread {
				override def run {
		    		for (i <- 1 to maxProducts) {
			    		val product: Product = cache.get(company + "_" + "product" + i)
			    		//println(product)
			    	}
			    }
			}    	
	    	thread.start
			threads(count) = thread
			count = count + 1			
	    }
	    for (t <- threads) {
	    	t.join
	    }
	    var time = java.lang.System.currentTimeMillis() - start
	    if (debug) println(s"ConcurrentSingleCacheGet TimeMillis ${time}")
	    time
    }

    def concurrentTenantCacheGet: Long = {
    	val threads:Array[Thread] = new Array[Thread](companies.size)
    	var count = 0
    	val start = java.lang.System.currentTimeMillis()    	
    	for (company <- companies) {
    		val tenantCache = createTeantnCache(company)
    		val thread = new Thread {
				override def run {
		    		for (i <- 1 to maxProducts) {
			    		val product: Product = tenantCache.get(company + "_" + "product" + i)
			    	}
			    }
			}    	
	    	thread.start
			threads(count) = thread
			count = count + 1			
	    }
	    for (t <- threads) {
	    	t.join
	    }
	    var time = java.lang.System.currentTimeMillis() - start
	    if (debug) println(s"ConcurrentTenantCacheGet TimeMillis ${time}")
	    time
    }
}
