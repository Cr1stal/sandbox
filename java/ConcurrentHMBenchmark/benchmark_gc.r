plot_gc_benchmark <- function(jpg_name, impls, thread_count, access_pattern, colors) {
	jpeg(jpg_name)

	subset2 <- subset(data, get_put_remove==access_pattern & threads==thread_count & chm_impl %in% impls,
						select=c("gc_algorithm", "chm_impl", "ops"), drop=TRUE)
	ops <- table(subset2$gc_algorithm, subset2$chm_impl)
	for (gc in subset2$gc_algorithm)
		for (impl in subset2$chm_impl)
			ops[gc, impl] <- subset(subset2, gc_algorithm==gc & chm_impl==impl)$ops / 1000
	barplot(ops[,impls], main="8 threads, get-put-remove % (70-15-15)",
			xlab="HM Impl", ylab="Throughput (k ops)",
			col=colors, legend=rownames(ops), beside=TRUE)

	dev.off()
}

data <- read.table("CHMBenchmark.csv", sep=",", header=T)
colors <- rainbow(2)

plot_gc_benchmark("benchmark_gc1.jpg", c("CHM_16", "NBHM"), 8, "70-15-15", colors)
plot_gc_benchmark("benchmark_gc2.jpg", c("CLHM_16", "CSLM", "SnapTree"), 8, "70-15-15", colors)
