plot_hm_benchmark <- function(jpg_name, gcflags, access_patterns, impls, colors, linetype, plotchar) {
	jpeg(jpg_name, width=1440, height=720)
	# 6 figures: 3 access patterns x 2 gc algorithms
	par(mfrow=c(2, 3), oma=c(1, 1, 0, 0), cex=1.0)

	gcindex <- 1
	for (gc in gcflags) {
		apindex <- 1
		for (access_pattern in access_patterns) {
			lhm_subset <- subset(data, chm_impl %in% impls)
			xrange <- range(lhm_subset$threads)
			yrange <- range(lhm_subset$ops / 1000)
			plot(xrange, yrange, log="x", xaxt="n", xlab="threads", ylab="throughput (k ops)")
			axis(1, lhm_subset$threads)
			t <- 1
			for ( impl in impls) {
				subset1 <- subset(data, chm_impl==impl
										& get_put_remove==access_pattern
										& gc_algorithm==gc)
				x <- subset1$threads
				y <- subset1$ops / 1000
				lines(x, y, type="o", lwd=1.5, lty=linetype[t], col=colors[t], pch=plotchar[t])
				t <- t + 1
			}
			legend(xrange[1], yrange[2], impls,
				cex=1.0, col=colors, pch=plotchar, lty=linetype)
			switch(apindex,
				mtext(access_pattern, side=1, outer=TRUE, cex=1.2, adj=0.15),
				mtext(access_pattern, side=1, outer=TRUE, cex=1.2, adj=0.52),
				mtext(access_pattern, side=1, outer=TRUE, cex=1.2, adj=0.88))
			apindex <- apindex + 1
		}
		switch(gcindex,
			mtext(gc, side=2, outer=TRUE, cex=1.2, adj=0.8),
			mtext(gc, side=2, outer=TRUE, cex=1.2, adj=0.25))
		gcindex <- gcindex + 1
	}

	dev.off()
}

data <- read.table("CHMBenchmark.csv", sep=",", header=T)
colors <- rainbow(5)
linetype <- c(1:5)
plotchar <- seq(18, 18+5,1)
gcflags <- c("CMS", "G1")
access_patterns <- c("00-50-50", "70-15-15", "90-05-05")

plot_hm_benchmark("benchmark_hm.jpg",
					gcflags, access_patterns, c("CHM_16", "NBHM"),
					colors[1:2], linetype[1:2], plotchar[1:2])
plot_hm_benchmark("benchmark_lhm.jpg",
					gcflags, access_patterns, c("CLHM_16", "CSLM", "SnapTree"),
					colors[3:5], linetype[3:5], plotchar[3:5])
