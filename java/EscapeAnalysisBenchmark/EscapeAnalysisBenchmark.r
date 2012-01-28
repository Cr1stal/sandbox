data <- read.table("results.csv", sep=",", header=T)
xrange <- range(data$normals / 1000)
yrange <- range(data$us / data$normals * 1000)
colors <- rainbow(4)
linetype <- c(1:4)
plotchar <- seq(18, 18+4,1)
vmflags <- c("-XX:-DoEscapeAnalysis", "-XX:+DoEscapeAnalysis")
cacheflags <- c("CacheUnfriendly", "CacheFriendly")

png("results.png")
plot(xrange, yrange, xlab="arraysize (k)", ylab="time / arraysize (ns)")
t <- 1
for ( vm in vmflags) {
	subset1 <- subset(data, escape==vm)
	for ( cache in cacheflags) {
		subset2 <- subset(subset1, benchmark==cache)
		x <- subset2$normals / 1000
		y <- subset2$us / subset2$normals * 1000
		lines(x, y, type="o", lwd=1.5, lty=linetype[t], col=colors[t], pch=plotchar[t])
		t <- t + 1
	}
}
title("Escape Analysis & Cache Benchmark")
legend(xrange[1], yrange[2],
	c("-EscapeAnalysis & CacheUnfriendly",
	  "-EscapeAnalysis & CacheFriendly",
	  "+EscapeAnalysis & CacheUnfriendly",
	  "+EscapeAnalysis & CacheFriendly"),
	cex=0.8, col=colors, pch=plotchar, lty=linetype)

dev.off()
