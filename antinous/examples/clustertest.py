from com.opendatagroup.antinous.producer.kmeans import VectorSet, KMeans, Moving, PrintChange, WhenAny, MaxIterations
import random

spots = [[1, 2, 3],
         [3, 4, 5],
         [1, 1, 1],
         [3, 3, 3]]

d = VectorSet()
for i in xrange(10000):
  spot = random.choice(spots)
  d.add([random.gauss(spot[0], 0.5), random.gauss(spot[1], 0.5), random.gauss(spot[2], 0.5)])

k = KMeans(4, d)
k.setStoppingCondition(WhenAny([PrintChange("%10g"), Moving(), MaxIterations(1000)]))

k.randomClusters()
k.optimize()
print k.model().pfa().meta(lambda i: {"name": "cluster-%02d" % i, "id": i})

