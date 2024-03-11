import numpy as np
from pyspark.sql import SparkSession

def ackley(x, y):
    """Ackley function"""
    a = 20
    b = 0.2
    c = 2 * np.pi
    term1 = -a * np.exp(-b * np.sqrt(0.5 * (x*2 + y*2)))
    term2 = -np.exp(0.5 * (np.cos(c*x) + np.cos(c*y)))
    return term1 + term2 + a + np.exp(1)

def update_particle(particle):
    """Update particle position and velocity"""
    global gbest, gbest_obj
    x, y, vx, vy = particle
    r1, r2 = np.random.rand(2)
    vx = w * vx + c1 * r1 * (pbest[0] - x) + c2 * r2 * (gbest[0] - x)
    vy = w * vy + c1 * r1 * (pbest[1] - y) + c2 * r2 * (gbest[1] - y)
    x += vx
    y += vy
    obj = ackley(x, y)
    if not np.isnan(obj) and obj < gbest_obj:
        gbest = (x, y)
        gbest_obj = obj
    return (x, y, vx, vy)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ParticleSwarmOptimization") \
    .getOrCreate()

# Hyper-parameters of the algorithm
c1 = c2 = 0.1
w = 0.8

# Create RDD of particles
n_particles = 100
np.random.seed(100)
particles = np.random.uniform(-5, 5, (n_particles, 4))  # (x, y, vx, vy)
particles_rdd = spark.sparkContext.parallelize(particles)

# Initialize global best
gbest = particles_rdd.min(key=lambda p: ackley(p[0], p[1]))
gbest_obj = ackley(gbest[0], gbest[1])

# Main loop of PSO
for i in range(50):
    pbest = particles_rdd.min(key=lambda p: ackley(p[0], p[1]))
    particles_rdd = particles_rdd.map(update_particle)

# Stop SparkSession
spark.stop()

print("PSO found best solution at f({}, {}) = {}".format(gbest[0], gbest[1], gbest_obj))
