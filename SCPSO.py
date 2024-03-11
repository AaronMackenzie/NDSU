import numpy as np
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Pi Calculation").getOrCreate()


def initialize_particles(num_particles, num_features):
    """
    Initialize particles with random positions and velocities.
    """
    particles = []
    for _ in range(num_particles):
        position = np.random.rand(num_features)
        velocity = np.random.rand(num_features)
        particles.append((position, velocity))
    return particles

def evaluate_fitness(particles):
    """
    Evaluate fitness for each particle.
    """
    fitness_values = []
    for position, _ in particles:
        fitness = np.mean(position)  # Simple fitness function, can be replaced with more complex logic
        fitness_values.append(fitness)
    return fitness_values

def update_particles(particles, global_best):
    """
    Update particle positions and velocities.
    """
    updated_particles = []
    for position, velocity in particles:
        new_velocity = np.random.rand(len(velocity)) * velocity + (global_best[0] - position)
        new_position = position + new_velocity
        updated_particles.append((new_position, new_velocity))
    return updated_particles

def run_sc_pso(num_particles, num_features, num_iterations):
    """
    Run SCPSO algorithm.
    """
    particles = initialize_particles(num_particles, num_features)
    global_best = None

    for _ in range(num_iterations):
        fitness_values = evaluate_fitness(particles)
        if global_best is None or max(fitness_values) > max(evaluate_fitness([global_best])):
            global_best_index = fitness_values.index(max(fitness_values))
            global_best = particles[global_best_index]

        particles = update_particles(particles, global_best)

    return global_best

def pretty_output(best_solution):
    """
    Print a descriptive and visually appealing output.
    """
    position, velocity = best_solution
    print("Best Position:")
    print(position)
    print("\nBest Velocity:")
    print(velocity)

# Parameters
num_particles = 10
num_features = 10
num_iterations = 100

# Run SCPSO algorithm
best_solution = run_sc_pso(num_particles, num_features, num_iterations)

# Print descriptive output
print("Best Solution found by SCPSO algorithm:")
pretty_output(best_solution)



spark.stop()
