from faker import Faker
import random
import csv

fake = Faker()

def generate_dataset(num_points):
    dataset = []
    for _ in range(num_points):
        x = random.uniform(0, 100)
        y = random.uniform(0, 100)
        label = random.choice(['A', 'B'])
        dataset.append((x, y, label))
    return dataset

def save_dataset_to_csv(dataset, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['x', 'y', 'label'])
        for data in dataset:
            writer.writerow(data)

if __name__ == "__main__":
    num_points = 1000  # You can adjust the number of points as needed
    dataset = generate_dataset(num_points)
    save_dataset_to_csv(dataset, 'ScpsoDataset.csv')
    print(f"Dataset with {num_points} points generated and saved to 'generated_dataset.csv'")
