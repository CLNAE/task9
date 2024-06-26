import os
import time
from multiprocessing import Pool, cpu_count
import redis

start = time.time()
print("The script has started running.")

r = redis.Redis(decode_responses=True)
print(r.ping()) #PONG
def create_matrix(line):
    cached_matrix = r.get(f"matrix:{line}")
    if cached_matrix:
        return eval(cached_matrix)

    size = line.split(':')[0]
    x = size.index('x')
    rows = int(size[:x])
    columns = int(size[x + 1:])
    values = line.split(':')[1].strip()
    index = 0
    matrix = [[-1 for _ in range(columns)] for _ in range(rows)]
    for i in range(rows):
        for j in range(columns):
            matrix[i][j] = int(values[index])
            index += 1
    matrix = tuple(tuple(row) for row in matrix)  # Convert to tuple of tuples

    r.set(f"matrix:{line}", str((matrix, rows, columns)))

    return matrix, rows, columns


def count_neighboring_ones(matrix, rows, columns):
    cached_result = r.get(f"count:{matrix}")
    if cached_result:
        return eval(cached_result)

    def neighbors_coordinates(i, j):
        neighbors = []
        for x in range(max(0, i - 1), min(rows, i + 2)):
            for y in range(max(0, j - 1), min(columns, j + 2)):
                if (x != i or y != j) and matrix[x][y] == 1:
                    neighbors.append((x, y))
        return neighbors

    def mark_as_verified(i, j):
        verified.add((i, j))

    verified = set()
    iso = 0
    iso_clu2 = 0
    iso_clu3 = 0

    for i in range(rows):
        for j in range(columns):
            if matrix[i][j] == 1 and (i, j) not in verified:
                nr_of_neighbors1 = neighbors_coordinates(i, j)
                if not nr_of_neighbors1:
                    iso += 1
                    mark_as_verified(i, j)
                elif len(nr_of_neighbors1) == 1:
                    neighbor1_row, neighbor1_column = nr_of_neighbors1[0]
                    if (neighbor1_row, neighbor1_column) not in verified:
                        nr_of_neighbors2 = neighbors_coordinates(neighbor1_row, neighbor1_column)
                        if len(nr_of_neighbors2) == 1:
                            iso_clu2 += 1
                            mark_as_verified(i, j)
                            mark_as_verified(neighbor1_row, neighbor1_column)
                elif len(nr_of_neighbors1) == 2:
                    neighbor1_row, neighbor1_column = nr_of_neighbors1[0]
                    neighbor2_row, neighbor2_column = nr_of_neighbors1[1]
                    if (neighbor1_row, neighbor1_column) not in verified and (
                    neighbor2_row, neighbor2_column) not in verified:
                        nr_of_neighbors3_1 = neighbors_coordinates(neighbor1_row, neighbor1_column)
                        nr_of_neighbors3_2 = neighbors_coordinates(neighbor2_row, neighbor2_column)
                        if len(nr_of_neighbors3_1) == 1 and len(nr_of_neighbors3_2) == 1:
                            iso_clu3 += 1
                            mark_as_verified(i, j)
                            mark_as_verified(neighbor1_row, neighbor1_column)
                            mark_as_verified(neighbor2_row, neighbor2_column)

    result = (iso, iso_clu2, iso_clu3)

    r.set(f"count:{matrix}", str(result))

    return result


def write_in_matout(iso, iso_clu2, iso_clu3, output_file):
    with open(output_file, "a") as output:
        output.write(f"{iso} {iso_clu2} {iso_clu3}\n")


def process_file(file_path):
    print(f"Processing file: {file_path}")
    output_file = file_path.replace('.in', '.out')
    with open(file_path, 'r') as input_file:
        for line in input_file:
            matrix, rows, columns = create_matrix(line)
            iso, iso_clu2, iso_clu3 = count_neighboring_ones(matrix, rows, columns)
            write_in_matout(iso, iso_clu2, iso_clu3, output_file)
    print(f"Finished processing file: {file_path}")


def main(folder_path):
    files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.in')]

    if not files:
        print("No .in files found in the folder.")
        return

    print(f"Found {len(files)} files to process.")

    num_cores = cpu_count()

    with Pool(processes=num_cores) as pool:
        pool.map(process_file, files)


if __name__ == '__main__':
    folder_path = '/home/calin2/task9'
    main(folder_path)
    end = time.time()
    print(f"The script ran for {end - start:.4} seconds.")
