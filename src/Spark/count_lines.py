def count_lines(file_path):
    line_count = 0
    with open(file_path, 'r') as file:
        for line in file:
            line_count += 1
    return line_count

file_path = '../in/search_data.sample'  # Replace with the actual file path
num_lines = count_lines(file_path)
print("Number of lines:", num_lines)