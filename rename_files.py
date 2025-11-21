import os

directory_path = "./photos"

try:
    for filename in os.listdir(directory_path):
        if filename.endswith(".png"):
            old_filepath = os.path.join(directory_path, filename)

            new_filename = "downloaded-" + filename
            new_path = os.path.join(directory_path, new_filename)

            os.rename(old_filepath, new_path)

            print(f"Renamed {filename} to {new_filename}")

except FileNotFoundError:
    print(f"Directory {directory_path} not found")

except OSError as e:
    print(f"Error processing files in directory: {e}")

print("================== DONE ============================")
