import os

def to_relative_path(absolute_path, start_dir=None):
  """Converts an absolute path to a relative path.

  Args:
    absolute_path: The absolute path to convert.
    start_dir: The directory relative to which the path should be.
               If None, the current working directory is used.

  Returns:
    The relative path, or the original path if an error occurs or if paths are on different drives.
  """
  try:
    relative_path = os.path.relpath(absolute_path, start=start_dir)
    return relative_path
  except ValueError as e:
      print(f"Error: {e}")
      return absolute_path
  except Exception as e:
      print(f"An unexpected error occurred: {e}")
      return absolute_path

# Example usage:
FLINK_PROJECT=os.getenv("HOME") + "/Code/customers/master-control/data-platform-flink"
SRC_FOLDER=os.getenv("HOME") + "/Code/customers/master-control/de-datawarehouse/models"
apth=os.path.commonpath([FLINK_PROJECT, SRC_FOLDER])
print(apth)
stg=os.path.dirname(FLINK_PROJECT).replace(apth, "",1)[1:]
print(stg)
absolute_path = "/home/user/documents/file.txt"
current_dir = os.getcwd()
relative_path = to_relative_path(absolute_path)
print(f"Absolute path: {absolute_path}")
print(f"Current directory: {current_dir}")
print(f"Relative path: {relative_path}")

# Example with a specified start directory:
start_dir = "/home/user"
relative_path_with_start = to_relative_path(absolute_path, start_dir)
print(f"Relative path from {start_dir}: {relative_path_with_start}")

#Handling edge cases
absolute_path_different_drive = "D:/another_folder/file.txt"
relative_path_different_drive = to_relative_path(absolute_path_different_drive)
print(f"Relative path from {current_dir} to {absolute_path_different_drive}: {relative_path_different_drive}")