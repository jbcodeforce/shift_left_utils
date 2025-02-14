import os


TABLES_TO_PROCESS="./process_out/tables_to_process.txt"


# DEAD CODE !


def update_with_uri(file: str):
    """
    """
    new_lines=set()
    file_paths=build_all_file_inventory()
    with open(file, 'r') as f:
        old_entries=set(line.strip() for line in f)
        for base_name in old_entries:
            found=False
            for apath in file_paths:
                if base_name+'.' in apath:
                    new_lines.add(f"{base_name},{apath}")
                    found=True
                    break
            if not found:
                new_lines.add(f"{base_name},None")
    print(len(old_entries))
    return new_lines

def _testcase1():
    new_content=update_with_uri(TABLES_TO_PROCESS)
    with open(TABLES_TO_PROCESS, 'w') as file:
        for line in new_content:
            file.write(line + "\n")


if __name__ == "__main__":
    _testcase1()
    