

def compare_files_unordered(reference_file, created_file, allow_extra_lines=True) -> dict:
    """
    Compare files ignoring line order.

    Args:
        reference_file: Path to reference file
        created_file: Path to created/generated file
        allow_extra_lines: If True, created file can have extra lines not in reference

    Returns:
        dict with comparison results
    """
    with open(reference_file, 'r') as f:
        reference_lines = set(line.strip()[:-1] if line.strip().endswith(',') else line.strip() for line in f if line.strip())

    with open(created_file, 'r') as f:
        created_lines = set(line.strip()[:-1] if line.strip().endswith(',') else line.strip() for line in f if line.strip())

    missing = reference_lines.difference(created_lines)
    extra = created_lines - reference_lines
    result = {
        'all_reference_lines_present': len(missing) == 0,
        'missing_lines': list(missing),
        'extra_lines': list(extra),
        'reference_count': len(reference_lines),
        'created_count': len(created_lines),
        'match_percentage': len(reference_lines & created_lines) / len(reference_lines) * 100 if reference_lines else 100
    }

    if not allow_extra_lines:
        result['exact_match'] = len(missing) == 0 and len(extra) == 0

    return result



