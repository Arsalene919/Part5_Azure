from collections import defaultdict

def main(Mapresults: list) -> dict:
    grouped = defaultdict(list)
    for result in Mapresults:
        for key, value in result:
            grouped[key].append(value)
    return grouped
