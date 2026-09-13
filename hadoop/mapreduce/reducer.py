#!/usr/bin/env python3
import sys
import re

PRODUCT_KEY = re.compile(r"^P\d{3}$")


def main():
    current_key = None
    running_total = 0

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        key, sep, value = line.partition("\t")
        if not sep:
            continue  # ignore anything not k=v pairs

        if key == current_key:
            # accumulate the count
            running_total += 1
        else:
            if current_key is not None and PRODUCT_KEY.match(current_key):
                print(f"{current_key}\t{running_total}")
            current_key = key
            running_total = 1  # this new line counts as 1

    # flush the last key
    if current_key is not None and PRODUCT_KEY.match(current_key):
        print(f"{current_key}\t{running_total}")


if __name__ == "__main__":
    main()

