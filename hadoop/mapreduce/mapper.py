#!/usr/bin/env python3
import sys


def main():
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        fields = line.split(",")

        # timestamp, customer_id, product_id, event_type, session_id, device,
        # location, order_id
        if len(fields) < 8:
            continue  # malformed record -> drop
        if fields[0] == "timestamp":
            continue  # header row

        product_id = fields[2]
        event_type = fields[3]

        if event_type == "VIEW":
            # (key, value) pair that the reducer will aggregate
            print(f"{product_id}\t1")
        else:
            # land everything else on a single utility key so the total
            # number of processed lines can be recovered if ever needed
            print("ALL_EVENTS\t1")


if __name__ == "__main__":
    main()

