#!/usr/bin/env python3
import sys

def mapper():
    for line in sys.stdin:
        for word in line.strip().split():
            print(f"{word.lower()}\t1")

def reducer():
    current_word = None
    current_count = 0

    for line in sys.stdin:
        word, count = line.strip().split("\t", 1)
        try:
            count = int(count)
        except ValueError:
            continue

        if current_word == word:
            current_count += count
        else:
            if current_word:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    if current_word:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "mapper":
        mapper()
    elif len(sys.argv) > 1 and sys.argv[1] == "reducer":
        reducer()
    else:
        sys.stderr.write("Specify either 'mapper' or 'reducer' as argument\n")
        sys.exit(1)
