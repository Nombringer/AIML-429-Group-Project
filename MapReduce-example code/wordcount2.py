#!/usr/bin/env python3
import sys
import re
import os

case_sensitive = False
patterns_to_skip = set()

def load_skip_patterns(file_path):
    try:
        with open(file_path, 'r') as f:
            for line in f:
                patterns_to_skip.add(line.strip())
    except Exception as e:
        print(f"Failed to load skip patterns: {e}", file=sys.stderr)

def apply_skip_patterns(line):
    for pattern in patterns_to_skip:
        line = re.sub(pattern, '', line)
    return line

def mapper():
    global case_sensitive

    # Parse environment variables set by Hadoop Streaming
    case_sensitive = os.environ.get("WORDCOUNT_CASE_SENSITIVE", "false").lower() == "true"
    skip_file = os.environ.get("WORDCOUNT_SKIP_PATTERNS", "")
    if skip_file:
        load_skip_patterns(skip_file)

    word_count = 0
    for line in sys.stdin:
        line = line.strip()
        if not case_sensitive:
            line = line.lower()
        line = apply_skip_patterns(line)
        for word in line.split():
            print(f"{word}\t1")
            word_count += 1

    # Emit word count to stderr for counters (can be captured separately)
    print(f"reporter:counter:Mapper Counters,INPUT_WORDS,{word_count}", file=sys.stderr)

def reducer():
    current_word = None
    current_count = 0

    for line in sys.stdin:
        word, count = line.strip().split("\t", 1)
        try:
            count = int(count)
        except ValueError:
            continue

        if word == current_word:
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
        print("Specify 'mapper' or 'reducer' as argument", file=sys.stderr)
        sys.exit(1)
