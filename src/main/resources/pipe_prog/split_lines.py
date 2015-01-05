#!/usr/bin/python
while True:
    try:
        line = raw_input()
        words = line.split(" ")
        for word in words:
            print word
    except (EOFError):
        break