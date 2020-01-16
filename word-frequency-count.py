#!/usr/bin/env python3
# Full name:             Christopher R. Bilger
# TUID:                  915310826
# Assignment name:       Homework 1: Scalability in the Small - Achieving concurrency with threads
# Course number:         CIS 4307
# Semester:              Spring 2020


import math
import re
import sys
import threading
import time

totalWordsInDocument = {}
runningTime = 0

def getLinesFromFilename(filename):
    lines = []
    with open(filename, "r") as document:
        for line in document:
            lines.append(line)

    return lines

def chunk(xs, n):
    L = len(xs)
    assert 0 < n <= L
    s, r = divmod(L, n)
    chunks = [xs[p:p+s] for p in range(0, L, s)]
    chunks[n-1:] = [xs[-r-s:]]
    return chunks

def countWordsInLine(line):
    totalWords = 0
    
    for item in line:
        # Clean the line of words
        item = re.sub('[\"\'!@#$%^&*()_+-=`~\{\}\[\]:;,<.>\?\/]', '', item)

        # Iterrate over the line word-by-word to count the number of each unique word
        # Also, convert all "words" to lowercase
        for word in item.lower().split():
            if word in totalWordsInDocument:
                totalWordsInDocument[word] += 1
            else:
                totalWordsInDocument[word] = 1
                
    return

def main():
    argumentLength = len(sys.argv)
    if argumentLength < 3:
        print("Correct syntax: " + sys.argv[0] + " <input filename> <number of threads>")
        pass

    # Initialize argument variables
    filename = str(sys.argv[1])
    numThreads = int(sys.argv[2])

    # Required output
    print("Number of threads: " + str(numThreads))
    print("Input filename: " + str(filename))

    # Get the individual lines from the file
    lines = getLinesFromFilename(filename)
    lineCount = len(lines)
    lineChunks = list(chunk(lines, numThreads))
    
    # Create N threads and give them each their tasks
    threads = []
    for i in range(numThreads):
        thread = threading.Thread(target=countWordsInLine,
                                  args=(lineChunks[i],))
        threads.append(thread)
        thread.start()
        continue
    
    for i in range(numThreads):
        threads[i].join()
        continue

    return
    

if __name__ == "__main__":
    startTime = time.time()
    main()
    runningTime = time.time() - startTime

    print("Elapsed running time: " + str(runningTime) + " seconds")
    print("Word Counter:")
    print(sorted(totalWordsInDocument.items(), key=lambda kv:(kv[1], kv[0])))
    pass
