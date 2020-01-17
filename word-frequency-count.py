#!/usr/bin/env python3
# Full name:             Christopher R. Bilger
# TUID:                  915310826
# Assignment name:       Homework 1: Scalability in the Small - Achieving concurrency with threads
# Course number:         CIS 4307
# Semester:              Spring 2020


import math
import os
import re
import sys
import threading
import time

totalWordsInDocument = {}
runningTime = 0
outputFilename = "hw1_out.txt"


def getLinesFromFilename(filename):
    # Initialize the list of all lines
    lines = []

    # Add all lines from the opened file to the previously initialized list
    with open(filename, "r") as document:
        for line in document:
            lines.append(line)
            continue
        pass

    # Return the list of all lines in the previously opened file
    return lines


def chunkList(array, numChunks):
    # Get the number of elements in the list
    length = len(array)

    # Simple assertion check to verify that we are creating more than 0 chunks,
    # and less than or the same number of chunks as elements in the list
    assert (0 < numChunks <= length)

    # Get the quotient and remainder of (length % n)
    q, r = divmod(length, numChunks)

    # "Chunk" the original list into a list of lists containing
    # the individual lines as elements
    allChunks = [array[p:(p+q)] for p in range(0, length, q)]
    allChunks[numChunks-1:] = [array[(-r)-q:]]

    # Return the list of lists created from the original list named "array"
    return allChunks


def countWordsInLine(line):
    totalWords = 0

    for item in line:
        # Clean the line of words
        item = re.sub('[\"\'!@#$%^&*()_+-=`~{}[]:;,<.>?/]', '', item)

        # Iterrate over the line word-by-word to count the number of
        # each unique word, and also convert all "words" to lowercase.
        for word in item.lower().split():
            if word in totalWordsInDocument:
                totalWordsInDocument[word] += 1
            else:
                totalWordsInDocument[word] = 1

    return


def addStringToFile(filename, string):
    # Open the file named "filename" and append write the string to it
    with open(filename, "a+") as document:
        document.write(str(string))


def main():
    # Make sure that the correct number of arguments are present
    # before starting the bulk program
    argumentLength = len(sys.argv)
    if argumentLength < 3:
        print("Correct syntax: "
              + sys.argv[0]
              + " <input filename> <number of threads>")
        return

    # Initialize argument variables
    filename = str(sys.argv[1])
    numThreads = int(sys.argv[2])

    # Required output
    addStringToFile(outputFilename, "Number of threads: " + str(numThreads) + "\n")
    addStringToFile(outputFilename, "Input filename: " + str(filename) + "\n")

    # Get the individual lines from the file
    lines = getLinesFromFilename(filename)
    lineCount = len(lines)
    lineChunks = list(chunkList(lines, numThreads))

    # Create N threads and give them each their tasks
    threads = []
    for i in range(numThreads):
        thread = threading.Thread(target=countWordsInLine,
                                  args=(lineChunks[i],))
        threads.append(thread)
        thread.start()
        continue

    # Wait for all threads to finish before returning out
    # of this function
    for i in range(numThreads):
        threads[i].join()
        continue

    return


if __name__ == "__main__":
    # If "hw1_out.txt" exists, delete it
    if os.path.exists(outputFilename):
        os.remove(outputFilename)
    
    # Start the main program and time it
    startTime = time.time()
    main()
    runningTime = time.time() - startTime

    # Some function calls to display general data
    addStringToFile(outputFilename,
                    "Elapsed running time: " + str(runningTime) + " seconds\n")
    addStringToFile(outputFilename, "Word Counter:\n")
    addStringToFile(outputFilename, sorted(totalWordsInDocument.items(),
                                          key=lambda kv: (kv[1], kv[0])))
    pass
