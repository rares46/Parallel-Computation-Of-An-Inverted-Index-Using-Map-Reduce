# Parallel-computation-of-an-inverted-index-in-Pthreads-using-the-Map-Reduce
The index will contain a list of all different words from the input files along with the IDs of the files in which they are found.

The program will receive as parameters the number of mapper threads, reducer threads, and a file containing the list of files to be processed.

First, the file with the list of files is read and sorted based on file size to distribute the workload evenly among the mapper threads.

Structures are used to pass arguments to the main mapper and reducer functions. These structures contain fields of various data types and are passed as arguments when the threads are created.

The mapper threads are responsible for saving each word from every file into new files named "partial_(letter).txt", based on the first letter of the word. For example, all words starting with "a" are stored in the file "partial_a.txt". Each word is saved along with the ID of the file in which it was found.

Before the reducers start their work, all mappers must complete their tasks using a join.
The reducer threads are assigned to process partial files corresponding to specific letters. They consolidate all distinct occurrences of a word into a single entry, along with the IDs of all the files where the word appears. This forms the final result.
