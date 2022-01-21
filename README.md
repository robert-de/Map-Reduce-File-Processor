# Map-Reduce File Processor #

    The purpose of the current assignment was to create a multi-threaded
program able to process files using the map-reduce paradigm. It relies on
a variable number of worker threads to complete tasks assigned using the
Java executor service.
    The program debuts with the parsing of program arguments, reading and
parsing the file containing the name of the files to be processed and creating
new Mapper tasks to be assigned the Executor's pool. Some care went into
ensuring the appropriately assigned length for the task handling the last
portion of a file, which may or may not have the required number of characters
to satisfy the entire fragment size.
    The mapper tasks start off at the given offset - 1 to determine if the
beginning of the fragment contains a word which is to be processed by a
separate task. It does so by checking if the currently read character is
a separator or a regular character. If it is a regular character, the
skipWord() function is called, which skips all characters before the separator
from the given reader stream and returns the number of characters skipped.
This is then used to adjust the current size of the buffer so far processed.
    It then enters a loop that processes all but the last element of the
required fragment to be processed and calls the processWord() function for
each word found. The last character is read which, in case it is not a
separator, triggers the addition of the getNextWord() function result to
the current word being formed, which is then also processed.
    After the completion of all the mapper tasks, a new Executor service is
created, which runs RunnableReducer tasks created by the main thread.
Notable additions to this process include: the sorting of the map results by
file name to ease their processing and an additional task submission after the
ending of the loop to ensure all tasks are processed.
    The RunnableReducer tasks simply iterate through all the hashtables
contained in the preReducedResults array and updated certain fields of the
end results if words of current maximum length or greater are found, as well
as adding to the sum the Fibonacci valuation. This sum is divided by the
number of words determined in the previous loop, leading to the result's
synchronized addition to the results array.
    The end results are sorted by rank and written to the desired output file.

    NOTES:
    Line by line explanations are found in source code comments.
    The program's usage is as follows:
    > "Usage: Tema2 <workers> <in_file> <out_file>"

    The files' requiring processing reading is also parallelized as it seemed
desirable in a hyper scalar environment, even though on a single hard drive
the sequential reading performance advantage is probably lost.