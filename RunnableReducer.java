import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RunnableReducer implements Runnable {
    String docName;
    ArrayList<ConcurrentHashMap<Integer, Integer>> preReducedResults = new ArrayList<ConcurrentHashMap<Integer, Integer> >();
    ArrayList<String> Words = new ArrayList<String>();
    AtomicInteger inQueue;
    ArrayList<ReducedResult> reducedResultsList;
    ExecutorService tpe;
    String mutex;

    public RunnableReducer (String docName, AtomicInteger inQueue, ArrayList<ReducedResult> reducedResultsList, ExecutorService tpe, final String mutex) {
        this.inQueue = inQueue;
        this.docName = docName;
        this.reducedResultsList = reducedResultsList;
        this.tpe = tpe;
        this.mutex = mutex;
    }


    @Override
    public void run() {
        int nrWords = 0;
        // THE NEW RESULT TO BE ADDED TO THE RESULTS ARRAY AFTER COMPLETION
        ReducedResult rr = new ReducedResult();
        rr.maxLen = 0;
        rr.docName = this.docName;

        // https://en.wikipedia.org/wiki/The_Game_(mind_game), TRYING TO MAXIMIZE LOSSES
        // THE FOLLOWING LOOP PROCESSES ALL ENTRIES FROM THE MAPPING RESULTS PROCESSED BY THE
        // WORKER COORDINATOR, NOT MUCH THOUGHT WENT INTO IT
        for (ConcurrentHashMap<Integer, Integer> theGame : preReducedResults) {
            for (Map.Entry<Integer, Integer> entry : theGame.entrySet()) {
                Integer entryVal= entry.getValue();
                Integer entryLen= entry.getKey();

                // IF A WORD OF CURRENT MAX LEN OR GREATER IS FOUND, THE RESULT IS UPDATED
                if (entryLen > rr.maxLen) {
                    rr.maxLen = entryLen;
                    rr.nrMaxLenWords = entryVal;
                } else if (entryLen == rr.maxLen) {
                    rr.nrMaxLenWords += entryVal;
                }

                rr.rank += entryVal * fibValuation(entryLen + 1);
                nrWords += entryVal;

            }
        }

        rr.rank /= (float) nrWords;

        // SYNCHRONIZING THE ADDITION OF THE REDUCED RESULTS TO THE LIST
        // IN THE PREVIOUS EXAMPLE, THE SEPARATOR STRING WAS A GOOD OPTION FOR A LOCK
        // IN THIS CASE, A NEW FINAL STRING WAS USED TO PREVENT UNDEFINED BEHAVIOUR
        synchronized (mutex) {
            reducedResultsList.add(rr);
        }

        int left = inQueue.decrementAndGet();
        if (left == 0) {
            tpe.shutdown();
        }

    }

    // ITERATIVE FIBONACCI SUM FUNCTION
    public int fibValuation(int i) {
        if (i <= 1) return i;

        int a = 1, b = 1, tmp;
        for (int j = 2; j < i; j ++) {
            tmp = a;
            a += b;
            b = tmp;
        }

        return a;
    }

    public String toString() {
        return "RunRed{" +
                "docName='" + docName + '\'' +
                ", results =" + reducedResultsList +
                '}';
    }
}

class ReducedResult implements Comparable {
    String docName;
    float rank;
    int maxLen;
    int nrMaxLenWords;

    @Override
    public String toString() {
        return "ReducedResult{" +
                "docName='" + docName + '\'' +
                ", rank=" + rank +
                ", maxLen=" + maxLen +
                ", nrMaxLenWords=" + nrMaxLenWords +
                '}';
    }

    // METHOD USED IN SORTING THE FINAL RESULTS BASED ON RANKS
    @Override
    public int compareTo(Object o) {
        ReducedResult rr2 = (ReducedResult) o;
        // MULTIPLICATION BY A SUFFICIENTLY LARGE NUMBER TO ACCOUNT FOR PRECISION LOSS
        // DUE TO ROUNDING
        return Math.round(1000 * rr2.rank - 1000 * this.rank);
    }
}
