//import RunnableReducer;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Tema2 {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }
        int fragmentSize = 0;
        int numberOfDocuments;
        // CONTAINERS FOR STORING RESULTS
        ArrayList<MapTaskResult> mapTaskResults = new ArrayList<MapTaskResult>();
        ArrayList<ReducedResult> reducedResultsList = new ArrayList<ReducedResult>();
        // NUMBER OF TASKS IN THE WORKERS' POOL
        AtomicInteger inQueue = new AtomicInteger(0);
        // NUMBER OF THREADS FOR THE EXECUTOR SERVICE READ FROM PROGRAM ARGUMENTS
        ExecutorService tpe = Executors.newFixedThreadPool(Integer.parseInt(args[0]));

        try {
            BufferedReader bfr = new BufferedReader(new FileReader(args[1]));

            fragmentSize = Integer.parseInt(bfr.readLine());
            numberOfDocuments = Integer.parseInt(bfr.readLine());

            for (int i = 0; i < numberOfDocuments; i++) {
                String fileName = bfr.readLine();
                File newFile = new File(fileName);
                int fileLength = (int) newFile.length();

                // CREATING OFFSETS FOR THE TASKS, AS WELL AS ADDING THEM TO THE POOL
                for (int j = 0; j < fileLength; j+= fragmentSize) {
                    inQueue.incrementAndGet();

                    // ENSURING THE LAST TASK FOR A CERTAIN FILE HAS APPROPRIATE SIZE
                    if (j + fragmentSize < newFile.length()) {
                        tpe.submit(new MyRunnableMapper(newFile, tpe, inQueue, fragmentSize, j, mapTaskResults));
                    } else {
                        tpe.submit(new MyRunnableMapper(newFile, tpe, inQueue, fileLength - j, j, mapTaskResults));
                    }
                }
            }
            // AWAITING COMPLETION OF ALL TASKS BEFORE PROCEEDING TO PROCESS THEM FOR REDUCING
            long maxTimeout = 5;
            tpe.awaitTermination(maxTimeout, TimeUnit.SECONDS);

            // CREATING A NEW EXECUTOR SERVICE
            inQueue = new AtomicInteger(0);
            tpe = Executors.newFixedThreadPool(Integer.parseInt(args[0]));

            // LOCK USED FOR THE REDUCER WORKERS
            final String mutex = "mutex";
            String prevDoc = "";
            // THE INSTANCE OF RUNNABLE REDUCER CREATED HERE IS SOON DISCARDED
            // IT WAS DONE TO PREVENT A VARIABLE NOT INSTANTIATED ERROR
            RunnableReducer runRed = new RunnableReducer("IDEwon'tletmecompileotherwise", inQueue, reducedResultsList, tpe, mutex);

            // avoids using a map for all the different files by sorting the task results
            Collections.sort(mapTaskResults);

            // ADDED IN ORDER FOR THE EXECUTOR TO NOT SHUTDOWN BEFORE ADDING THE LAST TASK
            inQueue.incrementAndGet();

            for (MapTaskResult result : mapTaskResults) {
                // SINCE THE ARRAY OF RESULTS IS SORTED BY NAME,
                // THE APPEARANCE OF A NEW STRING MEANS A NEW REDUCER
                // TASK MUST BE CREATED
                if  (prevDoc.compareTo(result.docName) != 0) {
                    // THE CURRENT TASK IS ONLY SUBMITTED TO THE POOL ONLY IF THE CURRENT PREVIOUS DOCUMENT
                    // ISN'T THE INITIAL BLANK STRING, MEANING THE FIRST DOCUMENT IS BEING PROCESSED
                    if (prevDoc != "") {
                        inQueue.incrementAndGet();
                        tpe.submit(runRed);
                    }

                    prevDoc = result.docName;
                    runRed = new RunnableReducer(prevDoc, inQueue, reducedResultsList, tpe, mutex);
                }

                // THE MAP TASK RESULTS ARE TRANSFERRED
                runRed.preReducedResults.add(result.nrApps);
                for (String word : result.longestWords) {
                    runRed.Words.add(word);
                }

            }
            // SUBMITTING THE LAST TASK
            tpe.submit(runRed);
            // WAITING FOR ALL TASKS TO FINISH BEFORE OUTPUTTING TO FILE
            tpe.awaitTermination(maxTimeout, TimeUnit.SECONDS);

            //SORTING BY RANK
            Collections.sort(reducedResultsList);

            File outputFile = new File(args[2]);
            outputFile.createNewFile();

            FileWriter myWriter = new FileWriter(args[2]);

            // OUTPUTTING THE FINAL RESULTS IN THE REQUIRED FORMAT
            for (ReducedResult rrr : reducedResultsList) {
                myWriter.write(rrr.docName + "," + String.format("%.2f", rrr.rank) + "," + rrr.maxLen + "," + rrr.nrMaxLenWords + "\n");
            }

            myWriter.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}


