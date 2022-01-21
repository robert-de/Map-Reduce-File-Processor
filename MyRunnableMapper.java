import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class MyRunnableMapper implements Runnable {
    File file;
    ExecutorService tpe;
    AtomicInteger inQueue;
    int fragmentSize;
    int offset;
    String separators = ";:/?~\\.,><`[]{}()!@#$%^&-_+'=*\"| \t\r\n";
    ArrayList<MapTaskResult> results;
    long fileSize;
    int longestWordSize;

    public MyRunnableMapper(File path, ExecutorService tpe, AtomicInteger inQueue, int fragmentSize, int offset, ArrayList<MapTaskResult> results) {
        this.file = path;
        this.tpe = tpe;
        this.inQueue = inQueue;
        this.fragmentSize = fragmentSize;
        this.offset = offset;
        this.fileSize = file.length();
        this.results = results;
        longestWordSize = 0;
    }

    boolean isSeparator(int c) {
        if (separators.indexOf(c) != -1)
            return true;
        else
            return false;
    }
    // FUNCTION THAT SKIPS THE WORD FOLLOWING THE READER POINTER
    int skipWord(BufferedReader br) {
        int size = 0, c;
        try {
            while (!isSeparator(c = br.read())) {
                size += 1;

                if (offset + size >= fileSize || c == -1) {
                    break;
                }
            }

            if (isSeparator(c))
                size += 1;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return size;
    }

    // FUNCTION THAT RETURNS THE WORD FOLLOWING THE READER POINTER
    String getNextWord(BufferedReader br) {
        int size = 0, c;
        String word = "";
        try {
            while (!isSeparator(c = br.read())) {
                if (offset + size >= fileSize || c == -1) {
                    break;
                }
                word += (char) c;
                size++;
            }

            if (isSeparator(c))
                size += 1;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return word;
    }

    void processWord(String word, MapTaskResult result){

        int l = word.length();

        if (l > longestWordSize) {
            longestWordSize = l;
            result.longestWords.clear();
            result.longestWords.add(word);
        } else if (l == longestWordSize) {
            result.longestWords.add(word);
        }
        if (result.nrApps.containsKey(l)) {
            int oldSize = result.nrApps.get(l);
            result.nrApps.replace(l, oldSize + 1);
        } else {
            result.nrApps.put(l, 1);
        }

    }

    @Override
    public void run() {
        if (file.isFile()) {
            FileReader fr = null;

            try {
                fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                // CREATING A NEW RESULT FOR CURRENT TASK
                MapTaskResult result = new MapTaskResult(file.getName());

                String tempWord = "";

                int c = 0, size = 0;
                boolean eof = false;

                // IF FIRST SEGMENT IN A FILE, THERE IS NO PREVIOUS WORD TO SKIP
                if (offset > 0) {
                    try {
                        // CHECKING ONE CHARACTER BEFORE OFFSET TO DETERMINE IF A WORD STARTS
                        // IN THE DESIRED FRAGMENT
                        br.skip(offset - 1);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    c = br.read();
                    // SKIP WORD IN CURRENT FRAGMENT THAT IS ASSIGNED TO A DIFFERENT WORKER
                    if (!isSeparator(c)) {
                        size += skipWord(br);
                    }
                }

                // THE PREVIOUS WORD SKIPPING MIGHT HAVE ARRIVED AT EOF, ENSURING IT HASN'T
                if (offset + size < fileSize) {
                    for (int i = 0; i < fragmentSize - size - 1; i++) {
                        c = br.read();

                        // IF CURRENT READ CHARACTER ISN'T A SEPARATOR, ADD IT TO THE CURRENT WORD BEING CREATED
                        // OTHERWISE, PROCESS THE WORD AND RESET THE TEMPORARY WORD CONTAINER
                        if (!isSeparator(c)) {
                            tempWord += (char) c;
                        } else if (isSeparator(c) && tempWord.length()>0) {
                            processWord(tempWord, result);
                            tempWord = "";
                        }
                    }
                    // PREVIOUS LOOP ONLY WENT UNTIL THE SECOND TO LAST CHARACTER,
                    // THIS READS THE LAST ONE FOR FURTHER PROCESSING
                    c = br.read();
                } else {
                    // EOF WAS REACHED
                    eof = true;
                }
                // IF THE CHARACTER READ IS A LETTER, IT IS ADDED TO THE WORD CURRENTLY
                // BEING FORMED ALONG WITH ALL LETTERS FOLLOWING IT, OTHERWISE,
                // THE CONTAINER HAS A FULL WORD WHICH IS  THEN PROCESSED
                if (!isSeparator(c) && !eof) {
                    tempWord = tempWord + (char) c + getNextWord(br);
                    if(tempWord.length() > 0)
                        processWord(tempWord, result);
                } else if (!eof){
                    if(tempWord.length() > 0) {
                        processWord(tempWord, result);
                    }
                }

                // SYNCHRONIZED THE ADDITION OF RESULTS TO THE OUTPUT ARRAY TO PREVENT UNDEFINED BEHAVIOUR
                synchronized (separators) {
                    results.add(result);
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            // TASK IS DONE, SO THE NUMBER OF ELEMENTS IN QUEUE IS DECREMENTED
            int left = inQueue.decrementAndGet();
            if (left == 0) {
                tpe.shutdown();
            }
        } else {
            System.out.println("Nu sant file, sant director");
        }
    }


}


class MapTaskResult implements Comparable{
    String docName;
    ConcurrentHashMap<Integer, Integer> nrApps;
    ArrayList<String> longestWords;

    public MapTaskResult (String docName) {
        this.docName = docName;
        nrApps = new ConcurrentHashMap<Integer, Integer>();
        longestWords = new ArrayList<String>();
    }

    @Override
    public String toString() {
        return "MapTaskResult{" +
                "docName='" + docName + '\'' +
                ", nrApps=" + nrApps +
                ", longestWords=" + longestWords +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        String file2 = ((MapTaskResult) o).docName;
        return -file2.compareTo(this.docName);
    }
}