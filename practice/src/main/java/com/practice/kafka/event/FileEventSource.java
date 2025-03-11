package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class FileEventSource implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class);
    public boolean keepRunning = true;
    private int updateInterval;
    private File file;
    private long filePointer = 0;
    private EventHandler eventHandler;

    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (keepRunning) {
                Thread.sleep(updateInterval);
                long len = file.length();
                if (len < filePointer) {
                    logger.info("file was reset as filePoint is longer than file length");
                } else if (len > filePointer) {
                    readAppendAndSend();
                } else {
                    continue;
                }
            }
        } catch (InterruptedException | ExecutionException | IOException e) {
            logger.info(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(filePointer);
        String line = null;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }
        filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String delimiter = ",";
        String[] tokens = line.split(delimiter);
        StringJoiner valueJoiner = new StringJoiner(delimiter);

        String key = tokens[0];
        IntStream.range(1, tokens.length)
                .forEach(i -> valueJoiner.add(tokens[i]));
        String value = valueJoiner.toString();
        MessageEvent messageEvent = new MessageEvent(key, value);

        eventHandler.onMessage(messageEvent);
    }
}
