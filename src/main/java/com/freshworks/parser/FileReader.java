package com.freshworks.parser;

import java.io.IOException;
import java.io.InputStream;

public class FileReader {
    public String read(String file) throws IOException {
        // Use the class loader to load the resource
        ClassLoader classLoader = this.getClass().getClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(file)) {
            if (inputStream == null) {
                throw new IllegalArgumentException("File not found! " + file);
            }

            StringBuilder content = readContent(inputStream);

            return content.toString();
        }
    }

    private static StringBuilder readContent(InputStream inputStream) throws IOException {
        // Read from the input stream
        int bytesRead;
        byte[] buffer = new byte[1024];
        StringBuilder content = new StringBuilder();
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            content.append(new String(buffer, 0, bytesRead));
        }
        return content;
    }
}
