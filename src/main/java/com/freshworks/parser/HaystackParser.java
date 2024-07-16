package com.freshworks.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class HaystackParser {
    public static void main(String args[]) throws IOException {
        FileReader reader = new FileReader();
        String content = reader.read("./response.json");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(content);
        JsonNode hits = jsonNode.get("body").get("responses").get(0).get("hits").get("hits");
        List<Map<String, String>> messages = process((ArrayNode) hits);
        System.out.println(messages.size());


        TreeMap<Integer, String> message = new TreeMap<>();
        for (Map<String, String> messageMap : messages) {
            Integer key = Integer.parseInt(messageMap.get("batch_seq"));
            if (message.containsKey(key)) {
                System.out.println("Duplicate: " + key);
            } else {
                message.put(key, messageMap.get("msg"));
            }
        }
    }

    private static List<Map<String, String>> process(ArrayNode hits) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, String>> collection = new ArrayList<>();
        for(int i=0;i<hits.size();i++) {
            JsonNode hit = hits.get(i);
            String message = hit.get("_source").get("message").textValue();
            JsonNode messageJson = objectMapper.readTree(message);
            collection.add(parse(messageJson));
        }
        return collection;
    }

    private static Map<String, String> parse(JsonNode messageJson) {
        Iterator<Map.Entry<String, JsonNode>> iterator = messageJson.fields();
        Map<String, String> map = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            map.put(entry.getKey(), entry.getValue().textValue());
        }
        return map;
    }
}
