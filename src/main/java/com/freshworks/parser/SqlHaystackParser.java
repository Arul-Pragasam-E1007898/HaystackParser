package com.freshworks.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class SqlHaystackParser {
    public static void main(String args[]) throws IOException {
        FileReader reader = new FileReader();
        String content = reader.read("./response.json");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(content);
        JsonNode hits = jsonNode.get("body").get("responses").get(0).get("hits").get("hits");
        List<Map<String, String>> messages = process((ArrayNode) hits);
        System.out.println(messages.size());


        HashMap<String, Integer> message = new HashMap<>();
        for (Map<String, String> messageMap : messages) {
            String sql = messageMap.get("msg");
            if(message.containsKey(sql)) {
                Integer integer = message.get(sql);
                message.put(sql, integer + 1);
            } else {
                message.put(sql, 1);
            }
        }

        for (Map.Entry<String, Integer> entry : sortByValue(message).entrySet()) {
            System.out.println(entry.getKey() + "::" + entry.getValue());
        }

    }

    public static HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list = new LinkedList<>(hm.entrySet());

        // Sort the list
        list.sort((o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
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
