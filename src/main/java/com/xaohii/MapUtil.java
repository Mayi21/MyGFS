package com.xaohii;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MapUtil {

    public Map<String, Integer> map(String text) {
        String[] s = text.split("[,;\\s!]+");
        Map<String, Integer> res = new HashMap<>();
        for (String v : s) {
            if (res.containsKey(v)) {
                res.put(v, res.get(v) + 1);
            } else {
                res.put(v, 1);
            }
        }
        return res;
    }
}
