package com.xaohii;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReduceUtil {

    public Map<String, Integer> reduce(String key, List<Integer> value) {
        int count = 0;
        for (Integer v : value) {
            count += v;
        }
        Map<String, Integer> res = new HashMap<>();
        res.put(key, count);
        return res;
    }
}
