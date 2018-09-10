package es.us.idea.adt;

import com.bazaarvoice.jolt.Chainr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoltGettingStartedJava {

    public static void main(String[] args) {

        // specs
        Map<String, Object> spec = new HashMap<>();
        spec.put("tarifa", "T");

        Map<String, Object> m = new HashMap<String, Object>();
        m.put("operation", "shift");
        m.put("spec", spec);

        List<Object> specs = new ArrayList<>();
        specs.add(m);

        // Input
        Map<String, Object> input = new HashMap<>();
        input.put("tarifa", "3.0A");

        Chainr chainr = Chainr.fromSpec(specs);

        Object output = chainr.transform(input);

        System.out.println(output);
        System.out.println(output instanceof Map);

    }

}
