package fiap;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Set;

@Path("/apichat")
public class GreetingResource {
    
    // REST API
    static protected Set<String> msgs = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<>()));
    
    @Inject
    StartWebSocket socket;

    @GET
    public Set<String> list() {
        return msgs;
    }

    @POST
    public Set<String> add(String message) {
        String username = "APICHAT";
        msgs.add(message);
        socket.broadcast(">> " + username + ": " + message + "\n");
        return msgs;
    }
    
}
