package com.consdata.kouncil.track;

import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class DestinationStore {

    /**
     * (SessionId, Destination)
     */
    private final Map<String, String> activeDestinations = new HashMap<>();

    public void registerDestination(String sessionId, String destination) {
        activeDestinations.put(sessionId, destination);
    }

    public void unregisterDestination(String sessionId) {
        activeDestinations.remove(sessionId);
    }

    public boolean destinationIsActive(String destination) {
        return activeDestinations.containsValue(destination);
    }

    public Map<String, String> getActiveDestinations() {
        return this.activeDestinations;
    }
}
