package com.pemc.crss.dataflow.app.util;

import org.springframework.security.oauth2.provider.OAuth2Authentication;

import java.security.Principal;
import java.util.LinkedHashMap;

/**
 * Created by jdogillo on 5/9/17.
 */
public class SecurityUtil {

    public static final String ANONYMOUS = "anonymous";

    public static String getCurrentUser(Principal principal) {
        String currentUser = ANONYMOUS;
        if (principal != null) {
            if (principal instanceof OAuth2Authentication) {
                OAuth2Authentication auth = (OAuth2Authentication) principal;
                LinkedHashMap<String, Object> userDetails = (LinkedHashMap<String, Object>) auth.getPrincipal();
                return (String) userDetails.get("name");
            }
        }
        return currentUser;
    }
}
