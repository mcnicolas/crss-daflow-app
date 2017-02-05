package com.pemc.crss.restresource.app.util;

import com.pemc.crss.restresource.app.exception.InvalidFileException;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ChannelConfig;
import com.pemc.crss.meterprocess.core.main.entity.mtn.MTNLocation;
import org.springframework.web.multipart.MultipartFile;

public class ValidationUtil {

    public static boolean isChannelConfigNameValid(String channelConfig) {
        for (ChannelConfig config : ChannelConfig.values()) {
            if (config.name().equalsIgnoreCase(channelConfig)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isMtnLocationValid(String mtnLocation) {
        for (MTNLocation loc : MTNLocation.values()) {
            if (loc.name().equalsIgnoreCase(mtnLocation)) {
                return true;
            }
        }
        return false;
    }

    public static void validateConfigFile(MultipartFile configFile) throws InvalidFileException {
        if (configFile.isEmpty()) {
            throw new InvalidFileException("File is empty.");
        }

        if (!configFile.getOriginalFilename().endsWith("csv")) {
            throw new InvalidFileException("File is not csv.");
        }
    }

    public static boolean validateRecordMaxValue(int recordSize, int maxValue) {
        return recordSize <= maxValue;
    }
}
