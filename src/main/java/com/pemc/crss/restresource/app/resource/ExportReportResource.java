package com.pemc.crss.restresource.app.resource;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.pemc.crss.shared.commons.util.SSHUtil;
import com.pemc.crss.shared.commons.util.ZipUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@RestController
@RequestMapping("/reports/download")
public class ExportReportResource {

    @Value("${sftp.credentials.username}")
    private String username;
    @Value("${sftp.credentials.password}")
    private String password;
    @Value("${sftp.credentials.host}")
    private String host;
    @Value("${sftp.credentials.port}")
    private int port;
    @Value("${sftp.credentials.privateKey}")
    private String privateKey;
    @Value("${sftp.credentials.workingDirectory}")
    private String workingDirectory;

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportReportResource.class);

    @RequestMapping(value = "/zip", produces = "application/zip")
    public ResponseEntity downloadReports(@RequestParam int version,
                                          @RequestParam boolean isDaily,
                                          @RequestParam(required = false) String processType,
                                          @RequestParam String tradingDate,
                                          @RequestParam String runDate,
                                          HttpServletResponse response) {

        try {
            LOGGER.debug("Downloading file structure: {}", workingDirectory+"/"+version);

            SSHUtil sshUtils = new SSHUtil(username, host, password, port, privateKey);

            File file = new File("/tmp/"+version);
            File zip = new File("/tmp/"+version+".zip");

            if (!file.exists() && !zip.exists()) {
                sshUtils.downloadDir(workingDirectory+"/"+version, "/tmp/"+version);
                ZipUtil.zip(file, zip);
            } else if (file.exists() && !zip.exists()) {
                ZipUtil.zip(file, zip);
            }

            String zipFilename = isDaily ? "\"Daily+" + tradingDate + "_" + runDate + ".zip\"" : "\"" + (processType != null ? processType : "") + "+" + tradingDate + "_" + runDate + ".zip\"";
            response.addHeader("Content-Disposition", "attachment; filename=" + zipFilename);
            response.getOutputStream().write(Files.readAllBytes(zip.toPath()));

//            FileUtils.forceDelete(file);
//            FileUtils.forceDelete(zip);

            sshUtils.closeConnection();

        } catch (IOException | JSchException | SftpException e) {
            LOGGER.error("Exception occurred while downloading mq report zip file: ", e);
        }

        return new ResponseEntity(HttpStatus.OK);
    }
}
