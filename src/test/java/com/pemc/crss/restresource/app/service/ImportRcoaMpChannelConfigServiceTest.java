package com.pemc.crss.restresource.app.service;

import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.service.impl.importconfig.ImportRcoaMpChannelConfigServiceImpl;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ChannelConfig;
import com.pemc.crss.meterprocess.core.main.entity.mtn.RcoaMeteringPointChannelConfig;
import com.pemc.crss.meterprocess.core.main.repository.ConfigImportRepository;
import com.pemc.crss.meterprocess.core.main.repository.RcoaMeteringPointChannelConfigRepository;
import com.pemc.crss.shared.core.registration.entity.model.MeteringRegistration;
import com.pemc.crss.shared.core.registration.entity.model.MeteringRegistrationChannel;
import com.pemc.crss.shared.core.registration.repository.RegistrationRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockMultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ImportRcoaMpChannelConfigServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ImportRcoaMpChannelConfigServiceTest.class);

    private ClassLoader classLoader = getClass().getClassLoader();

    private String testFilesDirectory = "testfiles/rcoampchannelconfig/";

    @InjectMocks
    private ImportConfigService importMpChannelConfigService = new ImportRcoaMpChannelConfigServiceImpl();

    @Mock
    private RcoaMeteringPointChannelConfigRepository rcoaMeteringPointChannelConfigRepository;

    @Mock
    private RegistrationRepository registrationRepository;

    @Mock
    private ConfigImportRepository configImportRepository;

    @Test
    public void fileIsEmpty() throws IOException {
        ImportConfigResponseDto response = importConfig(new MockMultipartFile("mp_channel_config.csv", new byte[0]));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("File is empty.", response.getRemarks());
    }

    @Test
    public void fileIsNull() throws IOException {
        ImportConfigResponseDto response = importConfig(new MockMultipartFile("mp_channel_config.xls", (byte[]) null));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("File is empty.", response.getRemarks());
    }

    @Test
    public void invalidFileType() throws IOException {
        ImportConfigResponseDto response = importConfig(new MockMultipartFile("rcoampchcfg.xls", new byte[1]));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("File is not csv.", response.getRemarks());
    }

    @Test
    public void noChannelConfig() throws IOException {
        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_nochannelconfig.csv"));
        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1 has no sein and/or channel config.", response.getRemarks());
    }

    @Test
    public void recordHasMoreThanThreeValues() throws IOException {
        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_recordhasmorethan3values.csv"));
        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1 has more than the maximum values.", response.getRemarks());
    }

    @Test
    public void seinNotFoundInMirf() throws IOException {
        when(registrationRepository.findBySein("ABC123"))
                .thenReturn(new ArrayList<>());
        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_invalidseinchconfigname.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1: Sein not found in MIRF.", response.getRemarks());
    }

    @Test
    public void meterTypeIsNotRcoa() throws IOException {

        MeteringRegistration reg1 = new MeteringRegistration();
        reg1.setSein("MF3MMEXRASL02");
        reg1.setMeterType("WESM");

        List<MeteringRegistration> meteringRegistrationList = new ArrayList<>();
        meteringRegistrationList.add(reg1);

        when(registrationRepository.findBySein("MF3MMEXRASL02")).thenReturn(meteringRegistrationList);

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1: Meter type is not RCOA.", response.getRemarks());
    }

    @Test
    public void invalidChannelConfigName() throws IOException {
        when(registrationRepository.findBySein("ABC123"))
                .thenReturn(createMeteringRegistration("ABC123", null));
        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_invalidseinchconfigname.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1: DEF is invalid channel config name.", response.getRemarks());
    }

    @Test
    public void duplicateMpChannelConfigInDb() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01", new HashSet<>()));
        when(rcoaMeteringPointChannelConfigRepository.findBySeinAndChannelConfig("MF3MABAMSUZ01", ChannelConfig.DEL))
                .thenReturn(new RcoaMeteringPointChannelConfig());
        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_duplicatemp.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1: RCOA Metering Point Channel Config already exist in the database.", response.getRemarks());
    }

    @Test
    public void invalidLoadChannelForDelChConfig() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01",
                        createMeteringRegistrationChannel(new String[]{"kvarhd"})));

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_invalidloadchannels_del.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1: Invalid channel config DEL", response.getRemarks());
    }

    @Test
    public void invalidLoadChannelForRecChConfig() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01",
                        createMeteringRegistrationChannel(new String[]{"kwhd"})));

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_invalidloadchannels_rec.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1: Invalid channel config REC", response.getRemarks());
    }

    @Test
    public void invalidLoadChannelForNetChConfig_kwhrIsMissig() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01",
                        createMeteringRegistrationChannel(new String[]{"kwhd"})));

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_invalidloadchannels_net.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 1: Invalid channel config NET", response.getRemarks());
    }

    @Test
    public void invalidLoadChannelForNetChConfig_kwhdIsMissig() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01",
                        createMeteringRegistrationChannel(new String[]{"kwhr", "kwhd"})));

        when(registrationRepository.findBySein("MF3MABAMSUZ02"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ02",
                        createMeteringRegistrationChannel(new String[]{"kwhr"})));

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_invalidloadchannels_net.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 2: Invalid channel config NET", response.getRemarks());
    }

    @Test
    public void mpSetToOtherChCfg() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01",
                        createMeteringRegistrationChannel(new String[]{"kwhr", "kwhd"})));

        when(registrationRepository.findBySein("MF3MABAMSUZ02"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ02",
                        createMeteringRegistrationChannel(new String[]{"kwhd"})));

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_mpsettootherchcfg.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 2: RCOA Metering Point Channel Config is already set to other channel config.", response.getRemarks());
    }

    @Test
    public void duplicateMp() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01",
                        createMeteringRegistrationChannel(new String[]{"kwhd"})));

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg_duplicatemp.csv"));

        Assert.assertEquals("Failed", response.getStatus());
        Assert.assertEquals("Record 2: RCOA Metering Point Channel Config already exists in the import file.", response.getRemarks());
    }

    @Test
    public void importRcoaMpChCfg() throws IOException {
        when(registrationRepository.findBySein("MF3MABAMSUZ01"))
                .thenReturn(createMeteringRegistration("MF3MABAMSUZ01",
                        createMeteringRegistrationChannel(new String[]{"kwhd", "kwhr"})));

        when(registrationRepository.findBySein("MF3MMEXRASL02"))
                .thenReturn(createMeteringRegistration("MF3MMEXRASL02",
                        createMeteringRegistrationChannel(new String[]{"kwhd", "kwhr"})));

        when(rcoaMeteringPointChannelConfigRepository.findBySein("MF3MABAMSUZ01")).thenReturn(new ArrayList<>());
        when(rcoaMeteringPointChannelConfigRepository.findBySein("MF3MMEXRASL02")).thenReturn(new ArrayList<>());

        ImportConfigResponseDto response = importConfig(createMockMultipartFile(testFilesDirectory + "rcoampchcfg.csv"));

        Assert.assertEquals("Completed", response.getStatus());
    }


    private Set<MeteringRegistrationChannel> createMeteringRegistrationChannel(String[] loadChannels) {
        Set<MeteringRegistrationChannel> meteringRegistrationChannels = new HashSet<>();
        for (String ch : loadChannels) {
            MeteringRegistrationChannel channel = new MeteringRegistrationChannel();
            channel.setName(ch);
            meteringRegistrationChannels.add(channel);
        }
        return meteringRegistrationChannels;
    }

    private List<MeteringRegistration> createMeteringRegistration(String sein, Set<MeteringRegistrationChannel> loadChannels) {
        MeteringRegistration reg1 = new MeteringRegistration();
        reg1.setSein(sein);
        reg1.setMeteringRegistrationChannelList(loadChannels);
        reg1.setMeterType("RCOA");
        List<MeteringRegistration> meteringRegistrationList = new ArrayList<>();
        meteringRegistrationList.add(reg1);
        return meteringRegistrationList;
    }

    private MockMultipartFile createMockMultipartFile(String fileDirectory) throws IOException {
        InputStream file = new FileInputStream (new File(classLoader.getResource(fileDirectory).getFile()));
        return new MockMultipartFile("mp_channel_config.csv",
                "mp_channel_config.csv", "/text/csv", file);
    }

    private ImportConfigResponseDto importConfig(MockMultipartFile file) throws IOException {
        Date importDate = new Date();
        return importMpChannelConfigService.importConfig("RCOA Metering Point Channel Configuration",
                file, importDate);
    }
}
