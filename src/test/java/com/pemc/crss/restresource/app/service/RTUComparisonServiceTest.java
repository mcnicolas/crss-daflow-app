package com.pemc.crss.restresource.app.service;

import com.pemc.crss.restresource.app.dto.GraphDto;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created on 1/10/17.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@ContextConfiguration(classes=CrssNmmsDatasourceConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class RTUComparisonServiceTest {


    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Autowired
    RTUComparisonService rtuComparisonService;


    @Test
    @Ignore
    public void test() throws ParseException {
        GraphDto result = rtuComparisonService.getRTUData(sdf.parse("2016-12-01 00:00:00"),
                sdf.parse("2016-12-01 00:00:00"), "3BAT_LIMA");

    }

}
