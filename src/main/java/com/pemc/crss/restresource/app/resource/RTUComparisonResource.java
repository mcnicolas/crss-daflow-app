package com.pemc.crss.restresource.app.resource;

import com.pemc.crss.restresource.app.dto.GroupDto;
import com.pemc.crss.restresource.app.dto.GroupScheduleDto;
import com.pemc.crss.restresource.app.dto.MTNDto;
import com.pemc.crss.restresource.app.service.RTUComparisonService;
import com.pemc.crss.shared.commons.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/rtu-comparison")
public class RTUComparisonResource {

    @Autowired
    private RTUComparisonService rtuComparisonService;


    @RequestMapping(value = "/all-group-mtn", method = RequestMethod.GET, produces = "application/json")
    public List<MTNDto> getAllGroupMtns() {
        return rtuComparisonService.getAllMTNGroupConfigs();
    }

    @RequestMapping(value = "/rtu", method = RequestMethod.GET, produces = "application/json")
    public List<Object[]> getRTU(@RequestParam String startDate,
                                 @RequestParam String endDate,
                                 @RequestParam String mtn) throws ParseException {
        if (mtn.isEmpty()) {
            return new ArrayList<>();
        }
        SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);
        return rtuComparisonService.getRTU(sdf.parse(startDate), sdf.parse(endDate), mtn);
    }

    @RequestMapping(value = "/raw", method = RequestMethod.GET, produces = "application/json")
    public List<Object[]> getRaw(@RequestParam String startDate,
                                 @RequestParam String endDate,
                                 @RequestParam String meterDataType,
                                 @RequestParam String mtn) throws ParseException {
        if (mtn.isEmpty()) {
            return new ArrayList<>();
        }
        SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);
        return rtuComparisonService.getRaw(sdf.parse(startDate), sdf.parse(endDate), meterDataType, mtn);
    }

    @RequestMapping(value = "/group-mtn", method = RequestMethod.GET)
    public List<GroupDto> getGroupsByMtn(@RequestParam String mtn) throws ParseException {
        return rtuComparisonService.getGroupsByMtn(mtn);
    }

    @RequestMapping(value = "/save-shift", method = RequestMethod.POST)
    public ResponseEntity<Boolean> saveShift(@RequestBody GroupScheduleDto dto) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);
        return new ResponseEntity<>(rtuComparisonService.saveShift(sdf.parse(dto.getEffDate()), dto.getMtnGroupName()), HttpStatus.OK);
    }

    @RequestMapping(value = "/mtn-data-shift/{type}", method = RequestMethod.GET, produces = "application/json")
    public List<Object[]> getMTNDataShift(@PathVariable String type,
                                          @RequestParam String startDate,
                                          @RequestParam String endDate,
                                          @RequestParam String mtnGroupName,
                                          @RequestParam String effDate,
                                          @RequestParam String meterDataType,
                                          @RequestParam String mtn) throws ParseException {
        if (type.equalsIgnoreCase("rtu")) {
            return getRTU(startDate, endDate, mtn);
        } else if (mtn.isEmpty()) {
            return new ArrayList<>();
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);
            return rtuComparisonService.getMTNDataShift(sdf.parse(startDate), sdf.parse(endDate), mtnGroupName, sdf.parse(effDate), meterDataType, mtn);
        }
    }

    @RequestMapping(value = "/apply-shift", method = RequestMethod.POST)
    public ResponseEntity<Boolean> applyShift(@RequestParam String mtn,
                                              @RequestParam String startDate,
                                              @RequestParam String endDate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);
        return new ResponseEntity<>(rtuComparisonService.applyShift(mtn, sdf.parse(startDate), sdf.parse(endDate)), HttpStatus.OK);
    }

    @RequestMapping(value = "/saved-sched", method = RequestMethod.GET)
    public List<GroupDto> getSavedGroupSchedulesByMtn(Pageable pageable, @RequestParam String mtn, @RequestParam String startDate, @RequestParam String endDate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);
        return rtuComparisonService.getSavedGroupSchedulesByMtn(mtn, sdf.parse(startDate), sdf.parse(endDate));
    }
}