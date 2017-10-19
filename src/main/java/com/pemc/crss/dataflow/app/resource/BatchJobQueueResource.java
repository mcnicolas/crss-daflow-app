package com.pemc.crss.dataflow.app.resource;

import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.BatchJobQueueDisplay;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/job-queue")
public class BatchJobQueueResource {

    @Autowired
    private BatchJobQueueService batchJobQueueService;

    @PostMapping
    public ResponseEntity<Page<BatchJobQueueDisplay>> getAllWithStatus(@RequestBody PageableRequest pageRequest) {

        String status = pageRequest.getMapParams().getOrDefault("status", null);
        QueueStatus queueStatus = status != null ? QueueStatus.valueOf(status) : null;
        Pageable pageable = pageRequest.getPageable();

        Page<BatchJobQueue> jobQueuePage = batchJobQueueService.getAllWithStatus(queueStatus, pageable);
        List<BatchJobQueueDisplay> jobQueueDisplays = jobQueuePage.getContent().stream().map(BatchJobQueueDisplay::new)
                .collect(Collectors.toList());
        return ResponseEntity.ok(new PageImpl<>(jobQueueDisplays, pageable, jobQueuePage.getTotalElements()));
    }

    @PutMapping("/{id}/update-status")
    public void updateStatus(@PathVariable("id") Long id, @RequestParam("status") QueueStatus status) {
        log.debug("Request for updating status of job queue, id = {}, status = {}", id, status);
        batchJobQueueService.updateStatus(id, status);
    }

    @GetMapping("/get-status-names")
    public ResponseEntity<Map<QueueStatus, String>> getStatusNames() {
        Map<QueueStatus, String> statusNames = Maps.newTreeMap();
        for (QueueStatus statusName : QueueStatus.values()) {
            statusNames.put(statusName, statusName.getDescription());
        }
        return ResponseEntity.ok(statusNames);
    }

}
