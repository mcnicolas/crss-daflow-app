package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.BatchJobQueueDisplay;
import com.pemc.crss.dataflow.app.dto.BatchJobQueueForm;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/job-queue")
public class BatchJobQueueResource {

    @Autowired
    private BatchJobQueueService batchJobQueueService;

    @GetMapping
    public ResponseEntity<Page<BatchJobQueueDisplay>> getAllWithStatus(@RequestParam(value = "status", required = false)
                                                                       QueueStatus status, Pageable pageable) {

        log.debug("Request for getting job queue page. status = {}, pageable = {}", status, pageable);
        Page<BatchJobQueue> jobQueuePage = batchJobQueueService.getAllWithStatus(status, pageable);
        List<BatchJobQueueDisplay> jobQueueDisplays = jobQueuePage.getContent().stream().map(BatchJobQueueDisplay::new)
                .collect(Collectors.toList());
        return ResponseEntity.ok(new PageImpl<>(jobQueueDisplays));
    }

    @PostMapping("/save")
    public void save(@RequestBody BatchJobQueueForm batchJobQueueForm) {
        log.debug("Request for saving job queue. form = {}", batchJobQueueForm);
        batchJobQueueService.save(batchJobQueueForm.toBatchJobQueue());
    }

    @PutMapping("/{id}/update-status")
    public void updateStatus(@PathVariable("id") Long id, @RequestParam("status") QueueStatus status) {
        log.debug("Request for updating status of job queue, id = {}, status = {}", id, status);
        BatchJobQueue batchJobQueue = batchJobQueueService.get(id);
        if (batchJobQueue == null) {
            throw new RuntimeException("Batch job queue with id: {} cannot be found");
        }
        batchJobQueue.setStatus(status);
        batchJobQueueService.save(batchJobQueue);
    }

}
