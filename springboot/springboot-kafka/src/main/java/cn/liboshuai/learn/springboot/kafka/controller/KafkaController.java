package cn.liboshuai.learn.springboot.kafka.controller;

import cn.liboshuai.learn.springboot.kafka.dto.KafkaEventDTO;
import cn.liboshuai.learn.springboot.kafka.provider.KafkaEventProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;

@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaEventProvider kafkaEventProvider;

    @PostMapping("/send")
    public String sendEvent(@RequestBody KafkaEventDTO kafkaEventDTO){
        kafkaEventProvider.sendEvent(kafkaEventDTO);
        return "success";
    }

    @PostMapping("/batchSend")
    public String batchSend(@RequestBody KafkaEventDTO kafkaEventDTO){
        kafkaEventProvider.batchSend(Collections.singletonList(kafkaEventDTO));
        return "success";
    }
}
