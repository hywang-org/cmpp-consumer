package com.i.server.util;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.QueueInfo;

@Component
public class QueueUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueUtils.class);

    @Value("${mq_username}")
    private String mqUserName;

    @Value("${mq_password}")
    private String mqPassWord;

    @Value("${mq_host}")
    private String mqHost;

    private List<String> queueNameList;

    private Set<String> queueNameSet;

    public List<String> getQueueNameList() {
        return queueNameList;
    }

    public Set<String> getQueueNameSet() {
        return queueNameSet;
    }

    /**
     * 获取rabbitMq服务器中现有的queue，并把queueName缓存到set,保存在内存中
     * 
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    @PostConstruct
    public void getQueueInfoList() throws MalformedURLException, URISyntaxException {
        Client c = new Client("http://" + mqHost + ":15672/api/", mqUserName, mqPassWord);
        List<QueueInfo> queueInfos = c.getQueues();
        if (queueInfos == null) {
            return;
        }
        queueNameList = new ArrayList<>();
        for (QueueInfo queueInfo : queueInfos) {
            queueNameList.add(queueInfo.getName());
        }
        LOGGER.info("rabbitMQ中已存在的队列有{}", queueNameList);
        queueNameSet = new HashSet<>(queueNameList);
    }
}
