package manke.spider.processor.douban;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.scheduler.Scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LinkedBlockScheduler implements Scheduler {

    private Logger logger = LoggerFactory.getLogger(LinkedBlockScheduler.class);


    private BlockingQueue<Request> queue = new LinkedBlockingQueue<Request>();


    @Override
    public void push(Request request, Task task) {
        queue.add(request);
        logger.info("url_push {}", request.getUrl());
    }

    @Override
    public Request poll(Task task) {
        Request request = queue.poll();
        if (request != null) {
            logger.info("url_poll {}", request.getUrl());
        }
        return request;
    }
}
