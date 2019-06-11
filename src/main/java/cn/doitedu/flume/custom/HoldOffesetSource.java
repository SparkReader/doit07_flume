package cn.doitedu.flume.custom;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 能够记录读取位置偏移量的自定义source
 */
public class HoldOffesetSource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger logger = LoggerFactory.getLogger(HoldOffesetSource.class);

    private String positionfilepath;
    private String logfile;
    private int batchsize;

    private ExecutorService exec;


    /**
     * 框架调用本方法，开始采集数据
     * 自定义代码去读取数据，转为event
     * 用getChannelProcessor（）方法（定义在父类中）去获取框架的channelprocessor（channel处理器）
     * 调用这个channelprocessor将event提交给channel
     */
    @Override
    public synchronized void start() {

        super.start();
        // 用于向channel提交数据的一个处理器
        ChannelProcessor channelProcessor = getChannelProcessor();

        // 获取历史偏移量
        long offset = 0;
        try {
            File positionfile = new File(this.positionfilepath);
            String s = FileUtils.readFileToString(positionfile);
            offset = Long.parseLong(s);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 构造一个线程池
        exec = Executors.newSingleThreadExecutor();
        // 向线程池提交数据采集任务
        exec.execute(new HoldOffsetRunnable(offset, logfile, channelProcessor, batchsize, positionfilepath));

    }


    /**
     * 停止前要调用的方法
     * 可以在这里做一些资源关闭清理工作
     */
    @Override
    public synchronized void stop() {
        super.stop();

       try{
           exec.shutdown();
       }catch (Exception e){
           exec.shutdown();
       }
    }

    /**
     * 获取配置文件中的参数，来配置本source实例
     * <p>
     * 要哪些参数：
     * 偏移量记录文件所在路径
     * 要采集的文件所在路径
     *
     * @param context
     */
    public void configure(Context context) {

        // 这是我们source用来记录偏移量的文件路径
        this.positionfilepath = context.getString("positionfile", "./");

        // 这是我们source要采集的日志文件的路径
        this.logfile = context.getString("logfile");

        // 这是用户配置的采集事务批次最大值
        this.batchsize = context.getInteger("batchsize", 100);

        // 如果日志文件路径没有指定，则抛异常
        if (StringUtils.isBlank(logfile)) throw new RuntimeException("请配置需要采集的文件路径");

    }


    /**
     * 采集文件的具体工作线程任务类
     */
    private static class HoldOffsetRunnable implements Runnable {

        long offset;
        String logfilepath;
        String positionfilepath;
        ChannelProcessor channelProcessor;  // channel提交器 （里面会调拦截器，会开启写入channel的事务）
        int batchsize; // 批次大小
        List<Event> events = new ArrayList<Event>();  // 用来保存一批事件
        SystemClock systemClock = new SystemClock();

        public HoldOffsetRunnable(long offset, String logfilepath, ChannelProcessor channelProcessor, int batchsize, String positionfilepath) {
            this.offset = offset;
            this.logfilepath = logfilepath;
            this.channelProcessor = channelProcessor;
            this.batchsize = batchsize;
            this.positionfilepath = positionfilepath;
        }

        public void run() {

            try {
                // 先定位到指定的offset
                RandomAccessFile raf = new RandomAccessFile(logfilepath, "r");
                raf.seek(offset);

                // 循环读数据
                String line = null;

                // 记录上一批提交的时间
                long lastBatchTime = 0;
                while (true) {
                    line = raf.readLine();
                    if(line == null ){
                        Thread.sleep(2000);
                        continue;
                    }

                    // 将数据转成event
                    Event event = EventBuilder.withBody(line.getBytes());
                    // 装入list batch
                    synchronized (HoldOffesetSource.class) {
                        events.add(event);
                    }

                    // 判断批次大小是否满 或者 时间到了没有
                    if (events.size() >= batchsize || timeout(lastBatchTime)) {
                        // 满足，则提交
                        channelProcessor.processEventBatch(events);

                        // 记录提交时间
                        lastBatchTime = systemClock.currentTimeMillis();

                        // 记录偏移量
                        long offset = raf.getFilePointer();
                        FileUtils.writeStringToFile(new File(positionfilepath), offset + "");

                        // 清空本批event
                        events.clear();

                    }

                    // 不满足，继续读
                }
            } catch (FileNotFoundException e) {
                logger.error("要采集的文件不存在");
            } catch (IOException e) {
                logger.error("我也不知道怎么搞的，不好意思，我罢工了");
            } catch (InterruptedException e) {
                logger.error("线程休眠出问题了");
            }
        }

        // 判断是否批次间隔超时
        private boolean timeout(long lastBatchTime) {
            return systemClock.currentTimeMillis() - lastBatchTime > 2000;
        }

    }


}


