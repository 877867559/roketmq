package org.apache.rocketmq.store.schedule;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ArbitraryDelayScheduleMessageService {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private final DefaultMessageStore defaultMessageStore;

    private static final int DEFAULT_PERIOD = 1;

    private HashedWheelTimer wheelTimer;

    private volatile String curTime;

    private Timer timer;

    public ArbitraryDelayScheduleMessageService(DefaultMessageStore defaultMessageStore){
        this.defaultMessageStore = defaultMessageStore;
        this.wheelTimer = new HashedWheelTimer();
        addTimer();
        new Thread(new Runnable() {
            @Override
            public void run() {
                recover();
            }
        }).start();
    }
    private void addTimer(){
        Date date = new Date();
        this.curTime = getDateTime(date);
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                loadNextHourData(null);
            }
        },getNextHour(date));
    }
    public void recover(){
        //恢复数据
        File rootFile = new File(getRootFileName());
        File[] files = rootFile.listFiles();
        Long tmpCurTime = Long.parseLong(curTime);
        for(File file:files){
            if(!file.isDirectory()){
                try{
                    String fileName = file.getName();
                    if(StringUtils.isEmpty(fileName) || fileName.length() > 8){
                        continue;
                    }
                    if(tmpCurTime.equals(Long.parseLong(fileName))){
                        loadNextHourData(fileName);
                    }
                }catch (Exception e){
                    log.error("恢复数据异常:{}",e);
                }
            }
        }
    }
    private void loadNextHourData(String preTime){
        //加载数据
        if(preTime == null || preTime.isEmpty()) preTime = curTime;
        load(preTime);
        File file = new File(getFileName(preTime));
        file.renameTo(new File(preTime + "1"));
        Date date = new Date();
        curTime = getDateTime(date);
        addTimer();
    }

    private void load(String deadlineTime){
        try {
            FileChannel fileChannel = getFileChannel(deadlineTime);
            List<DelayMessageExtBrokerInner> list = getMessage(fileChannel);
            fileChannel.close();
            for(DelayMessageExtBrokerInner inner:list){
                newTimeout(inner,inner.getDeadlineSeconds());
            }
        }catch (Exception e){
            log.error("加载数据异常:{}",e);
        }
    }
    public AppendMessageResult appendMessage(MessageExtBrokerInner msgInner)  {
        AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
        try{
            String delayTime = msgInner.getProperties().get("delayTime");
            if(delayTime == null) throw new IllegalArgumentException("延时时间不能为空");
            if(!UtilAll.isNumeric(delayTime))  throw new IllegalArgumentException("延时时间非整数");
            Long curDelayTime = Long.parseLong(delayTime);
            int  curDelayCalendar = Calendar.SECOND;
            String delayCalendar = msgInner.getProperties().get("delayCalendar");
            if(delayCalendar != null && UtilAll.isNumeric(delayCalendar)) curDelayCalendar = Integer.parseInt(delayCalendar);
            TimeUnit timeUnit = getTimeUnit(curDelayCalendar);
            if(timeUnit == null) timeUnit = TimeUnit.SECONDS;
            long deadline = System.currentTimeMillis() + timeUnit.toMillis(curDelayTime);
            String deadlineTime = getDateTime(new Date(deadline));
            Date deadlineDate = new Date(deadline);
            //计算据整点多少秒
            int deadlineSeconds = deadlineDate.getMinutes() *  60 + deadlineDate.getSeconds();
            //将数据写入到文件
            FileChannel fileChannel = getFileChannel(deadlineTime);
            result = putMessage(msgInner, fileChannel, deadlineSeconds);
            //是否正在当前消费时间内，若是将数据写入HashedWheelTimer
            if(curTime.equals(deadlineTime)) newTimeout(msgInner,deadlineSeconds);
        }catch (Exception e){
            log.error("添加数据异常：{}",e);
        }
        return result;
    }
    private AppendMessageResult putMessage(MessageExtBrokerInner msgInner,FileChannel fileChannel,int deadlineSeconds) throws IOException{
        long beginTimeMills = defaultMessageStore.now();
        int sysflag = msgInner.getSysFlag();
        int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
        ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
        /**
         * Serialize message
         */
        final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

        final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

        final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        final int topicLength = topicData.length;

        final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

        final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

        ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);
        // 1 TOTALSIZE
        msgStoreItemMemory.putInt(msgLen);
        // 2 MAGICCODE
        msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
        // 3 BODYCRC
        msgStoreItemMemory.putInt(msgInner.getBodyCRC());
        // 4 QUEUEID
        msgStoreItemMemory.putInt(msgInner.getQueueId());
        // 5 FLAG
        msgStoreItemMemory.putInt(msgInner.getFlag());
        // 6 QUEUEOFFSET
        msgStoreItemMemory.putLong(0);
        // 7 PHYSICALOFFSET
        msgStoreItemMemory.putLong(0);
        // 8 SYSFLAG
        msgStoreItemMemory.putInt(msgInner.getSysFlag());
        // 9 BORNTIMESTAMP
        msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
        // 10 BORNHOST
         resetByteBuffer(bornHostHolder, bornHostLength);
        msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
        // 11 STORETIMESTAMP
        msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
        // 12 STOREHOSTADDRESS
         resetByteBuffer(storeHostHolder, storeHostLength);
         msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
        // 13 RECONSUMETIMES
        msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
        // 14 Prepared Transaction Offset
        msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
        // 15 BODY
        msgStoreItemMemory.putInt(bodyLength);
        if (bodyLength > 0)
            msgStoreItemMemory.put(msgInner.getBody());
        // 16 TOPIC
        msgStoreItemMemory.put((byte) topicLength);
        msgStoreItemMemory.put(topicData);
        // 17 PROPERTIES
        msgStoreItemMemory.putShort((short) propertiesLength);
        if (propertiesLength > 0)
            msgStoreItemMemory.put(propertiesData);
        // 18 延时时间
        msgStoreItemMemory.putInt(deadlineSeconds);
        fileChannel.write(msgStoreItemMemory);
        fileChannel.force(true);
        fileChannel.close();

        AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, 0, msgLen, UUID.randomUUID().toString(),
                msgInner.getStoreTimestamp(), 0, defaultMessageStore.now() - beginTimeMills);
        return result;
    }
    private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
        byteBuffer.flip();
        byteBuffer.limit(limit);
    }
    private List<DelayMessageExtBrokerInner> getMessage(FileChannel fileChannel) throws IOException{
        long size = fileChannel.size();
        ByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size).asReadOnlyBuffer();
        List<DelayMessageExtBrokerInner> list = new ArrayList<>();
        while (byteBuffer.hasRemaining()){
            DelayMessageExtBrokerInner msgExt = new DelayMessageExtBrokerInner();
            // 1 TOTALSIZE
            int storeSize = byteBuffer.getInt();
            msgExt.setStoreSize(storeSize);

            // 2 MAGICCODE
            byteBuffer.getInt();

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();
            msgExt.setBodyCRC(bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();
            msgExt.setQueueId(queueId);

            // 5 FLAG
            int flag = byteBuffer.getInt();
            msgExt.setFlag(flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            msgExt.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            msgExt.setCommitLogOffset(physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            msgExt.setSysFlag(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            msgExt.setBornTimestamp(bornTimeStamp);

            // 10 BORNHOST
            int bornhostIPLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 : 16;
            byte[] bornHost = new byte[bornhostIPLength];
            byteBuffer.get(bornHost, 0, bornhostIPLength);
            int port = byteBuffer.getInt();
            msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            msgExt.setStoreTimestamp(storeTimestamp);

            // 12 STOREHOST
            int storehostIPLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
            byte[] storeHost = new byte[storehostIPLength];
            byteBuffer.get(storeHost, 0, storehostIPLength);
            port = byteBuffer.getInt();
            msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();
            msgExt.setReconsumeTimes(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                byte[] body = new byte[bodyLen];
                byteBuffer.get(body);
                msgExt.setBody(body);
            }

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            msgExt.setTopic(new String(topic, CHARSET_UTF8));

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byte[] properties = new byte[propertiesLength];
                byteBuffer.get(properties);
                String propertiesString = new String(properties, CHARSET_UTF8);
                Map<String, String> map = MessageDecoder.string2messageProperties(propertiesString);
                msgExt.setProperties(map);
            }
            // 18 延时时间
            int  deadlineSeconds = byteBuffer.getInt();
            msgExt.setDeadlineSeconds(deadlineSeconds);
            list.add(msgExt);
        }
        return list;
    }

    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
      //  int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        //int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
              //  + bornhostLength //BORNHOST
                + 8 //STORETIMESTAMP
              //  + storehostAddressLength //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 4 //延时时间
                + 0;
        return msgLen;
    }

    private FileChannel getFileChannel(String date) throws FileNotFoundException,IOException{
        File file = new File(getFileName(date));
        if(!file.exists()) file.mkdirs();
        RandomAccessFile  randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();
        fileChannel.position(fileChannel.size());//定位到文件末尾
        return fileChannel;
    }
    private String getRootFileName(){
        return defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()
                + File.separator + "delay";
    }
    private String getFileName(String date){
        return  getRootFileName() + File.separator + date;
    }
    private String getDateTime(Date date){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
        return  formatter.format(date);
    }
    private long getNextHour(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR,calendar.get(Calendar.HOUR_OF_DAY) + 1);
        calendar.add(Calendar.MINUTE,0);
        calendar.add(Calendar.MILLISECOND,0);
        return calendar.getTime().getTime();
    }
    private TimeUnit getTimeUnit( int  curDelayCalendar){
        if(curDelayCalendar == Calendar.DAY_OF_MONTH) return TimeUnit.DAYS;
        if(curDelayCalendar == Calendar.HOUR)  return TimeUnit.HOURS;
        if(curDelayCalendar == Calendar.MINUTE)  return TimeUnit.MINUTES;
        if(curDelayCalendar == Calendar.SECOND)  return TimeUnit.SECONDS;
        return null;
    }
    private Timeout newTimeout(MessageExt msgExt,long delay){
        io.netty.util.TimerTask timerTask = new Task(msgExt);
       return wheelTimer.newTimeout(timerTask,delay,TimeUnit.SECONDS);
    }
    class Task implements io.netty.util.TimerTask{

        private MessageExt msgExt;

        public Task(MessageExt msgExt){
            this.msgExt = msgExt;
        }

       @Override
       public void run(Timeout var1) throws Exception{

           PutMessageResult putMessageResult = defaultMessageStore.putMessage(messageTimeup(msgExt));

           if (putMessageResult != null
                   && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
               return;
           } else {
               // XXX: warn and notify me
               log.error(
                       "ArbitraryDelayScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                       msgExt.getTopic(), msgExt.getMsgId());
               newTimeout(msgExt,1);
               return;
           }
        }
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
