package com.tangxc.maxcompute.mr.wordcount;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.utils.StringUtils;

import java.io.IOException;

/**
 * @author Xicheng.Tang
 */
public class WordCountMapper extends MapperBase {

    private Record word;
    private Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
        word = context.createMapOutputKeyRecord();
        one = context.createMapOutputValueRecord();
        one.set(new Object[] {1});
    }

    @Override
    public void map(long key, Record record, TaskContext context) throws IOException {
        String content = record.getString("content");
        if (StringUtils.isBlank(content)) {
            return;
        }
        String[] tokens = content.split("\\s+");
        for (String token : tokens) {
            word.set(new Object[] {token});
            context.write(word, one);
        }
    }
}
