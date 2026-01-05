package com.mini.redis.command.zset;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisZSet;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * ZRANGE 命令实现
 * 按索引范围获取有序集合成�? *
 * 语法：ZRANGE key start stop [WITHSCORES]
 * 返回值：指定范围内的成员列表
 *
 * 面试知识点：
 * 1. 跳表的范围查�? * 2. 时间复杂�?O(log(N)+M)，N 为集合大小，M 为返回元素数�? * 3. 负数索引的处�? *
 * @author Mini Redis
 */
public class ZRangeCommand implements Command {

    @Override
    public String getName() {
        return "ZRANGE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 4 || args.size() > 5) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'zrange' command"));
                return;
            }

            // 获取参数
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();
            long start = Long.parseLong(((RespMessage.BulkString) args.get(2)).getStringValue());
            long stop = Long.parseLong(((RespMessage.BulkString) args.get(3)).getStringValue());

            // 检�?WITHSCORES 选项
            boolean withScores = false;
            if (args.size() == 5) {
                String option = ((RespMessage.BulkString) args.get(4)).getStringValue();
                if ("WITHSCORES".equalsIgnoreCase(option)) {
                    withScores = true;
                } else {
                    ctx.writeAndFlush(new RespMessage.Error("ERR syntax error"));
                    return;
                }
            }

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            if (obj == null) {
                // key 不存在，返回空列�?                ctx.writeAndFlush(new RespMessage.Array(new ArrayList<>()));
                return;
            }

            if (obj.getType() != RedisDataType.ZSET) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 获取范围内的成员
            RedisZSet zset = (RedisZSet) obj.getValue();
            List<String> range = zset.zrange(start, stop, withScores);

            // 构建响应
            List<RespMessage> response = new ArrayList<>();
            for (String value : range) {
                response.add(new RespMessage.BulkString(value));
            }

            ctx.writeAndFlush(new RespMessage.Array(response));

        } catch (NumberFormatException e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR value is not an integer or out of range"));
        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}