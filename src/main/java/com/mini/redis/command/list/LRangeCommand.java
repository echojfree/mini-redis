package com.mini.redis.command.list;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisList;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * LRANGE 命令实现
 * 获取列表指定范围内的元素
 *
 * 语法：LRANGE key start stop
 * 返回值：指定范围内的元素列表
 *
 * 面试知识点：
 * 1. 列表的范围查�? * 2. 负数索引的处�? * 3. 时间复杂�?O(S+N)，S 为偏移量，N 为返回元素数�? *
 * @author Mini Redis
 */
public class LRangeCommand implements Command {

    @Override
    public String getName() {
        return "LRANGE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() != 4) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'lrange' command"));
                return;
            }

            // 获取参数
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();
            long start = Long.parseLong(((RespMessage.BulkString) args.get(2)).getStringValue());
            long stop = Long.parseLong(((RespMessage.BulkString) args.get(3)).getStringValue());

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            if (obj == null) {
                // key 不存在，返回空列�?                ctx.writeAndFlush(new RespMessage.Array(new ArrayList<>()));
                return;
            }

            if (obj.getType() != RedisDataType.LIST) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 获取范围内的元素
            RedisList list = (RedisList) obj.getValue();
            List<String> range = list.lrange(start, stop);

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