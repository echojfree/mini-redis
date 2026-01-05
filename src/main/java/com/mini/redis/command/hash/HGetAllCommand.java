package com.mini.redis.command.hash;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisHash;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HGETALL 命令实现
 * 获取哈希表中的所有字段和�? *
 * 语法：HGETALL key
 * 返回值：字段和值的列表
 *
 * 面试知识点：
 * 1. 哈希表的遍历
 * 2. 时间复杂�?O(N)，N 为哈希表大小
 * 3. 大数据量的性能影响
 *
 * @author Mini Redis
 */
public class HGetAllCommand implements Command {

    @Override
    public String getName() {
        return "HGETALL";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() != 2) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'hgetall' command"));
                return;
            }

            // 获取 key
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            if (obj == null) {
                // key 不存在，返回空列�?                ctx.writeAndFlush(new RespMessage.Array(new ArrayList<>()));
                return;
            }

            if (obj.getType() != RedisDataType.HASH) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 获取所有字段和�?            RedisHash hash = (RedisHash) obj.getValue();
            Map<String, String> all = hash.hgetall();

            // 构建响应
            List<RespMessage> response = new ArrayList<>();
            for (Map.Entry<String, String> entry : all.entrySet()) {
                response.add(new RespMessage.BulkString(entry.getKey()));
                response.add(new RespMessage.BulkString(entry.getValue()));
            }

            ctx.writeAndFlush(new RespMessage.Array(response));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}