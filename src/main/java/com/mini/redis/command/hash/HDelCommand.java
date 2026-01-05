package com.mini.redis.command.hash;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisHash;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * HDEL 命令实现
 * 删除哈希表中的字�? *
 * 语法：HDEL key field [field ...]
 * 返回值：成功删除的字段数�? *
 * 面试知识点：
 * 1. 哈希表的删除操作
 * 2. 时间复杂�?O(N)，N 为删除的字段数量
 * 3. 批量删除的实�? *
 * @author Mini Redis
 */
public class HDelCommand implements Command {

    @Override
    public String getName() {
        return "HDEL";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'hdel' command"));
                return;
            }

            // 获取 key
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            if (obj == null) {
                // key 不存�?                ctx.writeAndFlush(new RespMessage.Integer(0));
                return;
            }

            if (obj.getType() != RedisDataType.HASH) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 删除字段
            RedisHash hash = (RedisHash) obj.getValue();
            String[] fields = new String[args.size() - 2];
            for (int i = 2; i < args.size(); i++) {
                fields[i - 2] = ((RespMessage.BulkString) args.get(i)).getStringValue();
            }

            int deleted = hash.hdel(fields);

            // 如果哈希表变为空，删�?key
            if (hash.isEmpty()) {
                db.delete(key);
            }

            // 返回删除的字段数�?            ctx.writeAndFlush(new RespMessage.Integer(deleted));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}