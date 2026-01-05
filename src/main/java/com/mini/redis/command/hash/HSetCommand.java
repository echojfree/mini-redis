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
 * HSET 命令实现
 * 设置哈希表字段的�? *
 * 语法：HSET key field value [field value ...]
 * 返回值：新增的字段数�? *
 * 面试知识点：
 * 1. 哈希表的实现
 * 2. 时间复杂�?O(1)
 * 3. 字段级别的操�? *
 * @author Mini Redis
 */
public class HSetCommand implements Command {

    @Override
    public String getName() {
        return "HSET";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'hset' command"));
                return;
            }

            // 获取 key
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            RedisHash hash;
            if (obj == null) {
                // 新建哈希�?                hash = new RedisHash();
                db.set(key, new RedisObject(RedisDataType.HASH, hash));
            } else if (obj.getType() == RedisDataType.HASH) {
                // 使用已存在的哈希�?                hash = (RedisHash) obj.getValue();
            } else {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 设置所有字�?            int added = 0;
            for (int i = 2; i < args.size(); i += 2) {
                String field = ((RespMessage.BulkString) args.get(i)).getStringValue();
                String value = ((RespMessage.BulkString) args.get(i + 1)).getStringValue();
                added += hash.hset(field, value);
            }

            // 返回新增的字段数�?            ctx.writeAndFlush(new RespMessage.Integer(added));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}