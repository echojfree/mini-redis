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
 * HGET 命令实现
 * 获取哈希表字段的�? *
 * 语法：HGET key field
 * 返回值：字段的值，不存在返�?nil
 *
 * 面试知识点：
 * 1. 哈希表的查询
 * 2. 时间复杂�?O(1)
 * 3. 空值的处理
 *
 * @author Mini Redis
 */
public class HGetCommand implements Command {

    @Override
    public String getName() {
        return "HGET";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() != 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'hget' command"));
                return;
            }

            // 获取参数
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();
            String field = ((RespMessage.BulkString) args.get(2)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            if (obj == null) {
                // key 不存�?                ctx.writeAndFlush(new RespMessage.BulkString(null));
                return;
            }

            if (obj.getType() != RedisDataType.HASH) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 获取字段�?            RedisHash hash = (RedisHash) obj.getValue();
            String value = hash.hget(field);

            ctx.writeAndFlush(new RespMessage.BulkString(value));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}