package com.mini.redis.command.zset;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisZSet;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * ZREM 命令实现
 * 从有序集合中移除成员
 *
 * 语法：ZREM key member [member ...]
 * 返回值：成功移除的成员数�? *
 * 面试知识点：
 * 1. 跳表的删除操�? * 2. 时间复杂�?O(M*log(N))，M 为删除的成员数量，N 为集合大�? * 3. 批量删除的实�? *
 * @author Mini Redis
 */
public class ZRemCommand implements Command {

    @Override
    public String getName() {
        return "ZREM";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'zrem' command"));
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

            if (obj.getType() != RedisDataType.ZSET) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 移除成员
            RedisZSet zset = (RedisZSet) obj.getValue();
            String[] members = new String[args.size() - 2];
            for (int i = 2; i < args.size(); i++) {
                members[i - 2] = ((RespMessage.BulkString) args.get(i)).getStringValue();
            }

            int removed = zset.zrem(members);

            // 如果有序集合变为空，删除 key
            if (zset.isEmpty()) {
                db.delete(key);
            }

            // 返回移除的成员数�?            ctx.writeAndFlush(new RespMessage.Integer(removed));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}