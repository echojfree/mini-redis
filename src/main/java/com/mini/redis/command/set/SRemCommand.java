package com.mini.redis.command.set;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisSet;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * SREM 命令实现
 * 从集合中移除元素
 *
 * 语法：SREM key member [member ...]
 * 返回值：成功移除的元素数�? *
 * 面试知识点：
 * 1. Set 的删除操�? * 2. 时间复杂�?O(N)，N 为移除的元素数量
 * 3. 批量删除的实�? *
 * @author Mini Redis
 */
public class SRemCommand implements Command {

    @Override
    public String getName() {
        return "SREM";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'srem' command"));
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

            if (obj.getType() != RedisDataType.SET) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 移除元素
            RedisSet set = (RedisSet) obj.getValue();
            String[] members = new String[args.size() - 2];
            for (int i = 2; i < args.size(); i++) {
                members[i - 2] = ((RespMessage.BulkString) args.get(i)).getStringValue();
            }

            int removed = set.srem(members);

            // 如果集合变为空，删除 key
            if (set.isEmpty()) {
                db.delete(key);
            }

            // 返回移除的元素数�?            ctx.writeAndFlush(new RespMessage.Integer(removed));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}