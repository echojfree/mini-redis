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
 * SADD 命令实现
 * 向集合添加元�? *
 * 语法：SADD key member [member ...]
 * 返回值：成功添加的新元素数量
 *
 * 面试知识点：
 * 1. Set 数据结构的实�? * 2. 时间复杂�?O(N)，N 为添加的元素数量
 * 3. 去重的实�? *
 * @author Mini Redis
 */
public class SAddCommand implements Command {

    @Override
    public String getName() {
        return "SADD";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'sadd' command"));
                return;
            }

            // 获取 key
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            RedisSet set;
            if (obj == null) {
                // 新建集合
                set = new RedisSet();
                db.set(key, new RedisObject(RedisDataType.SET, set));
            } else if (obj.getType() == RedisDataType.SET) {
                // 使用已存在的集合
                set = (RedisSet) obj.getValue();
            } else {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 添加所有元�?            String[] members = new String[args.size() - 2];
            for (int i = 2; i < args.size(); i++) {
                members[i - 2] = ((RespMessage.BulkString) args.get(i)).getStringValue();
            }

            int added = set.sadd(members);

            // 返回成功添加的元素数�?            ctx.writeAndFlush(new RespMessage.Integer(added));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}