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
 * ZADD 命令实现
 * 向有序集合添加成�? *
 * 语法：ZADD key score member [score member ...]
 * 返回值：成功添加的新成员数量
 *
 * 面试知识点：
 * 1. 跳表（Skip List）的实现
 * 2. 时间复杂�?O(log(N))
 * 3. 分数和成员的组合索引
 *
 * @author Mini Redis
 */
public class ZAddCommand implements Command {

    @Override
    public String getName() {
        return "ZADD";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'zadd' command"));
                return;
            }

            // 获取 key
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            RedisZSet zset;
            if (obj == null) {
                // 新建有序集合
                zset = new RedisZSet();
                db.set(key, new RedisObject(RedisDataType.ZSET, zset));
            } else if (obj.getType() == RedisDataType.ZSET) {
                // 使用已存在的有序集合
                zset = (RedisZSet) obj.getValue();
            } else {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 添加所有成�?            int added = 0;
            for (int i = 2; i < args.size(); i += 2) {
                double score = Double.parseDouble(((RespMessage.BulkString) args.get(i)).getStringValue());
                String member = ((RespMessage.BulkString) args.get(i + 1)).getStringValue();
                added += zset.zadd(score, member);
            }

            // 返回成功添加的新成员数量
            ctx.writeAndFlush(new RespMessage.Integer(added));

        } catch (NumberFormatException e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR value is not a valid float"));
        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}