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
 * ZSCORE 命令实现
 * 获取有序集合中成员的分数
 *
 * 语法：ZSCORE key member
 * 返回值：成员的分数，如果成员不存在返�?nil
 *
 * 面试知识点：
 * 1. 哈希表的 O(1) 查询
 * 2. 跳表与哈希表的组合使�? * 3. 空值的处理
 *
 * @author Mini Redis
 */
public class ZScoreCommand implements Command {

    @Override
    public String getName() {
        return "ZSCORE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() != 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'zscore' command"));
                return;
            }

            // 获取参数
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();
            String member = ((RespMessage.BulkString) args.get(2)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            if (obj == null) {
                // key 不存�?                ctx.writeAndFlush(new RespMessage.BulkString(null));
                return;
            }

            if (obj.getType() != RedisDataType.ZSET) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 获取成员的分�?            RedisZSet zset = (RedisZSet) obj.getValue();
            Double score = zset.zscore(member);

            if (score == null) {
                ctx.writeAndFlush(new RespMessage.BulkString(null));
            } else {
                ctx.writeAndFlush(new RespMessage.BulkString(score.toString()));
            }

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}