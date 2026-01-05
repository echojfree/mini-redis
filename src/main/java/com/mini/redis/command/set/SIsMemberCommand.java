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
 * SISMEMBER 命令实现
 * 检查元素是否在集合�? *
 * 语法：SISMEMBER key member
 * 返回值：如果成员存在返回 1，否则返�?0
 *
 * 面试知识点：
 * 1. Set 的查找操�? * 2. 时间复杂�?O(1)
 * 3. 哈希表的查找性能
 *
 * @author Mini Redis
 */
public class SIsMemberCommand implements Command {

    @Override
    public String getName() {
        return "SISMEMBER";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() != 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'sismember' command"));
                return;
            }

            // 获取参数
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();
            String member = ((RespMessage.BulkString) args.get(2)).getStringValue();

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

            // 检查成员是否存�?            RedisSet set = (RedisSet) obj.getValue();
            boolean exists = set.sismember(member);

            ctx.writeAndFlush(new RespMessage.Integer(exists ? 1 : 0));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}