package com.mini.redis.command.set;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisSet;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * SMEMBERS 命令实现
 * 获取集合中的所有元�? *
 * 语法：SMEMBERS key
 * 返回值：集合中的所有元�? *
 * 面试知识点：
 * 1. Set 的遍�? * 2. 时间复杂�?O(N)，N 为集合大�? * 3. 无序返回的特�? *
 * @author Mini Redis
 */
public class SMembersCommand implements Command {

    @Override
    public String getName() {
        return "SMEMBERS";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() != 2) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'smembers' command"));
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

            if (obj.getType() != RedisDataType.SET) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 获取所有成�?            RedisSet set = (RedisSet) obj.getValue();
            Set<String> members = set.smembers();

            // 构建响应
            List<RespMessage> response = new ArrayList<>();
            for (String member : members) {
                response.add(new RespMessage.BulkString(member));
            }

            ctx.writeAndFlush(new RespMessage.Array(response));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}