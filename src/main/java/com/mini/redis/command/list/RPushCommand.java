package com.mini.redis.command.list;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.protocol.RespType;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisList;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * RPUSH 命令实现
 * 从列表右侧推入元�? *
 * 语法：RPUSH key value [value ...]
 * 返回值：推入后列表的长度
 *
 * 面试知识点：
 * 1. 双向链表的尾部插�? * 2. 时间复杂�?O(1)
 * 3. �?LPUSH 的对称设�? *
 * @author Mini Redis
 */
public class RPushCommand implements Command {

    @Override
    public String getName() {
        return "RPUSH";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() < 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'rpush' command"));
                return;
            }

            // 获取 key
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            RedisList list;
            if (obj == null) {
                // 新建列表
                list = new RedisList();
                db.set(key, new RedisObject(RedisDataType.LIST, list));
            } else if (obj.getType() == RedisDataType.LIST) {
                // 使用已存在的列表
                list = (RedisList) obj.getValue();
            } else {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 推入所有元�?            String[] values = new String[args.size() - 2];
            for (int i = 2; i < args.size(); i++) {
                values[i - 2] = ((RespMessage.BulkString) args.get(i)).getStringValue();
            }

            int length = list.rpush(values);

            // 返回列表长度
            ctx.writeAndFlush(new RespMessage.Integer(length));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}