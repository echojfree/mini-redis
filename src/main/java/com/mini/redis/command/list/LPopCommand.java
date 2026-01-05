package com.mini.redis.command.list;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;
import com.mini.redis.storage.impl.RedisList;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * LPOP 命令实现
 * 从列表左侧弹出元�? *
 * 语法：LPOP key
 * 返回值：弹出的元素，列表为空时返�?nil
 *
 * 面试知识点：
 * 1. 双向链表的头部删�? * 2. 时间复杂�?O(1)
 * 3. 空列表的处理
 *
 * @author Mini Redis
 */
public class LPopCommand implements Command {

    @Override
    public String getName() {
        return "LPOP";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            List<RespMessage> args = ((RespMessage.Array) msg).getElements();
            if (args.size() != 2) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'lpop' command"));
                return;
            }

            // 获取 key
            String key = ((RespMessage.BulkString) args.get(1)).getStringValue();

            // 获取数据库和对象
            RedisDatabase db = client.getCurrentDatabase();
            RedisObject obj = db.get(key);

            if (obj == null) {
                // key 不存�?                ctx.writeAndFlush(new RespMessage.BulkString(null));
                return;
            }

            if (obj.getType() != RedisDataType.LIST) {
                // 类型错误
                ctx.writeAndFlush(new RespMessage.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // 弹出元素
            RedisList list = (RedisList) obj.getValue();
            String value = list.lpop();

            if (value == null) {
                // 列表为空
                ctx.writeAndFlush(new RespMessage.BulkString(null));

                // 如果列表变为空，删除 key
                if (list.isEmpty()) {
                    db.delete(key);
                }
            } else {
                ctx.writeAndFlush(new RespMessage.BulkString(value));
            }

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}