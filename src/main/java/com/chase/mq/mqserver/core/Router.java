package com.chase.mq.mqserver.core;

import com.chase.mq.common.MQException;

/**
 * 用来实现交换机的转发规则，并验证 bindingKey 是否合法
 */
public class Router {

    /**
     * 检查指定的路由键是否合法(规则):
     * 1，数字，字母，下划线
     * 2，使用 . 分割若干部分
     * 3，允许存在 * 和 # 作为通配符，但是通配符只能作为独立的分段
     * 4，检查 * / . 是否是独立的部分
     * 注意：在分割字符的时候：首先，"." 在正则表达式中，是一个特殊的符号，此处是把 . 当作原始文本
     * 来进行匹配，要想使用 . 原始文本，就需要进行转义，在正则中就需要 \. 的方式来表示
     * 又因为，在 Java 字符串中，写入 "\." 这样的文本 \ 又是一个特殊的支付，就需要使用 \\ 先对反斜杠转义，此时才能够真正录入
     * \ 这个文本 \. 这个转义字符在正则中才可以生效，因此写法要变成 "\\."
     * 约定：以下是通配符相邻关系的判断：
     * 1, aaa.#.#.bbb ==> 非法
     * 2, aaa.#.*.bbb ==> 非法
     * 3, aaa.*.#.bbb ==> 非法
     * 4, aaa.*.*.bbb ==> 合法
     * @param bindingKey 传入的路由键
     * @return 是否合法
     */
    public boolean checkBindingKey(String bindingKey) {
        if(bindingKey.isEmpty())
            return true;
        for (int i = 0; i < bindingKey.length(); i++) {
            char ch = bindingKey.charAt(i);
            if(ch >= 'A' && ch <= 'Z') continue;
            if(ch >= 'a' && ch <= 'z') continue;
            if (ch >= '0' && ch <= '9') continue;
            if(ch == '_' || ch == '.' || ch == '*' || ch == '#') continue;
            return false;
        }
        String[] words = bindingKey.split("\\.");
        for (String word : words) {
            if(word.length() > 1 && (word.contains("*") || word.contains("#")))
                return false;
        }
        return true;
    }

    /**
     * routingKey 的构造规则:
     * 1，数字，字母，下划线
     * 2，使用 . 分割若干部分
     * @param routingKey 传入的匹配规则
     * @return 是否合法
     */
    public boolean checkRoutingKey(String routingKey){
        if(routingKey.isEmpty())
            return true;
        for (int i = 0; i < routingKey.length(); i++) {
            char ch = routingKey.charAt(i);
            if(ch >= 'A' && ch <= 'Z') continue;
            if(ch >= 'a' && ch <= 'z') continue;
            if (ch >= '0' && ch <= '9') continue;
            if(ch == '_' || ch == '.' || ch == '*' || ch == '#') continue;
            return false;
        }
        return true;
    }

    /**
     * 这个方法用来判断当前消息是否可以转发给这个绑定的队列 -> 根据不同的 exchangeType 使用不同的判定转发规则
     * FANOUT : 该交换机上绑定的所有队列都需要转发
     * TOPIC : 按照规则转发
     * @param exchangeType 交换机类型
     * @return 是否可以转发
     */
    public boolean route(ExchangeType exchangeType,Binding binding,Message message){
        if(exchangeType == ExchangeType.FANOUT)
            return true;
        else if (exchangeType == ExchangeType.TOPIC)
            return routeTopicOld(binding,message);
        else
            throw new MQException("[Router] 交换机类型非法! exchangeType=" + exchangeType);
    }

    /**
     * 常规方法实现通配符匹配问题
     * [情况1] : 如果遇到普通字符串就要求两边内容一样
     * [情况2] : 如果遇到 * ，直接进入下一轮, * 可以匹配任意字符串
     * [情况3] : # 后面没有东西了，说明此时一定匹配成功了
     * [情况4] : # 后面还存在东西，拿着这个内容去 routingKey 中往后找，找到对应的位置
     * [情况5] : 判断是否双发同时到达末尾
     * @param binding 绑定对象
     * @param message 消息对象
     * @return 是否匹配成功
     */
    private boolean routeTopicOld(Binding binding,Message message){
        String[] routingKey = message.getRoutingKey().split("\\.");
        String[] bindingKey = binding.getBindingKey().split("\\.");
        int bindingIndex = 0;
        int routingIndex = 0;
        while(bindingIndex < bindingKey.length && routingIndex < routingKey.length){
            if(bindingKey[bindingIndex].equals("*")){
                bindingIndex ++;
                routingIndex ++;
            } else if (bindingKey[bindingIndex].equals("#")) {
                bindingIndex ++;
                if(bindingIndex == bindingKey.length) return true;
                routingIndex = findNextMatch(routingKey,routingIndex,bindingKey[bindingIndex]);
                if(routingIndex == -1) return false;
                bindingIndex ++;
                routingIndex ++;
            } else {
                if(!bindingKey[bindingIndex].equals(routingKey[routingIndex])) return false;
                bindingIndex ++;
                routingIndex ++;
            }
        }
        return bindingIndex == bindingKey.length && routingIndex == routingKey.length;
    }

    /**
     * 这个方法用来查找该部分在 routingKey 的位置，返回该下标，没找到，就返回 -1
     * 这里还是有待改进：由于返回的是第一个匹配的字符串，比如:bindingKey:aaa.#.bbb.ccc routingKey:aaa.bbb.ccc.aaa.bbb.ccc
     * 上面这种情况应该是合法的，而且也不能简单地改找最后一个,比如：bindingKey:aaa.#.bbb.ccc.bbb.aaa routingKey:aaa.bbb.ccc.aaa.bbb.ccc.bbb.aaa
     * 这种也是合法的，但是如果找最后一个 bbb 的话就会导致与 bindingKey 的前一个 bbb 进行匹配
     * 所以目前这种算法仍然需要改进
     * TODO
     */
    private int findNextMatch(String[] routingTokens,int routingIndex,String bindingToken){
        for (int i = routingIndex; i < routingTokens.length; i++) {
            if(routingTokens[i].equals(bindingToken))
                return i;
        }
        return -1;
    }

    /**
     * 使用 DP 方式实现通配符匹配问题:
     * 1，初始化 dp 表，由于要考虑空串，dp 表的长和宽都需要 + 1
     *      a) dp[i][j] 的含义是 bindingTokens 中 [0,j] 能否和 routingTokens 中 [0,i] 匹配
     *      b) 空的 bindingKey 和 空的 routingKey 可以匹配
     *      c) 如果 routingKey 为空，bindingKey 只有连续为 # 的时候才可以匹配
     * 2，遍历所有情况
     *      a) 这块状态转移方程推导过程很复杂，参考算法视频讲解
     *      b) 如果 bindingTokens j 位置为 *，那么 bindingTokens j - 1 位置和 routingKey i - 1 位置匹配即可
     *      c) 如果 bindingTokens j 位置为普通字符串, 那么要求 bindingTokens j - 1 位置 和 routingKey i - 1 不仅要匹配，还要位置相同才认为是最匹配的
     * 3，处理返回值，字节返回 dp 表的最后一个位置
     * @param binding 绑定对象
     * @param message 消息对象
     * @return 是否能转发
     * TODO
     */
    private boolean routeTopicNew(Binding binding, Message message) {
        String routingKey = message.getRoutingKey();
        String bindingKey = binding.getBindingKey();
        int m = routingKey.length();
        int n = bindingKey.length();
        return true;
    }
}
