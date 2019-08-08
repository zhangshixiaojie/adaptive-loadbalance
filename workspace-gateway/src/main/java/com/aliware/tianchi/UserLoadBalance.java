package com.aliware.tianchi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static Map<String, Integer> weightMap = new ConcurrentHashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        return doSelect(invokers, url, invocation);
    }

    private <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation){
        int len = invokers.size();

        int weightSum = 0;
        int[] weights = new int[len];
        for(int i=0; i<len; i++){
            weights[i] = weightMap.get(invokers.get(i).getUrl().getHost());
            weightSum += weights[i];
        }

        int random = ThreadLocalRandom.current().nextInt(weightSum);
        int idx = 0;
        for (int i = 0; i < len; i++) {
            random -= weights[i];
            if (random < 0) {
                idx = i;
                break;
            }
        }

        return invokers.get(idx);
    }

    protected static void addEvaluateValues(Map<String, String> map){
        int weight = Integer.valueOf(map.get("weight"));
        weightMap.put(map.get("host"), weight);
    }

}
